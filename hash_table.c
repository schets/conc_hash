
#include "hash_table.h"
#include "atomics.h"

#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>

#define hash_load 3
#define is_del 1

#define inc_size 1;
#define no_inc 0;
//simple test for 0
#define test_empty(key) ((key) == 0)

#define test_dead(key) ((key) == 1)

//returns if any bits after the first two exist
//so returns false for 0, 1, 2
#define has_elem(key) ((key) & (~3))

//elements are only inserted by writer, so that's easy
//elements are removed by storing removal candidates
//in a list with the table. When the table is resized,
//and later deleted from the hazard pointers,
//these elements are deleted as well

typedef char buffer[128];
typedef uint16_t hz_ct;

typedef struct {
	buffer back;
	hz_ct nactive;
	buffer front;
} hz_st;

typedef struct item {
	size_t key;
	const char *data;
	size_t dsize; //used for determining cleanup time
	struct item *next; //not super relevant, useful for cleanup
} item;



typedef struct hash_table {
	uint64_t n_elements;
	uint64_t active_count;
	uint64_t n_hazards;
	struct item *elems;
	item *cleanup_with_me;
	uint16_t *hazard_start;
	struct hash_table *next;
	int fd;
	char actual_data[];
} hash_table;

typedef struct shared_hash_table {
	struct hash_table *current_table;
	struct hash_table *old_tables;
	size_t nhazards;
	hz_st hazard_refs[];
} shared_hash_table;

static void *alloc_mem(size_t s) {
	return malloc(s);
}

static void free_mem(void *tof) {
	free(tof);
}

static size_t calc_ht_size(size_t n_elements, size_t n_hazards) {
	return sizeof(hash_table) + n_elements * sizeof(item) + sizeof(hz_ct) * n_hazards;
}

static void free_htable(hash_table *ht) {
	//free attached elements
	item *tofree = ht->cleanup_with_me;
	while (tofree) {
		//const qualifier...
		free_mem((void *)tofree->data);
		tofree = tofree->next;
	}
	free_mem(ht);
}

static hash_table *create_ht(size_t n_el, size_t n_hz) {
	size_t hsize = calc_ht_size(n_el, n_hz);
	hash_table *ht = alloc_mem(hsize);
	memset(ht, 0, hsize);
	ht->n_elements = n_el;
	ht->elems = (item *)ht->actual_data;
	ht->hazard_start = (hz_ct *)(ht->elems + n_el);
	return ht;
}

static hash_table *acquire_table(shared_hash_table *tbl, size_t id) {
	assert(id < tbl->nhazards);

	//tbl is assumed to be unchanging ever

	//acquire prevents the load from being reordered
	//to happen before this operation
	atomic_fetch_add(tbl->hazard_refs[id].nactive, 1, mem_acquire);

	hash_table *mytbl = atomic_load(tbl->current_table, mem_relaxed);
	consume_barrier;

	return mytbl;
}

static void release_table(shared_hash_table *tbl, size_t id) {
	assert(id < tbl->nhazards);
	//although this is just reading, we must use a release ordering
	//so that the writer thread doesn't think that we are done
	//when actually we are still reading
	atomic_fetch_sub(tbl->hazard_refs[id].nactive, 1, mem_release);
}

static void update_table(shared_hash_table *sht, hash_table *ht) {
	//no barrier since this thread is the only one making changes
	//so this thread will see all updates to current table
	hash_table *old = sht->current_table;

	//release on the store to prevent
	//any previous working from being reordered here
	atomic_store(sht->current_table, ht, mem_release);


	//For this, we need a store-load barrier,
	//which is only provided by mem_seq_cst
	//To prevent the store to current_table
	//from being reordered with the loads from the
	//current hazard table
	atomic_barrier(mem_seq_cst);

	//once this point is reached, it is impossible for a
	//reader which has not signed the hazards table yet
	//to see the older version of the hash table
	//why? Any thread which has not signed the hazard table
	//yet will view the new table upon loading.
	//In fact, any thread which has not actually loaded the table yet
	//will see the new one, at this point.
	//As a result, any new signatures
	//that race with this copy will all be seeing
	//the new version of the pointer.
	hz_ct hasv = 0;
	for (size_t i = 0; i < sht->nhazards; i++) {
		//can do relaxed loads, thanks to the barrier
		//none of them will be happen before the update
		hz_ct cur = sht->hazard_refs[i].nactive;
		hasv |= cur; //see if there are any active
		old->hazard_start[i] = cur;
	}
	if (!hasv) {
		//can delete right away
		free_htable(old);
	}
	else {
		//put it in the list!
		//do some things with it...
		old->next = sht->old_tables;
		sht->old_tables = old;
	}
}

//thanks internet
static inline uint64_t rotl64(uint64_t x, int8_t r)
{
  return (x << r) | (x >> (64 - r));
}


static uint64_t avalanche64(uint64_t h) {
	h ^= h >> 33;
	h *= 0xff51afd7ed558ccd;
	h ^= h >> 33;
	h *= 0xc4ceb9fe1a85ec53;
	h ^= h >> 33;

	//probably some ultra-bit-expr
	//that does this and satisfies the pipeline
	//better. This wil almost certainly not be a
	//bottleneck though
	return h <= 2 ? 3 : h;
}

static uint64_t hash_str64(const char *data, size_t klen) {
	//all keys end up being multiples of 8 bytes,
	//for a simpler version of this algorithm
	//the rest zero padded
	uint64_t hash = *(uint64_t *)data;
	if (klen <= 8) {
		return avalanche64(hash);
	}

	//ensure divisible by 8...
	assert(!(klen % 8));
	uint64_t *dat64 = (uint64_t *)data;
	size_t nblocks = klen / 8;

	const uint64_t c1 = 0x87c37b91114253d5;
	const uint64_t c2 = 0x4cf5ad432745937f;

	//do the actual hash
	for (size_t i = 0; i < nblocks; i++) {
		uint64_t k = dat64[i];
		k *= c1;
		k = rotl64(k, 31);
		k *= c2;
		hash ^= k;
		hash = rotl64(hash, 27);
		hash = hash*5+0x52dce729;
	}
	return avalanche64(hash ^ klen);
}

static inline item *insert_into(item *elems,
								uint64_t n_elems,
						    	uint64_t key,
						    	int *res) {
	uint64_t lkey = key;
	int nbad = 0;
	for (size_t i = 0; i < hash_load; i++) {
		uint64_t act_key = lkey & (n_elems - 1);
		item *item_at = &elems[act_key];
		if (test_empty(item_at->key)) {
			return item_at;
		}
		else if (test_dead(item_at->key)) {
			nbad++;
		}
		lkey = avalanche64(lkey);
	}
	if (nbad > (hash_load/2)) {
		*res = no_inc;
	}
	else {
		*res = inc_size;
	}
	return 0;
}

static inline item *lookup_exist(item *elems,
								uint64_t n_elems,
						    	uint64_t key,
						    	const char *ks,
						    	size_t klen) {
	uint64_t lkey = key;
	for (size_t i = 0; i < hash_load; i++) {
		uint64_t act_key = lkey & (n_elems - 1);
		item *item_at = &elems[act_key];
		if (has_elem(item_at->key)
			&& !memcmp(item_at->data, ks, klen)) {
			return item_at;
		}
		lkey = avalanche64(lkey);
	}
	return 0;
}

static hash_table *resize_into(const hash_table *ht, int inc_size) {
	size_t newer_elements = ht->n_elements;
	hash_table *ntbl = 0;
	for (;;) {
		//0 if just clearing,
		//1 if reopening
		newer_elements <<= inc_size;
		ntbl = create_ht(newer_elements, ht->n_hazards);

		for (size_t i = 0; i < ht->n_elements; i++) {
			item *celem = &ht->elems[i];
			uint64_t rkey = celem->key;
			if (has_elem(rkey)) {
				item *item_at = insert_into(ntbl->elems,
					newer_elements,
					rkey);
				if (!item_at) {
					goto retry;
				}

				item_at->key = rkey;
				item_at->data = celem->data;
			}
		}
		break;
		retry:
		free_htable(ntbl);
	}
	return ntbl;
}

void insert_chars(shared_hash_table *sht, const char *data, size_t klen) {
	uint64_t keyh = hash_str64(data, klen);
	item *add_to;
	hash_table *ht = sht->current_table;
	int ins_res;
	while (!(add_to = insert_into(ht->elems, ht->n_elements, keyh, &ins_res))) {
		ht = resize_into(ht, ins_res);
		update_table(sht, ht);
	}
	add_to->data = data + klen;
	atomic_store(add_to->key, keyh, mem_release);
}

const char *remove_element(struct shared_hash_table *sht, const char *key, size_t klen) {
	uint64_t keyh = hash_str64(key, klen);
	hash_table *ht = sht->current_table;
	item *add_to = lookup_exist(ht->elems, ht->n_elements, keyh, key, klen);
	if (add_to) {
		//no synchronization here,
		//doesn't matter if someone is looking/looks this up
		add_to->key = is_del;
		add_to->next = ht->cleanup_with_me;
		ht->cleanup_with_me = add_to;
		return add_to->data;
	}
	return 0;
}

char apply_elem(struct shared_hash_table *sht,
				size_t id,
			    const char *key,
			    size_t klen,
			    void (*appfn)(const char *, void *),
			    void *params) {
	uint64_t keyh = hash_str64(key, klen);
	hash_table *ht = acquire_table(sht, id);
	item *add_to = lookup_exist(ht->elems, ht->n_elements, keyh, key, klen);
	if (add_to) {
		appfn(add_to->data + klen, params);
		release_table(sht, id);
		return 1;
	}
	release_table(sht, id);
	return 0;
}

