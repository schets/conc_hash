
#include "hash_table.h"
#include "atomics.h"

#include <string.h>
#include <stdint.h>
#include <stdlib.h>

#define hash_load 2
#define is_del 1

#define _inc_size 1
#define _no_inc 0
#define _exists 2
//simple test for 0
#define test_empty(key) ((key) == 0)

#define test_dead(key) ((key) == is_del)

//returns if any bits after the first two exist
//so returns false for 0, 1, 2
#define has_elem(key) ((key) > 1)

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
	uint64_t key;
	const void *keyp;
	void *data;
	struct item *next; //not super relevant, useful for cleanup
	struct item *iter_next; //used for iterating. hence the name
} item;

typedef struct hash_table {
	uint64_t n_elements;
	uint64_t active_count;
	uint64_t n_hazards;
	item *elems;
	item *active_l;
	item *cleanup_with_me;
	uint16_t *hazard_start;
	struct hash_table *next;
	char actual_data[];
} hash_table;

typedef struct shared_hash_table {
	struct hash_table *current_table;
	struct hash_table *old_tables;
	size_t nhazards;
	hashfn_type hashfn;
	compfn_type compfn;
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
		//free_mem((void *)tofree->data);
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
	ht->n_hazards = n_hz;
	return ht;
}

shared_hash_table *create_tbl(hashfn_type hashfn, compfn_type compfn) {
	size_t nstart = 128;
	size_t nhaz = 8;
	struct shared_hash_table *sht;
	sht = malloc(sizeof(*sht) + nhaz * sizeof(hz_st));
	memset(sht, 0, sizeof(*sht));
	for (size_t i = 0; i < nhaz; i++) {
		sht->hazard_refs[i].nactive = 0;
	}
	sht->current_table = create_ht(nstart, nhaz);
	sht->nhazards = nhaz;
	sht->hashfn = hashfn;
	sht->compfn = compfn;
	sht->old_tables = 0;
	atomic_barrier(mem_release);
	return sht;
}

static hash_table *acquire_table(shared_hash_table *tbl, size_t id) {

	//tbl is assumed to be unchanging ever

	//acquire prevents the load from being reordered
	//to happen before this operation
	atomic_fetch_add(tbl->hazard_refs[id].nactive, 1, mem_acquire);

	hash_table *mytbl = atomic_load(tbl->current_table, mem_relaxed);
	consume_barrier;

	return mytbl;
}

static void release_table(shared_hash_table *tbl, size_t id) {
	//although this is just reading, we must use a release ordering
	//so that the writer thread doesn't think that we are done
	//when actually we are still reading
	atomic_fetch_sub(tbl->hazard_refs[id].nactive, 1, mem_release);
}

static char update_del(shared_hash_table *tbl, hash_table *htbl) {
	hz_st *href = tbl->hazard_refs;
	hz_ct *ohz = htbl->hazard_start;
	size_t nhz = tbl->nhazards;
	char del = 1;
	for (size_t i = 0; i < nhz; i++) {
		if (ohz[i]) {
			if (!href[i].nactive) {
				atomic_barrier(mem_acquire);
				ohz[i] = 0;
			}
			else {
				del = 0;
			}
		}
	}
	if (del) {
	}
	return del;
}

void clear_tables(shared_hash_table *sht) {
	//first pop off the top
	hash_table *ntop = sht->old_tables;
	hash_table *ctbl = sht->old_tables;
	while (ctbl) {
		hash_table *nxt = ctbl->next;
		if (update_del(sht, ctbl)) {
			free_htable(ctbl);
			ntop = nxt;
			ctbl = nxt;
		}
		else {
			break;
		}
	}
	sht->old_tables = ntop;

	//go through the list and clear/update tables left in the middle
	if (ntop) {
		hash_table *prev_t = ntop;
		ctbl = ntop->next;
		while (ctbl) {
			hash_table *nxt = ctbl->next;
			if (update_del(sht, ctbl)) {
				free_htable(ctbl);
				prev_t->next = nxt;
			}
			else {
				prev_t = ctbl;
			}
			ctbl = nxt;
		}
	}
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
	//try to clear out existing tables

	clear_tables(sht);

	if (!hasv) {
		free_htable(old);
	}
	else {
		//put it in the list!
		//do some things with it...
		old->next = sht->old_tables;
		sht->old_tables = old;
	}
}

void try_clean_mem(shared_hash_table *sh) {
	clear_tables(sh);
}

void clean_all_mem(shared_hash_table *sh) {
	while (sh->old_tables) {
		clear_tables(sh);
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
	return h < 2 ? 2 : h;
}

static inline item *insert_into(item *elems,
								uint64_t n_elems,
						    	uint64_t key,
						    	const void *keyp,
						    	compfn_type cmp,
						    	int *res) {
	uint64_t lkey = key;
	int nbad = 0;
	*res = _inc_size;
	for (size_t i = 0; i < hash_load; i++) {
		uint64_t act_key = lkey & (n_elems - 1);
		item *item_at = &elems[act_key];
		if (test_empty(item_at->key)) {
			return item_at;
		}
		else if (test_dead(item_at->key)) {
			*res = _no_inc;
		}
		else if (cmp && cmp(item_at->keyp, keyp)) {
			*res = _exists;
			return 0;
		}
		lkey = avalanche64(lkey);
	}
	return 0;
}

static inline item *lookup_exist(item *elems,
							     uint64_t n_elems,
							     uint64_t keyh,
							     const void *key,
							     compfn_type cmp) {
	uint64_t lkey = keyh;
	for (size_t i = 0; i < hash_load; i++) {
		uint64_t act_key = lkey & (n_elems - 1);
		item *item_at = &elems[act_key];
		if (has_elem(item_at->key)
			&& cmp(item_at->keyp, key)) {
			return item_at;
		}
		lkey = avalanche64(lkey);
	}
	return 0;
}

static hash_table *resize_into(const hash_table *ht, int inc_size) {
	size_t newer_elements = ht->n_elements;
	hash_table *ntbl = 0;
	if (inc_size == _no_inc) {
	}
	else {
	}
	for (;;) {
		//0 if just clearing,
		//1 if reopening
		newer_elements <<= inc_size;
		ntbl = create_ht(newer_elements, ht->n_hazards);
		item *celem = ht->active_l;
		while (celem) {
			if (has_elem(celem->key)) {
				uint64_t rkey = celem->key;
				int useless;
				item *item_at = insert_into(ntbl->elems,
											newer_elements,
											rkey,
											NULL,
											NULL,
											&useless);
				if (!item_at) {
					goto retry;
				}

				item_at->key = celem->key;
				item_at->data = celem->data;
				item_at->keyp = celem->keyp;
				item_at->iter_next = ntbl->active_l;
				ntbl->active_l = item_at;
			}
			celem = celem->next;
		}
		break;
		retry:
		free_htable(ntbl);
	}
	return ntbl;
}

void insert(shared_hash_table *sht, const void *key, void *data) {
	uint64_t keyh = sht->hashfn(key);
	item *add_to;
	hash_table *ht = sht->current_table;
	int ins_res;
	while (!(add_to = insert_into(ht->elems, ht->n_elements, keyh,
							      key, sht->compfn, &ins_res))) {
		ht = resize_into(ht, ins_res);
	}
	if (ht != sht->current_table) {
		update_table(sht, ht);
	}
	add_to->data = data;
	add_to->key = keyh;
	add_to->keyp = key;
	add_to->iter_next = ht->active_l;
	atomic_barrier(mem_release);

	ht->active_l = add_to;
}

void *remove_element(struct shared_hash_table *sht, const void *key) {
	hash_table *ht = sht->current_table;
	uint64_t keyh = sht->hashfn(key);
	item *add_to = lookup_exist(ht->elems, ht->n_elements, keyh, key, sht->compfn);
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

char apply_to_elem(struct shared_hash_table *sht,
   				   size_t id,
			       const void *key,
			       void (*appfn)(const void *, void *, void *),
			       void *params) {
	uint64_t keyh = sht->hashfn(key);
	hash_table *ht = acquire_table(sht, id);
	item *add_to = lookup_exist(ht->elems, ht->n_elements, keyh, key, sht->compfn);
	if (add_to) {
		atomic_barrier(mem_acquire);
		appfn(add_to->keyp, add_to->data, params);
		release_table(sht, id);
		return 1;
	}
	release_table(sht, id);
	return 0;
}

void shared_table_for_each(shared_hash_table *sht,
						   size_t id,
						   char (*appfnc)(const void*, const void *, void *),
						   void *params) {
	hash_table *ctbl = acquire_table(sht, id);

	item *citem = ctbl->active_l;
	while (citem) {
		consume_barrier;
		if (has_elem(citem->key)) {
			//need an acquire barrier here since we are synchronizing
			//with stores to key, not just loads of citem
			atomic_barrier(mem_acquire);
			appfnc(citem->keyp, citem->data, params);
		}
		citem = citem->iter_next;
	}
	release_table(sht, id);
}

size_t get_size(shared_hash_table *sht) {
	return sht->current_table->n_elements;
}


uint64_t hash_string(const void *_instr) {
	uint64_t hash = 0xcbf29ce484222325;
	if (_instr) {
		const unsigned char* instr = (const unsigned char *)_instr;
		while (*instr) {
			hash ^= *instr;
			hash *= 0x100000001b3;
			instr++;
		}
	}
	return avalanche64(hash);
}

uint64_t hash_integer(const void *in) {
	return avalanche64((uint64_t)in);
}