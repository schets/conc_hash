#include <pthread.h>
#include <stdint.h>
#include <assert.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>

#include <unistd.h>

#include "hash_table.h"

#define max_size 4096 * 8
#define nread (100*100*100*100)
#define nwrite 1000
#define mod_batch 8
#define nthread 3

char keep_modding;
typedef struct timespec timespec;

typedef struct keystr {
    uint64_t keyval;
    uint64_t value;
} keystr;

size_t writect;

long myclock() {
      struct timeval tv;
      gettimeofday(&tv, NULL);
      return (tv.tv_sec * 1000000) + tv.tv_usec;
}

int comp_keys(const void *k1, const void* k2) {
	return (uint64_t)k1 == (uint64_t)k2;
}

keystr keys[nwrite*2];

uint32_t nextrand(uint64_t *cur) {
    *cur = (*cur * 2862933555777941757) + 3037000493;
    return *cur >> 32;
}

struct shared_hash_table *sht;

void dummyfn(const void *_k, void *_v, void *_par) {
	uint64_t k = (uint64_t)_k;
	uint64_t v = (uint64_t)_v;
	keystr *par = (keystr *)_par;
	if ((k != par->keyval) || (v != par->value)) {
		printf ("Fuck\n");
	}
}

void test_lookup(uint64_t cid, size_t id) {
	keystr *rm = &keys[cid];
	apply_to_elem(sht,
				  id,
				  (void *)rm->keyval,
		          dummyfn,
		          rm);
}

void *read_table(void *val) {
	size_t id = (size_t)val;
	printf ("Running with %ld\n", id);
	uint64_t rng = id ^ (id * 100) ^ ((id + 2) * 43234455);
    for (size_t i = 0; i < nread; i++) {
    	test_lookup(nextrand(&rng) % (nwrite * 2), id);
    }
    return 0;
}

void init_keys() {
	uint64_t rng = time(NULL);
	for (size_t i = 0; i < nwrite*2; i++) {
		keystr *km = &keys[i];
		km->keyval = nextrand(&rng);
		km->keyval <<= 32;
		km->keyval += nextrand(&rng);
		km->value = i;
	}
}

void do_inserts() {
    for (size_t i = 0; i < nwrite; i++) {
    	insert(sht, (void *)keys[i].keyval, (void *)keys[i].value);
    }
}

void test_exists(const void *k, void *v, void *bres) {
	*(char *)bres = 1;
}

void test_real() {
	char res;
	for (size_t i = 0; i < nwrite; i++) {
		res = 0;
		apply_to_elem(sht,
					  0,
					  (const void *)keys[i].keyval,
					  test_exists,
					  &res);
		if (res == 0) {
			printf("Failed %d\n", (int)i);
		}
	}
}

void *modify(void *val) {
	uint64_t rng = (uint64_t)val;
	while(__atomic_load_n(&keep_modding, __ATOMIC_RELAXED)) {
		++writect;
		keystr *rm = &keys[nextrand(&rng) % (nwrite * 2)];
		if (!remove_element(sht, (void *)rm->keyval)) {
			insert(sht, (void *)rm->keyval, (void *)rm->value);
		}
	}
	printf("done!!!\n");
	return 0;
}

timespec diff(timespec start, timespec end) {
	timespec temp;
	if ((end.tv_nsec-start.tv_nsec)<0) {
		temp.tv_sec = end.tv_sec-start.tv_sec-1;
		temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
	} else {
		temp.tv_sec = end.tv_sec-start.tv_sec;
		temp.tv_nsec = end.tv_nsec-start.tv_nsec;
	}
	return temp;
}

int main() {
	//initialize things
	keep_modding = 1;
	writect = 0;
	pthread_t threads[nthread];
	pthread_t modt;
	sht = create_tbl(hash_integer, comp_keys);
	init_keys();
	do_inserts();
	test_real();
	//return 0;
	long ts = myclock();
	keep_modding = 1;
	pthread_create(&modt, NULL, modify, (void *)time(NULL));
	for (size_t i = 0; i < nthread; i++) {
		pthread_create(&threads[i], NULL, read_table, (void *)i);
	}
	for (size_t i = 0; i < nthread; i++) {
		pthread_join(threads[i], 0);
	}
	printf("\n\nJoining stuff\n");
	ts = myclock() - ts;
	keep_modding = 0;
	pthread_join(modt, NULL);
	uint64_t total_nanos = ts * 1000;
	double seconds = total_nanos * (1.0/1e9);

	printf("Took %f seconds\n", seconds);
	printf("Took %f nanoseconds per lookup\n", 1e9*seconds/nread);
	printf("Performed %e reads per second\n", nthread*nread/seconds);
	printf("Performed %e reads per second per thread\n", nread/seconds);
	printf("Performed %ld writes, %e writes/second\nTook %f nanoseconds/write\n",
		   writect, writect*1.0/seconds, 1e9*seconds/writect);
	printf("Final hash table held %d elements total\n", get_size(sht));
	return 0;
}
