#include <pthread.h>
#include <stdint.h>
#include <assert.h>
#include <stdio.h>
#include <time.h>

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

keystr keys[nwrite*2];

uint32_t nextrand(size_t *cur) {
    *cur = (*cur * 2862933555777941757) + 3037000493;
    return *cur >> 32;
}

struct shared_hash_table *sht;

void dummyfn(const char *a, void *b) {
	uint64_t *kmb = (uint64_t *)b;
	uint64_t *kma = (uint64_t *)a;
	if (*kma != *kmb) {
		printf ("Fuck\n");
	}
}

void test_lookup(uint64_t cid, size_t id) {
	keystr *rm = &keys[cid];
	apply_to_elem(sht,
		          id,
		          (char *)rm,
		          sizeof(uint64_t),
		          dummyfn,
		          &rm->value);
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
    	insert_chars(sht, (char *)&keys[i], sizeof(uint64_t));	
    }
}

void *modify(void *val) {
	uint64_t rng = (uint64_t)val;
	while(keep_modding) {
		keystr *rm = &keys[nextrand(&rng) % (nwrite * 2)];
		if (!remove_element(sht, (char *)rm, sizeof(uint64_t))) {
			insert_chars(sht, (char *)rm, sizeof(uint64_t));
		}
	}
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
	pthread_t threads[nthread];
	pthread_t modt;
	sht = create_tbl();
	init_keys();
	do_inserts();
	timespec time1, time2, timed;
	clock_gettime(CLOCK_REALTIME, &time1);
	keep_modding = 1;
	pthread_create(&modt, NULL, modify, (void *)time(NULL));
	for (size_t i = 0; i < nthread; i++) {
		pthread_create(&threads[i], NULL, read_table, (void *)i);
	}
	for (size_t i = 0; i < nthread; i++) {
		pthread_join(threads[i], 0);
	}
	clock_gettime(CLOCK_REALTIME, &time2);
	keep_modding = 0;
	pthread_join(modt, NULL);
	timed = diff(time1, time2);
	uint64_t total_nanos = timed.tv_nsec + 1e9*timed.tv_sec;
	double seconds = total_nanos * (1.0/1e9);

	printf("Took %f seconds\n", seconds);
	printf("Took %f nanoseconds per lookup\n", 1e9*seconds/nread);
	printf("Performed %e reads per second\n", nthread*nread/seconds);
	printf("Final hash table held %d elements total", get_size(sht));
	return 0;
}