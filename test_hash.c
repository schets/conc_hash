#include <pthread.h>
#include <stdint.h>

#include "hash_table.h"

#define max_size 4096 * 8;
#define nread 1e6;
#define nwrite 1e4;

struct keystr {
    uint64_t keyvals[2];
    uint64_t value;
};

uint32_t nextrand(size_t *cur) {
    *cur = (*cur * 2862933555777941757) + 3037000493;
    return cur >> 32;
}

struct shared_hash_table *sht;

void *read_table(void *dummy) {
    for (size_t i)
}

void do_inserts() {
    for (size_t i = 0; i < nwrite; i++)
}
