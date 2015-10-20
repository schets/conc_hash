#ifndef SHARED_HASH_TABLE_H
#define SHARED_HASH_TABLE_H

#include <stddef.h>
#include <stdint.h>

//single-writer many-reader hash table;
struct shared_hash_table;

//first klen characters are the key
//the rest is a null-terminated string

typedef uint64_t (*hashfn_type)(const void *);
typedef int (*compfn_type)(const void*, const void*);
typedef void (*delfn_type)(const void *, void *, void *);

void insert(struct shared_hash_table *c, const void *key, void *data);
void *remove_element(struct shared_hash_table *c, const void *key);

char apply_to_elem(struct shared_hash_table *sht,
			       size_t id,
				   const void *key,
                   void (*appfn)(const void *, void *, void *),
                   void *params);

struct shared_hash_table *create_tbl(hashfn_type h, compfn_type c);
size_t get_size(struct shared_hash_table *sht);

uint64_t hash_string(const void* elem);

//!hashes the value in the pointer
uint64_t hash_integer(const void* elem);

void try_clean_mem(struct shared_hash_table *sht);

#endif
