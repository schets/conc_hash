#ifndef TASK_STRUCT_H
#define TASK_STRUCT_H

#include <stddef.h>

//single-writer many-reader hash table;
//also will support shared interprocess memory
struct shared_hash_table;

//first klen characters are the key
//the rest is a null-terminated string

void insert_chars(struct shared_hash_table *c, const char *data, size_t klen);
const char *remove_element(struct shared_hash_table *c, const char *key, size_t klen);

char apply_to_elem(struct shared_hash_table *sht,
				   const char *key,
                   size_t klen,
                   void (*appfn)(const char *, void *),
                   void *params);

struct shared_hash_table *create_tbl();
size_t get_size(struct shared_hash_table *sht);

#endif
