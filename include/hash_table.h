#ifndef HASHTABLE_H
#define HASHTABLE_H

#include <sys/mman.h>

void hercules_serialize_pool(int shm_fd);
void hercules_deserialize_pool(int shm_fd);

#endif