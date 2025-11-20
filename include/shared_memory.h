#ifndef SHARED_M_H
#define SHARED_M_H

#include <stdlib.h>
#include <semaphore.h>

typedef struct SharedMemory SharedMemory;
// Used for shared memory synchronization.
static sem_t *sem_shared_memory;

struct SharedMemory
{
	void *content;
	long size;
	key_t key;
	int id;
	char error[100];
};


void freeSM(int idSM);
void *createSM(int idSM);
void unlinkSM(void *ptrSM);
// key_t getKeySM(char *file, int identifier);
key_t getKeySM();
key_t getKeySMByBlock(const char *file_name,  int block_id);
key_t MurmurOAAT32(const char *key);
int getIdentifierSM(key_t keySM, long sizeSM);
void copyContentSM(void *ptrSM, const void *content, long contentSize);
SharedMemory *setContentSM(key_t, long contentSize, const void *content);
SharedMemory *getContentSM(int key, long contentSize);
void *getContentSMByID(int id);
// SharedMemory *setContentSMByID(int id, long contentSize, const void *content);
void *setContentSMByID(int id, long contentSize, const void *content);

#endif
