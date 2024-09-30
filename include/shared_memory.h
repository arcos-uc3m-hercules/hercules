#ifndef SHARED_M_H
#define SHARED_M_H

#include <stdlib.h>

typedef struct SharedMemory SharedMemory;

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
key_t MurmurOAAT32(const char *key);
int getIdentifierSM(key_t keySM, long sizeSM);
void copyContentSM(void *ptrSM, const void *content, long contentSize);
SharedMemory *setContentSM(key_t, long contentSize, const void *content);
SharedMemory *getContentSM(int key, long contentSize);

#endif
