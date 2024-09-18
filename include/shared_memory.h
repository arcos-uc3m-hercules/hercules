#ifndef SHARED_M_H
#define SHARED_M_H

#include <stdlib.h>

typedef struct SharedMemory SharedMemory;

struct SharedMemory {
	char *content;
	long size;
	key_t key;
	int id;
	char error[100];
};

void freeSM(int idSM);
char *createSM(int idSM);
void unlinkSM(char *ptrSM);
key_t getKeySM(char *file, int identifier);
int getIdentifierSM(key_t keySM, long sizeSM);
void copyContentSM(char *ptrSM, char *content, long contentSize);
SharedMemory *setContentSM(int idContent, long contentSize, const char *content);

#endif