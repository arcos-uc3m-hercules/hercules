#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "shared_memory.h"

// key_t getKeySM(char *file, int identifier)
key_t getKeySM()
{
	// key_t keySM = -1;

	// keySM = ftok(
	// 	file,
	// 	identifier); // genera la llave para la memoria compartida

	// if (keySM == -1)
	// {
	// 	fprintf(stderr, "--Error: Can't generate key for shared memory--\n");
	// 	exit(1);
	// }

	// return keySM;
	char hostname[512];
	int ret = gethostname(&hostname[0], 512);
	if (ret == -1)
	{
		perror("ERR_HERCULES_GETHOSTNAME");
		return -1;
	}
	return MurmurOAAT32(hostname);
}

key_t MurmurOAAT32(const char *key)
{
	key_t h = 335ul;
	for (; *key; ++key)
	{
		h ^= *key;
		h *= 0x5bd1e995;
		h ^= h >> 15;
	}
	return abs(h);
}

// key_t getKey()
// {
// 	char hostname[512];
// 	int ret = gethostname(&hostname[0], 512);
// 	if (ret == -1)
// 	{
// 		perror("ERR_HERCULES_GETHOSTNAME");
// 		return -1;
// 	}
// 	return MurmurOAAT32(hostname);
// }

int getIdentifierSM(key_t keySM, long sizeSM)
{
	int idSM;

	idSM = shmget(
		keySM,
		sizeSM,
		IPC_CREAT | 0666); // genera la memoria compartida

	if (idSM == -1)
	{
		printf("--Error: Can't generate ID for shared memory--\n");
		// exit(1);
		return -1;
	}

	return idSM;
}

void *createSM(int idSM)
{
	void *ptrSM;

	ptrSM = shmat(
		idSM,
		NULL,
		0); // creando puntero a la memoria compartida

	if (ptrSM == NULL)
	{
		printf("--Error: Shared memory was not created--\n");
		// exit(1);
	}

	return ptrSM;
}

void copyContentSM(void *ptrSM, const void *content, long contentSize)
{
	memcpy(ptrSM, content, contentSize);
}

SharedMemory *setContentSM(key_t key, long contentSize, const void *content)
{
	SharedMemory *SM;

	SM = (SharedMemory *)malloc(sizeof(SharedMemory));
	SM->key = -1;
	SM->id = -1;

	// SM->key = getpid() * idContent;
	SM->key = key;
	SM->id = shmget(
		SM->key,
		contentSize,
		IPC_CREAT | IPC_EXCL | 0666); // genera la memoria compartida

	srand(time(NULL));
	while (SM->id == -1)
	{
		fprintf(stderr, "Recalculating shared memory ID for content %i...\n", key);
		int r = rand();
		SM->key = getpid() * r;
		SM->id = shmget(
			SM->key,
			contentSize,
			IPC_CREAT | IPC_EXCL | 0666); // genera la memoria compartida
	}

	SM->content = shmat(
		SM->id,
		NULL,
		0); // creando puntero a la memoria compartida

	if (SM->content == NULL)
	{
		printf("--Error: Shared memory was not created--\n");
		strcpy(SM->error, "Shared memory was not created");
		SM->key = -1;
		SM->id = -1;
	}

	memcpy(SM->content, content, contentSize);
	SM->size = contentSize;
	return SM;
}

SharedMemory *getContentSM(int key, long contentSize)
{
	SharedMemory *SM;

	SM = (SharedMemory *)malloc(sizeof(SharedMemory));
	SM->id = -1;
	SM->key = key;
	SM->id = shmget(
		SM->key,
		contentSize,
		IPC_CREAT | 0666); // genera la memoria compartida

	if (SM->id == -1)
	{
		printf("--Error: Can't generate ID for shared memory--\n");
		exit(1);
	}

	SM->content = shmat(
		SM->id,
		NULL,
		0); // creando puntero a la memoria compartida

	if (SM->content == NULL)
	{
		printf("--Error: Shared memory was not created--\n");
		exit(1);
	}

	SM->size = contentSize;
	return SM;
}

void unlinkSM(void *ptrSM)
{
	shmdt(ptrSM);
}

void freeSM(int idSM)
{
	shmctl(idSM, 0, IPC_RMID);
}
