#define _DEFAULT_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "slog.h"
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
		perror("ERR_HERCULES_GETHOSTNAME_GETKEYSM");
		return -1;
	}
	return MurmurOAAT32(hostname);
}

static int unique_value = 0;
key_t getKeySMByBlock(const char *file_name, int block_id)
{
	// char hostname[1024], full_name[2048];
	// int ret = gethostname(&hostname[0], 512);
	// if (ret == -1)
	// {
	// 	perror("ERR_HERCULES_GETHOSTNAME_GETKEYSMBYBLOCK");
	// 	return -1;
	// }

	// sprintf(full_name, "%s/%s$%d", hostname, file_name, block_id);
	if (block_id == 0)
	{
		unique_value--;
		block_id=unique_value;
	}
	

	// return MurmurOAAT32(full_name);
	key_t key;
	if ((key = ftok("/lustre/uc3m_a0/sciot/gesanche/test-io500/shm_aux", block_id)) == -1) {
        perror("ftok");
        exit(1);
    }
	return key;
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

/**
 * @brief To create a shared memory descriptor (like a file descriptor).
 *
 * @param keySM key identifier of the shared memory space (must to be bigger than zero).
 * @param sizeSM size of the sahred memory space.
 * @return int shared memory descritor used to open the space, -1 on error.
 */
int getIdentifierSM(key_t keySM, long sizeSM)
{
	if (keySM == -1)
	{
		fprintf(stderr, "HERCULES_ERR_GET_IDENTIFIER_SHM_INVALID_KEY: %d\n", keySM);
		slog_error("HERCULES_ERR_GET_IDENTIFIER_SHM_INVALID_KEY: %d\n", keySM);
		return -1;
	}

	if (sizeSM < 0)
	{
		fprintf(stderr, "HERCULES_ERR_GET_IDENTIFIER_SHM_INVALID_SIZE: %ld\n", sizeSM);
		slog_error("HERCULES_ERR_GET_IDENTIFIER_SHM_INVALID_SIZE: %ld\n", sizeSM);
		return -1;
	}
	
	int idSM = 0;
	slog_debug("Creating identifier for key %d and size %ld", keySM, sizeSM);
	// fprintf(stderr, "Creating identifier for key %d and size %ld\n", keySM, sizeSM);

	idSM = shmget(
		keySM,
		sizeSM,
		IPC_CREAT | IPC_EXCL  | 0666); // genera la memoria compartida

	
	int max_tries = 100;
	srand(time(NULL));
	while (idSM == -1 && max_tries != 0)
	{
		// perror("--Error: Can't generate ID for shared memory--");
		// fprintf(stderr, "Recalculating shared memory ID for content %i with size %d...\n", keySM, sizeSM);
		slog_debug("Recalculating shared memory ID for content %i with size %d...", keySM, sizeSM);
		int r = rand();
		keySM = getpid() * r;
		idSM = shmget(
			keySM,
			sizeSM,
			IPC_CREAT | IPC_EXCL | 0666); // genera la memoria compartida
		max_tries--;
	}
	if (idSM == -1)
	{
		fprintf(stderr, "Cannot generated a valid id for key %d with size %d\n", keySM, sizeSM);
		perror("--Error: Can't generate ID for shared memory--");
		slog_error("--Error: Can't generate ID for shared memory--");
		// exit(1);
		return -1;
	}

	slog_debug("Shared memory Identifier created: %d", idSM);
	// fprintf(stderr, "Shared memory Identifier created: %d\n", idSM);
	return idSM;
}

/**
 * @brief To create a shared memory space.
 *
 * @param idSM shared memory descritor given by "getIdentifierSM".
 * @return void* pointer to the shared memory space. NULL on error.
 */
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

	slog_debug("Creating shared memory with the Identifier: %d", idSM);
	return ptrSM;
}

void copyContentSM(void *ptrSM, const void *content, long contentSize)
{
	memcpy(ptrSM, content, contentSize);
}

SharedMemory *setContentSM(key_t key, long contentSize, const void *content)
{
	// fprintf(stderr, "[SHM] setting content with key %d and size %d\n", key, contentSize);
	slog_debug("[SHM] setting content with key %d and size %d", key, contentSize);

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
		slog_debug("Recalculating shared memory ID for content %i...", key);
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
		fprintf(stderr, "--Error: Shared memory was not created--\n");
		slog_fatal("--Error: Shared memory was not created--");
		strcpy(SM->error, "Shared memory was not created");
		SM->key = -1;
		SM->id = -1;
	}

	memcpy(SM->content, content, contentSize);
	SM->size = contentSize;
	slog_debug("setting data on shared memory with Identifier: %d", SM->id);
	return SM;
}

void *setContentSMByID(int id, long contentSize, const void *content)
{
	// fprintf(stderr, "[SHM] setting content with key %d and size %d\n", key, contentSize);
	slog_debug("[SHM] setting content with id %d and size %d", id, contentSize);

	// SharedMemory *SM;
	if (id < 0)
	{
		slog_error("HERCULES_ERR_SET_CONTENT_SHM_BY_ID_INVALID_ID: %d", id);
		fprintf(stderr, "HERCULES_ERR_SET_CONTENT_SHM_BY_ID_INVALID_ID: %d\n", id);
		return NULL;
	}

	if (content == NULL)
	{
		slog_error("HERCULES_ERR_SET_CONTENT_SHM_BY_ID_INVALID_CONTENT");
		fprintf(stderr, "HERCULES_ERR_SET_CONTENT_SHM_BY_ID_INVALID_CONTENT\n");
		return NULL;
	}
	

	void *shm_pointer = shmat(
		id,
		NULL,
		0); // creando puntero a la memoria compartida

	if (shm_pointer == (void *) -1)
	{
		char err_msg[1024];
		sprintf(err_msg, "--Error: Shared memory %d was not created--", id);
		perror(err_msg);
		slog_fatal("%s\n", err_msg);
		return NULL;
	}

	memcpy(shm_pointer, content, contentSize);
	// SM->size = contentSize;
	slog_debug("setting data on shared memory with Identifier: %d", id);
	return shm_pointer;
}

void *getContentSMByID(int id)
{
	if (id < 0)
	{
		fprintf(stderr, "HERCULES_ERR_INVALID_SHM_ID: %d\n", id);
		slog_error("HERCULES_ERR_INVALID_SHM_ID\n: %d", id);
		return NULL;
	}

	void *content = shmat(
		id,
		NULL,
		0); // creando puntero a la memoria compartida

	if (content == (void *) -1)
	{
		char err_msg[1024];
		sprintf(err_msg, "HERCULES_ERR_GET_CONTENT_SHM_BY_ID: %d", id);
		perror(err_msg);
		slog_error("%s\n", err_msg);
		return NULL;
	}
	

	slog_debug("getting data from shared memory with identifier %d", id);
	return content;
}

SharedMemory *getContentSM(int key, long contentSize)
{
	if (key < 0)
	{
		fprintf(stderr, "HERCULES_ERR_INVALID_SHM_KEY\n: %d", key);
		slog_error("HERCULES_ERR_INVALID_SHM_KEY: %d", key);
		return NULL;
	}

	if (contentSize <= 0)
	{
		fprintf(stderr, "HERCULES_ERR_INVALID_SHM_SIZE\n: %d", key);
		slog_error("HERCULES_ERR_INVALID_SHM_SIZE: %d", key);
		return NULL;
	}

	// fprintf(stderr, "[SHM] getting content with key %d and size %d\n", key, contentSize);
	slog_debug("[SHM] getting content with key %d and size %d", key, contentSize);
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
		fprintf(stderr, "--Error: Can't generate ID for shared memory--\n");
		slog_error("--Error: Can't generate ID for shared memory--");
		exit(1);
	}

	SM->content = shmat(
		SM->id,
		NULL,
		0); // creando puntero a la memoria compartida

	if (SM->content == NULL)
	{
		fprintf(stderr, "--Error: Shared memory was not created--\n");
		slog_error("--Error: Shared memory was not created--");
		exit(1);
	}

	SM->size = contentSize;
	slog_debug("getting data from shared memory with identifier %d", SM->id);
	return SM;
}

void unlinkSM(void *ptrSM)
{
	int ret = shmdt(ptrSM);
	if (ret == -1)
	{
		perror("HERCULES_ERR_UNLINK_SHM");
		slog_error("HERCULES_ERR_UNLINK_SHM");
	}
}

void freeSM(int idSM)
{
	slog_debug("Deleting shared memory with id %d", idSM);
	shmctl(idSM, 0, IPC_RMID);
}
