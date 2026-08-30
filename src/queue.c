#include "queue.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

static StsHeader *create();
static StsHeader *create()
{
	StsHeader *handle = malloc(sizeof(*handle));
	if (handle == NULL)
		return NULL;

	handle->head = NULL;
	handle->tail = NULL;

	pthread_mutex_t *mutex = malloc(sizeof(*mutex));
	if (mutex == NULL)
	{
		free(handle);
		return NULL;
	}
	handle->mutex = mutex;
	pthread_mutex_init(handle->mutex, NULL);

	handle->size = 0;

	return handle;
}

static void destroy(StsHeader *header);
static void destroy(StsHeader *header)
{
	if (header == NULL)
		return;

	// Drain all remaining elements to avoid leaking StsElement nodes.
	// NOTE: the caller is responsible for freeing the ->value buffers
	// before calling destroy.
	pthread_mutex_lock(header->mutex);
	StsElement *curr = header->head;
	while (curr != NULL)
	{
		StsElement *next = curr->next;
		free(curr);
		curr = next;
	}
	header->head = NULL;
	header->tail = NULL;
	header->size = 0;
	pthread_mutex_unlock(header->mutex);

	pthread_mutex_destroy(header->mutex);
	free(header->mutex);
	free(header);
	header = NULL;
}

static int size(StsHeader *header);
static int size(StsHeader *header)
{
	// Create new element
	int size = 0;
	pthread_mutex_lock(header->mutex);
	size = header->size;
	pthread_mutex_unlock(header->mutex);
	return size;
}

static void push(StsHeader *header, void *elem);
static void push(StsHeader *header, void *elem)
{
	// Create new element
	StsElement *element = malloc(sizeof(*element));
	if (element == NULL)
		return;

	element->value = elem;
	element->next = NULL;

	pthread_mutex_lock(header->mutex);
	// Is list empty
	if (header->head == NULL)
	{
		header->head = element;
		header->tail = element;
	}
	else
	{
		// Rewire
		StsElement *oldTail = header->tail;
		oldTail->next = element;
		header->tail = element;
	}

	header->size++;
	pthread_mutex_unlock(header->mutex);
}

static void *pop(StsHeader *header);
static void *pop(StsHeader *header)
{
	pthread_mutex_lock(header->mutex);
	StsElement *head = header->head;

	// Is empty?
	if (head == NULL)
	{
		pthread_mutex_unlock(header->mutex);
		return NULL;
	}
	else
	{
		// Rewire
		header->head = head->next;

		// If we just removed the last element, reset tail to avoid dangling pointer.
		if (header->head == NULL)
		{
			header->tail = NULL;
		}

		// Get head and free element memory
		void *value = head->value;
		free(head);

		if (header->size > 0)
			header->size--;
		pthread_mutex_unlock(header->mutex);
		return value;
	}
}

_StsQueue const StsQueue = {
    create,
    destroy,
    size,
    push,
    pop};
