#ifndef WORKER_H
#define WORKER_H

// #include "imss.h"
#include "comms.h"
// #include "records.hpp"
#include "hierarchical_records.hpp"
#include "shared_memory.h"
// #include "hercules.hpp"
#include <memory>

// Backend operations.
#define READ_OP 0
#define GETDIR 1
#define RELEASE 2
#define WHO 3
#define DELETE_OP 4
#define RENAME_OP 5
#define RENAME_DIR_DIR_OP 6
#define CLOSE_OP 7
#define OPEN_OP 8
// #define STATFS			  7
// #define DATASET_OP 8
#define INSTANCE_OP 9
#define PERFORMANCE_OP 10

#define WRITE_OP 1
#define WRITEV 7
#define READV 8
#define SPLIT_READV 9
#define SPLIT_WRITEV 10

#define GET_OP 0
#define SET_OP 1

#define LOCAL_DATASET_UPDATE 0

#define KB 1024
#define MB 1048576
#define GB 1073741824


// #define MAX_THREAD_POOL_SIZE 16
extern void *hierarchical_map;

// Set of arguments passed to each server thread.
typedef struct
{
	// Pointer to the corresponding type storing key-address couples.
	std::shared_ptr<map_records> map = NULL;
	void *hierarchical_map;
	// Pointer to the corresponding buffer region assigned to a thread.
	char *pt;
	// Integer specifying the port that a certain thread will listen to.
	uint64_t port;
	// URI assigned to the current HERCULES instance.
	char my_uri[URI_];
	// Pointer to the struct related to the current HERCULES istance.
	imss_info *hercules_info_struct = NULL;
	int64_t total_size;
	ucp_context_h ucp_context;
	ucp_worker_h ucp_worker;
	ucp_worker_h ucp_data_worker;
	ucp_ep_h server_ep;
	size_t blocksize;
	uint64_t storage_size;
	ucp_address_t *peer_address;
	uint64_t worker_uid;
	char *tmp_file_path;
	u_int16_t hercules_thread_pool_size;
	int thread_id;
	struct arguments args;
	char curr_req[PATH_MAX];
} p_argv;

// Structure to pass arguments to the client handling thread (Dispatcher).
typedef struct
{
	int client_socket;
	uint32_t client_id_counter; // To maintain the client_id_ for modulo operation
	u_int16_t hercules_thread_pool_size;
	// Add other necessary arguments here (e.g., local_addr, local_addr_len, slog functions if not global)
} client_handler_args;

// Thread method attending client data requests.
void *hercules_ucx_server(void *th_argv);
int srv_worker_helper(p_argv *arguments, const char *req, void *map_server_eps);
void *Checkpoint(void *th_argv);
void *Snapshot(void *th_argv);

// Thread method searching and cleaning nodes with st_nlink=0
void *GarbageCollector(void *th_argv);

// Thread method attending client metadata requests.
int stat_worker_helper(p_argv *arguments, char *req, void *map_server_eps);

// Dispatcher thread method distributing clients among the pool server threads.
void *srv_attached_dispatcher(void *th_argv);

// Dispatcher thread method distributing clients among the pool of metadata server threads.
void *Dispatcher(void *th_argv);
void *HandleClient(void *args);

/**
 * @brief Function to write on disk the status of an Hercules process.
 *
 * @param tmp_file_path pathname where the file will be written.
 * @param msg string message to be written in the file.
 * @return int, 0 if the file was correctly write, -1 on error.
 */
int ready(char *tmp_file_path, const char *msg);

#endif
