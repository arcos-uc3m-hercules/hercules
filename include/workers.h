#ifndef WORKER_H
#define WORKER_H

// #include "imss.h"
#include "comms.h"
// #include "records.hpp"
#include "hierarchical_records.hpp"
#include "shared_memory.h"
// #include "hercules.hpp"
#include "utils.h"
#include <cstdint>
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
#define INSTANCE_OP 9
#define PERFORMANCE_OP 10
#define READV2_OP 11
#define UNLINK_OP 12
#define DISK_OP 13
#define DECOMISSIONING_OP 20

#define WRITE_OP 1
#define WRITEV 7
#define READV 8
#define SPLIT_READV 9
#define SPLIT_WRITEV 10

#define GET_OP 0
#define SET_OP 1

// #define GET_OP_STR "GET"
// #define SET_OP_STR "SET"
// #define MALL_OP_STR "MALLEABILITY"
// Metadata operations.

#define LOCAL_DATASET_UPDATE 0

extern HierarchicalRecords *global_hierarchical_map;
extern HierarchicalRecords *garbage_collector_map;

// Set of arguments passed to each server thread.
typedef struct
{
	HierarchicalRecords *hierarchical_map;
	HierarchicalRecords *garbage_collector_map;
	// Pointer to the struct related to the current HERCULES istance.
	imss_info *hercules_info_struct;
	// Pointer to the corresponding type storing key-address couples.
	std::shared_ptr<map_records> map = NULL;
	struct arguments *args;	
	uint64_t port;
	int64_t total_size;
	ucp_context_h ucp_context;
	ucp_worker_h ucp_worker;
	ucp_worker_h ucp_data_worker;
	ucp_ep_h server_ep;
	size_t blocksize;
	uint64_t storage_size;
	ucp_address_t *peer_address;
	uint64_t worker_uid;
	u_int16_t hercules_thread_pool_size;
	uint32_t thread_id;
	char curr_req[PATH_MAX];
	// Integer specifying the port that a certain thread will listen to.
	// URI assigned to the current HERCULES instance.
	char my_uri[URI_];
	char *tmp_file_path;
	// Pointer to the corresponding buffer region assigned to a thread.
	// char *pt;
	bool status;
} p_argv;

// Structure to pass arguments to the client handling thread (Dispatcher).
typedef struct
{
	int client_socket;
	uint32_t client_id_counter;
	u_int16_t hercules_thread_pool_size;
} client_handler_args;

/**
 * @brief Arguments specifically for the CommissioningStage thread.
 */
typedef struct
{
	// The entire 'args' substruct containing configuration.
	struct arguments *args;

	// A pointer to the shared Hercules instance information.
	imss_info *hercules_info_struct;

	// Fields needed to call SendConfirmationMessage and CheckForMalleability.
	ucp_worker_h ucp_worker;
	ucp_ep_h server_ep;
	uint64_t worker_uid;
	uint32_t thread_id;

	char curr_req[PATH_MAX];
} MalleabilityArgs;

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

/**
 * @brief Reads "deployfile" line by line and fill "my_imss.ips".
 *
 * @param deployfile path to the hostfile.
 * @param my_imss structure to fill "my_imss.ips".
 * @return int, number of lineas read, -1 on error.
 */
int ReadHostfile(char *deployfile, imss_info *my_imss);
int AddIPS(imss_info *my_imss, char *line, int32_t n_chars, int at_position);
int CheckForMalleability(const p_argv *arguments, const char *req);
scaling_action make_scaling_decision(const std::map<std::string, std::vector<ElasticityMetric>> &history, int32_t analysis_window_size, double minimum_performance_threshold, ElasticityMetric &slowest_server);

// Malleability functions.
void *get_performance_metrics(void *th_argv);
void *run_malleability(void *th_argv);
void *comissioning_stage(MalleabilityArgs *arguments);
int decomissioning_stage(MalleabilityArgs *arguments, int id_server_to_remove);
int ShutdownServer();
// void Decomissioning_stage(p_argv *arguments, int id_server_to_remove);
void Update_data_endpoint_list(int id_server_to_remove, size_t num_elements_to_shift);
size_t update_ips_list(int id_server_to_remove);
int send_node_list_2_frontend(p_argv temp_p_argv_for_calls);

/**
 * @brief Re-distribute the blocks of this server to another servers
 * following the distribution policy choose by the user.
 * @return 0 on success, on error -1 is returned.
 */
void *move_blocks_2_server(void *th_argv);



#endif
