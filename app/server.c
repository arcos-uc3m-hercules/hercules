#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/signal.h>
#include "metadata_stat.h"
#include "memalloc.h"
#include "directory.h"
#include "records.hpp"
#include "map_ep.hpp"
#include <inttypes.h>
#include <unistd.h>

// Pointer to the tree's root node.
extern GNode *tree_root;
extern pthread_mutex_t tree_mut;
extern int32_t __thread current_dataset;   // Dataset whose policy has been set last.
extern dataset_info __thread curr_dataset; // Currently managed dataset.
extern imss __thread curr_imss;

// Initial buffer address.
extern char *buffer_address;
// Set of locks dealing with the memory buffer access.
extern pthread_mutex_t *region_locks;
// Segment size (amount of memory assigned to each thread).
extern uint64_t buffer_segment;
char POLICY[MAX_POLICY_LEN];

extern ucp_worker_h *ucp_worker_threads;
extern ucp_address_t **local_addr;
extern size_t *local_addr_len;
extern StsHeader *mem_pool;

struct arguments args;
std::shared_ptr<map_records> g_map;

/* UCP objects */
ucp_context_h ucp_context;
ucp_worker_h ucp_worker;

// ucp_ep_h pub_ep;
ucp_address_t *req_addr;
ucp_ep_h **metadata_endpoints;
size_t req_addr_len;

unsigned long number_active_storage_servers = 0; // stores the current number of active storage servers.
pthread_t *threads;

// global variables usted to finish threads.
extern int global_finish_threads;
extern int global_finish_checkpoint;
extern int global_finish_snapshot;
extern int global_server_fd_thread;
extern pthread_cond_t global_finish_cond;
extern pthread_cond_t global_run_snapshot_cond;
extern pthread_cond_t global_run_checkpoint_cond;
extern pthread_mutex_t global_finish_mut;

#define RAM_STORAGE_USE_PCT 0.75f // percentage of free system RAM to be used for storage

/**
 * @brief Re-distribute the blocks of this server to another servers
 * following the distribution policy choose by the user.
 * @return 0 on success, on error -1 is returned.
 */
int move_blocks_2_server(uint64_t stat_port, uint32_t server_id, char *imss_uri, std::shared_ptr<map_records> map)
{
	// Creates endpoints to all data servers. It is use in case of
	// malleability to move blocks between data servers.
	slog_debug("Connecting to data servers\n");
	open_imss(imss_uri); // TODO: Check if this is still necessary due we called it on the main function.
	if (number_active_storage_servers < 0)
	{
		slog_fatal("Error creating HERCULES's resources, the process cannot be started");
		return -1;
	}

	// Here data server should to move the datablocks.
	// print all key/value elements.
	double time_taken;
	time_t t = clock();
	void *address_;
	uint64_t block_size;
	int curr_map_size = 0;
	const char *uri_;
	size_t size;
	char key_[REQUEST_SIZE];
	// Get the number of blocks stored by this data server.
	int number_of_blocks_2_move = map->size();

	slog_info("Server %d, has %d blocks, active storage servers=%lu", args.id, map->size(), number_active_storage_servers);
	while ((curr_map_size = map->size()) > 0 && number_active_storage_servers > 0)
	{
		std::string key;
		// get next key (block identifier) with the format <block_name>$<block_number>
		// for example "myfile$199", where block_name = myfile, and block_number = 199.
		key = map->get_head_element();

		// get the element data and store it in "address_".
		map->get(key, &address_, &block_size);
		// fprintf(stderr, "**** curr_map_size=%d, head element=%s, block_size=%ld\n", curr_map_size, key.c_str(), block_size);
		slog_debug("**** curr_map_size=%d, head element=%s, block_size=%ld\n", curr_map_size, key.c_str(), block_size);

		int pos = key.find('$') + 1;						   // +1 to skip '$' on the block number.
		std::string block = key.substr(pos, key.length() + 1); // substract the block number from the key.
		int block_number = stoi(block, 0, 10);				   //  string to number.
		pos -= 1;											   // -1 to skip '$' on the data uri.
		std::string data_uri = key.substr(0, pos);			   // substract the data uri from the key.
		slog_debug("key='%s',\turi='%s',\tblock='%s'\n", key.c_str(), data_uri.c_str(), block.c_str());
		int next_server = find_server(number_active_storage_servers, block_number, data_uri.c_str(), 0);

		slog_info("key='%s',\turi='%s%s',\tfrom server %d to server %d,\tactive servers=%lu\n", key.c_str(), data_uri.c_str(), block.c_str(), server_id, next_server, number_active_storage_servers);
		slog_debug("new server=%d, curr_server=%d\n", next_server, server_id);

		// here we can send key.c_str() directly to reduce the number of operations.
		if (set_data_server(data_uri.c_str(), block_number, address_, block_size, 0, next_server) < 0)
		{
			slog_error("ERR_HERCULES_SET_DATA_IN_SERVER\n");
			perror("ERR_HERCULES_SET_DATA_IN_SERVER");
			return -1;
		}

		// delete the element from the map.
		map->erase_head_element();
		// get new map size to print it.
		curr_map_size = map->size();
		// fprintf(stderr, "**** curr_map_size=%d\n", curr_map_size);
		slog_debug("**** curr_map_size=%d\n", curr_map_size);
	}

	t = clock() - t;
	time_taken = ((double)t) / (CLOCKS_PER_SEC);

	if (number_active_storage_servers > 0)
	{
		// fprintf(stderr, "[HS] Data movement %d blocks %lu %f sec.\n", number_of_blocks_2_move, number_active_storage_servers, time_taken);
		fprintf(stderr, "\033[0;34m [HS] Server %d has moved %d blocks to %lu servers in %f sec. \033[0m\n", args.id, number_of_blocks_2_move, number_active_storage_servers, time_taken);
	}

	return 0;
}

/**
 * @brief Comunicates data servers to metadata servers to
 * update the number of active servers and to update the
 * status of this data server (non active).
 * @return 0 on success, on error -1 is returned.
 */
int stop_server()
{
	// Get the current number of active nodes.
	number_active_storage_servers = get_number_of_active_nodes(args.hercules_path);

	if (number_active_storage_servers < 0)
	{
		return -1;
	}

	// Tell metadata server to reduce number of servers.
	char key_plus_size[REQUEST_SIZE];
	// Send the created structure to the metadata server.
	// last "0" is the server status to be set.
	sprintf(key_plus_size, "%d SET %lu %s %d", args.id, number_active_storage_servers, args.imss_uri, 0);
	slog_debug("[main] Request - %s", key_plus_size);
	if (send_req(ucp_worker, *metadata_endpoints[0], req_addr, req_addr_len, key_plus_size) == 0)
	{
		perror("HERCULES_ERR_STOP_SERVER_SEND_REQ");
		return -1;
	}

	return 0;
}

/**
 * @brief Comunicates data servers to metadata servers to
 * update the number of active servers and to update the
 * status of this data server (active).
 * @return 0 on success, on error -1 is returned.
 */
int wakeup_server()
{
	// Get the current number of active nodes.
	number_active_storage_servers = get_number_of_active_nodes(args.hercules_path);

	if (number_active_storage_servers < 0)
	{
		return -1;
	}

	// Tell metadata server to update (increase) the number of servers.
	char key_plus_size[REQUEST_SIZE];
	// Send the created structure to the metadata server.
	// last "1" is the server status to be set.
	sprintf(key_plus_size, "%d SET %lu %s %d", args.id, number_active_storage_servers, args.imss_uri, 1);
	// fprintf(stderr, "Request - %s\n", key_plus_size);
	slog_debug("[main] Request - %s", key_plus_size);
	if (send_req(ucp_worker, *metadata_endpoints[0], req_addr, req_addr_len, key_plus_size) == 0)
	{
		perror("ERR_HERCULES_RLS_SERVER_SEND_REQ");
		return -1;
	}

	return 0;
}

/**
 * @brief Defines the actions to be doing when the
 * server receives a signal from the hercules script.
 * SIGUSR1 is used to srink and SIGUSR2 is used to
 * increase the number of servers.
 */
void handle_signal_server(int signal)
{
	if (signal == SIGUSR1) // suspend or shutdown this server.
	{
		slog_info("SIGUSR1 received");
		int pkill_operation = 0, ret = 0;
		char buf[10], action[20], temporal_path[PATH_MAX];
		char tmp_file_path[PATH_MAX];

		sprintf(temporal_path, "%s/tmp/hercules_pkill_operation", args.hercules_path);
		// fprintf(stderr,"Temporal path: %s\n", temporal_path);

		// Get the operation number.
		int fd = open(temporal_path, O_RDONLY);
		if (fd == -1)
		{
			char err_msg[MAX_ERR_MSG_LEN];
			sprintf(err_msg, "ERR_HERCULES_OPEN_PKILL_OPERATION:%s", temporal_path);
			perror(err_msg);
			return;
		}

		ret = read(fd, buf, sizeof(buf) - 1);
		buf[ret] = '\0';
		// In case of read error, pkill_operation must be 0
		// to suspend the server but not shutdown it.
		if (ret == -1)
		{
			pkill_operation = 0;
			perror("HERCULES_ERR_READ_PKILL_OPERATION");
		}
		else
		{
			pkill_operation = atoi(buf);
		}

		ret = close(fd);
		if (fd == -1)
		{
			perror("ERR_HERCULES_CLOSE_PKILL_OPERATION");
		}
		slog_info("pkill_operation = %d", pkill_operation);
		// fprintf(stderr, "pkill_operation = %d\n", pkill_operation);
		switch (pkill_operation)
		{
		case 1: // finish data server processes (shutdown).
			// "global_finish_threads" is a gloabl variable readed by the
			// dispatcher and workers threads. 1 indicates those threads
			// must finish their execution.
			sprintf(action, "stop");

			// if (args.type == TYPE_METADATA_SERVER || global_finish_checkpoint == 1)
			if (args.type == TYPE_METADATA_SERVER)
			{
				global_finish_threads = 1;
			}
			else
			{
				global_finish_checkpoint = 1;
				global_finish_snapshot = 1;

				pthread_cond_signal(&global_run_snapshot_cond);
				pthread_cond_signal(&global_run_checkpoint_cond);

				pthread_mutex_lock(&global_finish_mut);
				pthread_cond_wait(&global_finish_cond, &global_finish_mut);

				fprintf(stderr, "Waiting for snapshot and checkpointing in server %d\n", args.id);
				// This file is readed by the hercules script to know if this server
				// was correctly shutting down.
				sprintf(tmp_file_path, "%s/tmp/%c-hercules-%d-%s", args.hercules_path, args.type, args.id, action);
				ready(tmp_file_path, "LOCKED");

				pthread_mutex_unlock(&global_finish_mut);
				fprintf(stderr, "Server %d has been unlocked\n", args.id);
			}

			// Shutdown or close the socket used by the dispatcher pointed
			// by the file descriptor "global_server_fd_thread".
			if (shutdown(global_server_fd_thread, SHUT_RD) == -1)
			{
				perror("ERR_HERCULES_SHUTDOWN_SERVER_FD\n");
			}
			break;
		default: // suspend the data server.
			sprintf(action, "remove");
			// Data servers processes will still running to be reused on
			// the future. On shrink process, this server won't be used,
			// but backend processes will be still running.
			break;
		}

		// Data servers performs malleability operations if it is enabled.
		if (args.type == TYPE_DATA_SERVER && args.malleability == 1)
		{
			ret = stop_server();
			if (ret == 0) // success.
			{
				ret = move_blocks_2_server(args.stat_port, args.id, args.imss_uri, g_map);
				if (ret < 0) // error.
				{
					// TODO: if "move_blocks_2_server" fails, try again?
				}
			}
		}
		// This file is readed by the hercules script to know if this server
		// was correctly shutting down.
		sprintf(tmp_file_path, "%s/tmp/%c-hercules-%d-%s", args.hercules_path, args.type, args.id, action);
		ready(tmp_file_path, "OK");
	}
	if (signal == SIGUSR2) // wake up this server.
	{
		slog_info("SIGUSR2 received");
		if (args.type == TYPE_DATA_SERVER) // only data servers.
		{
			fprintf(stderr, " \033[0;32m Waking up server %d \033[0m\n", args.id);
			// Changes the number of active servers in the metadata server
			// and the status of this server.
			wakeup_server();

			// This file is readed by the hercules script to know if this server
			// was correctly waking up.
			char tmp_file_path[PATH_MAX];
			sprintf(tmp_file_path, "%s/tmp/%c-hercules-%d-up", args.hercules_path, args.type, args.id);
			fprintf(stderr, "Writting file %s\n", tmp_file_path);
			ready(tmp_file_path, "OK");
		}
	}
}

void print_usage(const char *msg)
{
	fprintf(stderr, "%s\n usage for METADATA server: hercules_server m <server_id>\n usage for DATA server: hercules_server d <server_id> <metadata_host> <initial_number_of_data_servers> \n", msg);
}

int32_t main(int32_t argc, char **argv)
{
	signal(SIGUSR1, handle_signal_server);
	signal(SIGUSR2, handle_signal_server);

	pthread_cond_init(&global_run_snapshot_cond, NULL);
	pthread_cond_init(&global_finish_cond, NULL);

	// clock_t t;
	double time_taken;
	int init_number_of_server = 1;

	uint64_t bind_port;
	char *stat_add = NULL;
	// char *metadata_file;
	char *deployfile = NULL;
	int64_t buffer_size, stat_port, num_servers;
	ucp_ep_params_t ep_params;
	ucp_address_t *peer_addr;
	size_t addr_len;

	// memory pool stuff
	size_t block_size;	   // In KB
	uint64_t storage_size; // In GB

	ucs_status_t status;

	ucp_am_handler_param_t param;
	int ret = 0;
	ucp_config_t *config;
	ucp_worker_address_attr_t attr;

	uint64_t max_system_ram_allowed;
	uint64_t max_storage_size; // memory pool size
	uint32_t num_blocks;

	// shared memory.
	int shm_data_id = 0;
	char tmp_file_path[PATH_MAX];

	time_t t = time(NULL);

	if (argc < 3)
	{
		print_usage("Too few arguments");
		return 0;
	}

	args.type = argv[1][0];
	args.id = atoi(argv[2]);

	if (args.type != TYPE_METADATA_SERVER && args.type != TYPE_DATA_SERVER)
	{
		// fprintf(stderr, "%c is not a valid server type \n usage: hercules_server <m|d> <server_id> <metadata_host> <0|1>\n", args.type);
		char usage_msg[64];
		sprintf(usage_msg, "%c is not a valid server type", args.type);
		print_usage((const char *)usage_msg);
		return 0;
	}

	// Checks arguments for metadata and data servers.
	if ((argc != 3 && args.type == TYPE_METADATA_SERVER) || (argc != 5 && args.type == TYPE_DATA_SERVER))
	{
		print_usage("Wrong number of arguments");
		return 0;
	}

	// Fill the args struct with the enviroment variables or config file values.
	ret = getConfiguration(&args);
	if (ret == -1)
	{
		return 0;
	}

	sprintf(tmp_file_path, "%s/tmp/%c-hercules-%d-start", args.hercules_path, args.type, args.id);

	// args.logging.hercules_debug_level = SLOG_NONE;
	IMSS_THREAD_POOL = args.thread_pool;
	// POLICY = args.policy;
	strncpy(POLICY, args.policy, sizeof(POLICY));

	char log_path[PATH_MAX];
	char *workdir = getenv("PWD");
	slog_debug("Server type=%c\n", args.type);
	struct tm tm = *localtime(&t);
	sprintf(log_path, "%s/%c-server-%d.%02d-%02d-%02d", workdir, args.type, args.id, tm.tm_hour, tm.tm_min, tm.tm_sec);

	// Initializate logger.
	slog_init(log_path, args.logging.hercules_debug_level, args.logging.hercules_debug_file, args.logging.hercules_debug_screen, 1, 1, 1, args.id);
	// slog_time("Server %d started", args.id);

	if (args.logging.hercules_debug_file > 0)
	{
		printf("Log path = %s\n", log_path);
		fflush(stdout);
	}
	slog_info(",Time(msec), Comment, RetCode");

	slog_debug("[SERVER] Starting server.");
	if (args.type == TYPE_DATA_SERVER)
	{
		args.stat_host = argv[3];
		slog_debug("imss_uri = %s stat-host = %s stat-port = %" PRId64 " num-servers = %" PRId64 " deploy-hostfile = %s block-size = %" PRIu64 " (kB) storage-size = %" PRIu64 " (gB), args.bufsize = %" PRId64 "", args.imss_uri, args.stat_host, args.stat_port, args.num_data_servers, args.data_hostfile, args.block_size, args.storage_size, args.bufsize);
		// fprintf(stderr, "imss_uri = %s stat-host = %s stat-port = %" PRId64 " num-servers = %" PRId64 " deploy-hostfile = %s block-size = %" PRIu64 " (kB) storage-size = %" PRIu64 " (gB), args.bufsize = %" PRId64 "\n", args.imss_uri, args.stat_host, args.stat_port, args.num_data_servers, args.data_hostfile, args.block_size, args.storage_size, args.bufsize);
		// bind port number.
		bind_port = args.data_port;
		init_number_of_server = atoi(argv[4]);
	}
	else
	{
		slog_debug("[CLI PARAMS] type = %c port = %" PRId64 " bufsize = %" PRId64 "", args.type, args.stat_port, args.bufsize);
		// fprintf(stderr, "[CLI PARAMS] type = %c port = %" PRId64 " bufsize = %" PRId64 "\n", args.type, args.stat_port, args.bufsize);
		// fprintf(stderr, "[CLI PARAMS] type = %c port = %" PRId64 " bufsize = %" PRId64 "\n", args.type, args.stat_port, args.bufsize);
		// slog_debug("stat-logfile = %s", args.stat_logfile);
		// bind port number.
		bind_port = args.stat_port;
	}

	// buffer size provided
	buffer_size = args.bufsize;
	// fprintf(stderr, "buffer_size=%lu\n", buffer_size);

	/* Initialize the UCX required objects */
	slog_info("Before init context");
	ret = init_context(&ucp_context, config, &ucp_worker, CLIENT_SERVER_SEND_RECV_STREAM);
	slog_info("After init context");
	if (ret != 0)
	{
		perror("HERCULES_ERR_INIT_CONTEXT");
		return -1;
	}

	/* Set memory pool size */
	max_storage_size = args.storage_size * GB;
	// get max RAM we could use for storage
	max_system_ram_allowed = (uint64_t)sysconf(_SC_AVPHYS_PAGES) * sysconf(_SC_PAGESIZE) * RAM_STORAGE_USE_PCT;

	// make sure we don't use more memory than available
	if (max_storage_size >= max_system_ram_allowed || max_storage_size < 0)
	{
		max_storage_size = max_system_ram_allowed;
	}

	// init memory pool
	slog_info("[main] before sts queue create");
	// fprintf(stderr, "max_storage_size=%lu\n", max_storage_size);
	mem_pool = StsQueue.create();
	// figure out how many blocks we need and allocate them
	num_blocks = max_storage_size / (args.block_size * KB);
	slog_info("[main] num_blocks=%lu", num_blocks);
	for (int i = 0; i < num_blocks; ++i)
	{
		void *buffer = (void *)calloc(args.block_size * KB, sizeof(char));
		// memset(buffer, 0, args.block_size * KB);
		StsQueue.push(mem_pool, buffer);
	}

	/* CHECK THIS OUT!
	 ***************************************************
	 In relation to the type argument provided, an HERCULES or a metadata server will be deployed. */

	// HERCULES server.
	if (args.type == TYPE_DATA_SERVER)
	{
		// machine name where the metadata server is being executed.
		stat_add = args.stat_host;
		// port that the metadata server is listening on.
		stat_port = args.stat_port;
		// number of servers conforming the HERCULES deployment.
		num_servers = args.num_data_servers;
		// Dynamic number of servers conforming the HERCULES deployment used by malleability.
		number_active_storage_servers = num_servers;
		// HERCULES' MPI deployment file.
		deployfile = args.data_hostfile;
		// data block size
		block_size = args.block_size; // in kilobytes.
		// total storage size
		// storage_size = max_storage_size;

		int32_t imss_exists = 0;

		// Check if the provided URI has been already reserved by any other instance.
		slog_info("args.id=%d", args.id);

		ucs_status_t status;
		int oob_sock;
		int ret = 0;
		ucs_status_t ep_status = UCS_OK;

		uint32_t id = args.id;

		// %%%%%%%%%%%%%%%%%%%%%%%%%%
		// Read the metadata hostfile.
		// FILE entity managing the HERCULES deployfile.
		FILE *metadata_nodes_fd;

		if (args.meta_hostfile[0] == '\0')
		{
			perror("HERCULES_ERR_META_HOSTFILE_NOT_SET");
			exit(1);
		}
		slog_debug("Opening file %s", args.meta_hostfile)
		if ((metadata_nodes_fd = fopen(args.meta_hostfile, "r+")) == NULL)
		{
			char err_msg[MAX_ERR_MSG_LEN];
			sprintf(err_msg, "HERCULES_ERR_DEPLOYFILE_OPEN:%s", deployfile);
			perror(err_msg);
			slog_fatal("%s", err_msg);
			return -1;
		}

		// Number of characters successfully read from the line.
		int32_t n_chars;
		int init_server_status = 1;
		// int num_active_data_servers = 0;
		metadata_endpoints = (ucp_ep_h **)malloc(args.num_metadata_servers * sizeof(ucp_ep_h *));
		for (int32_t i = 0; i < args.num_metadata_servers; i++)
		{
			metadata_endpoints[i] = (ucp_ep_h *)malloc(sizeof(ucp_ep_h));
			// Allocate resources in the metadata structure so as to store the current HERCULES's IP.
			// (my_imss.ips)[i] = (char *)calloc(LINE_LENGTH, sizeof(char));
			size_t num_bytes_for_line = 0;
			stat_add = NULL;

			// Save HERCULES metadata deployment.
			n_chars = getline(&stat_add, &num_bytes_for_line, metadata_nodes_fd);

			// Erase the new line character ('') from the string.
			if (stat_add[n_chars - 1] == '\n')
			{
				stat_add[n_chars - 1] = '\0';
			}

			slog_debug("Establishing a connection with %s:%ld\n", stat_add, stat_port);

			oob_sock = connect_common(stat_add, stat_port, AF_INET);

			char request[REQUEST_SIZE];
			sprintf(request, "%" PRIu32 " GET %s", id, "MAIN!QUERRY");
			slog_debug("Request - %s", request);
			if (send(oob_sock, request, REQUEST_SIZE, 0) < 0)
			{
				perror("HERCULES_ERR_STAT_HELLO");
				slog_error("HERCULES_ERR_STAT_HELLO");
				return -1;
			}

			ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
			slog_debug("Address len=%lu", addr_len);
			peer_addr = (ucp_address *)malloc(addr_len);
			ret = recv(oob_sock, peer_addr, addr_len, MSG_WAITALL);
			slog_debug("Peer Address=%lu", peer_addr);
			close(oob_sock);
			free(stat_add);

			/* Send client UCX address to server */
			ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
								   UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
								   UCP_EP_PARAM_FIELD_ERR_HANDLER |
								   UCP_EP_PARAM_FIELD_USER_DATA;
			ep_params.address = peer_addr;
			ep_params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
			ep_params.err_handler.cb = err_cb_client;
			ep_params.err_handler.arg = NULL;
			ep_params.user_data = &ep_status;
			slog_debug("Creating endpoint with the metadata server %d", i);
			status = ucp_ep_create(ucp_worker, &ep_params, metadata_endpoints[i]);
			slog_debug("Endpoint with the metadata %d created", i);
			// status = ucp_worker_get_address(ucp_worker, &req_addr, &req_addr_len);

			ucp_worker_attr_t worker_attr;
			worker_attr.field_mask = UCP_WORKER_ATTR_FIELD_ADDRESS;
			status = ucp_worker_query(ucp_worker, &worker_attr);
			req_addr_len = worker_attr.address_length;
			req_addr = worker_attr.address;

			attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
			ucp_worker_address_query(req_addr, &attr);
			slog_debug("[srv_worker_thread] Server UID %" PRIu64 ".", attr.worker_uid);

			if (!args.id)
			{ // Only performs by the data server with ID = 0.
				// Formated HERCULES uri to be sent to the metadata server.
				char formated_uri[REQUEST_SIZE];
				sprintf(formated_uri, "%" PRIu32 " GET 0 %s", id, args.imss_uri);
				slog_debug("Request - %s", formated_uri);
				// Send the request.
				if (send_req(ucp_worker, *metadata_endpoints[i], req_addr, req_addr_len, formated_uri) == 0)
				{
					slog_error("HERCULES_ERR__SEND_REQ");
					perror("HERCULES_ERR__SEND_REQ");
					return -1;
				}

				// Get the length of the message to be received.
				size_t length = 0;
				length = get_recv_data_length(ucp_worker, attr.worker_uid);
				if (length == 0)
				{
					slog_error("HERCULES_ERR__GET_RECV_DATA_LENGTH");
					perror("HERCULES_ERR__GET_RECV_DATA_LENGTH");
					return -1;
				}
				// Receive the associated structure.
				imss_info *imss_info_ = (imss_info *)malloc(sizeof(imss_info) * length);
				ret = recv_dynamic_stream(ucp_worker, *metadata_endpoints[i], imss_info_, IMSS_INFO, attr.worker_uid, length);
				// If another data server has taken the URI, this HERCULES configuration should not be deployed.
				// Two HERCULES configurations cannot have the same URI.
				// We check if "recv_dynamic_stream" has successed, if so, there are another HERCULES instance using
				// the same URI.
				// On success, we free memory and stop this instance.
				int new_id = 0;
				if (ret != -1)
				{ // success "recv_dynamic_stream".
					// fprintf(stderr, "imss_info_.num_storages=%d, length=%lu\n", imss_info_->num_storages, length);
					imss_exists = 1;
					for (int32_t i = 0; i < imss_info_->num_storages; i++)
					{
						// fprintf(stderr,"ip[%d]=%s\n", i, imss_info_->ips[i]);
						free(imss_info_->ips[i]);
						new_id++;
					}
					free(imss_info_->ips);
				}
				free(imss_info_);
				if (args.id != new_id)
				{
					fprintf(stderr, "Data server with id = %d already in use, changing to %d\n", args.id, new_id);
					// Set a new id to this server.
					args.id = new_id;
					/* code */
				}
			}

			if (imss_exists)
			{
				// Here we need to stop all HERCULES data servers
				// related to this configuration, or check if them
				// are not running anymore to continue deploying this configuration.

				perror("HERCULES_ERR_SERVER_URI_TAKEN");
				slog_error("HERCULES_ERR_SERVER_URI_TAKEN, ret=%d", ret);
				// ready(tmp_file_path, "ERROR");
				// return 0;
			}

			// When LOCAL policy is used, the server creates a shared memory region.
			if (!strcmp(POLICY, "LOCAL") || !strcmp(POLICY, "ZCOPY"))
			{
				// Get the shared memory key and tries to create the shared memory region (pool).
				key_t key = getKeySM();
				slog_info("Generated Key = %d\n", key);

				shm_data_id = getIdentifierSM(key, SHM_SIZE);
				if (shm_data_id == -1)
				{
					perror("ERR_HERCULES_GET_SM_IDENTIFIER");
					// Do not stop the process.
				}
				else
				{
					void *pool_memory = createSM(shm_data_id);
					if (pool_memory == NULL)
					{ // error creating the shared memory region.
						perror("HERCULES_ERR_CREATE_SM");
						slog_error("HERCULES_ERR_CREATE_SM");
						ready(tmp_file_path, "ERROR");
						exit(0);
					}
					else
					{
						// Shared memory has been created.
						args.pool_memory = pool_memory;
						// Becasue the shared memory was successfully created, we
						// initializate a semaphore to sincronize block 0.
						sem_shared_memory = sem_open("/hercules_shm_sem", O_CREAT, 0644, 1);
						if (sem_shared_memory == SEM_FAILED)
						{
							perror("HERCULES_ERR_SHM_SEM_OPEN");
							exit(-1);
						}
						// Close the semaphore. The semaphore will remain and can
						// be used by the front-end until unlink is called.
						sem_close(sem_shared_memory);
					}
				}
			}
		}

		// Close the file.
		if (fclose(metadata_nodes_fd) != 0)
		{
			perror("HERCULES_ERR_DEPLOYFILE_CLOSE");
			slog_fatal("HERCULES_ERR_DEPLOYFILE_CLOSE");
			return -1;
		}
	}
	// Metadata server.
	else
	{
		// Create the tree_root node.
		char *root_data = (char *)calloc(8, sizeof(char));
		strcpy(root_data, "imss://");
		tree_root = g_node_new((void *)root_data);

		if (pthread_mutex_init(&tree_mut, NULL) != 0)
		{
			perror("HERCULES_ERR_TREE_MUT_INIT");
			pthread_exit(NULL);
		}
	}

	/***************************************************************/
	/******************** INPROC COMMUNICATIONS ********************/
	/***************************************************************/

	// Map tracking saved records.
	std::shared_ptr<map_records> map(new map_records(buffer_size * KB));

	// copy the reference to a global map.
	g_map = map;

	int64_t data_reserved;
	// Pointer to the allocated buffer memory.
	char *buffer;
	// Size of the buffer involved.
	uint64_t size = (uint64_t)buffer_size * KB;
	// Check if the requested data is available in the current node.
	if ((data_reserved = memalloc(size, &buffer)) == -1)
		return -1;
	// fprintf(stderr, "data_reserved=%lu\n", data_reserved);
	buffer_address = buffer;

	// Metadata bytes written into the buffer.
	uint64_t bytes_written = 0;

	if (args.type == TYPE_METADATA_SERVER)
	{
		// if ((buffer_address = metadata_read(metadata_file, map.get(), buffer, &bytes_written)) == NULL)
		// 	return -1;

		// Obtain the remaining free amount of data reserved to the buffer after the metadata read operation.
		data_reserved -= bytes_written;
	}

	// Buffer segment size assigned to each thread.
	buffer_segment = data_reserved / args.thread_pool;
	slog_info("buffer_segment=%ld", buffer_segment);

	// Initialize pool of threads.
	// pthread_t threads[(args.thread_pool + 1)];
	int extra_threads = 0, total_threads = 0;
	if (args.type == TYPE_DATA_SERVER)
	{
		region_locks = (pthread_mutex_t *)calloc(args.thread_pool, sizeof(pthread_mutex_t));
		extra_threads = 2;
	}
	else
	{
		extra_threads = 1;
	}

	total_threads = args.thread_pool + extra_threads;
	threads = (pthread_t *)malloc(total_threads * sizeof(pthread_t));
	// Thread arguments.
	p_argv arguments[total_threads];

	ucp_worker_threads = (ucp_worker_h *)malloc(args.thread_pool * sizeof(ucp_worker_h));
	local_addr = (ucp_address_t **)malloc(args.thread_pool * sizeof(ucp_address_t *));
	local_addr_len = (size_t *)malloc(args.thread_pool * sizeof(size_t));

	// Execute all threads.
	int32_t aux_idx = 0;
	for (int32_t i = 0; i < total_threads; i++)
	{
		// Add port number to thread arguments.
		arguments[i].ucp_context = ucp_context;
		arguments[i].blocksize = block_size;
		arguments[i].storage_size = max_storage_size;
		arguments[i].port = bind_port;
		arguments[i].tmp_file_path = tmp_file_path;

		// Add the instance URI to the thread arguments.
		strcpy(arguments[i].my_uri, args.imss_uri);
		arguments[i].args = args;

		// Deploy all dispatcher + service threads.
		if (i == 0)
		{
			slog_debug("[SERVER] Creating dispatcher thread.");
			// Deploy a thread distributing incomming clients among all ports.
			if (pthread_create(&threads[i], NULL, dispatcher, (void *)&arguments[i]) == -1)
			{
				// Notify thread error deployment.
				ready(tmp_file_path, "ERROR");
				perror("HERCULES_ERR_DISPATCHER_DEPLOY");
				slog_error("HERCULES_ERR_DISPATCHER_DEPLOY");
				return -1;
			}
		}
		else if (i == 1 && args.type == TYPE_DATA_SERVER)
		{
			// TO FIX: This thread must be running only by the data server.
			slog_debug("[SERVER] Creating checkpoint thread.");
			// Add the reference to the map into the set of thread arguments.
			arguments[i].map = map;
			// if (pthread_create(&threads[i], NULL, Checkpoint, (void *)&arguments[i]) == -1)
			if (pthread_create(&threads[i], NULL, Snapshot, (void *)&arguments[i]) == -1)
			{
				// Notify thread error deployment.
				ready(tmp_file_path, "ERROR");
				perror("HERCULES_ERR_CHECKPOINT_DEPLOY");
				slog_error("HERCULES_ERR_CHECKPOINT_DEPLOY");
				pthread_exit(NULL);
			}
		}
		else
		{
			aux_idx = i - extra_threads;
			ret = init_worker(ucp_context, &ucp_worker_threads[aux_idx]);
			if (ret != 0)
			{
				ready(tmp_file_path, "ERROR");
				perror("HERCULES_ERR_INIT_WORKER_ON_THREAD");
				slog_error("HERCULES_ERR_INIT_WORKER_ON_THREAD");
				return -1;
			}

			arguments[i].ucp_worker = ucp_worker_threads[aux_idx];

			ucp_worker_attr_t worker_attr;
			worker_attr.field_mask = UCP_WORKER_ATTR_FIELD_ADDRESS;
			status = ucp_worker_query(ucp_worker_threads[aux_idx], &worker_attr);
			slog_debug("Setting address=%lu (len=%lu) to local_addr at %d", worker_attr.address, worker_attr.address_length, aux_idx)
			local_addr_len[aux_idx] = worker_attr.address_length;
			local_addr[aux_idx] = worker_attr.address;

			// Add the reference to the map into the set of thread arguments.
			arguments[i].map = map;
			// arguments[i].secondary_map = secondary_map;
			// Specify the address used by each thread to write inside the buffer.
			arguments[i].pt = (char *)(aux_idx * buffer_segment + buffer_address);

			// HERCULES data server.
			if (args.type == TYPE_DATA_SERVER)
			{
				slog_debug("[SERVER] Creating data thread.");
				if (pthread_create(&threads[i], NULL, srv_worker, (void *)&arguments[i]) == -1)
				{
					// Notify thread error deployment.
					perror("HERCULES_ERR_SRV_WORKER_DEPLOY");
					slog_fatal("HERCULES_ERR_SRV_WORKER_DEPLOY");
					return -1;
				}
			}
			// HERCULES Metadata server.
			else
			{
				slog_debug("[SERVER] Creating metadata thread.");
				if (pthread_create(&threads[i], NULL, stat_worker, (void *)&arguments[i]) == -1)
				{
					// Notify thread error deployment.
					perror("HERCULES_ERR_STAT_WORKER_DEPLOY");
					slog_fatal("HERCULES_ERR_STAT_WORKER_DEPLOY");
					return -1;
				}
			}
		}
	}

	// Notify to the metadata server the deployment of a new HERCULES.
	if ((args.type == TYPE_DATA_SERVER) && !args.id && stat_port)
	{
		// Metadata structure containing the novel HERCULES info.
		imss_info my_imss;

		strcpy(my_imss.uri_, args.imss_uri);
		my_imss.ips = (char **)calloc(num_servers, sizeof(char *));
		my_imss.status = (int *)malloc(num_servers * sizeof(int));
		my_imss.arr_num_active_storages = (int *)malloc(num_servers * sizeof(int));
		my_imss.num_storages = num_servers;
		my_imss.num_active_storages = init_number_of_server;
		my_imss.conn_port = bind_port;
		my_imss.type = 'I'; // extremely important
		// FILE entity managing the HERCULES deployfile.
		FILE *svr_nodes;

		if ((svr_nodes = fopen(deployfile, "r+")) == NULL)
		{
			char err_msg[MAX_ERR_MSG_LEN];
			sprintf(err_msg, "HERCULES_ERR_DEPLOYFILE_OPEN:%s", deployfile);
			perror(err_msg);
			slog_fatal("%s", err_msg);
			return -1;
		}

		// Number of characters successfully read from the line.
		int32_t n_chars;
		int init_server_status = 1;
		// int num_active_data_servers = 0;
		for (int32_t i = 0; i < num_servers; i++)
		{
			// Allocate resources in the metadata structure so as to store the current HERCULES's IP.
			(my_imss.ips)[i] = (char *)calloc(LINE_LENGTH, sizeof(char));
			size_t l_size = LINE_LENGTH;

			// Save HERCULES metadata deployment.
			n_chars = getline(&((my_imss.ips)[i]), &l_size, svr_nodes);

			// Erase the new line character ('') from the string.
			if (((my_imss.ips)[i])[n_chars - 1] == '\n')
			{
				((my_imss.ips)[i])[n_chars - 1] = '\0';
			}

			if (i < init_number_of_server)
			{
				// fprintf(stderr, "Server %d is active\n", i);
				init_server_status = 1;
			}
			else
			{
				// fprintf(stderr, "Server %d is inactive\n", i);
				init_server_status = 0;
			}
			my_imss.status[i] = init_server_status;
			my_imss.arr_num_active_storages[i] = init_number_of_server;
		}

		// Close the file.
		if (fclose(svr_nodes) != 0)
		{
			perror("HERCULES_ERR_DEPLOYFILE_CLOSE");
			slog_fatal("HERCULES_ERR_DEPLOYFILE_CLOSE");
			return -1;
		}

		char key_plus_size[REQUEST_SIZE];
		uint32_t id = CLOSE_EP;
		// Send the created structure to the metadata server.
		sprintf(key_plus_size, "%" PRIu32 " SET %lu %s", id, (sizeof(imss_info) + my_imss.num_storages * LINE_LENGTH + my_imss.num_storages * sizeof(int) + my_imss.num_storages * sizeof(int)), my_imss.uri_);
		slog_debug("[main] Request - %s", key_plus_size);
		for (size_t j = 0; j < args.num_metadata_servers; j++)
		{

			if (send_req(ucp_worker, *metadata_endpoints[j], req_addr, req_addr_len, key_plus_size) == 0)
			{
				perror("HERCULES_ERR_SEND_REQ_SET_STR");
				slog_fatal("HERCULES_ERR_SEND_REQ_SET_STR");
				return -1;
			}

			slog_debug("[SERVER] Creating IMSS_INFO at metadata server. ");
			// Send the new HERCULES metadata structure to the metadata server entity.
			if (send_dynamic_stream(ucp_worker, *metadata_endpoints[j], (char *)&my_imss, IMSS_INFO, attr.worker_uid) == -1)
			{
				return -1;
			}
		}

		for (int32_t i = 0; i < num_servers; i++)
			free(my_imss.ips[i]);
		free(my_imss.ips);
	}

	if (args.type == TYPE_DATA_SERVER)
	{
		// Init an endpoint to the metadata server, it is use
		// to notify to the metadata server the status of this server.
		// e.g., in malleability scenarios, this servers send a
		// request to change the metadata to status = 0 (not avaiable).
		// fix: set real number of metadata servers.
		// fprintf(stderr, "Connecting to metadata server\n");
		slog_debug("Connecting to metadata server\n");
		if (stat_init(args.meta_hostfile, stat_port, args.num_metadata_servers, args.id) == -1)
		{
			// In case of error notify and exit
			slog_error("Stat init failed, cannot connect to Metadata server.");
			perror("Stat init failed, cannot connect to Metadata server.");
			return -1;
		}

		sleep(10);
		int num_active_storages = 0;
		while (true)
		{
			num_active_storages = open_imss(args.imss_uri);
			// fprintf(stderr, "Hercules = %s\n", curr_imss.info.uri_);
			if (num_active_storages < 0)
			{
				// slog_fatal("Error creating HERCULES's resources, the process cannot be started");
				// printf("Error creating HERCULES's resources, the process cannot be started. Please, make sure servers are running and clients can establish connections.\n");
				// return -1;
				sleep(3);
			}
			break;
		}
	}

	ret = ready(tmp_file_path, "OK");
	fprintf(stderr, "Server %d is ready = %d\n", args.id, ret);
	// Wait for threads to finish.
	for (int32_t i = 0; i < total_threads; i++)
	{
		// final deployment time.
		t = clock() - t;
		time_taken = ((double)t) / (CLOCKS_PER_SEC);

		if (pthread_join(threads[i], NULL) != 0)
		{
			perror("HERCULES_ERR_SERVER_THREAD_JOIN");
			return -1;
		}
		fprintf(stderr, "Server %d, ending thread %d/%d\n", args.id, i + 1, total_threads);
		// fprintf(stderr,"Ending %c server %d\n", args.type, args.id);
		unlink(tmp_file_path);
	}

	// Write the metadata structures retrieved by the metadata server threads.
	if (args.type == TYPE_METADATA_SERVER)
	{
		// save metadata info in disk.
		// if (metadata_write(metadata_file, buffer, map.get(), arguments, buffer_segment, bytes_written) == -1)
		// 	return -1;

		// Send a message to all data servers to shutdown.

		// Freeing all resources of the tree structure.
		g_node_traverse(tree_root, G_PRE_ORDER, G_TRAVERSE_ALL, -1, gnodetraverse, NULL);

		if (pthread_mutex_destroy(&tree_mut) != 0)
		{
			perror("HERCULES_ERR_TREE_MUT_DESTROY");
			pthread_exit(NULL);
		}
		// fprintf(stderr, "Ending metadata server.\n");
	}
	else
	{
		// Destroy the shared memory segment.
		freeSM(shm_data_id);
		// Remove the named semaphore.
		sem_unlink("/hercules_shm_sem");

		free(region_locks);
	}

	// Close publisher socket.
	// ep_close(ucp_worker, pub_ep, UCP_EP_CLOSE_MODE_FORCE);
	// ep_close(ucp_worker, metadata_endpoints, UCP_EP_CLOSE_MODE_FORCE);
	// ucp_cleanup(ucp_context);

	// sprintf(tmp_file_path, "%s/tmp/%c-hercules-%d-stop", args.hercules_path, args.type, args.id);
	// ready(tmp_file_path, "OK");

	// Free the publisher release address.
	fprintf(stderr, "Ending %c server\n", args.type);

	// Free the memory buffer.
	free(buffer);
	return 0;
}
