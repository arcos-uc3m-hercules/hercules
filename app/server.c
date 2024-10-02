#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/signal.h>
// #include "imss.h"
#include "metadata_stat.h"
// #include "comms.h"
// #include "workers.h"
#include "memalloc.h"
#include "directory.h"
#include "records.hpp"
#include "arg_parser.h"
#include "map_ep.hpp"
#include "cfg_parse.h"
#include <inttypes.h>
#include <unistd.h>
// #include <fcntl.h>
// #include <disk.h>

// Pointer to the tree's root node.
extern GNode *tree_root;
extern pthread_mutex_t tree_mut;

// Initial buffer address.
extern char *buffer_address;
// Set of locks dealing with the memory buffer access.
extern pthread_mutex_t *region_locks;
// Segment size (amount of memory assigned to each thread).
extern uint64_t buffer_segment;
char *POLICY = NULL;

extern ucp_worker_h *ucp_worker_threads;
extern ucp_address_t **local_addr;
extern size_t *local_addr_len;
extern StsHeader *mem_pool;

// URI of the created IMSS.
char *imss_uri;
char META_HOSTFILE[512];

struct arguments args;
std::shared_ptr<map_records> g_map;

/* UCP objects */
ucp_context_h ucp_context;
ucp_worker_h ucp_worker;

// ucp_ep_h pub_ep;
ucp_address_t *req_addr;
ucp_ep_h client_ep;
size_t req_addr_len;

// void *map_ep;		   // map_ep used for async write; server doesn't use it
// int32_t is_client = 0; // also used for async write

int32_t IMSS_DEBUG_FILE = 0;
int IMSS_DEBUG_LEVEL = SLOG_FATAL;
int32_t IMSS_DEBUG_SCREEN = 0;
int IMSS_THREAD_POOL = 1;
int32_t MALLEABILITY = 0;
unsigned long number_active_storage_servers = 0; // stores the current number of active storage servers.
pthread_t *threads;

// global variables usted to finish threads.
extern int global_finish_threads;
extern int global_server_fd_thread;

// #define SHM_SIZE 20L * 1024L * 1024L * 1024L

#define RAM_STORAGE_USE_PCT 0.75f // percentage of free system RAM to be used for storage

/**
 * @brief Read the file "hercules_num_act_nodes" from disk, which contains
 * the current number of active data nodes.
 * @return Current number of active data nodes, on error -1 is returned.
 */
int get_number_of_active_nodes()
{
	char buf[10];
	// Open the "hercules_num_act_nodes" file. This file should be created by the
	// user application or the malleability manager.
	int fd = open("./hercules_num_act_nodes", O_RDONLY);
	if (fd == -1)
	{
		perror("ERR_HERCULES_OPEN_NUM_ACTVIES_NODES");
		return -1;
	}
	// Read the content.
	int ret = read(fd, buf, sizeof(buf) - 1);
	buf[ret] = '\0';

	// In case of error, the number of active storage servers
	// is not updated.
	if (ret == -1)
	{
		perror("ERR_HERCULES_READ_NUM_ACTIVES_NODES");
		ret = close(fd);
		if (fd == -1)
		{
			perror("ERR_HERCULES_CLOSE_NUM_ACTVIES_NODES");
		}
		return -1;
	}
	else
	{
		number_active_storage_servers = atoi(buf);
		fprintf(stderr, "[Wake up server] The new number of active data nodes is %s\n", buf);
		slog_debug("[Wake up server] The new number of active data nodes is %s\n", buf);
	}
	// Close the file.
	ret = close(fd);
	if (fd == -1)
	{
		perror("ERR_HERCULES_CLOSE_NUM_ACTVIES_NODES");
	}
	return number_active_storage_servers;
}

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
	open_imss(imss_uri); // open_imss_temp(imss_uri, number_active_storage_servers);
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

	// fprintf(stderr, "Server %d, has %d blocks, active storage servers=%lu, UCX_IB_RCACHE_MAX_REGIONS=%s\n", args.id, map->size(), number_active_storage_servers, getenv("UCX_IB_RCACHE_MAX_REGIONS"));
	// fprintf(stderr, "Server %d, has %d blocks, active storage servers=%lu\n", args.id, map->size(), number_active_storage_servers);
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
	number_active_storage_servers = get_number_of_active_nodes();

	if (number_active_storage_servers < 0)
	{
		return -1;
	}

	// Tell metadata server to reduce number of servers.
	char key_plus_size[REQUEST_SIZE];
	// Send the created structure to the metadata server.
	// last "0" is the server status to be set.
	sprintf(key_plus_size, "%d SET %lu %s %d", args.id, number_active_storage_servers, imss_uri, 0);
	slog_debug("[main] Request - %s", key_plus_size);
	if (send_req(ucp_worker, client_ep, req_addr, req_addr_len, key_plus_size) == 0)
	{
		perror("ERR_HERCULES_STOP_SERVER_SEND_REQ");
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
	number_active_storage_servers = get_number_of_active_nodes();

	if (number_active_storage_servers < 0)
	{
		return -1;
	}

	// Tell metadata server to update (increase) the number of servers.
	char key_plus_size[REQUEST_SIZE];
	// Send the created structure to the metadata server.
	// last "1" is the server status to be set.
	sprintf(key_plus_size, "%d SET %lu %s %d", args.id, number_active_storage_servers, imss_uri, 1);
	// fprintf(stderr, "Request - %s\n", key_plus_size);
	slog_debug("[main] Request - %s", key_plus_size);
	if (send_req(ucp_worker, client_ep, req_addr, req_addr_len, key_plus_size) == 0)
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
		char buf[10], action[20];
		;
		// Get the operation number.
		int fd = open("/tmp/hercules_pkill_operation", O_RDONLY);
		if (fd == -1)
		{
			perror("ERR_HERCULES_OPEN_PKILL_OPERATION");
			return;
		}

		ret = read(fd, buf, sizeof(buf) - 1);
		buf[ret] = '\0';
		// In case of read error, pkill_operation must be 0
		// to suspend the server but not shutdown it.
		if (ret == -1)
		{
			pkill_operation = 0;
			perror("ERR_HERCULES_READ_PKILL_OPERATION");
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
		switch (pkill_operation)
		{
		case 1: // finish data server processes (shutdown).
			// "global_finish_threads" is a gloabl variable readed by the
			// dispatcher and workers threads. 1 indicates those threads
			// must finish their execution.
			global_finish_threads = 1;
			sprintf(action, "stop");

			// Shutdown or close the socket used by the dispatcher pointed
			// by the file descriptor "global_server_fd_thread".
			if (shutdown(global_server_fd_thread, SHUT_RD) == -1)
			{
				perror("ERR_HERCULES_SHUTDOWN_SERVER_FD\n");
			}
			break;
		default: // suspend the data server.
			sprintf(action, "down");
			// Data servers processes will still running to be reused on
			// the future. On shrink process, this server won't be used,
			// but backend processes will be still running.
			break;
		}

		// Data servers performs malleability operations if it is enabled.
		if (args.type == TYPE_DATA_SERVER && MALLEABILITY == 1)
		{
			ret = stop_server();
			if (ret == 0) // success.
			{
				ret = move_blocks_2_server(args.stat_port, args.id, imss_uri, g_map);
				if (ret < 0) // error.
				{
					// TODO: if "move_blocks_2_server" fails, try again?
				}
			}
		}
		// This file is readed by the hercules script to know if this server
		// was correctly shutting down.
		char tmp_file_path[100];
		sprintf(tmp_file_path, "/tmp/%c-hercules-%d-%s", args.type, args.id, action);
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
			char tmp_file_path[100];
			sprintf(tmp_file_path, "/tmp/%c-hercules-%d-up", args.type, args.id);
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

	// Print off a hello world message
	struct cfg_struct *cfg;
	// clock_t t;
	double time_taken;
	int init_number_of_server = 1;

	uint64_t bind_port;
	char *stat_add;
	// char *metadata_file;
	char *deployfile;
	int64_t buffer_size, stat_port, num_servers;
	void *socket;
	ucp_ep_params_t ep_params;
	ucp_address_t *peer_addr;
	size_t addr_len;

	// memory pool stuff
	uint64_t block_size;   // In KB
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
	int shm_data_id;

	char tmp_file_path[100];

	char *conf_path;
	char abs_exe_path[1024];
	char *aux;

	// t = clock();
	time_t t = time(NULL);

	/***************************************************************/
	/******************* PARSE FILE ARGUMENTS **********************/
	/***************************************************************/

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

	sprintf(tmp_file_path, "/tmp/%c-hercules-%d-start", args.type, args.id);

	cfg = cfg_init();
	conf_path = getenv("HERCULES_CONF");
	if (conf_path != NULL)
	{
		fprintf(stderr, "Loading %s\n", conf_path);
		ret = cfg_load(cfg, conf_path);
		if (ret)
		{
			fprintf(stderr, "%s has not been loaded\n", conf_path);
		}
	}
	else
	{
		ret = 1;
	}

	if (ret)
	{
		char default_paths[3][PATH_MAX] = {
			"/etc/hercules.conf",
			"./hercules.conf",
			"hercules.conf"};

		for (size_t i = 0; i < 3; i++)
		{
			// fprintf(stderr, "Loading %s\n", default_paths[i]);
			if (cfg_load(cfg, default_paths[i]) == 0)
			{
				ret = 0;
				break;
			}
		}
		if (ret)
		{
			if (getcwd(abs_exe_path, sizeof(abs_exe_path)) != NULL)
			{
				conf_path = (char *)malloc(sizeof(char) * PATH_MAX);
				sprintf(conf_path, "%s/%s", abs_exe_path, "../conf/hercules.conf");
				if (cfg_load(cfg, conf_path) == 0)
				{
					ret = 0;
				}
			}
		}

		if (!ret)
		{
			// fprintf(stderr, "Configuration file loaded: %s\n", conf_path);
		}
		else
		{
			fprintf(stderr, "Configuration file not found\n");
			perror("ERR_HERCULES_CONF_NOT_FOUND");
			return -1;
		}
		free(conf_path);
	}
	else
	{
		// fprintf(stderr, "Configuration file loaded: %s\n", conf_path);
	}

	if (cfg_get(cfg, "URI"))
	{
		aux = cfg_get(cfg, "URI");
		strcpy(args.imss_uri, aux);
	}

	if (cfg_get(cfg, "BLOCK_SIZE"))
		args.block_size = atoi(cfg_get(cfg, "BLOCK_SIZE")); // block size in kilobytes.

	if (args.type == TYPE_DATA_SERVER)
	{
		if (cfg_get(cfg, "NUM_DATA_SERVERS"))
			args.num_servers = atoi(cfg_get(cfg, "NUM_DATA_SERVERS"));
	}

	if (cfg_get(cfg, "MALLEABILITY"))
		MALLEABILITY = atoi(cfg_get(cfg, "MALLEABILITY"));

	if (MALLEABILITY != 0 && MALLEABILITY != 1)
	{
		perror("ERR_HERCULES_BAD_MALLEABILITY_OPTION");
	}

	if (cfg_get(cfg, "THREAD_POOL"))
		args.thread_pool = atoi(cfg_get(cfg, "THREAD_POOL"));

	if (cfg_get(cfg, "STORAGE_SIZE"))
		args.storage_size = atoi(cfg_get(cfg, "STORAGE_SIZE")); // in giga bytes.

	if (cfg_get(cfg, "METADA_PERSISTENCE_FILE"))
	{
		aux = cfg_get(cfg, "METADA_PERSISTENCE_FILE");
		strcpy(args.stat_logfile, aux);
	}

	if (cfg_get(cfg, "METADATA_PORT"))
		args.stat_port = atol(cfg_get(cfg, "METADATA_PORT"));

	if (cfg_get(cfg, "METADATA_HOSTFILE"))
	{
		aux = cfg_get(cfg, "METADATA_HOSTFILE");
		strcpy(META_HOSTFILE, aux);
	}

	if (cfg_get(cfg, "DATA_PORT"))
		args.port = atol(cfg_get(cfg, "DATA_PORT"));

	if (cfg_get(cfg, "DATA_HOSTFILE"))
	{
		aux = cfg_get(cfg, "DATA_HOSTFILE");
		strcpy(args.deploy_hostfile, aux);
	}

	if (cfg_get(cfg, "POLICY"))
		POLICY = cfg_get(cfg, "POLICY");
	else
	{
		fprintf(stderr, "Distributiin Policy has not been stablish. \n Please, add the following line in your configuration file POLICY = RR\n");
		perror("ERR_HERCULES_POLICY_NOT_FOUND");
		return -1;
	}

	if (getenv("HERCULES_DEBUG_LEVEL") != NULL)
	{
		aux = getenv("HERCULES_DEBUG_LEVEL");
	}
	else if (cfg_get(cfg, "DEBUG_LEVEL"))
	{
		aux = cfg_get(cfg, "DEBUG_LEVEL");
	}
	else
	{
		aux = NULL;
	}

	if (aux != NULL)
	{
		if (strstr(aux, "file"))
		{
			IMSS_DEBUG_FILE = 1;
			IMSS_DEBUG_SCREEN = 0;
			IMSS_DEBUG_LEVEL = SLOG_LIVE;
		}
		else if (strstr(aux, "stdout"))
			IMSS_DEBUG_SCREEN = 1;
		else if (strstr(aux, "debug"))
			IMSS_DEBUG_LEVEL = SLOG_DEBUG;
		else if (strstr(aux, "live"))
			IMSS_DEBUG_LEVEL = SLOG_LIVE;
		else if (strstr(aux, "all"))
		{
			IMSS_DEBUG_FILE = 1;
			IMSS_DEBUG_SCREEN = 1;
			IMSS_DEBUG_LEVEL = SLOG_PANIC;
		}
		else if (strstr(aux, "none"))
		{
			IMSS_DEBUG_FILE = 0;
			IMSS_DEBUG_SCREEN = 0;
			IMSS_DEBUG_LEVEL = SLOG_NONE;
			unsetenv("IMSS_DEBUG");
		}
		else
		{
			IMSS_DEBUG_FILE = 1;
			IMSS_DEBUG_LEVEL = getLevel(aux);
		}
	}

	/***************************************************************/
	/******************** PARSE INPUT ARGUMENTS ********************/
	/***************************************************************/

	if (getenv("IMSS_THREAD_POOL") != NULL)
	{
		args.thread_pool = atol(getenv("IMSS_THREAD_POOL"));
	}

	IMSS_THREAD_POOL = args.thread_pool;

	char log_path[1000];
	slog_debug("Server type=%c\n", args.type);
	struct tm tm = *localtime(&t);
	sprintf(log_path, "./%c-server-%d.%02d-%02d-%02d", args.type, args.id, tm.tm_hour, tm.tm_min, tm.tm_sec);
	// sprintf(log_path, "./%c-server", args.type);
	slog_init(log_path, IMSS_DEBUG_LEVEL, IMSS_DEBUG_FILE, IMSS_DEBUG_SCREEN, 1, 1, 1, args.id);

	if (IMSS_DEBUG_FILE > 0)
	{
		printf("Log path = %s\n", log_path);
		fflush(stdout);
	}
	// fprintf(stderr, "IMSS DEBUG FILE AT %s\n", log_path);
	slog_info(",Time(msec), Comment, RetCode");

	slog_debug("[SERVER] Starting server.");
	if (args.type == TYPE_DATA_SERVER)
	{
		args.stat_host = argv[3];
		slog_debug("imss_uri = %s stat-host = %s stat-port = %" PRId64 " num-servers = %" PRId64 " deploy-hostfile = %s block-size = %" PRIu64 " (kB) storage-size = %" PRIu64 " (gB, errno=%d:%s", args.imss_uri, args.stat_host, args.stat_port, args.num_servers, args.deploy_hostfile, args.block_size, args.storage_size, errno, strerror(errno));
		// bind port number.
		bind_port = args.port;

		init_number_of_server = atoi(argv[4]);
		// init_server_status = atoi(argv[4]);
		// switch (init_server_status)
		// {
		// case 0:
		// 	break;
		// case 1:
		// 	break;
		// default:
		// 	fprintf(stderr, "%d is not a valid server initial status \n usage hercules_server <m|d> <server_id> <metadata_host> <initial_server_status> \n <initial_server_status> = 0|1\n", init_server_status);
		// 	return 0;
		// }
	}
	else
	{
		slog_debug("[CLI PARAMS] type = %c port = %" PRId64 " bufsize = %" PRId64 "", args.type, args.stat_port, args.bufsize);
		// slog_debug("stat-logfile = %s", args.stat_logfile);
		// bind port number.
		bind_port = args.stat_port;
	}

	// status = ucp_config_read(NULL, NULL, &config);

	// buffer size provided
	buffer_size = args.bufsize;
	// set up imss uri (default value is already set up in args)
	imss_uri = (char *)calloc(32, sizeof(char));

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
	if (max_storage_size >= max_system_ram_allowed || max_storage_size <= 0)
	{
		max_storage_size = max_system_ram_allowed;
	}

	// init memory pool
	slog_info("[main] before sts queue create");
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
	 In relation to the type argument provided, an IMSS or a metadata server will be deployed. */

	// IMSS server.
	if (args.type == TYPE_DATA_SERVER)
	{
		// IMSS name.
		strcpy(imss_uri, args.imss_uri);
		// machine name where the metadata server is being executed.
		stat_add = args.stat_host;
		// port that the metadata server is listening on.
		stat_port = args.stat_port;
		// number of servers conforming the HERCULES deployment.
		num_servers = args.num_servers;
		// Dinamic number of servers conforming the HERCULES deployment used by malleability.
		number_active_storage_servers = num_servers;
		// IMSS' MPI deployment file.
		deployfile = args.deploy_hostfile;
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

		// fprintf(stderr, "Establishing a connection with %s:%ld\n", stat_add, stat_port);
		slog_debug("Establishing a connection with %s:%ld\n", stat_add, stat_port);

		oob_sock = connect_common(stat_add, stat_port, AF_INET);

		char request[REQUEST_SIZE];
		sprintf(request, "%" PRIu32 " GET %s", id, "MAIN!QUERRY");
		slog_debug("[main] Request - %s, errno=%d:%s", request, errno, strerror(errno));
		if (send(oob_sock, request, REQUEST_SIZE, 0) < 0)
		{
			perror("HERCULES_ERR_STAT_HELLO");
			slog_error("HERCULES_ERR_STAT_HELLO");
			return -1;
		}

		ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
		peer_addr = (ucp_address *)malloc(addr_len);
		ret = recv(oob_sock, peer_addr, addr_len, MSG_WAITALL);
		close(oob_sock);

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

		status = ucp_ep_create(ucp_worker, &ep_params, &client_ep);

		// status = ucp_worker_get_address(ucp_worker, &req_addr, &req_addr_len);

		ucp_worker_attr_t worker_attr;
		worker_attr.field_mask = UCP_WORKER_ATTR_FIELD_ADDRESS;
		status = ucp_worker_query(ucp_worker, &worker_attr);
		// printf ("Len %ld \n", worker_attr.address_length);
		req_addr_len = worker_attr.address_length;
		req_addr = worker_attr.address;

		attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
		ucp_worker_address_query(req_addr, &attr);
		slog_debug("[srv_worker_thread] Server UID %" PRIu64 ".", attr.worker_uid);

		if (!args.id)
		{
			// Formated imss uri to be sent to the metadata server.
			char formated_uri[REQUEST_SIZE];
			sprintf(formated_uri, "%" PRIu32 " GET 0 %s", id, imss_uri);
			slog_debug("[main] Request - %s, errno=%d:%s", formated_uri, errno, strerror(errno));
			// Send the request.
			if (send_req(ucp_worker, client_ep, req_addr, req_addr_len, formated_uri) == 0)
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
			// void *data = (void *)malloc(length);
			imss_info imss_info_ = *(imss_info *)malloc(sizeof(imss_info) * length);
			// memcpy(&imss_info_, data, sizeof(imss_info));
			// free(data);
			ret = recv_dynamic_stream(ucp_worker, client_ep, &imss_info_, BUFFER, attr.worker_uid, length);
			// ret = recv_dynamic_stream_opt(ucp_worker, client_ep, &data, BUFFER, attr.worker_uid, length);

			// fprintf(stderr, "Server %d, ret=%d, sizeof(imss_info)=%ld\n", args.id, ret, sizeof(imss_info));
			// if (ret > sizeof(imss_info))
			if (ret != -1)
			{
				// fprintf(stderr,"ret > imss_info\n");
				imss_exists = 1;
				for (int32_t i = 0; i < imss_info_.num_storages; i++)
					free(imss_info_.ips[i]);
				free(imss_info_.ips);
			}
		}

		if (imss_exists)
		{
			slog_error("HERCULES_ERR_SERVER_URITAKEN, ret=%d", ret);
			perror("HERCULES_ERR_SERVER_URITAKEN");
			ready(tmp_file_path, "ERROR");
			return 0;
		}

		// When LOCAL policy is used, the server creates a shared memory region.
		if (!strcmp(POLICY, "LOCAL"))
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
					perror("ERR_HERCULES_CREATE_SM");
					// Do not stop the process.
				}
				else
				{
					// Shared memory has been created, we unlink the segment becuase
					// this process won't use the shared memory, it is used by the front-end.
					unlinkSM(pool_memory);
					// Becasue the shared memory was successfully created, we
					// initializate a semaphore to sincronize block 0.
					sem_shared_memory = sem_open("/hercules_shm_sem", O_CREAT, 0644, 1);
					if (sem_shared_memory == SEM_FAILED)
					{
						perror("HERCULES_ERR_SHM_SEM_OPEN");
						exit(-1);
					}
					// Close the semaphore. The semaphore will remain and can be used by
					// the front-end until unlink is called.
					sem_close(sem_shared_memory);
				}
			}
		}
	}
	// Metadata server.
	else
	{
		// metadata file.
		// metadata_file = args.stat_logfile;

		// Create the tree_root node.
		char *root_data = (char *)calloc(8, sizeof(char));
		strcpy(root_data, "imss://");
		tree_root = g_node_new((void *)root_data);

		if (pthread_mutex_init(&tree_mut, NULL) != 0)
		{
			perror("ERR_HERCULES_TREEMUT_INIT");
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
	threads = (pthread_t *)malloc((args.thread_pool + 1) * sizeof(pthread_t));
	// Thread arguments.
	p_argv arguments[(args.thread_pool + 1)];

	if (args.type == TYPE_DATA_SERVER)
		region_locks = (pthread_mutex_t *)calloc(args.thread_pool, sizeof(pthread_mutex_t));

	ucp_worker_threads = (ucp_worker_h *)malloc((args.thread_pool + 1) * sizeof(ucp_worker_h));
	local_addr = (ucp_address_t **)malloc((args.thread_pool + 1) * sizeof(ucp_address_t *));
	local_addr_len = (size_t *)malloc((args.thread_pool + 1) * sizeof(size_t));

	// Execute all threads.
	for (int32_t i = 0; i < (args.thread_pool + 1); i++)
	{
		// Add port number to thread arguments.
		arguments[i].ucp_context = ucp_context;
		arguments[i].blocksize = block_size;
		arguments[i].storage_size = max_storage_size;
		arguments[i].port = bind_port;
		arguments[i].tmp_file_path = tmp_file_path;

		// Add the instance URI to the thread arguments.
		strcpy(arguments[i].my_uri, imss_uri);

		// Deploy all dispatcher + service threads.
		if (i == 0)
		{
			slog_debug("[SERVER] Creating dispatcher thread.");
			// Deploy a thread distributing incomming clients among all ports.
			if (pthread_create(&threads[i], NULL, dispatcher, (void *)&arguments[i]) == -1)
			{
				// Notify thread error deployment.
				ready(tmp_file_path, "ERROR");
				perror("ERR_HERCULES_DISPATCHER_DEPLOY");
				return -1;
			}
		}
		else
		{
			ret = init_worker(ucp_context, &ucp_worker_threads[i]);
			if (ret != 0)
			{
				return -1;
			}

			arguments[i].ucp_worker = ucp_worker_threads[i];

			// status = ucp_worker_get_address(ucp_worker_threads[i], &local_addr[i], &local_addr_len[i]);
			ucp_worker_attr_t worker_attr;
			worker_attr.field_mask = UCP_WORKER_ATTR_FIELD_ADDRESS;
			status = ucp_worker_query(ucp_worker_threads[i], &worker_attr);
			local_addr_len[i] = worker_attr.address_length;
			local_addr[i] = worker_attr.address;

			// Add the reference to the map into the set of thread arguments.
			arguments[i].map = map;
			// Specify the address used by each thread to write inside the buffer.
			arguments[i].pt = (char *)((i - 1) * buffer_segment + buffer_address);

			// IMSS server.
			if (args.type == TYPE_DATA_SERVER)
			{
				slog_debug("[SERVER] Creating data thread.");
				if (pthread_create(&threads[i], NULL, srv_worker, (void *)&arguments[i]) == -1)
				{
					// Notify thread error deployment.
					perror("ERRHERCULES__SRVWORKER_DEPLOY");
					slog_fatal("ERRHERCULES__SRVWORKER_DEPLOY");
					return -1;
				}
			}
			// Metadata server.
			else
			{
				slog_debug("[SERVER] Creating metadata thread.");
				if (pthread_create(&threads[i], NULL, stat_worker, (void *)&arguments[i]) == -1)
				{
					// Notify thread error deployment.
					perror("ERRIMSS_STATWORKER_DEPLOY");
					slog_fatal("ERRIMSS_STATWORKER_DEPLOY");
					return -1;
				}
			}
		}
	}

	// Notify to the metadata server the deployment of a new IMSS.
	if ((args.type == TYPE_DATA_SERVER) && !args.id && stat_port)
	{
		// Metadata structure containing the novel IMSS info.
		imss_info my_imss;

		strcpy(my_imss.uri_, imss_uri);
		my_imss.ips = (char **)calloc(num_servers, sizeof(char *));
		my_imss.status = (int *)malloc(num_servers * sizeof(int));					// calloc(num_servers, sizeof(int32_t));
		my_imss.arr_num_active_storages = (int *)malloc(num_servers * sizeof(int)); // calloc(num_servers, sizeof(int32_t));
		my_imss.num_storages = num_servers;
		my_imss.num_active_storages = init_number_of_server;
		my_imss.conn_port = bind_port;
		my_imss.type = 'I'; // extremely important
		// FILE entity managing the IMSS deployfile.
		FILE *svr_nodes;

		if ((svr_nodes = fopen(deployfile, "r+")) == NULL)
		{
			char err_msg[512];
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
			// Allocate resources in the metadata structure so as to store the current IMSS's IP.
			(my_imss.ips)[i] = (char *)calloc(LINE_LENGTH, sizeof(char));
			size_t l_size = LINE_LENGTH;

			// Save IMSS metadata deployment.
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
			// if (init_server_status)
			// {
			// 	fprintf(stderr, "Server %d is active\n", args.id);
			// }
			// else
			// {
			// 	fprintf(stderr, "Server %d is idle\n", args.id);
			// }
			my_imss.status[i] = init_server_status;
			my_imss.arr_num_active_storages[i] = init_number_of_server;
			// fprintf(stderr,"status=%d\n", my_imss.status[i]);
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
		// status = ucp_ep_create(ucp_worker, &ep_params, &client_ep);
		slog_debug("[main] Request - %s, errno=%d:%s", key_plus_size, errno, strerror(errno));
		if (send_req(ucp_worker, client_ep, req_addr, req_addr_len, key_plus_size) == 0)
		{
			perror("ERR_HERCULES_RLSIMSS_SENDADDR");
			return -1;
		}

		slog_debug("[SERVER] Creating IMSS_INFO at metadata server. ");
		// Send the new IMSS metadata structure to the metadata server entity.
		if (send_dynamic_stream(ucp_worker, client_ep, (char *)&my_imss, IMSS_INFO, attr.worker_uid) == -1)
			return -1;

		for (int32_t i = 0; i < num_servers; i++)
			free(my_imss.ips[i]);
		free(my_imss.ips);

		// ucp_ep_close_nb(client_ep, UCP_EP_CLOSE_MODE_FORCE);
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
		if (stat_init(META_HOSTFILE, stat_port, 1, args.id) == -1)
		{
			// In case of error notify and exit
			slog_error("Stat init failed, cannot connect to Metadata server.");
			perror("Stat init failed, cannot connect to Metadata server.");
			return -1;
		}
	}

	// Wait for threads to finish.
	for (int32_t i = 0; i < (args.thread_pool + 1); i++)
	{
		// final deployment time.
		t = clock() - t;
		time_taken = ((double)t) / (CLOCKS_PER_SEC);

		ready(tmp_file_path, "OK");
		fprintf(stderr, "Server %d is ready\n", args.id);
		if (pthread_join(threads[i], NULL) != 0)
		{
			perror("ERR_HERCULES_SERVER_THREAD_JOIN");
			return -1;
		}
		// fprintf(stderr,"Ending %c server %d\n", args.type, args.id);
		unlink(tmp_file_path);
	}

	// Write the metadata structures retrieved by the metadata server threads.
	if (args.type == TYPE_METADATA_SERVER)
	{
		// save metadata info in disk.
		// if (metadata_write(metadata_file, buffer, map.get(), arguments, buffer_segment, bytes_written) == -1)
		// 	return -1;

		// free(imss_uri);

		// Freeing all resources of the tree structure.
		g_node_traverse(tree_root, G_PRE_ORDER, G_TRAVERSE_ALL, -1, gnodetraverse, NULL);

		if (pthread_mutex_destroy(&tree_mut) != 0)
		{
			perror("ERRIMSS_TREEMUT_DESTROY");
			pthread_exit(NULL);
		}
		// fprintf(stderr, "Ending metadata server.\n");
	}
	else
	{

		// this sleep ensures all others servers have time to tell metadata server
		// they will be shut down, and then this servers has the updated value of the
		// active number of servers.
		// sleep(1);

		// char tmp_file_path[100];
		// sprintf(tmp_file_path, "/tmp/%c-hercules-%d-down", args.type, args.id);

		// stop_server();
		// move_blocks_2_server(args.stat_port, args.id, imss_uri, g_map);
		// // fprintf(stderr, "Writting file %s\n", tmp_file_path);
		// ret = ready(tmp_file_path, "OK");
		// if (ret < 0)
		// {
		// 	return -1;
		// }

		// Destroy the shared memory segment.
		freeSM(shm_data_id);
		// Remove the named semaphore.
		sem_unlink("/hercules_shm_sem");

		free(region_locks);
		// fprintf(stderr, "Ending data server.\n");
	}

	// Close publisher socket.
	// ep_close(ucp_worker, pub_ep, UCP_EP_CLOSE_MODE_FORCE);
	// ep_close(ucp_worker, client_ep, UCP_EP_CLOSE_MODE_FORCE);

	sprintf(tmp_file_path, "/tmp/%c-hercules-%d-stop", args.type, args.id);
	ready(tmp_file_path, "OK");

	// Free the memory buffer.
	free(buffer);
	// Free the publisher release address.
	fprintf(stderr, "Ending %c server\n", args.type);
	return 0;
}
