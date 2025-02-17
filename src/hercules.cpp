#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/signal.h>
#include "metadata_stat.h"
#include "imss.h"
#include "workers.h"
#include "hercules.hpp"
#include "redis.h"
#include "records.hpp"
#include "comms.h"
// #include "hercules.hpp"

/***************************************************************************
*******************************  STRUCTURES  *******************************
***************************************************************************/

// Structure storing configuration info for the IMSS buffer servers.
typedef struct
{

	// Port number that the server will be connecting to.
	int32_t port;

} server_arg;

// Structure storing configuration info for the IMSS metadata servers.
typedef struct
{

	// Port number that the server will be connecting to.
	int32_t port;
	// Memory amount that the process will be requesting to the corresponding node (in GB).
	uint64_t buffer_size;
	// Address of the matadata files' name.
	char metadata_file[512];

} metadata_arg;

/***************************************************************************
****************************  GLOBAL VARIABLES  ****************************
***************************************************************************/

// IMSS server thread.
pthread_t server_th;
// IMSS metadata server thread.
pthread_t metadata_th;
// ZeroMQ context entity conforming all sockets.

// Initial buffer address.
extern char *buffer_address;
// Set of locks dealing with the memory buffer access.
extern pthread_mutex_t *region_locks;
// Segment size (amount of memory assigned to each thread).
extern uint64_t buffer_segment;

// Memory amount (in GB) assigned to the buffer process.
extern uint64_t buffer_KB;
// Flag stating that the previous parameter has been received.
extern int32_t size_received;

// Communication resources with worker threads.
pthread_mutex_t comms_mut;
pthread_cond_t comms_cond;
int32_t not_copied;
extern pthread_mutex_t buff_size_mut;
extern pthread_cond_t buff_size_cond;
extern int32_t copied;

extern pthread_mutex_t hiredis_mut;

// Number of bytes assigned to the IMSS backend storage (in KB).
uint64_t backend_buffer_size;
pthread_mutex_t backend_buff_mut;

// URI of the attached deployment.
extern char att_imss_uri[URI_];

uint16_t connection_port; // FIXME

// Metadata deployment flag.
uint32_t metadata_server_deployed;

/***************************************************************************
*******************************  FUNCTIONS  ********************************
***************************************************************************/

// IMSS server process storing information.
void *imss_server(void *arg_)
{
	server_arg arg;

	// Notify that the arguments have been copied.
	pthread_mutex_lock(&comms_mut);
	memcpy(&arg, arg_, sizeof(server_arg));
	not_copied = 0;
	pthread_cond_signal(&comms_cond);
	pthread_mutex_unlock(&comms_mut);

	copied = 0;

	// Initialize communication resources.
	if (pthread_mutex_init(&buff_size_mut, NULL) != 0)
	{
		perror("HERCULES_ERR_SRVMUT_INIT");
		pthread_exit(NULL);
	}

	if (pthread_cond_init(&buff_size_cond, NULL) != 0)
	{
		perror("HERCULES_ERR_SRVCOND_INIT");
		pthread_exit(NULL);
	}

	// Amount of memory enabled for execution.
	uint64_t data_reserved;

	region_locks = (pthread_mutex_t *)malloc(IMSS_THREAD_POOL * sizeof(pthread_mutex_t));

	// Special independent thread as a garbage collector
	pthread_t thread_garbage_collector;

	// Initialize pool of threads.
	pthread_t threads[(IMSS_THREAD_POOL + 1)];
	// Thread arguments.
	p_argv arguments[(IMSS_THREAD_POOL + 1)];

	// Add port number to thread arguments.
	arguments[0].port = (arg.port)++;

	// Deploy a thread distributing incomming clients among all ports.
	if (pthread_create(&threads[0], NULL, srv_attached_dispatcher, (void *)&arguments[0]) == -1)
	{
		perror("HERCULES_ERR_SRVDISPATCHER_DEPLOY");
		pthread_exit(NULL);
	}

	// Wait until the buffer size is provided.

	pthread_mutex_lock(&buff_size_mut);

	while (!copied)

		pthread_cond_wait(&buff_size_cond, &buff_size_mut);

	pthread_mutex_unlock(&buff_size_mut);

	pthread_mutex_lock(&backend_buff_mut);

	// Check if there is enough space to create a new HERCULES server entity within the backend storage.
	printf("%ld > %ld", buffer_KB, backend_buffer_size);
	if (buffer_KB > backend_buffer_size) // Total storage size include data server . Then must be lagger
	{
		perror("HERCULES_ERR_BUFFTOOBIG. Total storage size overpass when allocating a new HERCULES data server buffer");
		pthread_exit(NULL);
	}
	backend_buffer_size -= buffer_KB;
	// Map tracking saved records.

	pthread_mutex_unlock(&backend_buff_mut);

	data_reserved = buffer_KB * GB;
	std::shared_ptr<map_records> buffer_map(new map_records(data_reserved));
	// buffer_address = (unsigned char *) malloc(sizeof(char)*data_reserved );

	buffer_segment = data_reserved / IMSS_THREAD_POOL;

	for (int32_t i = 1; i < (IMSS_THREAD_POOL + 1); i++)
	{
		arguments[i].port = (arg.port)++;
		// Add the reference to the map into the set of thread arguments.
		arguments[i].map = buffer_map;
		// Specify the address used by each thread to write inside the buffer.
		arguments[i].pt = 0;
		// URI of the corresponding IMSS instance.
		strcpy(arguments[i].my_uri, att_imss_uri);

		// Throw thread with the corresponding function and arguments.
		if (pthread_create(&threads[i], NULL, srv_worker, (void *)&arguments[i]) == -1)
		{
			perror("HERCULES_ERR_SRVWORKER_DEPLOY");
			pthread_exit(NULL);
		}
	}

	if (pthread_create(&thread_garbage_collector, NULL, garbage_collector, (void *)buffer_map.get()) == -1)
	{
		perror("HERCULES_ERR_GARBAGECOLLECTOR_DEPLOY");
		pthread_exit(NULL);
	}

	// Wait for the threads to conclude.
	if (pthread_join(thread_garbage_collector, NULL) != 0)
	{
		perror("HERCULES_ERR_METADISPATCHER_JOIN");
		pthread_exit(NULL);
	}

	// Release communication resources.
	if (pthread_mutex_destroy(&buff_size_mut) != 0)
	{
		perror("HERCULES_ERR_SRVMUT_DESTROY");
		pthread_exit(NULL);
	}

	if (pthread_cond_destroy(&buff_size_cond) != 0)
	{
		perror("HERCULES_ERR_SRVCOND_DESTROY");
		pthread_exit(NULL);
	}

	// Wait for threads to finish.
	for (int32_t i = 0; i < (IMSS_THREAD_POOL + 1); i++)
	{
		if (pthread_join(threads[i], NULL) != 0)
		{
			perror("HERCULES_ERR_SRVTH_JOIN");
			pthread_exit(NULL);
		}
	}

	free(region_locks);
	// Free the memory buffer.
	free(buffer_address);

	pthread_exit(NULL);
}

// IMSS metadata server storing structure information.
void *
imss_metadata(void *arg_)
{
	metadata_arg arg;

	// Notify that the arguments have been copied.
	pthread_mutex_lock(&comms_mut);
	memcpy(&arg, arg_, sizeof(metadata_arg));
	not_copied = 0;
	pthread_cond_signal(&comms_cond);
	pthread_mutex_unlock(&comms_mut);

	// Map tracking metadata saved records.
	std::shared_ptr<map_records> metadata_map(new map_records(arg.buffer_size * GB));
	// Pointer to the allocated metadata buffer memory.
	char *pt_met;

	pthread_mutex_lock(&backend_buff_mut);
	printf("%ld > %ld", buffer_KB, backend_buffer_size);
	if (backend_buffer_size > arg.buffer_size) // Total storage size inclue metadata. Then must be lagger
	{
		perror("HERCULES_ERR_BUFFTOOBIG. Total storage size overpass when allocating a new HERCULES metadata buffer.");
		pthread_exit(NULL);
	}
	backend_buffer_size -= arg.buffer_size;
	pthread_mutex_unlock(&backend_buff_mut);

	int64_t data_reserved = arg.buffer_size * GB;

	pt_met = (char *)malloc(sizeof(char) * data_reserved);

	if (pthread_mutex_init(&hiredis_mut, NULL) != 0)
	{
		perror("HERCULES_ERR_HIREDISMUT_INIT");
		pthread_exit(NULL);
	}

	
	// Address pointing to the end of the last metadata record.
	char *offset = pt_met;

	// Metadata bytes written into the buffer.
	uint64_t bytes_written;

	if ((offset = metadata_read(arg.metadata_file, metadata_map.get(), pt_met, &bytes_written)) == NULL)
		pthread_exit(NULL);

	// Obtain the remaining free amount of data reserved to the buffer after the metadata read operation.
	data_reserved -= bytes_written;

	// Buffer segment size assigned to each thread.
	int64_t buffer_segment_ = data_reserved / IMSS_THREAD_POOL;

	// Initialize pool of threads.
	pthread_t threads[(IMSS_THREAD_POOL + 1)];
	// Thread arguments.
	p_argv arguments[(IMSS_THREAD_POOL + 1)];

	// Execute all threads.
	for (int32_t i = 0; i < (IMSS_THREAD_POOL + 1); i++)
	{
		// Add port number to set of thread arguments.
		arguments[i].port = (arg.port)++;

		// Deploy all dispatcher + service threads.
		if (!i)
		{
			// Deploy a thread distributing incomming clients among all ports.
			if (pthread_create(&threads[i], NULL, dispatcher, (void *)&arguments[i]) == -1)
			{
				perror("HERCULES_ERR_METADISPATCHER_DEPLOY");
				pthread_exit(NULL);
			}
		}
		else
		{
			// Add the reference to the map into the set of thread arguments.
			arguments[i].map = metadata_map;
			// Specify the address used by each thread to write inside the buffer.
			arguments[i].pt = 0;
			// arguments[i].total_size = buffer_segment_;
			// Throw thread with the corresponding function and arguments.
			if (pthread_create(&threads[i], NULL, stat_worker, (void *)&arguments[i]) == -1)
			{
				perror("HERCULES_ERR_METAWORKER_DEPLOY");
				pthread_exit(NULL);
			}
		}
	}

	// Wait for the threads to conclude.
	for (int32_t i = 0; i < (IMSS_THREAD_POOL + 1); i++)
	{
		if (pthread_join(threads[i], NULL) != 0)
		{
			perror("HERCULES_ERR_METATH_JOIN");
			pthread_exit(NULL);
		}
	}

	// Save the current metadata information into a file.
	if (metadata_write(arg.metadata_file, pt_met, metadata_map.get(), arguments, buffer_segment_, bytes_written) == -1)
		pthread_exit(NULL);

	// Freeing all resources of the redis server.
	// redis_close(hiredis_context);

	if (pthread_mutex_destroy(&hiredis_mut) != 0)
	{
		perror("HERCULES_ERR_HIREDIS_DESTROY");
		pthread_exit(NULL);
	}

	free(pt_met);

	pthread_exit(NULL);
}

// Method initializing an instance of the HERCULES in-memory storage system.
int32_t
hercules_init(uint32_t rank,
			  uint64_t backend_strg_size,
			  uint16_t server_port_num,
			  int32_t deploy_metadata,
			  uint16_t metadata_port_num,
			  uint64_t metadata_buff_size,
			  char *metadata_file)
{
	// Save the total number of bytes that were assigned to the backend storage.
	backend_buffer_size = backend_strg_size;
	connection_port = server_port_num; // FIXME
	// Initialize communication resources.
	if (pthread_mutex_init(&comms_mut, NULL) != 0)
	{
		perror("HERCULES_ERR_MUT_INIT");
		return -1;
	}
	if (pthread_cond_init(&comms_cond, NULL) != 0)
	{
		perror("HERCULES_ERR_COND_INIT");
		return -1;
	}

	if (pthread_mutex_init(&backend_buff_mut, NULL) != 0)
	{
		perror("HERCULES_ERR_BACKENDMUT_INIT");
		return -1;
	}

	not_copied = 1;

	// Set of arguments provided to the server.
	server_arg srv_arg;
	srv_arg.port = server_port_num;

	// Deploy the previous thread.
	if (pthread_create(&server_th, NULL, imss_server, (void *)&srv_arg) != 0)
	{
		perror("HERCULES_ERR_SRVTH_CREATE");
		return -1;
	}
	// Wait until the thread copies the provided arguments.
	pthread_mutex_lock(&comms_mut);

	while (not_copied)

		pthread_cond_wait(&comms_cond, &comms_mut);

	pthread_mutex_unlock(&comms_mut);

	not_copied = 1;

	// No metadata server is deployed by default.
	metadata_server_deployed = 0;

	// The rank zero process will be also deploying the metadata server.
	if (deploy_metadata)
	{
		// A metadata server has been deployed in the current process.
		metadata_server_deployed = 1;

		// Set of arguments provided to the metadata server.
		metadata_arg metadata_arg;
		metadata_arg.port = metadata_port_num;
		metadata_arg.buffer_size = metadata_buff_size;
		strcpy(metadata_arg.metadata_file, metadata_file);

		// Deploy the previous thread.
		if (pthread_create(&metadata_th, NULL, imss_metadata, (void *)&metadata_arg) != 0)
		{
			perror("HERCULES_ERR_METATH_CREATE");
			return -1;
		}
		// Wait until the thread copies the provided arguments.

		pthread_mutex_lock(&comms_mut);

		while (not_copied)

			pthread_cond_wait(&comms_cond, &comms_mut);

		pthread_mutex_unlock(&comms_mut);
	}

	// Release communication resources.
	if (pthread_mutex_destroy(&comms_mut) != 0)
	{
		perror("HERCULES_ERR_MUT_DESTROY");
		return -1;
	}

	if (pthread_cond_destroy(&comms_cond) != 0)
	{
		perror("HERCULES_ERR_COND_DESTROY");
		return -1;
	}
	return 0;
}

// Method releasing an instance of the HERCULES in-memory storage system.
int32_t
hercules_release()
{
	// Publish RELEASE message to all worker threads.
	// memcpy(comm_msg_data(&release_rsc), "RELEASE\0", 8);

	// if ( comm_msg_send(&release_rsc, pub, 0)== -1)
	//{
	//	perror("HERCULES_ERR_PUBLISH_RELEASEMSG");
	//	return -1;
	// }

	// Join threads.
	if (metadata_server_deployed)
	{
		if (pthread_join(metadata_th, NULL) != 0)
		{
			perror("HERCULES_ERR_SRVTH_JOIN");
			return -1;
		}
	}

	if (pthread_join(server_th, NULL) != 0)
	{
		perror("HERCULES_ERR_METATH_JOIN");
		return -1;
	}

	// comm_msg_close(&release_rsc);

	// Close publisher socket.
	// if (comm_close(pub) == -1)
	//{
	//	perror("HERCULES_ERR_PUBSOCK_CLOSE");
	//	return -1;
	// }

	// free(pub_dir);

	if (pthread_mutex_destroy(&backend_buff_mut) != 0)
	{
		perror("HERCULES_ERR_BACKENDMUT_DESTROY");
		return -1;
	}

	return 0;
}

int getConfiguration(struct arguments *args)
{
	struct cfg_struct *cfg;

	/***************************************************************/
	/******************* PARSE FILE ARGUMENTS **********************/
	/***************************************************************/
	int ret = 0;

	char *conf_path = NULL;
	char abs_exe_path[PATH_MAX];
	char *aux = NULL;

	cfg = cfg_init();
	conf_path = getenv("HERCULES_CONF");
	if (conf_path != NULL)
	{
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
			// slog_live("Loading %s\n", default_paths[i]);
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
				char rpath[] = "../conf/hercules.conf";
				conf_path = (char *)malloc(sizeof(char) * (PATH_MAX - strlen(rpath) + 1));
				sprintf(conf_path, "%s/%s", abs_exe_path, rpath);
				if (cfg_load(cfg, conf_path) == 0)
				{
					ret = 0;
				}
			}
		}

		if (ret)
		{
			fprintf(stderr, "[HERCULES CLIENT] Configuration file '%s' not found\n", conf_path);
			perror("ERRIMSS_CONF_NOT_FOUND");
			return -1;
		}
		free(conf_path);
	}

	if (getenv("HERCULES_POLICY") != NULL)
		strcpy(args->policy, getenv("HERCULES_POLICY"));
	else if (cfg_get(cfg, "POLICY"))
		strcpy(args->policy, cfg_get(cfg, "POLICY"));
	else
	{
		fprintf(stderr, "Distribution Policy has not been established. \n Please, add the following line in your configuration file POLICY = RR\n");
		perror("HERCULES_ERR_POLICY_NOT_FOUND");
		return -1;
	}

	if (getenv("HERCULES_MOUNT_POINT") != NULL)
		strcpy(args->mount_point, getenv("HERCULES_MOUNT_POINT"));
	else if (cfg_get(cfg, "MOUNT_POINT"))
	{
		strcpy(args->mount_point, cfg_get(cfg, "MOUNT_POINT"));
	}

	if (getenv("HERCULES_URI") != NULL)
		strcpy(args->imss_uri, getenv("HERCULES_URI"));
	else if (cfg_get(cfg, "URI"))
		strcpy(args->imss_uri, cfg_get(cfg, "URI"));

	// block size in kilobytes.
	if (getenv("HERCULES_BLOCK_SIZE") != NULL)
		args->block_size = atol(getenv("HERCULES_BLOCK_SIZE"));
	else if (cfg_get(cfg, "BLOCK_SIZE"))
		args->block_size = atol(cfg_get(cfg, "BLOCK_SIZE"));

	if (getenv("HERCULES_PATH") != NULL)
		strcpy(args->hercules_path, getenv("HERCULES_PATH"));
	else if (cfg_get(cfg, "HERCULES_PATH"))
		strcpy(args->hercules_path, cfg_get(cfg, "HERCULES_PATH"));

	if (getenv("HERCULES_DATA_PORT") != NULL)
		args->data_port = atoi(getenv("HERCULES_DATA_PORT"));
	else if (cfg_get(cfg, "DATA_PORT"))
		args->data_port = atoi(cfg_get(cfg, "DATA_PORT"));

	if (getenv("HERCULES_METADATA_PORT") != NULL)
		args->stat_port = atoi(getenv("HERCULES_METADATA_PORT"));
	else if (cfg_get(cfg, "METADATA_PORT"))
		args->stat_port = atoi(cfg_get(cfg, "METADATA_PORT"));

	if (getenv("HERCULES_NUM_DATA_SERVERS") != NULL)
		args->num_data_servers = atoi(getenv("HERCULES_NUM_DATA_SERVERS"));
	else if (cfg_get(cfg, "NUM_DATA_SERVERS"))
		args->num_data_servers = atoi(cfg_get(cfg, "NUM_DATA_SERVERS"));

	if (getenv("HERCULES_NUM_META_SERVERS") != NULL)
		args->num_metadata_servers = atoi(getenv("HERCULES_NUM_META_SERVERS"));
	else if (cfg_get(cfg, "NUM_META_SERVERS"))
		args->num_metadata_servers = atoi(cfg_get(cfg, "NUM_META_SERVERS"));

	if (getenv("HERCULES_MALLEABILITY") != NULL)
		args->malleability = atoi(getenv("HERCULES_MALLEABILITY"));
	else if (cfg_get(cfg, "MALLEABILITY"))
		args->malleability = atoi(cfg_get(cfg, "MALLEABILITY"));
	else
		args->malleability = 0;

	if (getenv("HERCULES_MALLEABILITY_TYPE") != NULL)
		args->malleability_type = atoi(getenv("HERCULES_MALLEABILITY_TYPE"));
	else if (cfg_get(cfg, "MALLEABILITY_TYPE"))
		args->malleability_type = atoi(cfg_get(cfg, "MALLEABILITY_TYPE"));

	if (getenv("HERCULES_UPPER_BOUND_MALLEABILITY") != NULL)
		args->upper_bound_servers = atoi(getenv("HERCULES_UPPER_BOUND_MALLEABILITY"));
	else if (cfg_get(cfg, "UPPER_BOUND_MALLEABILITY"))
		args->upper_bound_servers = atoi(cfg_get(cfg, "UPPER_BOUND_MALLEABILITY"));

	if (getenv("HERCULES_LOWER_BOUND_MALLEABILITY") != NULL)
		args->lower_bound_servers = atoi(getenv("HERCULES_LOWER_BOUND_MALLEABILITY"));
	else if (cfg_get(cfg, "LOWER_BOUND_MALLEABILITY"))
		args->lower_bound_servers = atoi(cfg_get(cfg, "LOWER_BOUND_MALLEABILITY"));

	if (getenv("HERCULES_REPL_FACTOR") != NULL)
		args->repl_factor = atoi(getenv("HERCULES_REPL_FACTOR"));
	else if (cfg_get(cfg, "REPL_FACTOR"))
		args->repl_factor = atoi(cfg_get(cfg, "REPL_FACTOR"));
	else
		args->repl_factor = NONE;

	if (getenv("HERCULES_REPL_TYPE") != NULL)
		args->repl_type = atoi(getenv("HERCULES_REPL_TYPE"));
	else if (cfg_get(cfg, "REPL_TYPE"))
		args->repl_type = atoi(cfg_get(cfg, "REPL_TYPE"));
	else
		args->repl_type = ASYNC;

	if (getenv("HERCULES_BUFF_SIZE") != NULL)
		args->bufsize = atol(getenv("HERCULES_BUFF_SIZE"));

	if (getenv("HERCULES_METADATA_HOSTFILE") != NULL)
		strcpy(args->meta_hostfile, getenv("HERCULES_METADATA_HOSTFILE"));
	else if (cfg_get(cfg, "METADATA_HOSTFILE"))
		strcpy(args->meta_hostfile, cfg_get(cfg, "METADATA_HOSTFILE"));

	if (getenv("HERCULES_DATA_HOSTFILE") != NULL)
		strcpy(args->data_hostfile, getenv("HERCULES_DATA_HOSTFILE"));
	else if (cfg_get(cfg, "DATA_HOSTFILE"))
		strcpy(args->data_hostfile, cfg_get(cfg, "DATA_HOSTFILE"));

	if (getenv("HERCULES_THREAD_POOL") != NULL)
		args->thread_pool = atol(getenv("HERCULES_THREAD_POOL"));
	else if (cfg_get(cfg, "THREAD_POOL"))
		args->thread_pool = atol(cfg_get(cfg, "THREAD_POOL"));
	else
		args->thread_pool = 1;

	if (getenv("HERCULES_DEBUG_LEVEL") != NULL)
		aux = getenv("HERCULES_DEBUG_LEVEL");
	else if (cfg_get(cfg, "DEBUG_LEVEL"))
		aux = cfg_get(cfg, "DEBUG_LEVEL");
	else
		aux = NULL;

	if (aux != NULL)
	{
		if (strstr(aux, "file"))
		{
			args->logging.hercules_debug_file = 1;
			args->logging.hercules_debug_screen = 0;
			args->logging.hercules_debug_level = SLOG_LIVE;
		}
		else if (strstr(aux, "stdout"))
			args->logging.hercules_debug_screen = 1;
		else if (strstr(aux, "debug"))
			args->logging.hercules_debug_level = SLOG_DEBUG;
		else if (strstr(aux, "live"))
			args->logging.hercules_debug_level = SLOG_LIVE;
		else if (strstr(aux, "all"))
		{
			args->logging.hercules_debug_file = 1;
			args->logging.hercules_debug_screen = 1;
			args->logging.hercules_debug_level = SLOG_PANIC;
		}
		else if (strstr(aux, "none"))
		{
			args->logging.hercules_debug_file = 0;
			args->logging.hercules_debug_screen = 0;
			args->logging.hercules_debug_level = SLOG_NONE;
			unsetenv("IMSS_DEBUG");
		}
		else
		{
			args->logging.hercules_debug_file = 1;
			args->logging.hercules_debug_level = getLevel(aux);
		}
	}

	if (getenv("HERCULES_STORAGE_SIZE") != NULL)
		args->storage_size = atol(getenv("HERCULES_STORAGE_SIZE"));
	else if (cfg_get(cfg, "STORAGE_SIZE"))
		args->storage_size = atol(cfg_get(cfg, "STORAGE_SIZE"));
	else
		args->storage_size = 1;

	if (getenv("HERCULES_CHECKPOINT_PATH") != NULL)
		strcpy(args->hercules_checkpoint_path, getenv("HERCULES_CHECKPOINT_PATH"));
	else if (cfg_get(cfg, "HERCULES_CHECKPOINT_PATH"))
		strcpy(args->hercules_checkpoint_path, cfg_get(cfg, "HERCULES_CHECKPOINT_PATH"));
	else
		args->hercules_checkpoint_path[0] = '\0';

	if (getenv("HERCULES_SNAPSHOT_PATH") != NULL)
		strcpy(args->hercules_snapshot_path, getenv("HERCULES_SNAPSHOT_PATH"));
	else if (cfg_get(cfg, "HERCULES_SNAPSHOT_PATH"))
		strcpy(args->hercules_snapshot_path, cfg_get(cfg, "HERCULES_SNAPSHOT_PATH"));
	else
		args->hercules_snapshot_path[0] = '\0';

	if (getenv("HERCULES_SNAPSHOT_PATHS_LIST") != NULL)
		strcpy(args->snapshot_paths_list, getenv("HERCULES_SNAPSHOT_PATHS_LIST"));
	else if (cfg_get(cfg, "SNAPSHOT_PATHS_LIST"))
		strcpy(args->snapshot_paths_list, cfg_get(cfg, "SNAPSHOT_PATHS_LIST"));
	else
		args->snapshot_paths_list[0] = '\0';

	if (getenv("IGNORE_PATHS_LIST") != NULL)
		strcpy(args->ignore_paths_list, getenv("IGNORE_PATHS_LIST"));
	else if (cfg_get(cfg, "IGNORE_PATHS_LIST"))
		strcpy(args->ignore_paths_list, cfg_get(cfg, "IGNORE_PATHS_LIST"));
	else
		args->ignore_paths_list[0] = '\0';

	cfg_free(cfg);

	// char hostname_[512];
	// int ret = gethostname(&hostname_[0], 512);
	ret = gethostname(&args->data_hostname[0], PATH_MAX);
	if (ret == -1)
	{
		perror("HERCULES_ERR_GETHOSTNAME");
		args->data_hostname[0] = '\0';
	}

	return 1;
}

void getBlockInformation(std::string key, int *block_number, std::string *data_uri, std::string *file_name)
{
	int pos = key.find('$');
	// string block = key.substr(pos, key.length() + 1);
	// string data_uri = key.substr(0, pos);

	pos = key.find('$') + 1; // +1 to skip '$' on the block number.
	if (pos == std::string::npos)
	{
		perror("HERCULES_ERR_MISSFORMAT_KEY");
		slog_error("HERCULES_ERR_MISSFORMAT_KEY");
		return;
	}

	std::string block = key.substr(pos, key.length() + 1); // substract the block number from the key.
	if (block.empty())
	{
		fprintf(stderr, "Block number is missing in %s\n", key.c_str());
		return;
	}
	*block_number = std::stoi(block, 0, 10);		  //  string to number.
	pos -= 1;										  // -1 to skip '$' on the data uri.
	// data_uri = (char *)key.substr(0, pos).c_str(); // substract the data uri from the key.
	*data_uri = key.substr(0, pos);
	// strcpy(data_uri, key.substr(0, pos).c_str());
	*file_name = data_uri->substr(strlen("imss://"));
	// strcpy(file_name, data_uri.substr(strlen("imss://").c_str()));
}
