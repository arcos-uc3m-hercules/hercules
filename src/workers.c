#include <atomic>
#include <cstdint>
#include <errno.h>
#include <linux/limits.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/sysinfo.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <mcheck.h>
#include <fcntl.h>
#include <condition_variable>
#include <numeric>
#include "comms.h"
#include "hercules.hpp"
#include "imss.h"
#include "workers.h"
#include "directory.h"
#include "records.hpp"
#include "map_server_eps.hpp"
#include "policies.h"
#include "shared_memory.h"
#include "slog.h"

// void *map_server_eps = NULL;
// Get a copy of all endpoints addess.
imss_info imss_copy = {0};
imss_info *curr_global_imss = NULL;
int number_of_hosts = 0;

// Lock dealing when cleaning blocks
pthread_mutex_t mutex_snapshot = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_checkpoint = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t memory_protect = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutext_malleability = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutext_progress = PTHREAD_MUTEX_INITIALIZER;
std::mutex operation_lock;

// if malleability_status = 1, new requests will be not handled and server will
// respond with a "malleability" string.
std::atomic<int> malleability_status(MALLEABILITY_OFF);
std::atomic<bool> comissioning_on(false);
uint32_t number_active_storage_servers = 0;
int32_t id_server_to_modify = -1;
char *node_to_use = NULL;
int32_t acks_received = 0;
// #define MALLEABILITY_MESSAGE = "MALLEABILITY";
// malleability time measure.
clock_t global_malleability_t;
double global_malleability_time_taken = 0.0;

// Initial buffer address.
// char *buffer_address;
// Set of locks dealing with the memory buffer access.
pthread_mutex_t *region_locks;
// Segment size (amount of memory assigned to each thread).
uint64_t buffer_segment;

// Memory amount (in GB) assigned to the buffer process.
uint64_t buffer_KB;
// Flag stating that the previous parameter has been received.
int32_t size_received;
// Communication resources in order to retrieve the buffer_GB parameter.
pthread_mutex_t buff_size_mut;
pthread_cond_t buff_size_cond;
int32_t copied;

StsHeader *mem_pool;

ucp_ep_h data_endpoints[100];
ucp_worker_h *ucp_worker_threads;
ucp_address_t **local_addr;
size_t *local_addr_len;

int global_finish_threads = 0;
int global_finish_checkpoint = 1; // TODO: change to 0 when finish the implementation.
int global_finish_snapshot = 1;
// int global_finish_malleability = 1;
int global_finish_garbage_collector = 0;
int global_server_fd_thread = -1;
int global_finish_dispatcher = 0;
pthread_cond_t global_broadcast_cond;
pthread_cond_t global_finish_cond;
pthread_cond_t global_run_snapshot_cond;
pthread_cond_t global_run_checkpoint_cond;
pthread_mutex_t global_finish_mut = PTHREAD_MUTEX_INITIALIZER;

// Garbage collector.
pthread_mutex_t mutex_garbage = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t global_run_garbage_collector_cond;
pthread_cond_t global_free_space_cond;

// Malleability decomissioning.
int waiting_clients = 0;
pthread_mutex_t mutex_malleability = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t global_run_malleability_cond;
pthread_cond_t global_run_shutdown_cond;

HierarchicalRecords *hierarchical_map = nullptr;
HierarchicalRecords *garbage_collector_map = nullptr;
extern imss curr_imss;

size_t global_offset = 0;

std::vector<PendingRequestInfo> pending_requests;

// TODO: check if this variables can be moved to records.cpp
std::mutex mtx;
std::condition_variable cv;
int data_ready = 0;

// std::vector<ServerSendRequest*> async_requests;

#define GARBAGE_COLLECTOR_PERIOD 10
#define CKECKPOINT_PERIOD 10

int AddIPS(imss_info *my_imss, char *line, int32_t n_chars)
{
	int count = my_imss->num_storages;
#ifdef DPRINTF
	fprintf(stderr, "[AddIPS] Adding %s (%d), count=%d\n", line, n_chars, count);
#endif
	slog_debug("Adding %s (%d), count=%d, my_imss->num_storages add=%p", line, n_chars, count, &(my_imss)->num_storages);
	char **new_ips = (char **)realloc(my_imss->ips, (count + 1) * sizeof(char *));
	if (!new_ips)
	{
		perror("HERCULES_ERR_WORKER_ADD_IPS_MEM_ERR");
		slog_fatal("HERCULES_ERR_WORKER_ADD_IPS_MEM_ERR");
		return -1;
	}
	my_imss->ips = new_ips;

	my_imss->ips[count] = (char *)calloc(LINE_LENGTH, sizeof(char));
	if (!my_imss->ips[count])
	{
		perror("HERCULES_ERR_WORKER_ADD_IPS_CALLOC_ERR");
		slog_fatal("HERCULES_ERR_WORKER_ADD_IPS_CALLOC_ERR");
		return -1;
	}
	// Copy the line.
	strncpy(my_imss->ips[count], line, n_chars);

	// Erase the new line character ('') from the string.
	if (((my_imss->ips)[count])[n_chars - 1] == '\n')
	{
		((my_imss->ips)[count])[n_chars - 1] = '\0';
	}
	// Increase the num. of storages. This is the "master" value used on all code.
	my_imss->num_storages++;
	slog_debug("new num_storages=%d", my_imss->num_storages);

	// print all ips.
	// for (size_t j = 0; j < my_imss->num_storages; j++)
	// {
	// 	slog_debug("eps[%d] hostname=%s", j, my_imss->ips[j]);
	// }
	return 0;
}

int ReadHostfile(char *deployfile, imss_info *my_imss)
{
	fprintf(stderr, "Reading hostfile %s\n", deployfile);
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
	int32_t n_chars = 0;
	int init_server_status = 1;
	char *line = NULL;
	size_t l_size = 0;
	int32_t count = 0;
	int ret = 0;
	while ((n_chars = getline(&line, &l_size, svr_nodes)) != -1)
	{
		// Allocate resources in the metadata structure so as to store the current HERCULES's IP.
		ret = AddIPS(my_imss, line, n_chars);
		if (ret == -1)
		{
			fprintf(stderr, "HERCULES_ERR_WORKER_READ_HOSTFILE_ADD_IPS");
			slog_error("HERCULES_ERR_WORKER_READ_HOSTFILE_ADD_IPS");
			fclose(svr_nodes);
			free(line);
			return ret;
		}
		count++;
	}

	if (line)
		free(line);

	// Close the file.
	if (fclose(svr_nodes) != 0)
	{
		perror("HERCULES_ERR_WORKER_DEPLOYFILE_CLOSE");
		slog_fatal("HERCULES_ERR_WORKER_DEPLOYFILE_CLOSE");
		return -1;
	}
	return count;
}

int ready(char *tmp_file_path, const char *msg);

/**
 * Send a default confirmation message to the client side.
 * @param value 0 for OK message, any other value send the ERROR message.
 * @return number of bytes sent on success, on error, 0 is returned.
 */
int SendConfirmationMessage(const p_argv *arguments, const char *msg)
{
	int ret = 0;
	slog_debug("Sending msg %s", msg);
	pthread_mutex_lock(&lock_network);
	ret = NETWORK_TIMING(send_data(arguments->ucp_worker, arguments->server_ep, (const void *)msg, strlen(msg) + 1, arguments->worker_uid), "Send data", int);
	pthread_mutex_unlock(&lock_network);
	return ret;
}

void Attend_pending_requests()
{
	if (malleability_status.load(std::memory_order_acquire) <= MALLEABILITY_OFF)
	{
		slog_debug("Malleability is not running, malleability_status=%d", malleability_status.load());
		return;
	}

	if (id_server_to_modify < 0)
	{
		fprintf(stderr, "Invalid server ID %d to attend request\n", id_server_to_modify);
		slog_error("Invalid server ID %d to attend request\n", id_server_to_modify);
		return;
	}

	if (curr_global_imss == NULL)
	{
		fprintf(stderr, "Invalid Hercules struct.\n");
		slog_error("Invalid Hercules struct.");
		return;
	}

	// Process all requests that were pending when this was called.
	std::vector<PendingRequestInfo> requests_to_attend;
	pthread_mutex_lock(&mutex_malleability);
	requests_to_attend.swap(pending_requests);
	pthread_mutex_unlock(&mutex_malleability);

	char response[PATH_MAX] = {'\0'};
	char list_of_active_nodes[PATH_MAX] = {'\0'};
	slog_debug("Making list of avaiable nodes, num of storages %d.", curr_global_imss->num_storages);
	for (size_t i = 0; i < curr_global_imss->num_storages; i++)
	{
		slog_debug("Adding %s to the list", curr_global_imss->ips[i]);
		strcat(list_of_active_nodes, curr_global_imss->ips[i]);
		if (i + 1 < curr_global_imss->num_storages)
		{
			strcat(list_of_active_nodes, ",");
		}
	}
	slog_debug("List to send: %s", list_of_active_nodes);

	sprintf(response, "%s %" PRIu32 " %" PRId32 " %s", MSG_MALLEABILITY_DATASERVERS, number_active_storage_servers, id_server_to_modify, list_of_active_nodes);
	int ret = -1;
	// fprintf(stderr, "[AttendingPendingRequests] There are %d pending requests\n", pending_requests.size());
	slog_debug("There are %d pending requests", pending_requests.size());
	p_argv pending_argument;
	for (const auto &pending_info : requests_to_attend)
	{
		// pthread_mutex_lock(&mutex_malleability);
		// get the last element and remove it from the vector.
		// p_argv pending_argument = pending_requests.back();
		pending_argument.ucp_worker = pending_info.ucp_worker;
		pending_argument.server_ep = pending_info.server_ep;
		pending_argument.worker_uid = pending_info.worker_uid;
		// fprintf(stderr, "Replying to pending request %s, pool size=%zu\n", pending_info.curr_req, requests_to_attend.size());
		slog_info("Replying to pending request %s, pool size=%zu", pending_info.curr_req, requests_to_attend.size());
		ret = SendConfirmationMessage(&pending_argument, response);
		// pending_requests.pop_back();
		// pthread_cond_signal(&global_run_malleability_cond);
		// pthread_mutex_unlock(&mutex_malleability);
		if (ret == 0)
		{
			perror("HERCULES_ERR_CHECK_FOR_MALLEABITY_SEND_DATASERVERS");
			slog_error("HERCULES_ERR_CHECK_FOR_MALLEABITY_SEND_DATASERVERS");
			continue;
		}
	}
	node_to_use = NULL;
	// fprintf(stderr, "No more pending requests.\n");
	slog_debug("No more pending requests.");
}

int Shutdown_server()
{

	if (global_finish_dispatcher == FINISH_SYSTEM_STATUS)
	{ // the entire system is being shutting down. We do not need to wait for pending requests.
		return 0;
	}

	// sleep(3);
	while (1)
	{
		// pthread_cond_signal(&global_run_malleability_cond);
		// fprintf(stderr, "Shutdown_server is lock..., waiting connections=%ld\n", map_server_eps_get_size(map_server_eps));
		// pthread_cond_broadcast(&global_run_malleability_cond);
		// pthread_cond_wait(&global_run_shutdown_cond, &mutex_malleability);
		// fprintf(stderr, "[Shutdown] There are %zu pending requests\n", pending_requests.size());
		slog_debug("There are %d pending requests", pending_requests.size());
		slog_info("malleability_status=%d", malleability_status.load());
		if (malleability_status.load(std::memory_order_acquire) == MALLEABILITY_COMPLETE)
		{
			pthread_mutex_lock(&mutex_malleability);

			if (pending_requests.size() == 0)
			{
				// no more request are pending.
				// future requests will be regeted.
				// we have to control this case.
				break;
			}

			pthread_mutex_unlock(&mutex_malleability);
			Attend_pending_requests();
		}
		else
		{ // lock until malleability completes.
			pthread_mutex_lock(&mutex_malleability);
			pthread_cond_wait(&global_run_malleability_cond, &mutex_malleability);
			pthread_mutex_unlock(&mutex_malleability);
		}
	}

	global_finish_threads = 1;
	// finish the garbage collector.
	pthread_mutex_lock(&mutex_garbage);
	global_finish_garbage_collector = 1;
	pthread_cond_signal(&global_run_garbage_collector_cond);
	pthread_mutex_unlock(&mutex_garbage);
	return 0;
}

void *move_blocks_2_server(void *th_argv)
{
	const p_argv *arguments = (p_argv *)th_argv;

	uint64_t stat_port = arguments->args->stat_port;
	uint32_t server_id = arguments->args->id;
	const char *imss_uri = arguments->args->imss_uri;
	// fprintf(stderr, "[move_blocks_2_server] new number of active storage servers=%d\n", number_active_storage_servers);
	slog_debug("[move_blocks_2_server] new number of active storage servers=%d", number_active_storage_servers);

	slog_debug("Connecting to data servers");
	int32_t imss_found_in = -1;
	imss_found_in = find_imss(imss_uri, &curr_imss);
	curr_global_imss = &curr_imss.info;
	if(imss_found_in == -1)
	{ // request the metadata.
		number_active_storage_servers = open_imss((char *)arguments->args->imss_uri);
		if (number_active_storage_servers < 0)
		{
			slog_fatal("Error creating HERCULES's resources, the process cannot be started");
			pthread_exit(NULL);
		}
	} 
	else 
	{ // update current struct.
		size_t num_elements_to_shift = Update_ips_list(id_server_to_modify);
		Update_data_endpoint_list(id_server_to_modify, num_elements_to_shift);
	}


	// Here data server should to move the datablocks.
	// print all key/value elements.
	double time_taken;
	time_t t = clock();
	void *address_ = NULL;
	uint64_t block_size = -1;
	int curr_map_size = 0;
	const char *uri_ = NULL;
	size_t size = 0;
	char key_[REQUEST_SIZE] = {0};

	// Map to use.
	HierarchicalMap *hiermap = hierarchical_map->hiermap;

	// fprintf(stderr, "--- Root Map ---\n");
	slog_debug("--- Root Map ---");
	int number_of_blocks_sent = 0;
	for (const auto &pair : *hiermap)
	{
		const std::string &key = pair.first;
		const std::shared_ptr<map_records> &value = pair.second;
		// fprintf(stderr, "Key: %s\n", key.c_str());
		slog_debug("Dataset Key: %s", key.c_str());
		// Check if the shared pointer is not null before dereferencing
		if (value)
		{
			// Dereference the shared_ptr to get the inner map
			const auto &inner_map = *value;

			// Iterate through the inner map
			int next_server = 0;
			for (const auto &inner_pair : inner_map)
			{
				number_of_blocks_sent++;
				const std::string &inner_key = inner_pair.first;
				const BufferValue &inner_value = inner_pair.second;
				// fprintf(stderr, "Sub Key %s\n", inner_key.c_str());
				int pos = inner_key.find('$') + 1;																											  // +1 to skip '$' on the block number.
				std::string block = inner_key.substr(pos, inner_key.length() + 1);																			  // substract the block number from the key.
				int block_number = stoi(block, 0, 10);																										  //  string to number.
				pos -= 1;																																	  // -1 to skip '$' on the data uri.
				std::string data_uri = inner_key.substr(0, pos);																							  // substract the data uri from the key.
				next_server = find_server(number_active_storage_servers, block_number, data_uri.c_str(), SET, TYPE_DATA_SERVER, curr_imss.info.session_plcy); // TODO: check for the current data policy in the dataset, not in the imss configuration.

				// next_server = (next_server + 0 * (number_active_storage_servers / 1)) % number_active_storage_servers;
				// TODO: check replication factor.

				slog_live("key='%s',\turi='%s',\tblock='%s',\tnext server=%d", inner_key.c_str(), data_uri.c_str(), block.c_str(), next_server);

				// if (id_server_to_modify == -1 && next_server == server_id)
				if (next_server == server_id)
				{
					fprintf(stderr, "[SKIP] BLOCK %d TO %d SERVER\n", block_number, next_server);
					slog_debug("[SKIP] BLOCK %d TO %d SERVER", block_number, next_server);
					continue;
				}

				// 	slog_info("key='%s',\turi='%s%s',\tfrom server %d to server %d,\tactive servers=%lu\n", key.c_str(), data_uri.c_str(), block.c_str(), server_id, next_server, number_active_storage_servers);
				// 	slog_debug("new server=%d, curr_server=%d\n", next_server, server_id);

				// 	// here we can send key.c_str() directly to reduce the number of operations.
				if (set_data_server(data_uri.c_str(), block_number, inner_value.data, inner_value.size, 0, next_server) < 0)
				{
					slog_error("ERR_HERCULES_SET_DATA_IN_SERVER\n");
					perror("ERR_HERCULES_SET_DATA_IN_SERVER");
					// TODO: do not return, continue with the following blocks.
					return NULL;
				}

				hierarchical_map->HierarchicalMapPutInGarbageCollector(inner_key.c_str());
				// HierarchicalMapDeleteEntry(hierarchical_map, inner_key); // TODO: check if this is required here.
			}
		}
	}
	// fprintf(stderr, "++ number_of_blocks_sent=%d\n", number_of_blocks_sent);
	slog_debug("++ number_of_blocks_sent=%d", number_of_blocks_sent);
	imss_flush_data();
	// Check for pending requests.
	Attend_pending_requests();

	pthread_mutex_lock(&mutex_malleability);
	// fprintf(stderr, "Sending ACK to metadata server\n");
	slog_debug("Sending ACK to metadata server");
	// ep = stat_eps[m_srv];
	// pthread_mutex_lock(&lock_network);
	// Send the request.
	StatACK(MSG_DECOM_DATASERVERS, 0);

	// fprintf(stderr, "ACK to metadata server sent.\n");
	slog_debug("ACK to metadata server sent, id_server_to_modify=%d", id_server_to_modify);
	// if (id_server_to_modify != -1)
	if(global_finish_dispatcher == FINISH_SERVER_STATUS)
	{
		malleability_status.store(MALLEABILITY_COMPLETE, std::memory_order_release);
	} if(global_finish_dispatcher == RUNNING_SERVER_STATUS) {
		malleability_status.store(MALLEABILITY_OFF, std::memory_order_release);
	}
	pthread_cond_signal(&global_run_malleability_cond);
	pthread_mutex_unlock(&mutex_malleability);

	slog_debug("Ending move_blocks_2_server\n");

	pthread_exit(NULL);
}

size_t Update_ips_list(int id_server_to_remove)
{
	slog_debug("id_server_to_remove=%d", id_server_to_remove)
	imss_info *imss_info_struct = curr_global_imss;
	if (imss_info_struct->num_storages <= 0) {
		return 0;
	}
	char *element_to_delete = imss_info_struct->ips[id_server_to_remove];
	slog_debug("Stopping server %d:%s", id_server_to_remove, imss_info_struct->ips[id_server_to_remove]);
	// fprintf(stderr, "At this time, the slowest server is %d:%s\n", id_server_to_remove, element_to_delete);
	free(element_to_delete);
	size_t num_elements_to_shift = imss_info_struct->num_storages - id_server_to_remove - 1;

	// imss_info_struct->ips[imss_info_struct->num_storages - 1] = NULL;

	// TODO: after removing memory protect from all the this block, we have to protect next pointer.
	imss_info_struct->num_storages--;
	number_active_storage_servers = imss_info_struct->num_storages;
	imss_info_struct->num_active_storages = number_active_storage_servers;

	// move the pointers.
	memmove(&imss_info_struct->ips[id_server_to_remove],
			&imss_info_struct->ips[id_server_to_remove + 1],
			num_elements_to_shift * sizeof(char *));
	imss_info_struct->ips[imss_info_struct->num_storages] = NULL;

	slog_debug("num_elements_to_shift=%d, imss_info_struct->num_storages=%d", num_elements_to_shift, imss_info_struct->num_storages);

	return num_elements_to_shift;
}

void Update_data_endpoint_list(int id_server_to_remove, size_t num_elements_to_shift)
{
	if (num_elements_to_shift <= 0) {
		return;
	}
	memmove(&data_endpoints[id_server_to_remove],
			&data_endpoints[id_server_to_remove + 1],
			num_elements_to_shift * sizeof(char *));
	data_endpoints[number_active_storage_servers] = NULL;
	return;
}

/**
 * @brief Decrease the number of servers.
 * 
 * @param arguments 
 * @param id_server_to_remove 
 */
void Decomissioning_stage(p_argv *arguments, int id_server_to_remove)
{
	// p_argv *arguments = (p_argv *)th_argv;
	// Check if malleability is enable from the configuration file and if malleability is not running.
	// Setting MALLEABILITY_INPROGRESS helps to avoid requests until malleability is done.
	int expected_status = MALLEABILITY_OFF;
	if (arguments->args->malleability && malleability_status.compare_exchange_strong(expected_status, MALLEABILITY_INPROGRESS, std::memory_order_acq_rel))
	{
		// print all records.
		int index = 0;
		// removes the server from the ips list.
		size_t num_elements_to_shift = Update_ips_list(id_server_to_remove);
		arguments->args->num_data_servers = number_active_storage_servers;

		// malleability_status.store(MALLEABILITY_INPROGRESS, std::memory_order_release);
		// number_active_storage_servers = imss_info_struct->num_storages;

		char request[REQUEST_SIZE] = {0};
		int ret = 0;
		int32_t new_id = 0;
		// fprintf(stderr, "Sending %s to server ID %d\n", request, id_server_to_remove);
		for (size_t i = 0; i < number_active_storage_servers + 1; i++)
		{
			// Send the request to all servers.
			if (i == id_server_to_remove)
			{
				sprintf(request, "STOPSERVER %" PRId32 "", number_active_storage_servers);
			}
			else
			{
				sprintf(request, "REORDERSERVER %" PRId32 " %" PRId32 " %d", number_active_storage_servers, new_id, id_server_to_remove);
				new_id++;
			}
			slog_debug("Sending %s to server ID %d, number of active storage servers=%d", request, id_server_to_remove, number_active_storage_servers);
			ret = send_req(arguments->ucp_worker, data_endpoints[i], local_addr[arguments->thread_id], local_addr_len[arguments->thread_id], request);
			if (ret == 0)
			{
				perror("HERCULES_ERR_SEND_REQ_SETSERVER");
				slog_fatal("HERCULES_ERR_SEND_REQ_SETSERVER");
				// TODO: if it fails we continue the loop. We need to ensure consistency between servers so this operation should succeed.
				// return;
			}
		}

		// Close the endpoint connection.
		close_ucx_endpoint(arguments->ucp_worker, data_endpoints[id_server_to_remove]);

		Update_data_endpoint_list(id_server_to_remove, num_elements_to_shift);

		// se me ocurre cambiar todos los dataset que utilicen el número de servidores actual
		// al que se reduce. Al final no enviarán datos a los nuevos servidores si estos crecen
		// pero para nuevos datasets debería funcionar.

		id_server_to_modify = id_server_to_remove;
	}
	return;
}

void *Comissioning_stage(void *th_argv)
{
	CommissioningThreadArgs *arguments = (CommissioningThreadArgs *)th_argv;
#ifdef DPRINTF
	fprintf(stderr, "arguments->args->malleability=%" PRId32 ", comissioning_on=%d, consecutive_scale_up_signals=%d\n", arguments->args->malleability, comissioning_on, consecutive_scale_up_signals);
#endif
	slog_debug("arguments->args->malleability: %d comissioning_on.load:%d", arguments->args->malleability, comissioning_on.load(std::memory_order_acquire));
	if (arguments->args->malleability && comissioning_on.load(std::memory_order_acquire) == false)
	{
		// Always check the performance trend.
		bool to_add_server = make_scaling_decision(elasticity_records_history, arguments->args->malleability_windows_size, arguments->args->malleability_performance_threshold);
#ifdef DPRINTF
		fprintf(stderr, "to_add_server=%d\n", to_add_server);
#endif
		pthread_mutex_lock(&mutext_malleability);
		if (to_add_server)
		{
			consecutive_scale_up_signals++; // Increment counter if scaling is needed.
			// fprintf(stderr, "Scale-up signal received. Consecutive count: %d\n", consecutive_scale_up_signals);
			slog_debug("Scale-up signal received. Consecutive count: %d/%d", consecutive_scale_up_signals, arguments->args->malleability_tolerance);
		}
		else
		{
			consecutive_scale_up_signals = 0; // Reset counter if performance is fine.
		}

		// Only trigger the actual scaling operation if the threshold is met.
		if (consecutive_scale_up_signals >= arguments->args->malleability_tolerance)
		{
			consecutive_scale_up_signals = 0;
			pthread_mutex_unlock(&mutext_malleability);
			imss_info *imss_info_struct = curr_global_imss; // arguments->hercules_info_struct;

			if (imss_info_struct == NULL) {
				fprintf(stderr, "HERCULES_ERR_COMISSIONING_STAGE_INVALID_HERCULES_STRUCT");
				slog_error("HERCULES_ERR_COMISSIONING_STAGE_INVALID_HERCULES_STRUCT");
				return NULL;
			}

			int num_server_to_increase = 1;
			fprintf(stderr, "number_of_hosts=%d\n", number_of_hosts);
			slog_debug("number_of_hosts=%d\n", number_of_hosts);
			int found = 0;
			// Iterates over all hosts defined on the "hostfile" in order to find a
			// hostname that is not already under use.
			for (size_t j = 0; j < number_of_hosts; j++)
			{
				found = 0;
				// slog_debug("eps[%d] hostname=%s", j, imss_copy.ips[j]);
				// fprintf(stderr, "eps[%d] hostname=%s\n", j, imss_copy.ips[j]);
				for (size_t i = 0; i < imss_info_struct->num_storages; i++)
				{
					// check if the host is already on the struct.
					if (!strcmp(imss_info_struct->ips[i], imss_copy.ips[j]))
					{
						// this host is in the struct, so we have to skip it.
						found = 1;
						break;
					}
				}
				if (!found)
				{
					// if we have not found this host on the curren imss_info_struct, we break the loop.
					node_to_use = imss_copy.ips[j];
					break;
				}
			}

			if (node_to_use == NULL)
			{
				fprintf(stderr, "No avaiable nodes to launch more back-end data servers.\n");
				slog_debug("No avaiable nodes to launch more back-end data servers.");
			}
			else
			{
				// start timer to now how much time it takes the malleability.
				global_malleability_t = clock();

				// Update the variables.
				pthread_mutex_lock(&mutext_malleability);
				// Here we incresae imss_info_struct->num_storages + 1.
				// AddIPS(imss_info_struct, node_to_use, strlen(node_to_use));

				// new_number_data_servers = imss_info_struct->num_storages;
				number_active_storage_servers = imss_info_struct->num_storages + 1;
				imss_info_struct->num_active_storages = number_active_storage_servers;
				arguments->args->num_data_servers = number_active_storage_servers;
				// Set comissioning ON to avoid doing twice at same time.
				comissioning_on = true;
				id_server_to_modify = number_active_storage_servers - 1;

				slog_debug("number_active_storage_servers=%d", number_active_storage_servers);

				pthread_mutex_unlock(&mutext_malleability);
				char command_to_exec[PATH_MAX] = {0};
				char *workdir = getenv("PWD");
				sprintf(command_to_exec,
						"( ssh %s 'cd %s && UCX_NET_DEVICES=ib0 HERCULES_THREAD_POOL=1 HERCULES_CONF=%s %s/build/hercules_server d %d %d > %s/tmp/hercules_server_%d_log.txt 2>&1' ) &",
						node_to_use,
						workdir,
						arguments->args->configuration_file_path,
						arguments->args->hercules_path,
						id_server_to_modify,
						imss_info_struct->num_active_storages,
						arguments->args->hercules_path,
						id_server_to_modify);

				char msg[PATH_MAX] = {'\0'};
				sprintf(msg, "Running command: %s, id server=%d\n", command_to_exec, id_server_to_modify);
				fprintf(stderr, "%s", msg);
				slog_debug(msg);
				// Deploy the new hercules instance on the avaiable node.
				// system(command_to_exec);
				// clone the process to run the command.
				pid_t pid = fork();

				if (pid == -1) 
				{ // error
					char msg_err[PATH_MAX] = {'\0'};
					sprintf(msg_err, "HERCULES_ERR_FORK_COMISSIONING_STAGE: %s", node_to_use);
					perror(msg_err);
					slog_error("%s", msg_err);
				} 
				else if (pid == 0) 
				{ // child
					execlp("ssh", "ssh", node_to_use, command_to_exec, (char *)NULL);
					// the process is replaced by the ssh.
					slog_error("HERCULES_ERR_COMISSIONING_STAGE_CHILD_EXECLP");
					exit(-1);
				} 
				else 
				{ // parent
					slog_debug("Child process run with PID %d", pid);
					// waitpid is not here to avoid blocking the parent process.
				}
			}
		}
		else
		{
			// fprintf(stderr, "Waiting for more consecutive strakes. consecutive_scale_up_signals+1 mod CONSECUTIVE_SIGNALS_THRESHOLD=%d\n", consecutive_scale_up_signals + 1 % CONSECUTIVE_SIGNALS_THRESHOLD);
			slog_debug("Waiting for more consecutive strakes. consecutive_scale_up_signals+1 mod CONSECUTIVE_SIGNALS_THRESHOLD=%d", consecutive_scale_up_signals + 1 % arguments->args->malleability_tolerance);
			pthread_mutex_unlock(&mutext_malleability);
		}
	}
	else
	{
// just for testing. It must be comment on production.
#ifdef DPRINTF
		make_scaling_decision(elasticity_records_history, arguments->args->malleability_windows_size, arguments->args->malleability_performance_threshold);
		fprintf(stderr, "Testing block\n");
#endif
	}

	p_argv temp_p_argv_for_calls;
	temp_p_argv_for_calls.ucp_worker = arguments->ucp_worker;
	temp_p_argv_for_calls.server_ep = arguments->server_ep;
	temp_p_argv_for_calls.worker_uid = arguments->worker_uid;
	strncpy(temp_p_argv_for_calls.curr_req, arguments->curr_req, PATH_MAX);

	// if (malleability_status)
	// {
	// 	CheckForMalleability(&temp_p_argv_for_calls, arguments->curr_req);
	// }
	// else
	{
		// No extra operations, just sending an ACK to the client to continue.
		// char response[PATH_MAX] = {'\0'};
		// sprintf(response, "%s %" PRId32 " -1", MSG_MALLEABILITY_DATASERVERS, number_active_storage_servers);
		char response[PATH_MAX] = {'\0'};
		char list_of_active_nodes[PATH_MAX] = {'\0'};
		slog_debug("Making list of avaiable nodes.");

		if (curr_global_imss == NULL) {
			fprintf(stderr, "HERCULES_ERR_COMISSIONING_STAGE_INVALID_HERCULES");
			slog_error("HERCULES_ERR_COMISSIONING_STAGE_INVALID_HERCULES");
			return NULL;
		}

		for (size_t i = 0; i < curr_global_imss->num_storages; i++)
		{
			slog_debug("Adding %s to the list", curr_global_imss->ips[i]);
			strcat(list_of_active_nodes, curr_global_imss->ips[i]);
			if (i + 1 < curr_global_imss->num_storages)
			{
				strcat(list_of_active_nodes, ",");
			}
		}
		slog_debug("List to send: %s", list_of_active_nodes);

		// sprintf(response, "%s %" PRIu32 " %" PRId32 " %s", MSG_MALLEABILITY_DATASERVERS, number_active_storage_servers, id_server_to_modify, node_to_use);
		sprintf(response, "%s %" PRIu32 " %" PRId32 " %s", MSG_MALLEABILITY_DATASERVERS, number_active_storage_servers, id_server_to_modify, list_of_active_nodes);
		int ret = -1;
		ret = SendConfirmationMessage(&temp_p_argv_for_calls, response);
		if (ret == 0)
		{
			perror("HERCULES_ERR_STAT_WORKER_SEND_DATASERVERS");
			slog_error("HERCULES_ERR_STAT_WORKER_SEND_DATASERVERS");
			return NULL;
		}
		// fprintf(stderr, "Reponse sent to client=%lu\n", temp_p_argv_for_calls.worker_uid);
	}

	free(arguments);
	return NULL;
}

double calculate_trend_slope(const std::vector<double> &y)
{
	int n = y.size();
	if (n < 2)
	{
		return 0.0; // Cannot determine a trend from less than two points.
	}

	double sum_x = 0.0, sum_y = 0.0, sum_xy = 0.0, sum_xx = 0.0;
	for (int i = 0; i < n; ++i)
	{
		sum_x += i;
		sum_y += y[i];
		sum_xy += i * y[i];
		sum_xx += i * i;
	}

	double numerator = n * sum_xy - sum_x * sum_y;
	double denominator = n * sum_xx - sum_x * sum_x;

	if (denominator == 0)
	{
		return 0.0; // Avoid division by zero.
	}

	return numerator / denominator;
}

/**
 * @brief Analyzes the metrics history to decide if scaling is needed.
 * @param history The map containing the metrics history for all servers.
 * @return true if a server should be added, false otherwise.
 */
bool make_scaling_decision(const std::map<std::string, std::vector<ElasticityMetric>> &history, int32_t analysis_window_size, double minimum_performance_threshold)
{
	pthread_mutex_lock(&mutext_malleability); // history is a shared resource.
#ifdef DPRINTF
	fprintf(stderr, "analysis_window_size=%" PRId32 ", minimum_performance_threshold=%f, history.empty()=%d\n", analysis_window_size, minimum_performance_threshold, history.empty());
#endif
	slog_debug("analysis_window_size=%" PRId32 ", minimum_performance_threshold=%f, history.empty()=%d", analysis_window_size, minimum_performance_threshold, history.empty());
	if (history.empty())
	{
		pthread_mutex_unlock(&mutext_malleability);
		return false;
	}

	size_t num_records = history.begin()->second.size();

	if (num_records < analysis_window_size)
	{
		fprintf(stderr, "Not enough data to make a decision. Need %d records, have %zu.\n", analysis_window_size, num_records);
		slog_debug("Not enough data to make a decision. Need %d records, have %zu.", analysis_window_size, num_records);
		pthread_mutex_unlock(&mutext_malleability);
		return false;
	}

	// Calculate the aggregate system performance for the analysis window.
	std::vector<double> aggregate_performance_history;
	for (size_t i = num_records - analysis_window_size; i < num_records; ++i)
	{
		double total_performance_at_point_i = 0.0;
		int active_servers_count = 0;

		for (const auto &pair : history)
		{
			if (i < pair.second.size())
			{
				// fprintf(stderr, "Entry %d:%s, size=%ld, overall performance='%.2f' ('%.2f' MB), W='%.2f' ('%.2f' MB), R='%.2f' ('%.2f' MB)\n",
				// 		i,
				// 		pair.second[i].server_hostname,
				// 		pair.second.size(),
				// 		pair.second[i].overall_performance,
				// 		pair.second[i].overall_performance / MB,
				// 		pair.second[i].write_performance,
				// 		pair.second[i].write_performance / MB,
				// 		pair.second[i].read_performance,
				// 		pair.second[i].read_performance / MB);

				total_performance_at_point_i += pair.second[i].overall_performance;
				active_servers_count++;
			}
		}

		if (active_servers_count > 0)
		{
			// aggregate_performance_history.push_back(total_performance_at_point_i / active_servers_count);
			aggregate_performance_history.push_back(total_performance_at_point_i);
		}
	}
	pthread_mutex_unlock(&mutext_malleability);

	if (aggregate_performance_history.empty())
	{
		fprintf(stderr, "aggregate performance history is empty.\n");
		slog_debug("aggregate performance history is empty.");
		return false;
	}

	// Calculate the Simple Moving Average (SMA).
	double performance_sum = std::accumulate(aggregate_performance_history.begin(), aggregate_performance_history.end(), 0.0);
	double moving_average = performance_sum / aggregate_performance_history.size();
	// fprintf(stderr, "Performance sum=%.2f (%.2f MB), aggregate_performance_history.size()=%ld\n", performance_sum, performance_sum / MB, aggregate_performance_history.size());
	slog_debug("Performance sum=%.2f (%.2f MB), aggregate_performance_history.size()=%ld", performance_sum, performance_sum / MB, aggregate_performance_history.size());

	// fprintf(stderr, "Elasticity analysis: Moving Average Performance = %.2f MB/s\n", moving_average / MB);
	slog_debug("Elasticity analysis: Moving Average Performance = %.2f MB/s", moving_average / MB);

	// Calculate the trend slope.
	// double slope = calculate_trend_slope(aggregate_performance_history);
	// fprintf(stderr, "Elasticity analysis: Performance Trend Slope = %.2f\n", slope);
	// slog_debug("Elasticity analysis: Performance Trend Slope = %.2f", slope);

	// Decision Logic.
	if (moving_average < minimum_performance_threshold)
	{
#ifdef DPRINTF
		fprintf(stderr, "DECISION: SCALE UP! Moving average %.2f bytes (%.2f MB/s) is below threshold %.2f bytes (%.2f MB/s), number of active servers=%d, performance_sum=%.2f bytes (%.2f MB/s) \n", 
			moving_average, 
			moving_average / MB, 
			minimum_performance_threshold, 
			minimum_performance_threshold / MB, 
			number_active_storage_servers, 
			performance_sum, 
			performance_sum / MB);
#endif
		slog_debug("DECISION: SCALE UP! Moving average %.2f bytes (%.2f MB/s) is below threshold %.2f bytes (%.2f MB/s), number of active servers=%d, performance_sum=%.2f bytes (%.2f MB/s)", 
			moving_average, 
			moving_average / MB, 
			minimum_performance_threshold, 
			minimum_performance_threshold / MB, 
			number_active_storage_servers, 
			performance_sum, 
			performance_sum / MB);
		return true;
	}

	// if (slope < CRITICAL_SLOPE)
	// {
	// 	fprintf(stderr, "DECISION: SCALE UP! Performance trend is strongly negative (%.2f), number of active servers=%d\n", slope, number_active_storage_servers);
	// 	return true;
	// }
#ifdef DPRINTF
	fprintf(stderr, "DECISION: HOLD. Performance is stable and above threshold, %.2f bytes (%.2f MB/s) of %.2f bytes (%.2f MB/s), number of active servers=%d, performance_sum=%.2f bytes (%.2f MB/s)\n", moving_average, moving_average / MB, minimum_performance_threshold, minimum_performance_threshold / MB, number_active_storage_servers, performance_sum, performance_sum / MB);
#endif
	slog_debug("DECISION: HOLD. Performance is stable and above threshold, %.2f bytes (%.2f MB/s) of %.2f bytes (%.2f MB/s), number of active servers=%d, performance_sum=%.2f bytes (%.2f MB/s)", moving_average, moving_average / MB, minimum_performance_threshold, minimum_performance_threshold / MB, number_active_storage_servers, performance_sum, performance_sum / MB);
	return false;
}

void *Malleability(void *th_argv)
{
	p_argv *arguments = (p_argv *)th_argv;

	size_t msg_length = 0;
	void *data_ref = NULL;
	void *address_ = NULL;
	uint64_t block_size_rtvd = 0;
	std::string key;

	// Here the metadata server get the data server's performance measure by each client.
	msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
	if (msg_length == 0)
	{
		perror("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP_PERFORMAMCE_OP");
		slog_error("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP_PERFORMAMCE_OP");
		// pthread_mutex_unlock(&lock_network);
		return NULL;
	}
	std::vector<char> buffer_metrics_ser(msg_length);
	msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, buffer_metrics_ser.data(), msg_length, arguments->worker_uid, 0);
	if (msg_length == 0)
	{
		perror("HERCULES_ERR_METADATA_WORKER_RECV_DATA_SET_OP_PERFORMANCE_OP");
		slog_error("HERCULES_ERR_METADATA_WORKER_RECV_DATA_SET_OP_PERFORMANCE_OP");
		// pthread_mutex_unlock(&lock_network);
		return NULL;
	}
	char *current_ptr = buffer_metrics_ser.data();
	// Get the number of entries.
	uint64_t num_entries = 0;
	memcpy(&num_entries, current_ptr, sizeof(num_entries));
	current_ptr += sizeof(num_entries);

	// int32_t slowest_server_id = -1;
	// double min_overall_performance = -1.0;
	pthread_mutex_lock(&mutext_malleability);
	number_of_history_records++;
	pthread_mutex_unlock(&mutext_malleability);

	// decode all metrics sent from the client.
	for (size_t i = 0; i < num_entries; ++i)
	{
		ElasticityMetric received_metrics;
		
		// Get the key Size.
		uint64_t key_length = 0;
		memcpy(&key_length, current_ptr, sizeof(key_length));
		current_ptr += sizeof(key_length);

		// get the key (hostname).
		std::string key(current_ptr, key_length);
		received_metrics.server_hostname = key;
		current_ptr += key_length;

		// Deserialize the server ID
		memcpy(&received_metrics.server_id, current_ptr, sizeof(received_metrics.server_id));
		current_ptr += sizeof(received_metrics.server_id);

		// Deserialize write performance.
		memcpy(&received_metrics.write_performance, current_ptr, sizeof(received_metrics.write_performance));
		current_ptr += sizeof(received_metrics.write_performance);
		// fprintf(stderr, "Write Performance for server %d: %f\n", server_id, write_performance/MB);

		// Deserialize read performance.
		memcpy(&received_metrics.read_performance, current_ptr, sizeof(received_metrics.read_performance));
		current_ptr += sizeof(received_metrics.read_performance);
		// fprintf(stderr, "Read Performance for server %d: %.2f\n", server_id, read_performance/MB);

		int metric_recorded = 2;// 0;
		// Calculates overall performance.
		if (!double_are_equal(received_metrics.read_performance, 0.0))
		{ // read performance is not zero.
			// metric_recorded++;
		}
		else
		{
			slog_debug("read performance is near zero, %.2f", received_metrics.read_performance);
		}

		if (!double_are_equal(received_metrics.write_performance, 0.0))
		{ // write performance is not zero.
			// metric_recorded++;
		}
		else
		{
			slog_debug("write performance is near zero, %.2f", received_metrics.write_performance);
		}

		// if (metric_recorded == 0)
		// {
		// 	// both read and write performances was zero.
		// 	// skip this record.
		// 	slog_debug("both metrics are zero, skipping this entry.");
		// 	continue;
		// }

		received_metrics.overall_performance = (received_metrics.read_performance + received_metrics.write_performance) / metric_recorded;

		// TODO: use a different lock here to prevent delays on other operations.
		// if (i == 0)
		// {
		// 	fprintf(stderr, "Server id\t Write (MB/s)\t Read (MB/s)\t Overall (MB/s)\n");
		// }
		// fprintf(stderr, "%s\t %d\t %f\t %f\t %f\t %d\n",received_metrics.server_hostname.c_str(), received_metrics.server_id, received_metrics.write_performance / MB, received_metrics.read_performance / MB, received_metrics.overall_performance / MB, number_of_history_records);
		slog_debug("%s\t %d\t %f\t %f\t %f\t %d", received_metrics.server_hostname.c_str(), received_metrics.server_id, received_metrics.write_performance / MB, received_metrics.read_performance / MB, received_metrics.overall_performance / MB, number_of_history_records);

		// Add the new record to the history for the specific server ID.
		pthread_mutex_lock(&mutext_malleability);
		elasticity_records_history[received_metrics.server_hostname].push_back(received_metrics);
		pthread_mutex_unlock(&mutext_malleability);

		metric_recorded = 0;
	}

	// checks if the struct is not pointing to the hercules instance.
	// if (arguments->hercules_info_struct == NULL || curr_global_imss == NULL)
	if (curr_global_imss == NULL)
	{
		if (hierarchical_map->HierarchicalMapGet(arguments->args->imss_uri, &address_, &block_size_rtvd))
		{
			curr_global_imss = (imss_info *)address_;
			// arguments->hercules_info_struct = (imss_info *)address_;
		}
		else
		{
			fprintf(stderr, "Hercules instance information has not been found.\n");
			slog_error("Hercules instance information has not been found.");
			return NULL;
		}
		slog_live("Hercules instance found: %s", key.c_str());
	}

	// 	// remove the records for this server.
	// 	elasticity_records_history.erase(element_to_delete);
	// }

	// Increase the number of servers.
	pthread_t comissioning_stage_thread;
	CommissioningThreadArgs *comissioning_thread_args = (CommissioningThreadArgs *)malloc(sizeof(CommissioningThreadArgs));
	if (!comissioning_thread_args)
	{
		perror("HERCULES_ERR_MALLEABILITY_MEM_ERR_COMISSIONING_STATE_ARGS");
		slog_error("HERCULES_ERR_MALLEABILITY_MEM_ERR_COMISSIONING_STATE_ARGS");
		return (void *)-1;
	}
	p_argv *source_args = (p_argv *)arguments;
	comissioning_thread_args->args = source_args->args;
	comissioning_thread_args->hercules_info_struct = source_args->hercules_info_struct;
	comissioning_thread_args->ucp_worker = source_args->ucp_worker;
	comissioning_thread_args->server_ep = source_args->server_ep;
	comissioning_thread_args->worker_uid = source_args->worker_uid;
	strncpy(comissioning_thread_args->curr_req, source_args->curr_req, PATH_MAX);

	if (pthread_create(&comissioning_stage_thread, NULL, Comissioning_stage, (void *)comissioning_thread_args) != 0)
	{
		perror("HERCULES_ERR_MALLEABILITY_COMISSIONING_THREAD_CREATE");
		slog_error("HERCULES_ERR_MALLEABILITY_COMISSIONING_THREAD_CREATE");
		free(comissioning_thread_args);
		return (void *)-1;
	}

	// TO FIX: making a parallel thread is throwing an IOR WARNING on write operations.
	int *ret_thread = NULL;
	if (pthread_join(comissioning_stage_thread, (void **)&ret_thread) != 0)
	{
		perror("HERCULES_ERR_MALLEABILITY_PTHREAD_JOIN_COMISSIONING_STAGE");
		slog_error("HERCULES_ERR_MALLEABILITY_PTHREAD_JOIN_COMISSIONING_STAGE");
		return (void *)-1;
	}

	// if (pthread_detach(comissioning_stage_thread) != 0)
	// {
	// 	perror("HERCULES_ERR_MALLEABILITY_COMISSIONING_THREAD_DETACH");
	// 	slog_error("HERCULES_ERR_MALLEABILITY_COMISSIONING_THREAD_DETACH");
	// 	return (void *)-1;
	// }

	return NULL;
}

int check_endpoint(ucp_ep_h ep, uint64_t worker_uid, ucp_worker_h ucp_worker, void *map_server_eps)
{
	int ret = 0;
	ucs_status_ptr_t flush_req = ucp_ep_flush_nb(ep, 0, NULL);
	if (UCS_PTR_IS_ERR(flush_req))
	{
		// The Endpoint is dead (Timeout/Disconnected)
		slog_error("Cached Endpoint is dead: %s", ucs_status_string(UCS_PTR_STATUS(flush_req)));
		fprintf(stderr, "Cached Endpoint is dead: %s\n", ucs_status_string(UCS_PTR_STATUS(flush_req)));

		// REMOVE FROM MAP IMMEDIATELY
		// map_server_eps_remove(map_server_eps, attr.worker_uid);
		map_server_eps_erase(map_server_eps, worker_uid, ucp_worker);
		ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FORCE);

		ret = -1;
	}
	else if (UCS_PTR_IS_PTR(flush_req))
	{
		// Flush is happening in background. You can wait for it,
		// or just proceed hoping it completes.
		// For stability, it is better to wait or use a callback.
		ucp_request_free(flush_req);
	}
	return ret;
}

void *hercules_ucx_server(void *th_argv)
{
	// Enable thread cancellation
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

	// Set the cancellation type to deferred
	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

	ucp_am_handler_param_t param;
	ucs_status_t status;
	int ret = 0;
	p_argv *arguments = (p_argv *)th_argv;
	// Map that stores server side endpoints
	char server_name[PATH_MAX] = {0};
	char server_tag[PATH_MAX] = {0};

	if (arguments->ucp_worker == NULL)
	{
		slog_fatal("Thread %d has a NULL worker pointer!", arguments->thread_id);
		fprintf(stderr, "Thread %d has a NULL worker pointer!\n", arguments->thread_id);
		pthread_exit(NULL);
	}

	fprintf(stderr, "Thread %d using worker at address %p\n", arguments->thread_id, (void *)arguments->ucp_worker);

	// calculates the minimun performance threshold in megabytes.
	// MINIMUM_PERFORMANCE_THRESHOLD *= MB;
	// set the initial value to the global "active number of servers".
	number_active_storage_servers = arguments->args->num_data_servers;

	switch (arguments->args->type)
	{
	case TYPE_DATA_SERVER:
		sprintf(server_name, "data");
		sprintf(server_tag, "srv_worker");
		SERVER_TYPE = TYPE_DATA_SERVER;
		break;
	case TYPE_METADATA_SERVER:
		sprintf(server_name, "metadata");
		sprintf(server_tag, "stat_worker");
		SERVER_TYPE = TYPE_METADATA_SERVER;
		break;
	default:
		fprintf(stderr, "HERCULES_ERR_INVALID_SERVER_TYPE: %c\n", arguments->args->type);
		slog_error("HERCULES_ERR_INVALID_SERVER_TYPE: %c\n", arguments->args->type);
		pthread_exit((void *)-1);
	}

	// if (!arguments->thread_id) // thread 0.
	// {
	void *map_server_eps = NULL;
	map_server_eps = map_server_eps_create();
	BLOCK_SIZE = arguments->blocksize * 1024;
	// }

	for (;;)
	{
		// errno = 0;
		size_t peer_addr_len = 0;
		ucp_address_t *peer_addr;
		ucs_status_t ep_status = UCS_OK;
		ucp_ep_h ep = NULL;
		struct ucx_context *request = NULL;
		char *req = NULL;
		ucp_tag_recv_info_t info_tag;
		ucp_tag_message_h msg_tag;
		msg_req_t *msg = NULL;
		ucp_request_param_t recv_param;

		do
		{
			/* Progressing before probe to update the state */
			ucp_worker_progress(arguments->ucp_worker);
			/* Probing incoming events in non-block mode */
			msg_tag = ucp_tag_probe_nb(arguments->ucp_worker, tag_req, tag_mask, 1, &info_tag);
			if (global_finish_threads == 1)
			{
				map_server_eps_destroy(map_server_eps);
				fprintf(stderr, "Ending %s server thread.\n", server_name);
				slog_info("Ending %s server thread.", server_name);
				pthread_exit((void *)0);
			}
		} while (msg_tag == NULL);

		clock_t t;
		double time_taken;
		t = clock();

		slog_debug("Message length=%ld bytes.", info_tag.length);
		msg = (msg_req_t *)malloc(info_tag.length);
		if (msg == NULL)
		{
			perror("HERCULES_ERR_STAT_WORKER_MEMORY_ALLOC");
			slog_error("HERCULES_ERR_STAT_WORKER_MEMORY_ALLOC");
			continue;
		}

		recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
								  UCP_OP_ATTR_FIELD_DATATYPE |
								  UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
		recv_param.datatype = ucp_dt_make_contig(1);
		recv_param.cb.recv = recv_handler;

		request = (struct ucx_context *)ucp_tag_msg_recv_nbx(arguments->ucp_worker, msg, info_tag.length, msg_tag, &recv_param);

		status = ucx_wait(arguments->ucp_worker, request, "receive", server_tag);
		if (status != UCS_OK)
		{
			perror("HERCULES_ERR_SRV_WORKER_UCX_WAIT");
			slog_error("HERCULES_ERR_SRV_WORKER_UCX_WAIT");
			free(msg);
			continue;
		}

		peer_addr_len = msg->addr_len;
		peer_addr = (ucp_address *)malloc(peer_addr_len);
		if (peer_addr == NULL)
		{
			// unable to allocate memory for peer address
			perror("HERCULES_ERR_UCX_SERVER_MEMORY_ALLOC");
			slog_error("HERCULES_ERR_UCX_SERVER_MEMORY_ALLOC");
			free(msg);
			continue;
		}

		req = msg->request;

		memcpy(peer_addr, msg + 1, peer_addr_len);

		ucp_worker_address_attr_t attr;
		attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
		ucp_worker_address_query(peer_addr, &attr);
		slog_debug(" Receiving request from %" PRIu64 ".", attr.worker_uid);

		//  look for this peer_addr in the map and get the ep
		ret = map_server_eps_search(map_server_eps, attr.worker_uid, &ep);

		// if (ret > 0)
		// {
		// 	int check = check_endpoint(ep, attr.worker_uid, arguments->ucp_worker, map_server_eps);
		// 	if (check < 0)
		// 	{
		// 		ret = -1;
		// 	}
		// }

		// ret = -1;
		// create ep if it's not in the map
		if (ret < 0)
		{
			// ucp_ep_h new_ep;
			// ucp_ep_params_t ep_params;
			// ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
			// 					   UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
			// 					   UCP_EP_PARAM_FIELD_ERR_HANDLER |
			// 					   UCP_EP_PARAM_FIELD_USER_DATA;
			// ep_params.address = peer_addr;
			// ep_params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
			// // ep_params.err_mode = UCP_ERR_HANDLING_MODE_NONE;
			// // ep_params.err_handler.cb = err_cb_server;
			// ep_params.err_handler.cb = failure_handler;
			// ep_params.err_handler.arg = NULL;
			// ep_params.user_data = &ep_status;

			// // struct worker_info *worker_info = (struct worker_info*)malloc(sizeof(struct worker_info));
			// // worker_info->worker_uid = attr.worker_uid;
			// // worker_info->server_type = 'd';
			// // ep_params.err_handler.arg = &worker_info;
			// // ep_params.err_handler.arg = &attr.worker_uid;

			// status = ucp_ep_create(arguments->ucp_worker, &ep_params, &ep);
			// if (status != UCS_OK)
			// { // ucp ep create error.
			// 	perror("HERCULES_ERR_UCX_SERVER_UCP_EP_CREATE");
			// 	slog_error("HERCULES_ERR_UCX_SERVER_UCP_EP_CREATE");
			// 	fprintf(stderr, "Failed to request: %s\n", req);
			// 	continue;
			// }
			// size = const ucx_server_err_call_arg + UID=5 + UINT64 string representation size.
			const size_t msg_len = strlen(ucx_server_err_call_arg) + 5 + UINT64_MAX_STR_LEN;
			char tmp_msg[msg_len] = {0};
			snprintf(tmp_msg, msg_len, "%s UID %" PRIu64, ucx_server_err_call_arg, attr.worker_uid);
			client_create_ep_data(arguments->ucp_worker, &ep, peer_addr, tmp_msg);
			// add the new ep to the map
			map_server_eps_put(map_server_eps, attr.worker_uid, ep);
		}
		else
		{
			slog_debug("\t['%" PRIu64 "] Endpoint already exist'", attr.worker_uid);
		}

		arguments->peer_address = peer_addr;
		arguments->server_ep = ep;
		arguments->worker_uid = attr.worker_uid;
		arguments->hierarchical_map = hierarchical_map; // TODO: move this out of the loop.
		strncpy(arguments->curr_req, req, sizeof(arguments->curr_req));

		switch (arguments->args->type)
		{
		case TYPE_DATA_SERVER:
			TIMING_NO_RETURN(srv_worker_helper(arguments, req, map_server_eps), ("srv_worker_helper %s", req), arguments->thread_id);
			break;
		case TYPE_METADATA_SERVER:
			ret = TIMING(stat_worker_helper(arguments, req, map_server_eps), ("stat_worker_helper %s", req), int, arguments->thread_id);
			break;
		default:
			fprintf(stderr, "HERCULES_ERR_INVALID_SERVER_TYPE: %c\n", arguments->args->type);
			slog_error("HERCULES_ERR_INVALID_SERVER_TYPE: %c\n", arguments->args->type);
			pthread_exit((void *)-1);
		}

		t = clock() - t;

		// fflush(stdout);

		time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
		slog_info("Serving time %f s\n", time_taken);

		// status = flush_ep(arguments->ucp_worker, ep);
		// slog_debug("flush_ep completed with status %d (%s)\n", status, ucs_status_string(status));
		free(msg);
		free(peer_addr);
		// ep_close_err_mode(arguments->ucp_worker, ep);
		// ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FORCE);
	}
}

int CheckForMalleability(const p_argv *arguments, const char *req)
{
	if (malleability_status.load(std::memory_order_acquire) == MALLEABILITY_INPROGRESS)
	{
		pthread_mutex_lock(&mutex_malleability);
		if (malleability_status.load(std::memory_order_relaxed) != MALLEABILITY_INPROGRESS)
		{
			pthread_cond_signal(&global_run_malleability_cond);
			pthread_mutex_unlock(&mutex_malleability);	
			return 0;
		}
		waiting_clients++;
		// fprintf(stderr, "+ Clients waiting %d, req=%s\n", waiting_clients, req);
		slog_debug("+ Clients waiting %d, req=%s", waiting_clients, req);
		// copy the required arguments.
		PendingRequestInfo pending_info;
		pending_info.ucp_worker = arguments->ucp_worker;
		pending_info.server_ep = arguments->server_ep;
		pending_info.worker_uid = arguments->worker_uid;
		strncpy(pending_info.curr_req, arguments->curr_req, PATH_MAX - 1);
		pending_info.curr_req[PATH_MAX - 1] = '\0';
		// push the data into the vector.
		pending_requests.push_back(pending_info);
		pthread_cond_signal(&global_run_malleability_cond);
		pthread_mutex_unlock(&mutex_malleability);
		// fprintf(stderr, "+ Request saved, req=%s\n", req);
		slog_debug("+ Request saved, req=%s", req);
		return 1;
	}
	return 0;
}

int srv_worker_helper(p_argv *arguments, const char *req, void *map_server_eps)
{

	ucs_status_t status;
	int ret = -1;

	// Obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;
	// void *hierarchical_map = arguments->hierarchical_map;
	HierarchicalRecords *hierarchical_map = arguments->hierarchical_map;

	// Resources specifying if the request set in the sender.
	int64_t more = 0;
	size_t more_size = sizeof(more);
	int is_shared_memory = 0;
	int default_params = 1;
	int snapshot_op = 0;
	key_t shm_key = -1;

	// Code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";
	char mode[MODE_SIZE] = {0};
	const char *response_msg = NULL;

	// Save the request to be served.
	slog_debug(" request to be served %s", req);

	// Elements conforming the request.
	uint32_t block_size_recv, block_offset;
	char uri_[URI_] = {0};
	size_t to_read = 0;
	int sender = 0;

	// Get the GET, SET, ..., operation (mode).
	TIMING_NO_RETURN(sscanf(req, "%s", mode), "sscanf mode", arguments->thread_id);

	if (!strcmp(mode, "BROADCAST"))
	{
		sscanf(req, "%s %s %d", mode, uri_, &sender);
		slog_debug("BROADCAST condition, req=%s, mode=%s, uri_=%s, sender=%d", req, mode, uri_, sender);
		map->put_snapshot(uri_, sender);
		fprintf(stderr, "Sending signal to do Snapshot in server %d\n", arguments->args->id);
		pthread_cond_signal(&global_run_snapshot_cond);
		pthread_cond_signal(&global_run_checkpoint_cond);
		// nothing else to do.
		return 0;
	}

	// GET = read operation.
	// SET = write operation.
	if (!strcmp(mode, "GET"))
	{
		ret = CheckForMalleability(arguments, req);
		if (ret != 0)
		{ // request was saved to be attended after malleability.
			return 1;
		}
		more = GET_OP;
	}
	else if (!strcmp(mode, "SET"))
	{
		more = SET_OP;
	}
	else if (!strcmp(mode, "LOCALGET"))
	{
		more = GET_OP;
		is_shared_memory = 1;
	}
	else if (!strcmp(mode, "LOCALGETBLOCK"))
	{
		more = GET_OP;
		is_shared_memory = 1;
		default_params = 0;
	}
	// else if (!strcmp(mode, "LOCALSET"))
	// {
	// 	more = SET_OP;
	// 	is_shared_memory = 1;
	// }
	else if (!strcmp(mode, "LOCALSETBLOCK"))
	{
		more = SET_OP;
		is_shared_memory = 1;
		default_params = 0;
	}
	else if (!strcmp(mode, "SNAPSET"))
	{
		more = SET_OP;
		snapshot_op = 1;
	}
	else if (!strcmp(mode, "STOPSERVER"))
	{
		// TODO: move this to the correct case.
		// malleability_status = MALLEABILITY_INPROGRESS;
		malleability_status.store(MALLEABILITY_INPROGRESS, std::memory_order_release);
		global_finish_dispatcher = FINISH_SERVER_STATUS;
		id_server_to_modify = arguments->args->id;
		sscanf(req, "%s %" PRIu32 " %" PRIu32 "", mode, &number_active_storage_servers);
		pthread_t thread;
		p_argv *arguments_aux = new p_argv;

		*arguments_aux = *arguments;
		if (pthread_create(&thread, NULL, move_blocks_2_server, (void *)arguments_aux) != 0)
		{
			perror("HERCULES_ERR_SRV_WORKER_THREAD_MOVE_BLOCKS_2_SERVER");
			slog_error("HERCULES_ERR_SRV_WPORKER_THREAD_MOVE_BLOCKS_2_SERVER");
			delete arguments_aux;
			return -1;
		}

		if (pthread_detach(thread) != 0)
		{
			perror("HERCULES_ERR_SRV_WORKER_DETACH_THREAD_MOVE_BLOCKS_2_SERVER");
			slog_error("HERCULES_ERR_SRV_WORKER_DETACH_THREAD_MOVE_BLOCKS_2_SERVER");
			return 1;
		}

		// After moving all data, delete this server.
		// Shutdown or close the socket used by the dispatcher pointed
		// by the file descriptor "global_server_fd_thread".
		if (shutdown(global_server_fd_thread, SHUT_RD) == -1)
		{
			perror("HERCULES_ERR_SRV_WORKER_SHUTDOWN_SERVER_FD\n");
		}

		return 1;
	}
	else if (!strcmp(mode, "REORDERSERVER"))
	{
		// should I enable malleability here?
		// malleability_status = MALLEABILITY_INPROGRESS;
		

		int32_t new_id = 0;
		sscanf(req, "%s %" PRIu32 " %" PRIu32 " %d", mode, &number_active_storage_servers, &new_id, &id_server_to_modify);
		slog_debug("mode=%s, number_active_storage_servers=%" PRIu32 ", new_id=%" PRIu32 "", mode, number_active_storage_servers, new_id);
		arguments->args->id = new_id;
		slog_debug("new id on args=%d", arguments->args->id);
		pthread_t thread;
		p_argv *arguments_aux = new p_argv; //*arguments;
		*arguments_aux = *arguments;
		// p_argv arguments_aux = *arguments;
		// arguments_aux.args = arguments->args;
		// arguments_aux.args = arguments->args;
		// p_argv *arguments_aux = new p_argv(*arguments);
		if (pthread_create(&thread, NULL, move_blocks_2_server, (void *)arguments_aux) != 0)
		{
			perror("HERCULES_ERR_SRV_WORKER_THREAD_MOVE_BLOCKS_2_SERVER");
			slog_error("HERCULES_ERR_SRV_WPORKER_THREAD_MOVE_BLOCKS_2_SERVER");
			delete arguments_aux;
			return -1;
		}

		if (pthread_detach(thread) != 0)
		{
			perror("HERCULES_ERR_SRV_WORKER_DETACH_THREAD_MOVE_BLOCKS_2_SERVER");
			slog_error("HERCULES_ERR_SRV_WORKER_DETACH_THREAD_MOVE_BLOCKS_2_SERVER");
			return 1;
		}
		return 1;
	}
	else
	{
		char err_msg[MAX_ERR_MSG_LEN] = {0};
		sprintf(err_msg, "HERCULES_ERR_SRV_WORKER_UNSUPPORTED_MODE: %s", mode);
		// perror(err_msg);
		fprintf(stderr, "%s\n", err_msg);
		slog_error("%s", err_msg);
		return -1;
	}

	if (!default_params)
	{ // special case for "LOCALSETBLOCK" (shared memory).
		sscanf(req, "%s %" PRIu32 " %" PRIu32 " %d %s %lu", mode, &block_size_recv, &block_offset, &shm_key, uri_, &to_read);
		// fprintf(stderr, "request=%s, block_size_recv=%d, shm_key=%d\n", req, block_size_recv, shm_key);
	}
	else
	{
		TIMING_NO_RETURN(sscanf(req, "%s %" PRIu32 " %" PRIu32 " %s %lu", mode, &block_size_recv, &block_offset, uri_, &to_read), "sscanf requeest", arguments->thread_id);
	}

	slog_debug(" Request - mode '%s', block_size_recv '%" PRIu32 "', block_offset '%" PRIu32 "', uri_ '%s', more %ld", mode, block_size_recv, block_offset, uri_, more);

	// Create an std::string in order to be managed by the map structure.
	std::string key;
	key.assign((const char *)uri_);

	// Information associated to the arriving key.
	void *address_ = NULL;
	uint64_t block_size_rtvd = 0;

	// Differentiate between READ and WRITE operations.
	switch (more)
	{
	case READ_OP:
	{
		switch (block_size_recv)
		{
		case READ_OP:
		{
			int32_t ret = TIMING(hierarchical_map->HierarchicalMapGet(key, &address_, &block_size_rtvd), "HierarchicalMapGet", int32_t, arguments->thread_id);
			// fprintf(stderr, "%s returned %d\n", key.c_str(), ret);

			// Check if there was an associated block to the key.
			if (ret == 0)
			{ // block does not exist.
				// pthread_mutex_lock(&lock_network);
				//   Send the error code block.
				ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid);
				if (ret < 0)
				{
					SendConfirmationMessage(arguments, MSG_OK_OP);
					perror("HERCULES_ERR_WORKER_SEND_DYNAMIC_STREAM_READ_NON_EXISTING_BLOCK");
					slog_error("HERCULES_ERR_WORKER_SEND_DYNAMIC_STREAM_READ_NON_EXISTING_BLOCK");
					return -1;
				}
			}
			else
			{
				// Here we do not need to check the number of links to the dataset
				// because to know if the file is dirty, we use the metadata server.
				// check if it is the block 0.
				bool is_block_zero = false;
				std::size_t found = key.find("$0");
				if (found != std::string::npos)
				{
					is_block_zero = true;
				}

				// if (is_block_zero)
				// {
				// 	slog_debug("(struct stat *)address_=%ld", ((struct stat *)address_)->st_size);
				// }

				// if (is_block_zero)
				// { // if it is the block zero, check if it is dirty.
				// 	struct stat *stats;
				// 	stats = (struct stat *)address_;
				// 	if (stats->st_nlink == 0)
				// 	{
				// 		slog_debug("%s has %d links", stats->st_nlink);
				// 		ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid);
				// 		if (ret < 0)
				// 		{
				// 			perror("HERCULES_ERR_WORKER_SEND_DYNAMIC_STREAM_READ_DIRTY_BLOCK");
				// 			slog_error("HERCULES_ERR_WORKER_SEND_DYNAMIC_STREAM_READ_DIRTY_BLOCK");
				// 			//pthread_mutex_unlock(&lock_network);
				// 			return -1;
				// 		}
				// 	}
				// }

				if (to_read <= 0)
				{
					to_read = block_size_rtvd;
				}
				// if (is_shared_memory)
				// {
				// 	slog_debug("[READ_OP][READ_OP] Send requested block size, key=%s", key.c_str());
				// 	// Send the size of the block, without any data.
				// 	char size_of_block[10];
				// 	sprintf(size_of_block, "%lu", to_read);
				// 	ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, size_of_block, STRING, arguments->worker_uid);
				// 	if (ret < 0)
				// 	{
				// 		slog_error("ERR_HERCULES_WORKER_SEND_READ_OP");
				// 		perror("ERR_HERCULES_WORKER_SEND_READ_OP");
				// 		return -1;
				// 	}
				// }
				// else
				// {
				// Send the requested block.
				// slog_debug("[READ_OP][READ_OP] Send the requested block with key=%s, block_offset=%ld, block_size_rtvd=%ld kb, to_read=%ld kb, stat->st_nlink=%lu, is_shared_memory=%d", key.c_str(), block_offset, block_size_rtvd / 1024, to_read / 1024, stats->st_nlink, is_shared_memory);
				slog_debug("[READ_OP][READ_OP] Send the requested block with key=%s, block_offset=%ld, block_size_rtvd=%ld kb, to_read=%ld bytes (%ld kb), is_shared_memory=%d", key.c_str(), block_offset, block_size_rtvd / 1024, to_read, to_read / 1024, is_shared_memory);
				size_t ret_send_data = 0;
				if (is_shared_memory)
				{
					// put the private data on the shared memory.
					// uint64_t remaining = block_size_rtvd - block_offset;
					// SharedMemory *sh_memory_struct = setContentSMByID(shm_key, to_read, (char *)address_ + block_offset); //setContentSM(shm_key, to_read, (char *)address_ + block_offset);
					void *content = setContentSMByID(shm_key, to_read, (char *)address_ + block_offset);
					const char *response = MSG_OK_OP;

					if (content == NULL)
					{
						fprintf(stderr, "HERCULES_ERR_SETTING_SHM_CONTENT_READ_OP\n");
						slog_error("HERCULES_ERR_SETTING_SHM_CONTENT_READ_OP");
						response = MSG_ERROR_OP;
					}
					else
					{
						unlinkSM(content);
					}

					// send the confirmation to the client.
					ret = SendConfirmationMessage(arguments, response);
					if (ret == 0)
					{
						perror("HERCULES_ERR_PUBLISH_READ_BLOCK_SHM");
						slog_error("HERCULES_ERR_PUBLISH_READ_BLOCK_SHM");
						return -1;
					}

					// unlinkSM(sh_memory_struct->content);
					// free(sh_memory_struct);
					// ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, (void *)MSG_OK_OP, STRING, arguments->worker_uid);
					// if (ret < 0)
					// {
					// 	perror("HERCULES_ERR_WORKER_SEND_DYNAMIC_STREAM_READ_CONFIRMATION_SHM");
					// 	slog_error("HERCULES_ERR_WORKER_SEND_DYNAMIC_STREAM_READ_CONFIRMATION_SHM");
					// 	// pthread_mutex_unlock(&lock_network);
					// 	return -1;
					// }

					// // pthread_mutex_lock(&lock_network);
					// ret_send_data = send_data(arguments->ucp_worker, arguments->server_ep, (char *)address_, block_size_rtvd, arguments->worker_uid);
					// // pthread_mutex_unlock(&lock_network);
					// slog_debug("[READ_OP][READ_OP] ret_send_data=%lu", ret_send_data);
					// if (ret_send_data == 0)
					// {
					// 	slog_error("HERCULES_ERR_WORKER_SENDBLOCK");
					// 	perror("HERCULES_ERR_WORKER_SENDBLOCK");
					// 	return -1;
					// }
				}
				else
				{
					if (arguments->args->async_io == SYNC)
					{ // Synchronous.
						// pthread_mutex_lock(&lock_network);
						ret_send_data = send_data(arguments->ucp_worker, arguments->server_ep, (char *)address_ + block_offset, to_read, arguments->worker_uid);
						// ret_send_data = isend_data(arguments->ucp_worker, arguments->server_ep, (char *)address_ + block_offset, to_read, arguments->worker_uid);
						// pthread_mutex_unlock(&lock_network);
					}
					else
					{ // Asynchronous.
						// Create a tracking struct and add it to our list.
						ServerSendRequest *new_send = new ServerSendRequest();
						void *ucx_req_handle = isend_data2(arguments->ucp_worker, arguments->server_ep, (char *)address_ + block_offset, to_read, arguments->worker_uid, new_send);
						if (UCS_PTR_IS_PTR(ucx_req_handle))
						{
							// The request is sending. The callback will handle cleanup.
						}
						else if (UCS_PTR_IS_ERR(ucx_req_handle))
						{
							slog_error("Failed to initiate async send on server.");
							fprintf(stderr, "Failed to initiate async send on server.");
							// The send function already cleaned up new_send.
						}
						else
						{
							// It completed immediately. The send function also cleaned up.
						}
					}
				}
			}
			break;
		}
		case READV2_OP:
		{

			// use the prefetch size calculated on the front-end.
			uint64_t prefetch_size = to_read;

			const uint32_t BLOCK_ID_SIZE = sizeof(uint32_t);
			const size_t BLOCK_DATA_SIZE = BLOCK_SIZE;
			const size_t RECORD_SIZE = BLOCK_ID_SIZE + BLOCK_DATA_SIZE;
			slog_debug("BLOCK_ID_SIZE=%lu, BLOCK_DATA_SIZE=%lu, RECORD_SIZE=%lu, prefetch_size=%lu", BLOCK_ID_SIZE, BLOCK_DATA_SIZE, RECORD_SIZE, prefetch_size);

			if (prefetch_size <= 0)
			{
				// use the prefetch size defined on the configuration file.
				prefetch_size = arguments->args->prefetch_size;
			}

			if (prefetch_size < RECORD_SIZE)
			{
				slog_warn("Record size %lu is bigger than prefetch size %lu", RECORD_SIZE, prefetch_size);
				// record size is bigger than the defined prefetch size.
				// use the record size.
				prefetch_size = RECORD_SIZE;
			}

			// if (prefetch_size <= 0)
			// {
			// 	slog_error("HERCULES_ERR_SRV_WORKER_HELPER_READV2_OP_PREFETCH_INVALID_SIZE, %" PRIu64 " bytes.", prefetch_size);
			// 	fprintf(stderr, "WARNING: Invalid prefetch size, using a block size of %" PRIu64 " bytes instead.\n", BLOCK_SIZE);
			// 	// use the defined block size as prefetch size.
			// 	prefetch_size = BLOCK_SIZE;
			// }

			slog_debug("prefetch_size=%" PRIu64 "", prefetch_size);

			// create the prefetching block.
			char *prefetch_buffer = new(std::nothrow) char[prefetch_size];
			if (prefetch_buffer == nullptr)
			{
				slog_error("HERCULES_ERR_SRV_WORKER_HELPER_READV2_OP_PREFETCH_MEM_ERR %ld bytes.", prefetch_size);
				return -1;
			}

			size_t delimiter_pos = key.find('$');
			if (delimiter_pos == std::string::npos)
			{
				slog_error("Invalid format. Key: %s", key.c_str());
				delete[] prefetch_buffer;
				return -1;
			}

			std::string base_key = key.substr(0, delimiter_pos);
			uint32_t current_block_id = (uint32_t)std::stoul(key.substr(delimiter_pos + 1));
			ssize_t buffer_offset = hierarchical_map->HierarchicalMapGetPrefetch(
				base_key,
				current_block_id,
				arguments->args->num_data_servers,
				prefetch_buffer,
				prefetch_size);

			slog_debug("buffer_offset=%zu", buffer_offset);
			if (buffer_offset == 0)
			{
				delete[] prefetch_buffer;
				slog_debug("Sending not found code: %s", err_code);
				//  Send the error code msg.
				ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid);
				if (ret < 0)
				{
					perror("HERCULES_ERR_WORKER_SEND_DYNAMIC_STREAM_READ_NON_EXISTING_BLOCK");
					slog_error("HERCULES_ERR_WORKER_SEND_DYNAMIC_STREAM_READ_NON_EXISTING_BLOCK");
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				return 0;
			}

			// Create a tracking struct and add it to our list.
			ServerSendRequest *new_send = new ServerSendRequest();
			new_send->buffer_to_free = prefetch_buffer;

			void *ucx_req_handle = isend_data2(arguments->ucp_worker, arguments->server_ep, (char *)prefetch_buffer, buffer_offset, arguments->worker_uid, new_send);
			if (UCS_PTR_IS_PTR(ucx_req_handle))
			{
				// The request is sending. The callback will handle cleanup.
			}
			else if (UCS_PTR_IS_ERR(ucx_req_handle))
			{
				slog_error("Failed to initiate async send on server.");
				fprintf(stderr, "Failed to initiate async send on server.");
				// The send function already cleaned up new_send.
				delete[] prefetch_buffer;
				delete new_send;
			}
			else
			{
				// It completed immediately. The send function also cleaned up.
				// delete[] prefetch_buffer;
				// delete new_send;
			}

			break;
		}
		case RELEASE:
		{
			slog_debug("[READ_OP][RELEASE]");
			map_server_eps_erase(map_server_eps, arguments->worker_uid, arguments->ucp_worker);

			/*
			response_msg = MSG_RELEASE_OP;
			ret = SendConfirmationMessage(arguments, response_msg);
			if (ret == 0)
			{
				perror("HERCULES_ERR_SRV_SEND_DATA_RELEASE");
				slog_error("HERCULES_ERR_SRV_SEND_DATA_RELEASE");
				return -1;
			}
				*/
			break;
		}
		case UNLINK_OP:
		{
			slog_debug("UNLINK_OP");
			const char *response_msg = NULL;
			// "block offset" represent how many processes has the file open.
			// TODO: we have to change the way how we get the format.
			int num_opened = block_offset;
			// check if this is the last link.
			// this operation was perform by the client by getting the block 0 and then decressing the link number.
			// we are migrating that opearting here to avoid extra network operations.
			// key must to be block 0 always.
			int32_t ret = hierarchical_map->HierarchicalMapGet(key, &address_, &block_size_rtvd);
			// Check if there was an associated block to the key.
			if (ret == 0)
			{ // block does not exist.
				//   Send the error code block.
				ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid);
				if (ret < 0)
				{
					perror("HERCULES_ERR_WORKER_SEND_DYNAMIC_STREAM_READ_NON_EXISTING_BLOCK");
					slog_error("HERCULES_ERR_WORKER_SEND_DYNAMIC_STREAM_READ_NON_EXISTING_BLOCK");
					return -1;
				}
			}

			struct stat *header = (struct stat *)address_;
			// decrease the number of links.
			header->st_nlink--;
			slog_debug("new number of links=%d, number of opens=%d", header->st_nlink, num_opened);
			// if this is the last link, we put the file in the garbage collector.
			if (header->st_nlink == 0 && num_opened == 0)
			{
				hierarchical_map->HierarchicalMapPutInGarbageCollector(key);
				response_msg = MSG_DELETE_OP;
			}
			else
			{
				response_msg = MSG_NODELETE_OP;
			}

			ret = SendConfirmationMessage(arguments, response_msg);
			if (ret == 0)
			{
				perror("HERCULES_ERR_PUBLISH_DELETEOP");
				slog_error("HERCULES_ERR_PUBLISH_DELETEOP");
				return -1;
			}
			slog_debug("Message sent: %s", response_msg);
			break;
		}
		case DELETE_OP:
		{
			slog_debug("DELETE_OP");

			const char *response_msg = NULL;
			// for data server we need to look up for all the blocks.
			// map->put_garbage_collector(key);
			hierarchical_map->HierarchicalMapPutInGarbageCollector(key);

			response_msg = MSG_OK_OP;

			ret = SendConfirmationMessage(arguments, response_msg);
			if (ret == 0)
			{
				perror("HERCULES_ERR_PUBLISH_DELETEOP");
				slog_error("HERCULES_ERR_PUBLISH_DELETEOP");
				return -1;
			}
			slog_debug("Message sent: %s", response_msg);
			break;
		}
		case RENAME_OP:
		{
			std::size_t found = key.find(',');
			slog_debug("[RENAME_OP], key=%s, found=%d", key.c_str(), found);
			ret = -1;
			if (found != std::string::npos)
			{
				std::string old_key = key.substr(0, found);
				std::string new_key = key.substr(found + 1);
				slog_debug("[RENAME_OP], old_key=%s, new_key=%s", old_key.c_str(), new_key.c_str());
				// RENAME MAP
				// ret = map->cleaning_specific(old_key);
				// if (ret == -1)
				// {
				// 	size_t len = strlen(old_key.c_str());
				// 	if (len > 0 && old_key.c_str()[len - 1] != '/')
				// 	{
				// 		old_key += '/';
				// 		// std::unique_lock<std::mutex> unlock(*mut);
				// 		ret = map->cleaning_specific(old_key);
				// 	}
				// }
				// if (ret == 0)
				{
					// ret = map->rename_data_srv_worker(old_key, new_key);
					ret = hierarchical_map->HierarchicalMapRenameRegularFile(old_key, new_key);
				}
			}
			// else
			// {
			// 	slog_debug("[RENAME_OP], found == npos");
			// 	response_msg = MSG_ERROR_OP;
			// }

			if (ret != 0)
			{
				response_msg = MSG_ERROR_OP;
			}
			else
			{
				response_msg = MSG_OK_OP;
			}

			ret = SendConfirmationMessage(arguments, response_msg);
			if (ret == 0)
			{
				perror("HERCULES_ERR_PUBLISH_RENAMEMSG");
				slog_error("HERCULES_ERR_PUBLISH_RENAMEMSG");
				return -1;
			}
			break;
		}
		case RENAME_DIR_DIR_OP:
		{
			slog_debug("[RENAME_DIR_DIR_OP]");
			std::size_t found = key.find(',');
			ret = -1;
			// ret = 0;
			// check if the old key and new key has been passed in the string separed by a comma.
			if (found != std::string::npos)
			{
				std::string old_dir = key.substr(0, found);
				std::string rdir_dest = key.substr(found + 1);

				// RENAME MAP
				slog_debug("rename_data_dir_srv_worker, old_dir=%s, dest_dir=%s", old_dir.c_str(), rdir_dest.c_str());
				// ret = TIMING(map->rename_data_dir_srv_worker(old_dir, rdir_dest);,"rename_data_dir_srv_worker,RENAME_DIR_DIR_OP", int32_t, arguments->thread_id);
				ret = TIMING(hierarchical_map->BackEndHierarchicalMapRenameDirDir(old_dir, rdir_dest, NULL), "BackEndHierarchicalMapRenameDirDir,RENAME_DIR_DIR_OP", int32_t, arguments->thread_id);
				// Rename the old directory on the hierarchical map.
				slog_debug("Renaming %s to %s on the directory map", old_dir.c_str(), rdir_dest.c_str());
				ret = hierarchical_map->HierarchicalMapRenameKey(old_dir.c_str(), rdir_dest.c_str());
				if (ret != 0)
				{
					slog_error("HERCULES_ERR_HIERARCHICAL_MAP_RENAMING_DATA_KEY");
					perror("HERCULES_ERR_HIERARCHICAL_MAP_RENAMING_DATA_KEY");
					// return -1;
				}
			}

			if (ret != 0)
			{
				response_msg = MSG_ERROR_OP;
			}
			else
			{
				response_msg = MSG_RENAME_OP;
			}

			ret = TIMING(SendConfirmationMessage(arguments, response_msg), "SendConfirmationMessage,RENAME_DIR_DIR_OP", int, arguments->thread_id);
			if (ret == 0)
			{
				perror("ERR_HERCULES_PUBLISH_RENAMEMSG");
				slog_error("ERR_HERCULES_PUBLISH_RENAMEMSG");
				return 1;
			}
			break;
		}
		case READV: // Only 1 server work
		{
			// printf("READV CASE");
			std::size_t found = key.find('$');
			std::string path;
			if (found != std::string::npos)
			{
				path = key.substr(0, found + 1);
				// std::cout <<"path:" << path << '';
				key.erase(0, found + 1);
				std::size_t found = key.find(' ');
				int curr_blk = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int end_blk = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int blocksize = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int start_offset = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int64_t size = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				// Needed variables
				size_t byte_count = 0;
				int first = 0;
				int ds = 0;
				int64_t to_copy = 0;
				uint32_t filled = 0;
				size_t to_read = 0;

				int pos = path.find('$');
				std::string first_element = path.substr(0, pos + 1);
				first_element = first_element + std::to_string(0);
				// printf("first_element=%s",first_element.c_str());
				map->get(first_element, &address_, &block_size_rtvd);
				struct stat *stats = (struct stat *)address_;
				void *buf = (void *)malloc(size);

				while (curr_blk <= end_blk)
				{
					std::string element = path;
					element = element + std::to_string(curr_blk);
					// std::cout <<"SERVER READV element:" << element << '';
					if (map->get(element, &address_, &block_size_rtvd) == 0)
					{ // If dont exist
					  // Send the error code block.
					  // std::cout <<"SERVER READV NO EXISTE element:" << element << '';
						ret = NETWORK_TIMING(send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid), "[READ_OP][READV] send_dynamic_stream", int);
						if (ret < 0)
						{
							perror("ERRIMSS_WORKER_SENDERR");
							free(buf);
							return -1;
						}
					} // If was already stored:
					else
					{
						// First block case
						if (first == 0)
						{
							if (size < stats->st_size - start_offset)
							{
								// to_read = size;
								to_read = blocksize * KB - start_offset;
							}
							else
							{
								if (stats->st_size < blocksize * KB)
								{
									to_read = stats->st_size - start_offset;
								}
								else
								{
									to_read = blocksize * KB - start_offset;
								}
							}
							// Check if offset is bigger than filled, return 0 because is EOF case
							if (start_offset > stats->st_size)
								return 0;
							memcpy(buf, (char *)address_ + start_offset, to_read);
							byte_count += to_read;
							++first;

							// Middle block case
						}
						else if (curr_blk != end_blk)
						{
							memcpy((char *)buf + byte_count, address_, blocksize * KB);
							byte_count += blocksize * KB;
							// End block case
						}
						else
						{
							// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
							int64_t pending = size - byte_count;
							memcpy((char *)buf + byte_count, address_, pending);
							byte_count += pending;
						}
					}
					++curr_blk;
				}
				ret = NETWORK_TIMING(send_data(arguments->ucp_worker, arguments->server_ep, buf, size, arguments->worker_uid), "[READ_OP][READV] send", int);
				// Send the requested block.
				if (ret == 0)
				{
					perror("ERR_HERCULES_WORKER_SENDBLOCK");
					slog_error("ERR_HERCULES_WORKER_SENDBLOCK");
					free(buf);
					return -1;
				}
				free(buf);
			}
			break;
		}
		case SPLIT_READV:
		{
			// printf("SPLIT_READV CASE");
			slog_debug("key=%s", key.c_str());
			std::size_t found = key.find(' ');
			std::string path;
			if (found != std::string::npos)
			{
				path = key.substr(0, found);
				key.erase(0, found + 1);

				found = key.find(' ');
				int blocksize = stoi(key.substr(0, found)) * KB;
				key.erase(0, found + 1);

				found = key.find(' ');
				int start_offset = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				found = key.find(' ');
				int stats_size = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				size_t msg_size = stoi(key.substr(0, found));

				// char *msg = (char *)calloc(msg_size, sizeof(char));
				size_t msg_length = 0;
				msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
				if (msg_length == 0)
				{
					perror("ERRIMSS_DATA_WORKER_INVALID_MSG_LENGTH");
					slog_error("ERRIMSS_DATA_WORKER_INVALID_MSG_LENGTH");
					return -1;
				}
				void *msg = malloc(msg_length);

				msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, msg, msg_length, arguments->worker_uid, 0);
				// msg_length = recv_data_opt(arguments->ucp_worker, arguments->server_ep, &msg, msg_length, arguments->worker_uid, 0);
				// Send the requested block.
				if (msg_length == 0)
				{
					perror("ERRIMSS_DATA_WORKER_RECV_DATA");
					slog_error("ERRIMSS_DATA_WORKER_RECV_DATA");
					free(msg);
					return -1;
				}

				key = (char *)msg;
				found = key.find('$');
				int amount = stoi(key.substr(0, found));
				int size = amount * blocksize;
				key.erase(0, found + 1);

				slog_debug("msg=%s", key.c_str());
				slog_debug("msg_size=%lu", msg_size);
				slog_debug("*path=%s", path.c_str());
				slog_debug("*blocksize=%d", blocksize);
				slog_debug("*start_offset=%d", start_offset);
				slog_debug("*size=%d", size);
				slog_debug("*amount=%d", amount);

				char *buf = (char *)malloc(size);
				// Needed variables
				size_t byte_count = 0;
				int first = 0;
				int ds = 0;
				int64_t to_copy = 0;
				uint32_t filled = 0;
				size_t to_read = 0;
				int curr_blk = 0;

				for (int i = 0; i < amount; i++)
				{
					// substract current block
					found = key.find('$');
					int curr_blk = stoi(key.substr(0, found));
					key.erase(0, found + 1);

					std::string element = path;
					element = element + '$' + std::to_string(curr_blk);
					if (map->get(element, &address_, &block_size_rtvd) == 0)
					{ // If dont exist
					  // Send the error code block.
						if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
						{
							free(msg);
							return -1;
							pthread_exit(NULL);
						}
					} // If was already stored:

					memcpy(buf + byte_count, address_, blocksize);
					byte_count += blocksize;
				}
				// Send the requested block.
				ret = NETWORK_TIMING(send_data(arguments->ucp_worker, arguments->server_ep, buf, byte_count, arguments->worker_uid), "[READ_OP][READV] send buf", int);
				if (ret == 0)
				{
					free(msg);
					free(buf);
					perror("ERR_HERCULES_WORKER_SENDBLOCK");
					slog_error("ERR_HERCULES_WORKER_SENDBLOCK");
					return -1;
				}
				free(msg);
				free(buf);
			}
			break;
		}
		case WHO:
		{
			// Provide the uri of this instance.
			ret = SendConfirmationMessage(arguments, arguments->my_uri);
			if (ret == 0)
			{
				perror("ERR_HERCULES_WHOREQUEST");
				slog_error("ERR_HERCULES_WHOREQUEST");
				return -1;
			}
			break;
		}
		default:
			fprintf(stderr, "HERCULES_ERR_SRV_WORKER_INVALID_MSG_TYPE\n");
			slog_error("HERCULES_ERR_SRV_WORKER_INVALID_MSG_TYPE");
			break;
		}
		break;
	}
	// More messages will arrive to the socket.
	case WRITE_OP:
	{

		slog_debug("[WRITE_OP] WRITE NORMAL CASE. Size %ld, offset=%ld", block_size_recv, block_offset);
		//  search for the block to know if it was previously stored.
		int ret = 0;
		int is_block_zero = 0;
		int32_t insert_successful = -1;
		// Checks if it is data for the Snapshot operation or regular data.
		if (snapshot_op)
		{
			// Nothing to do.
		}
		else
		{
			ret = TIMING(hierarchical_map->HierarchicalMapGet(key, &address_, &block_size_rtvd), "Does it exist? map->get", int, arguments->thread_id);
		}

		std::size_t found = key.find("$0");
		if (found != std::string::npos)
		{
			is_block_zero = 1;
		}

		// if the block was not already stored:
		if (ret == 0)
		{
			slog_debug("[WRITE_OP] NO key find %s", key.c_str());
			// fprintf(stderr, "Inserting block %s\t", key.c_str());
			void *buffer = NULL;
			//  Receive the block into the buffer.
			clock_t tr;
			// get the buffer data length.
			size_t msg_length = 0;
			size_t size_asigned_to_block = 0;
			int reused_memory = 1;
			// If data is stored in shared memory due LOCAL policy, the server does not need to receive the data.
			if (!is_shared_memory)
			{ // Data is not in shared memory.
				slog_debug("[WRITE_OP] is_shared_memory=%d", is_shared_memory);
				// Get the length of the data to be received.
				msg_length = TIMING(get_recv_data_length(arguments->ucp_worker, arguments->worker_uid), "[write] get_recv_data_length", size_t, arguments->thread_id);
				// ucp_tag_recv_info_t info_tag;
				// ucp_tag_message_h msg_tag;
				// msg_length = TIMING(get_recv_data_length_2(arguments->ucp_worker, arguments->worker_uid, &info_tag, &msg_tag), "[write] get_recv_data_length_2", size_t, arguments->thread_id);
				// pthread_mutex_unlock(&lock_network);
				if (msg_length == 0)
				{
					perror("HERCULES_ERR_DATA_WORKER_WRITE_NEW_BLOCK_INVALID_MSG_LENGTH");
					slog_error("HERCULES_ERR_DATA_WORKER_WRITE_NEW_BLOCK_INVALID_MSG_LENGTH");
					// pthread_mutex_unlock(&memory_protect);
					SendConfirmationMessage(arguments, MSG_ERROR_OP);
					return -1;
				}
				slog_debug("[WRITE_OP] msg_length=%lu, is_block_zero=%d, snapshot_op=%d", msg_length, is_block_zero, snapshot_op);

				if (is_block_zero || snapshot_op)
				{
					// Snapshot operation sends data bigger than BLOCK_SIZE, and
					// block 0 is usually smaller than BLOCK_SIZE
					buffer = (void *)malloc(msg_length * sizeof(char));
					// fprintf(stdout, "Using %lu bytes for block %s\n", msg_length,  key.c_str());
					size_asigned_to_block = msg_length; // TODO: check if this follows the snapshot requirements.
					reused_memory = 0;
				}
				else
				{
					// reutilizate memory from the memory pool.
					buffer = (void *)StsQueue.pop(mem_pool);
					if (buffer == NULL)
					{
						buffer = (void *)malloc(BLOCK_SIZE * sizeof(char));
						reused_memory = 0;
						slog_debug("Allocating buffer of size %lu", BLOCK_SIZE);
					}
					else
					{
						slog_debug("Reusing buffer");
					}
					size_asigned_to_block = BLOCK_SIZE;
				}

				// if (msg_length > BLOCK_SIZE)
				// {
				// 	fprintf(stdout, "HERCULES_ERR_MEMORY_INCONSISTENCY_BLOCK\n");
				// 	slog_warn("HERCULES_ERR_MEMORY_INCONSISTENCY_BLOCK");
				// }

				if (buffer == NULL)
				{
					perror("HERCULES_ERR_MEMORY_ALLOCATION");
					slog_error("HERCULES_ERR_MEMORY_ALLOCATION");
					// pthread_mutex_unlock(&memory_protect);
					SendConfirmationMessage(arguments, MSG_ERROR_OP);
					return -1;
				}

				// Receive the data from the front end.
				// pthread_mutex_lock(&lock_network);
				msg_length = NETWORK_TIMING(recv_data(arguments->ucp_worker, arguments->server_ep, (char *)buffer + block_offset, msg_length, arguments->worker_uid, 1), "[write] recv_data", size_t);
				if (msg_length == 0)
				{
					perror("HERCULES_ERR_DATA_WORKER_WRITE_NEW_BLOCK_RECV_DATA");
					slog_error("HERCULES_ERR_DATA_WORKER_WRITE_NEW_BLOCK_RECV_DATA");
					SendConfirmationMessage(arguments, MSG_ERROR_OP);
					return -1;
				}
			}
			else
			{ // Data in shared memory.
				// get the content from the shared memory.
				// SharedMemory *sh_memory_struct = getContentSM(shm_key, block_size_recv);
				void *content = getContentSMByID(shm_key);
				if (content == NULL)
				{
					perror("HERCULES_ERR_GET_CONTENT_SHM_BY_ID_REGULAR_BLOCK_NEW_BLOCK");
					slog_error("HERCULES_ERR_GET_CONTENT_SHM_BY_ID_REGULAR_BLOCK_NEW_BLOCK");
					return -1;
				}

				// Send confirmation message.
				// NOTE: we put this confirmation message because
				// a pointer needs to be pointing to the shared memory in order to be alive.
				// if there are not at least one pointer, the system will remove the shared memory.
				// this is not an expected behaviour, so we have to check it later.
				ret = SendConfirmationMessage(arguments, MSG_OK_OP);
				if (ret == 0)
				{
					perror("HERCULES_ERR_PUBLISH_UPDATE_ZERO_BLOCK_SHM");
					slog_error("HERCULES_ERR_PUBLISH_UPDATE_ZERO_BLOCK_SHM");
					return -1;
				}

				// here we do not receive the data by UCX, we use the size specified on the request message.
				msg_length = block_size_recv;
				// alloc memory to copy from shared memory to private memory.
				slog_debug("[WRITE_OP SHM] msg_length=%lu, is_block_zero=%d, snapshot_op=%d", msg_length, is_block_zero, snapshot_op);
				if (is_block_zero || snapshot_op)
				{
					// Snapshot operation sends data bigger than BLOCK_SIZE, and
					// block 0 is usually smaller than BLOCK_SIZE
					// TOCHECK: does block_size_recv works for snapshot_op?
					buffer = (void *)malloc(msg_length * sizeof(char));
					// fprintf(stdout, "Using %lu bytes for block %s\n", msg_length,  key.c_str());
					size_asigned_to_block = msg_length; // TODO: check if this follows the snapshot requirements.
					reused_memory = 0;
				}
				else
				{
					// reutilizate memory from the memory pool.
					buffer = (void *)StsQueue.pop(mem_pool);
					if (buffer == NULL)
					{
						buffer = (void *)malloc(BLOCK_SIZE * sizeof(char));
						reused_memory = 0;
						slog_debug("Allocating buffer of size %lu", BLOCK_SIZE);
					}
					else
					{
						slog_debug("Reusing buffer");
					}
					size_asigned_to_block = BLOCK_SIZE;
				}

				if (buffer == NULL)
				{
					perror("HERCULES_ERR_MEMORY_ALLOCATION_SHM");
					slog_error("HERCULES_ERR_MEMORY_ALLOCATION_SHM");
					// pthread_mutex_unlock(&memory_protect);
					return -1;
				}

				// copy from shared memory to private memory.
				// copyContentSM(buffer, sh_memory_struct->content, sh_memory_struct->size);
				// pthread_mutex_lock(&memory_protect);
				copyContentSM((char *)buffer + block_offset, content, block_size_recv);
				// pthread_mutex_unlock(&memory_protect);

				// detach shared memory.
				unlinkSM(content);
				// Destroy the shared memory segment.
				freeSM(shm_key);
			}

			// INSERT THE ELEMENT TO FREE THE MEMORY PROTECT.
			pthread_mutex_lock(&memory_protect);
			// Check if another thread has created this key.
			int race_check = TIMING(hierarchical_map->HierarchicalMapGet(key, &address_, &block_size_rtvd), "Race check", int, arguments->thread_id);
			if (race_check == 0)
			{ // key still does not exist.
				// Include the new record in the tracking structure.
				slog_debug("[WRITE_OP] ****[PUT, block_size_recv=%ld, BLOCK_SIZE=%lu, msg_length=%lu]********* key=%s", block_size_recv, BLOCK_SIZE, msg_length, key.c_str());

				// TODO: should this be block_size_recv or a different size? block_size_recv might not be the full block size
				if (snapshot_op)
				{
					// Get the origin data server id from the received key.
					// Fill buffer_broadcast with the data received from the other servers.
					slog_debug("Snapshot operation, origin server=%s", key.c_str());
					insert_successful = TIMING(map->put_broadcast(key, buffer, msg_length), " new block map-put_broadcast", int, arguments->thread_id);
				}
				else
				{
					// fprintf(stderr, "Inserting buffer of size %lu/%lu\n", msg_length, size_asigned_to_block);
					insert_successful = hierarchical_map->HierarchicalMapPut(key, buffer, size_asigned_to_block, reused_memory, NULL, is_block_zero);
				}
				pthread_mutex_unlock(&memory_protect);

				slog_debug("[WRITE_OP] insert_successful=%d, key=%s, size_asigned_to_block=%lu", insert_successful, key.c_str(), size_asigned_to_block);

				// Include the new record in the tracking structure.
				if (insert_successful != 0)
				{
					if (insert_successful == 2)
					{
						SendConfirmationMessage(arguments, MSG_SPACE_OP);
					}
					else
					{
						SendConfirmationMessage(arguments, MSG_ERROR_OP);
					}

					perror("HERCULES_ERR_WORKER_MAPPUT");
					slog_error("HERCULES_ERR_WORKER_MAPPUT");
					free(buffer);
					// pthread_mutex_unlock(&memory_protect);
					return -1;
				}
			}
			else
			{
				// Another thread has inserted the same key.
				slog_debug("[WRITE_OP] Race lost! Key %s appeared while receiving data.", key.c_str());
				// We copy the temporal buffer into the one that is already on the map.
				memcpy((char *)address_ + block_offset, (char *)buffer + block_offset, msg_length);
				pthread_mutex_unlock(&memory_protect);
				free(buffer);
			}
			SendConfirmationMessage(arguments, MSG_OK_OP);
		}
		// if the block was already stored:
		else
		{
			slog_debug("[WRITE_OP] Key find %s", key.c_str());
			// Receive the block into the buffer.
			if (is_block_zero)
			{ // block 0.
				// check if it is in the garbage collector map.
				// TODO: on DELETE_OP, we can remove the dataset from the main map and inserting it on the garbage collector.
				// with that we can avoid searching for the dataset on the gargabe collector here.
				int ret = hierarchical_map->HierarchicalMapPopFromGarbageCollector(key);
				if (ret == 0)
				{
					slog_debug("%s has not been found on the garbage collector map.", key.c_str());
				}
				// //pthread_mutex_lock(&memory_protect);
				slog_debug("[WRITE_OP] Updating block $0 (%d)", block_size_rtvd);
				struct stat *old, *latest;
				size_t msg_length = 0;
				// If data is stored in shared memory due LOCAL policy, the server does not need to receive the data.
				if (!is_shared_memory)
				{ // non shared memory method.
				  // ucp_tag_recv_info_t info_tag;
				  // ucp_tag_message_h msg_tag = NULL;
					msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
					// msg_length = TIMING(get_recv_data_length_2(arguments->ucp_worker, arguments->worker_uid, &info_tag, &msg_tag), "[write] get_recv_data_length_2", size_t, arguments->thread_id);
					if (msg_length == 0)
					{
						perror("HERCULES_ERR_DATA_WORKER_WRITE_BLOCK_0_INVALID_MSG_LENGTH");
						slog_error("HERCULES_ERR_DATA_WORKER_WRITE_BLOCK_0_INVALID_MSG_LENGTH");
						// pthread_mutex_unlock(&memory_protect);
						return -1;
					}
					// slog_live("msg_length=%lu", msg_length);
					// void *buffer = malloc(block_size_recv);
					void *buffer = (void *)malloc(msg_length * sizeof(char));
					if (buffer == NULL)
					{
						perror("HERCULES_ERR_SRV_WORKER_MEMORY_ALLOCATION");
						slog_error("HERCULES_ERR_SRV_WORKER_MEMORY_ALLOCATION");
						SendConfirmationMessage(arguments, MSG_ERROR_OP);
						// pthread_mutex_unlock(&memory_protect);
						return -1;
					}

					msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, buffer, msg_length, arguments->worker_uid, 0);
					// msg_length = TIMING(recv_data_2(arguments->ucp_worker, arguments->server_ep, (char *)buffer, msg_length, arguments->worker_uid, 0, info_tag, msg_tag), "[write] recv_data_2", size_t, arguments->thread_id);
					if (msg_length == 0)
					{
						perror("HERCULES_ERR_DATA_WORKER_WRITE_BLOCK_0_RECV_DATA");
						slog_error("HERCULES_ERR_DATA_WORKER_WRITE_BLOCK_0_RECV_DATA");
						free(buffer);
						SendConfirmationMessage(arguments, MSG_ERROR_OP);
						// pthread_mutex_unlock(&memory_protect);
						return -1;
					}
					pthread_mutex_lock(&memory_protect);
					old = (struct stat *)address_;
					latest = (struct stat *)buffer;
					slog_debug(" File size new %ld old %ld", latest->st_size, old->st_size);
					latest->st_size = std::max(latest->st_size, old->st_size);
					// slog_debug(" buffer->st_size: %ld, block_offset=%ld", latest->st_size, block_offset);
					slog_debug(" buffer->st_size: %ld, block_offset=%ld, old->st_nlink: %ld, new->st_nlink: %ld", latest->st_size, block_offset, old->st_nlink, latest->st_nlink);
					// Overwrite block 0 data.
					memcpy((char *)address_ + block_offset, buffer, msg_length);
					pthread_mutex_unlock(&memory_protect);

					// TODO: should we update this block's size in the map?
					// map->update(key, address_, msg_length);
					// Updates the second map to update the data in disk.
					// map->update_simple(key, 1);
					free(buffer);
				}
				else
				{ // data is in shared memory.
					// SharedMemory *sh_memory_struct = getContentSM(shm_key, block_size_recv);
					void *content = getContentSMByID(shm_key);
					if (content == NULL)
					{
						perror("HERCULES_ERR_GET_CONTENT_SHM_BY_ID_BLOCK_ZERO");
						slog_error("HERCULES_ERR_GET_CONTENT_SHM_BY_ID_BLOCK_ZERO");
						return -1;
					}
					void *buffer = content; // sh_memory_struct->content;
					// copy from shared memory to private memory.
					// copyContentSM(buffer, sh_memory_struct->content, sh_memory_struct->size);
					pthread_mutex_lock(&memory_protect);
					old = (struct stat *)address_;
					latest = (struct stat *)buffer;
					slog_debug(" File size new %ld old %ld", latest->st_size, old->st_size);
					latest->st_size = std::max(latest->st_size, old->st_size);
					// slog_debug(" buffer->st_size: %ld, block_offset=%ld", latest->st_size, block_offset);
					slog_debug(" buffer->st_size: %ld, block_offset=%ld, old->st_nlink: %ld, new->st_nlink: %ld", latest->st_size, block_offset, old->st_nlink, latest->st_nlink);
					// Overwrite block 0 data.
					memcpy((char *)address_ + block_offset, buffer, block_size_recv);
					pthread_mutex_unlock(&memory_protect);

					// No confirmation is needed here.
					ret = SendConfirmationMessage(arguments, MSG_OK_OP);
					if (ret == 0)
					{
						perror("HERCULES_ERR_PUBLISH_UPDATE_ZERO_BLOCK_SHM");
						slog_error("HERCULES_ERR_PUBLISH_UPDATE_ZERO_BLOCK_SHM");
						return -1;
					}

					// detach shared memory.
					// slog_debug("Unlinking shm key %d", sh_memory_struct->key);
					// unlinkSM(sh_memory_struct->content);
					unlinkSM(content);
					// Destroy the shared memory segment.
					// freeSM(sh_memory_struct->id);
					freeSM(shm_key);
					// free(sh_memory_struct);
					//  Tell the client to update the shared memory.
					// char answer[RESPONSE_SIZE] = {0};
					// // "address_" is the shared memory offset.
					// sprintf(answer, "TOUPDATE %s", (char *)address_);
					// // pthread_mutex_lock(&lock_network);
					// ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, answer, STRING, arguments->worker_uid);
					// // pthread_mutex_unlock(&lock_network);
					// if (ret < 0)
					// {
					// 	slog_error("HERCULES_ERR_WORKER_SEND_DYNAMIC_BLOCK_0_WRITE_OP");
					// 	perror("HERCULES_ERR_WORKER_SEND_DYNAMIC_BLOCK_0_WRITE_OP");
					// 	// pthread_mutex_unlock(&memory_protect);
					// 	return -1;
					// }
				}
				// when snapshot is enabled we saved block 0.
				// TODO: fix this.
				// if (!global_finish_snapshot)
				// {
				// 	std::size_t found = TIMING(key.find("$0"), "check if block 0", std::size_t, arguments->thread_id);
				// 	if (found != std::string::npos) // block 0.
				// 	{
				// 		insert_successful = TIMING(map->put_snapshot(key, -1), "map->put_snapshot", int, arguments->thread_id);
				// 		// Include the new record in the tracking structure.
				// 		if (insert_successful != 0)
				// 		{
				// 			perror("HERCULES_ERR_WORKER_SEC_MAP_PUT");
				// 			slog_error("HERCULES_ERR_WORKER_SEC_MAP_PUT");
				// 			pthread_mutex_unlock(&memory_protect);
				// 			return -1;
				// 		}
				// 	}
				// }
			}
			else
			{ // non block 0.
				slog_debug("[WRITE_OP] Updated non 0 existing block, key.c_str(): %s", key.c_str());
				size_t msg_length = 0;
				if (!is_shared_memory)
				{ // non shared memory.
					msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
					if (msg_length == 0)
					{
						slog_error("HERCULES_ERR_DATA_WORKER_WRITE_NON_BLOCK_0_INVALID_MSG_LENGTH");
						perror("HERCULES_ERR_DATA_WORKER_WRITE_NON_BLOCK_0_INVALID_MSG_LENGTH");
						SendConfirmationMessage(arguments, MSG_ERROR_OP);
						// pthread_mutex_unlock(&memory_protect);
						return -1;
					}
					// Verify if the new size (msg_length + block_offset) is greater than the old size (block_size_rtvd).
					slog_debug("msg_length=%lu, block_offset=%d, msg_length=%d", msg_length, block_offset, msg_length);
					pthread_mutex_lock(&memory_protect);
					msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)address_ + block_offset, msg_length, arguments->worker_uid, 1);
					pthread_mutex_unlock(&memory_protect);
					//   msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)buffer + block_offset, msg_length, arguments->worker_uid, 1);
					if (msg_length == 0)
					{
						slog_error("HERCULES_ERR_DATA_WORKER_WRITE_NON_BLOCK_0_RECV_DATA");
						perror("HERCULES_ERR_DATA_WORKER_WRITE_NON_BLOCK_0_RECV_DATA");
						SendConfirmationMessage(arguments, MSG_ERROR_OP);
						// pthread_mutex_unlock(&memory_protect);
						return -1;
					}

					// Updates the second map to update the data in disk.
					map->update_simple(key, 1);
				}
				else
				{ // Data is in shared memory.
					// SharedMemory *sh_memory_struct = getContentSM(shm_key, block_size_recv);
					void *content = getContentSMByID(shm_key);
					if (content == NULL)
					{
						perror("HERCULES_ERR_GET_CONTENT_SHM_BY_ID_REGULAR_BLOCK");
						slog_error("HERCULES_ERR_GET_CONTENT_SHM_BY_ID_REGULAR_BLOCK");
						return -1;
					}

					pthread_mutex_lock(&memory_protect);
					// copy from shared memory to private memory.
					// copyContentSM((char *)address_ + block_offset, sh_memory_struct->content, block_size_recv);
					slog_debug("Updating address_ at %ld with size %d", block_offset, block_size_recv);
					copyContentSM((char *)address_ + block_offset, content, block_size_recv);
					pthread_mutex_unlock(&memory_protect);

					// No confirmation is needed here.
					ret = SendConfirmationMessage(arguments, MSG_OK_OP);
					if (ret == 0)
					{
						perror("HERCULES_ERR_PUBLISH_UPDATE_BLOCK_SHM");
						slog_error("HERCULES_ERR_PUBLISH_UPDATE_BLOCK_SHM");
						return -1;
					}

					// detach shared memory.
					// unlinkSM(sh_memory_struct->content);
					unlinkSM(content);
					// Destroy the shared memory segment.
					// freeSM(sh_memory_struct->id);
					freeSM(shm_key);
					// free(sh_memory_struct);
				}
			}
			SendConfirmationMessage(arguments, MSG_OK_OP);
		}
		break;
	}
	default:
		fprintf(stderr, "HERCULES_ERR_INVALID_READ_OR_WRITE_OPERATION_SRV_WORKER_HELPER\n");
		break;
	}

	slog_debug(" Terminated data helper");
	return 0;
}

// Thread method searching and cleaning nodes with st_nlink=0
void *GarbageCollector(void *th_argv)
{
	// fprintf(stderr, "Init garbage collector\n");
	slog_debug("Init garbage collector");
	// Obtain the current map class element from the set of arguments.
	p_argv *arguments = (p_argv *)th_argv;
	// std::shared_ptr<map_records> map = arguments->map;

	pthread_cond_init(&global_run_garbage_collector_cond, NULL);
	pthread_cond_init(&global_free_space_cond, NULL);

	for (;;)
	{
		// Gnodetraverse_garbage_collector(map);//Future
		// sleep(GARBAGE_COLLECTOR_PERIOD);
		pthread_mutex_lock(&mutex_garbage);
		while (global_finish_garbage_collector == 0)
		{
			pthread_cond_wait(&global_run_garbage_collector_cond, &mutex_garbage);
		}

		fprintf(stderr, "Running Garbage collector\n");

		if (global_finish_garbage_collector == 1)
		{
			fprintf(stderr, "Ending garbage collector thread.\n");
			pthread_mutex_unlock(&mutex_garbage);
			pthread_exit((void *)0);
		}
		// TODO: removes the next "continue".
		// pthread_mutex_unlock(&mutex_garbage);
		// continue;
		hierarchical_map->HierarchicalMapCleanGarbageCollector();
		// Unlock all threads waiting for resources.
		pthread_cond_broadcast(&global_free_space_cond);
		pthread_mutex_unlock(&mutex_garbage);
	}
	pthread_exit(NULL);
}

// Thread method to copy datasets from Hercules to Disk.
void *Checkpoint(void *th_argv)
{
	slog_debug("Init Snapshot");

	clock_t t;
	double time_taken;
	t = clock();

	p_argv *arguments = (p_argv *)th_argv;
	BLOCK_SIZE = arguments->blocksize * 1024;
	sleep(1);
	// Obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;

	const char *checkpoint_dir = arguments->args->hercules_snapshot_path;
	const int server_id = arguments->args->id;
	const char *POLICY = arguments->args->policy;

	if (strlen(checkpoint_dir) == 0)
	{
		printf("Checkpoint path has not been provided.\tHERCULES_SNAPSHOT_PATH = /home/user/snapshot_path/\n");
		fflush(stdout);
		global_finish_snapshot = 1;
		pthread_exit(NULL);
	}

	// if (!arguments->args->id)
	// { // only one server creates the snapshot directory.
	// 	Make_directory(checkpoint_dir);
	// }
	fprintf(stderr, "Running Checkpoint in %s\n", checkpoint_dir);
	int ret = 1;
	for (;;)
	{
		// sleep(CKECKPOINT_PERIOD);
		pthread_mutex_lock(&mutex_checkpoint);

		slog_debug("Running Checkpoint in %s", checkpoint_dir);

		TIMING_NO_RETURN(
			ret = map->Checkpoint(BLOCK_SIZE, checkpoint_dir, global_finish_snapshot, arguments->args->id, arguments->args->data_hostname, *arguments->args), "Checkpoint", arguments->thread_id);

		if (ret != 1)
		{
			fprintf(stderr, "Waiting for signal to unlock Checkpoint in server %d\n", server_id);
			pthread_cond_wait(&global_run_checkpoint_cond, &mutex_checkpoint);
			pthread_mutex_unlock(&mutex_checkpoint);
			if (map->get_buffer_size() == 0)
			{ // if there is no data to copy to disk, we will finish the snapshot.
				break;
			}
			continue;
		}

		pthread_mutex_unlock(&mutex_checkpoint);
		// To stop this thread we will wait for "hercules stop".
		if (global_finish_snapshot == 1)
		{
			// TODO: Call a barrier to stop all servers.
			slog_debug("Waiting to finish Snapshot thread.");
			global_finish_threads = 1;
			pthread_cond_signal(&global_finish_cond);
			fprintf(stderr, "Ending checkponting thread.\n");
			break;
		}
	}

	t = clock() - t;
	time_taken = ((double)t) / (CLOCKS_PER_SEC);

	pthread_exit(NULL);
}

// Thread method to copy datasets from Hercules to Disk.
void *Snapshot(void *th_argv)
{
	slog_debug("Init Snapshot");

	clock_t t;
	double time_taken;
	t = clock();

	p_argv *arguments = (p_argv *)th_argv;
	BLOCK_SIZE = arguments->blocksize * 1024;
	sleep(1);
	// Obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;
	// arguments->hercules_info_struct = NULL;

	const char *snapshot_dir = arguments->args->hercules_snapshot_path;
	const int server_id = arguments->args->id;
	const char *POLICY = arguments->args->policy;

	if (strlen(snapshot_dir) == 0)
	{
		printf("Snapshot path has not been provided.\tHERCULES_SNAPSHOT_PATH = /home/user/snapshot_path/\n");
		fflush(stdout);
		global_finish_snapshot = 1;
		pthread_exit(NULL);
	}

	// if (!arguments->args->id)
	// { // only one server creates the snapshot directory.
	// 	Make_directory(snapshot_dir);
	// }
	fprintf(stderr, "Running Snapshot in %s\n", snapshot_dir);
	int ret = 1;
	for (;;)
	{
		// sleep(CKECKPOINT_PERIOD);
		pthread_mutex_lock(&mutex_snapshot);

		slog_debug("Running Snapshot in %s", snapshot_dir);

		TIMING_NO_RETURN(
			ret = map->Snapshot(BLOCK_SIZE, snapshot_dir, global_finish_snapshot, arguments->args->id, arguments->args->data_hostname, *arguments->args), "Snapshot", arguments->thread_id);

		if (ret != 1)
		{
			fprintf(stderr, "Waiting for signal to unlock snapshot in server %d\n", server_id);
			pthread_cond_wait(&global_run_snapshot_cond, &mutex_snapshot);
			pthread_mutex_unlock(&mutex_snapshot);
			if (map->get_buffer_size() == 0)
			{ // if there is no data to copy to disk, we will finish the snapshot.
				break;
			}
			continue;
		}

		pthread_mutex_unlock(&mutex_snapshot);
		// To stop this thread we will wait for "hercules stop".
		if (global_finish_snapshot == 1)
		{
			// TODO: Call a barrier to stop all servers.
			slog_debug("Waiting to finish Snapshot thread.");
			global_finish_threads = 1;
			pthread_cond_signal(&global_finish_cond);
			fprintf(stderr, "Ending checkponting thread.\n");
			break;
		}
	}

	t = clock() - t;
	time_taken = ((double)t) / (CLOCKS_PER_SEC);

	pthread_exit(NULL);
}

int stat_worker_helper(p_argv *arguments, char *req, void *map_server_eps)
{
	ucs_status_t status;
	int ret = 0;
	const char *response_msg = NULL;

	// Obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;
	// void *hierarchical_map = arguments->hierarchical_map;
	HierarchicalRecords *hierarchical_map = arguments->hierarchical_map;

	uint16_t current_offset = 0;

	// Resources specifying if the ZMQ_SNDMORE flag was set in the sender.
	int64_t more = -1;
	size_t more_size = sizeof(more);

	// Code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";

	uint32_t operation = 0; // Hercules instance or dataset structure.
	char mode[MODE_SIZE] = {0};
	// int32_t req_size = 0;
	// char raw_msg[req_size + 1] = {0};
	char number[16] = {0};
	char uri_[URI_] = {0};
	int extra_info = 0;
	int num_characters_read = 0;
	int num_input_read = 0;
	int is_performance_operation = 0;

	// Save the request to be served.
	slog_info("Request - '%s'", req);
	// fprintf(stderr, "Request=%s\n", req);
	if (!strcmp(req, MSG_DECOM_DATASERVERS))
	{
		acks_received++;
		int expected_acks = arguments->args->num_data_servers + 1;
		fprintf(stderr, "[%d/%d] ACK received: %s\n", acks_received, expected_acks, req);
		slog_debug("[%d/%d] ACK received: %s", acks_received, expected_acks, req);
		if (acks_received >= expected_acks)
		{
			Attend_pending_requests();
			malleability_status.store(MALLEABILITY_OFF, std::memory_order_release);
			acks_received = 0;
		}
		return 0;
	}
	// TODO: GET and SET request does not have a consistent format. Try to change it.
	num_input_read = sscanf(req, "%" PRIu32 " %s %s %s %n", &operation, mode, number, uri_, &num_characters_read);

	if (!strcmp(mode, "GET"))
	{
		more = GET_OP;
	}
	else if (!strcmp(mode, "SET"))
	{
		more = SET_OP;
	}
	else if (!strcmp(mode, "SETPERFORMANCE"))
	{
		more = SET_OP;
		is_performance_operation = PERFORMANCE_OP;
	}
	else if (!strcmp(mode, "SETSERVER"))
	{
		int server_id_request = operation;
		imss new_imss;
		new_imss.conns.peer_addr = (ucp_address_t **)malloc(1 * sizeof(ucp_address_t *));

		int oob_sock = -1;
		size_t addr_len = 0;
		int ret = 0;
		// number  == client ip.
		// uri_ == hostname.
		char *added_hostname = uri_;
		slog_debug("connecting to %s:8500", number);
		oob_sock = connect_common(number, 85000, AF_INET);
		if (oob_sock < 0)
		{
			char err_msg[MAX_ERR_MSG_LEN] = {0};
			sprintf(err_msg, "HERCULES_ERR_STAT_WORKER_HELPER_CONNECT_COMMON - i=%d - %s:%d", server_id_request, new_imss.info.ips[server_id_request], new_imss.info.conn_port);
			slog_error("%s", err_msg);
			perror(err_msg);
			return -1;
		}

		char request[REQUEST_SIZE] = {0};
		sprintf(request, "%" PRIu32 " GET %s", arguments->args->id, "HELLO!JOIN");
		// slog_live("ip_address=%s:%d", new_imss.info.ips[i], new_imss.info.conn_port);

		if (send(oob_sock, request, REQUEST_SIZE, 0) < 0)
		{
			perror("HERCULES_ERR_IMSS_OPEN_IMSS_SEND_REQUEST");
			slog_error("HERCULES_ERR_IMSS_OPEN_IMSS_SEND_REQUEST");
			return -1;
		}

		ret = recv(oob_sock, &addr_len, sizeof(addr_len), MSG_WAITALL);
		if (ret < 0)
		{
			perror("HERCULES_ERR_IMSS_OPEN_IMSS_RECV_ADDR_LEN");
			slog_error("HERCULES_ERR_IMSS_OPEN_IMSS_RECV_ADDR_LEN");
			close(oob_sock);
			return -1;
		}

		new_imss.conns.peer_addr[0] = (ucp_address *)malloc(addr_len);
		if (new_imss.conns.peer_addr[0] == NULL)
		{
			perror("HERCULES_ERR_IMSS_OPEN_IMSS_MEMORY_ALLOC");
			slog_error("HERCULES_ERR_IMSS_OPEN_IMSS_MEMORY_ALLOC");
			exit(-1);
		}

		ret = recv(oob_sock, new_imss.conns.peer_addr[0], addr_len, MSG_WAITALL);
		if (ret < 0)
		{
			perror("HERCULES_ERR_IMSS_OPEN_IMSS_RECV_PEER_ADDR");
			slog_error("HERCULES_ERR_IMSS_OPEN_IMSS_RECV_PEER_ADDR");
			close(oob_sock);
			return -1;
		}

		if (close(oob_sock) < 0)
		{
			perror("HERCULES_ERR_IMSS_OPEN_IMSS_CLOSE_OOB_SOCK");
			slog_error("HERCULES_ERR_IMSS_OPEN_IMSS_CLOSE_OOB_SOCK");
		}

		// new_imss.conns.id[server_id_request] = server_id_request;

		client_create_ep_data(arguments->ucp_worker, &data_endpoints[server_id_request], new_imss.conns.peer_addr[0], set_server_err_call_arg);

		// finish malleability comissioning process.
		// Attend_pending_requests();
		comissioning_on = false;

		global_malleability_t = clock() - global_malleability_t;
		global_malleability_time_taken = ((double)global_malleability_t) / (CLOCKS_PER_SEC);
		fprintf(stderr, "Server %d connected in %.4f seconds\n", server_id_request, global_malleability_time_taken);
		slog_debug("Server %d connected in %.4f seconds", server_id_request, global_malleability_time_taken);

		free(new_imss.conns.peer_addr[0]);
		free(new_imss.conns.peer_addr);

		imss_info *imss_info_struct = curr_global_imss;
		// num_storages is increased inside AddIPS.
		fprintf(stderr, "Adding %s on the metadata server.\n", added_hostname);
		AddIPS(imss_info_struct, added_hostname, strlen(added_hostname));
		return 0;
	}
	else if (!strcmp(mode, MSG_REMOVE_SERVER))
	{
		slog_debug("Running decomissioning stage, number=%s", number);
		Decomissioning_stage(arguments, atoi(number));
		return 0;
	}

	// To check this guard condition.
	// if (num_input_read != 5)
	// {
	// 	fprintf(stderr, "HERCULES_ERR_BAD_REQUEST\n");
	// 	slog_error("HERCULES_ERR_BAD_REQUEST");
	// 	return -1;
	// }

	uint64_t block_size_recv = (uint64_t)atoi(number);

	slog_info("mode=%s, operation=%d, number=%s, uri=%s, block_size_recv=%ld, num_characters_read=%d", mode, operation, number, uri_, block_size_recv, num_characters_read);

	// Create an std::string in order to be managed by the map structure.
	std::string key;
	std::string key_for_tree;
	key.assign((const char *)uri_);
	key_for_tree.assign((const char *)uri_);

	// On the tree structure, we preserve the received key.
	// For the map strcuture, we normalice the key (removing the last slash if exists)
	// this allows to remove an extra "find" operation (with and without the last slash).
	// So, we are assuming that imsss://dir/mydir/ == imsss://dir/mydir and imsss://dir/mydir/myfile == imsss://dir/mydir/myfile/
	// TODO: check root case to avoid deleting last slash.
	slog_debug("arguments->args->imss_uri=%s", arguments->args->imss_uri);
	if (key.compare(arguments->args->imss_uri))
	{
		RemoveLastSlash(key);
	}

	// Information associated to the arriving key.
	void *address_ = NULL;
	uint64_t block_size_rtvd = 0;
	dataset_info *dataset;

	// Differentiate between READ and WRITE operations.
	switch (more)
	{
	// Read operations.
	case GET_OP:
	{
		switch (block_size_recv)
		{
		case GETDIR:
		{
			char *buffer = NULL;
			int32_t numelems_indir = -1;
			// Retrieve all elements inside the requested directory.
			// pthread_mutex_lock(&tree_mut);

			// directories always must to have the last slash.
			// ConcatLastSlash(key_for_tree);

			// slog_info("Calling GTree_getdir, key=%s", key_for_tree.c_str());
			// // buffer = GTree_getdir((char *)key_for_tree.c_str(), &numelems_indir, map);
			// buffer = GTree_getdir((char *)key_for_tree.c_str(), &numelems_indir, hierarchical_map);
			// slog_info("[workers] Ending GTree_getdir, key=%s, numelems_indir=%d", key_for_tree.c_str(), numelems_indir);
			buffer = hierarchical_map->HierarchicalMapListDir(key.c_str(), &numelems_indir);

			if (numelems_indir == -1)
			{ // error case.
				// pthread_mutex_lock(&lock_network);
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("HERCULES_ERR_STATWORKER_NODIR");
					slog_error("HERCULES_ERR_STATWORKER_NODIR");
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				// pthread_mutex_unlock(&lock_network);
				break;
			}
			if (numelems_indir == 0)
			{ // empty directory case.
				// pthread_mutex_lock(&lock_network);
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, MSG_EMPTY_DIRECTORY, STRING, arguments->worker_uid) < 0)
				{
					perror("HERCULES_ERR_STATWORKER_NODIR");
					slog_error("HERCULES_ERR_STATWORKER_NODIR");
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				// pthread_mutex_unlock(&lock_network);
				break;
			}

			// Send the serialized set of elements within the requested directory.
			msg_t m;
			m.size = numelems_indir * URI_;
			m.data = buffer;

			slog_info("numelems_indir=%ld, size=%ld", numelems_indir, m.size);
			// pthread_mutex_lock(&lock_network);
			if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, (char *)&m, MSG, arguments->worker_uid) < 0)
			{
				perror("HERCULES_ERR_WORKER_SEND_STREAM_GETDIR");
				slog_error("HERCULES_ERR_WORKER_SEND_STREAM_GETDIR");
				// pthread_mutex_unlock(&lock_network);
				return -1;
			}
			// pthread_mutex_unlock(&lock_network);

			slog_debug("[workers] buffer=%s", buffer);
			free(buffer);
		}
		break;
		case READ_OP:
		{ // case 0.
			slog_debug("[READ_OP]");
			// Check if there was an associated block to the key.
			std::unique_lock<std::mutex> lck(operation_lock);
			int err = TIMING(hierarchical_map->HierarchicalMapGet(key, &address_, &block_size_rtvd), "READ_OP,HierarchicalMapGet", int32_t, arguments->thread_id);
			// fprintf(stderr, "%s returned %d\n", key.c_str(), ret);

			slog_debug("map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// Send the error code block.
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("HERCULES_ERR_STAT_WORKER_READ_OP_SEND_DYNAMIC_STREAM_NON_EXISTING_BLOCK");
					slog_error("HERCULES_ERR_STAT_WORKER_READ_OP_SEND_DYNAMIC_STREAM_NON_EXISTING_BLOCK");
					return -1;
				}
			}
			else
			{ // dataset exists.
				if (operation == IMSS_INFO)
				{ // Hercules instance case.
					slog_info("Sending IMSS_INFO struct");
					err = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, address_, IMSS_INFO, arguments->worker_uid);
					// imss_info *my_imss = (imss_info *)address_;
				}
				else
				{
					// Send the requested block.
					dataset = (dataset_info *)address_;
					slog_debug("Before dataset->n_open=%d, dataset uri=%s, operation=%d", dataset->n_open, dataset->uri_, operation);

					// Check if it not a dirty dataset.
					// To know that, the status dataset must to be marked as "dirty".
					if (!strncmp(dataset->status, STATUS_DIRTY, strlen(STATUS_DIRTY)))
					{
						slog_debug("%s exists but is a dirty block", key.c_str());
						if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
						{
							perror("HERCULES_ERR_STAT_WORKER_READ_OP_SEND_DYNAMIC_STREAM_DIRTY_BLOCK");
							slog_error("HERCULES_ERR_STAT_WORKER_READ_OP_SEND_DYNAMIC_STREAM_DIRTY_BLOCK");
							return -1;
						}
					}
					else
					{
						// Checks if the clients wants to open the file.
						switch (operation)
						{
						case 1: // file opened.
							pthread_mutex_lock(&memory_protect);
							dataset->n_open += 1;
							pthread_mutex_unlock(&memory_protect);
							slog_debug("File opened");
							break;
						default:
							break;
						}
						slog_debug("After dataset->n_open=%d", dataset->n_open);

#ifdef DPRINTF
						slog_debug("Printing intervals of dataset.");
						PrintIntervals((dataset_info *)address_);
#endif
						// pthread_mutex_lock(&lock_network);
						// err = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, (char *)&m, MSG, arguments->worker_uid);
						err = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, address_, DATASET_INFO, arguments->worker_uid);
						if (err < 0)
						{
							perror("HERCULES_ERR_STAT_WORKER_READ_OP_SEND_STREAM");
							slog_error("HERCULES_ERR_STAT_WORKER_READ_OP_SEND_STREAM");
							// pthread_mutex_unlock(&lock_network);
							return -1;
						}
						// pthread_mutex_unlock(&lock_network);
					}
				}
			}
		}
		break;
		case RELEASE:
		{
			slog_debug("[stat_worker_thread][READ_OP][RELEASE] Deleting endpoint with %" PRIu64 "", arguments->worker_uid);
			map_server_eps_erase(map_server_eps, arguments->worker_uid, arguments->ucp_worker);
			// ucp_destroy(arguments->ucp_context);
			slog_debug("[stat_worker_thread][READ_OP][RELEASE] Endpoints deleted ");
		}
		break;
		case DELETE_OP:
		{
			slog_debug("DELETE_OP");
			std::unique_lock<std::mutex> lck(operation_lock);
			int err = TIMING(hierarchical_map->HierarchicalMapGet(key, &address_, &block_size_rtvd), "DELETE_OP,HierarchicalMapGet", int32_t, arguments->thread_id);

			slog_debug("map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// Send the error code block.
				// pthread_mutex_lock(&lock_network);
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("ERRIMSS_WORKER_SENDERR");
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				// pthread_mutex_unlock(&lock_network);
			}
			else
			{
				pthread_mutex_lock(&memory_protect);
				dataset = (dataset_info *)address_;

				// Checks if the clients wants to unlink the file.
				switch (operation)
				{
				case 4: // unlink.
					strncpy(dataset->status, STATUS_DEST, strlen(STATUS_DEST) + 1);
					slog_debug("Dataset mark as DEST:%s", dataset->status);
					break;
				default:
					break;
				}

				if (dataset->n_open == 0) // if no more process has the file opened.
				{
					// 	// TODO: before delete, it's better to check if the file is on the structures.
					strncpy(dataset->status, STATUS_DIRTY, strlen(STATUS_DIRTY) + 1);
					slog_debug("Dataset mark as DIRTY:%s", dataset->status);
					hierarchical_map->HierarchicalMapPutInGarbageCollector(key);
					hierarchical_map->HierarchicalMapDeleteEntry(key);
					response_msg = MSG_DELETE_OP;
				}
				else
				{
					response_msg = MSG_NODELETE_OP;
				}
				pthread_mutex_unlock(&memory_protect);

				slog_debug("response_msg=%s", response_msg);
				ret = SendConfirmationMessage(arguments, response_msg);
				if (ret == 0)
				{
					perror("ERR_HERCULES_PUBLISH_DELETEMSG");
					slog_error("ERR_HERCULES_PUBLISH_DELETEMSG");
					return -1;
				}
			}
		}
		break;
		case RENAME_OP:
		{
			int ret = -1;
			std::size_t found = key_for_tree.find(',');
			if (found != std::string::npos)
			{
				// Keys for the tree.
				std::string old_key_tree = key_for_tree.substr(0, found);
				std::string new_key_tree = key_for_tree.substr(found + 1);

				// Keys for the map.
				std::string old_key;
				std::string new_key;
				old_key = old_key_tree;
				new_key = new_key_tree;

				// Checks and removes the last slash for the keys to be used on the map.
				RemoveLastSlash(old_key);
				RemoveLastSlash(new_key);

				slog_debug("[RENAME] old_key=%s, new_key=%s, old_key_tree=%s, new_key_tree=%s", old_key.c_str(), new_key.c_str(), old_key_tree.c_str(), new_key_tree.c_str());

				// RENAME MAP
				std::unique_lock<std::mutex> lck(operation_lock);
				ret = hierarchical_map->HierarchicalMapRenameRegularFile(old_key, new_key);
				slog_live("rename metadata stat worker=%d", ret);

				// RENAME TREE
				// if (ret != -1)
				// {
				// 	ret = GTree_rename((char *)old_key_tree.c_str(), (char *)new_key_tree.c_str());
				// 	slog_debug("[RENAME] GTree_rename=%d", ret);
				// }
			}

			if (ret == -1)
			{
				response_msg = MSG_ERROR_OP;
			}
			else
			{
				response_msg = MSG_RENAME_OP;
			}

			ret = SendConfirmationMessage(arguments, response_msg);
			if (ret == 0)
			{
				perror("HERCULES_ERR_PUBLISH_RENAMEMSG");
				slog_error("HERCULES_ERR_PUBLISH_RENAMEMSG");
				// pthread_mutex_unlock(&lock_network);
				return -1;
			}
		}
		break;
		case RENAME_DIR_DIR_OP:
		{
			int ret = -1;
			std::size_t found = key_for_tree.find(',');
			if (found != std::string::npos)
			{
				std::string old_key_tree = key_for_tree.substr(0, found);
				std::string new_key_tree = key_for_tree.substr(found + 1);

				// Keys for the map.
				std::string old_key;
				std::string new_key;
				old_key = old_key_tree;
				new_key = new_key_tree;
				// Checks and removes the last slash for the keys to be used on the map.
				RemoveLastSlash(old_key);
				RemoveLastSlash(new_key);

				slog_debug("[RENAME_DIR_DIR_OP] old_key=%s, new_key=%s, old_key_tree=%s, new_key_tree=%s", old_key.c_str(), new_key.c_str(), old_key_tree.c_str(), new_key_tree.c_str());
				GNode *gnode = NULL;
				// RENAME MAP
				std::unique_lock<std::mutex> lck(operation_lock);
				ret = TIMING(hierarchical_map->BackEndHierarchicalMapRenameDirDir(old_key, new_key, &gnode), "RENAME_DIR_DIR_OP,BackEndHierarchicalMapRenameDirDir", int32_t, arguments->thread_id);

				// Rename the old directory on the hierarchical map.
				slog_debug("Renaming %s to %s on the directory map", old_key.c_str(), new_key.c_str());
				ret = TIMING(hierarchical_map->HierarchicalMapRenameKey(old_key.c_str(), new_key.c_str()), "RENAME_DIR_DIR_OP,HierarchicalMapRenameKey", int32_t, arguments->thread_id);
				if (ret != 0)
				{
					slog_error("HERCULES_ERR_HIERARCHICAL_MAP_RENAMING_STAT_KEY");
					perror("HERCULES_ERR_HIERARCHICAL_MAP_RENAMING_STAT_KEY");
					response_msg = MSG_ERROR_OP;
				}
				else
				{
					response_msg = MSG_RENAME_OP;
				}

				ret = SendConfirmationMessage(arguments, response_msg);
				if (ret == 0)
				{
					perror("HERCULES_ERR_SEND_CONFIRMATION_RENAME_DIR_DIR_OP");
					slog_error("HERCULES_ERR_SEND_CONFIRMATION_RENAME_DIR_DIR_OP");
					return -1;
				}
				// slog_debug("old_dir=%s, dir_dest=%s, ret=%d", old_key_tree.c_str(), new_key_tree.c_str(), ret);
				return 0;
			}
			else
			{
				slog_live("No extra characters found.\n");
				ret = -1;
				// fprintf(stderr, "No extra characters found.\n");
			}

			if (ret == -1)
			{
				response_msg = MSG_ERROR_OP;
				// pthread_mutex_lock(&lock_network);
				SendConfirmationMessage(arguments, response_msg);
				// pthread_mutex_unlock(&lock_network);
			}
		}
		break;
		case CLOSE_OP:
		{
			slog_debug("CLOSE_OP");
			std::unique_lock<std::mutex> lck(operation_lock);
			int err = TIMING(hierarchical_map->HierarchicalMapGet(key, &address_, &block_size_rtvd), "CLOSE_OP,HierarchicalMapGet", int32_t, arguments->thread_id);
			slog_debug("map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// Send the error code block.
				// pthread_mutex_lock(&lock_network);
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("HERCULES_ERR_STAT_CLOSE_OP");
					slog_error("HERCULES_ERR_STAT_CLOSE_OP");
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				// pthread_mutex_unlock(&lock_network);
			}
			else
			{
				pthread_mutex_lock(&memory_protect);
				dataset = (dataset_info *)address_;
				// Checks if the clients wants to open the file.
				slog_debug("Closing file, dataset->n_open=%d", dataset->n_open);
				if (dataset->n_open > 0)
				{
					dataset->n_open -= 1;
				}

				slog_debug("After dataset->n_open=%d, status=%s", dataset->n_open, dataset->status);
				// if file status is marked as "dest", it is delete after close.
				if (!strncmp(dataset->status, STATUS_DEST, strlen(STATUS_DEST)) && dataset->n_open == 0)
				{
					// Mark the dataset as dirty to prevent future reads.
					strncpy(dataset->status, STATUS_DIRTY, strlen(STATUS_DIRTY) + 1);
					slog_debug("Dataset mark as %s", dataset->status);
					// map->put_garbage_collector(key_for_tree);
					hierarchical_map->HierarchicalMapPutInGarbageCollector(key);
					hierarchical_map->HierarchicalMapDeleteEntry(key);
					response_msg = MSG_DELETE_OP;
				}
				else
				{
					response_msg = MSG_CLOSE_OP;
				}
				pthread_mutex_unlock(&memory_protect);

				// pthread_mutex_lock(&lock_network);
				ret = SendConfirmationMessage(arguments, response_msg);
				// pthread_mutex_unlock(&lock_network);
				if (ret == 0)
				{
					perror("HERCULES_ERR_PUBLISH_DELETEMSG");
					slog_error("HERCULES_ERR_PUBLISH_DELETEMSG");
					return -1;
				}
			}
		}
		break;
		case OPEN_OP:
		{
			slog_debug("OPEN_OP");
			std::unique_lock<std::mutex> lck(operation_lock);
			int err = TIMING(hierarchical_map->HierarchicalMapGet(key, &address_, &block_size_rtvd), "OPEN_OP,HierarchicalMapGet", int32_t, arguments->thread_id);

			slog_debug("map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// Send the error code block.
				// pthread_mutex_lock(&lock_network);
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("HERCULES_ERR_STAT_OPEN_OP_SEND_STREAM");
					slog_error("HERCULES_ERR_STAT_OPEN_OP_SEND_STREAM");
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				// pthread_mutex_unlock(&lock_network);
			}
			else
			{
				dataset = (dataset_info *)address_;
				if (!strncmp(dataset->status, STATUS_DIRTY, strlen(STATUS_DIRTY)))
				{ // dataset is dirty. We will delete from the garbage collector vector.
					slog_debug("Dirty dataset found %s on OPEN operation, recovering from the garbage collector.", key.c_str());
					// int ret = map->garbage_collector_pop(key);
					int ret = hierarchical_map->HierarchicalMapPopFromGarbageCollector(key);
					if (ret == 0)
					{
						slog_debug("%s has not been found on the garbage collector map, but it found as dirty.", key.c_str());
					}
					ClearIntervalsStructure(dataset);
					strncpy(dataset->status, STATUS_ATACH, strlen(STATUS_ATACH) + 1);
				}

				slog_debug("Before dataset->n_open=%d", dataset->n_open);
				pthread_mutex_lock(&memory_protect);
				dataset->n_open += 1;
				slog_debug("After dataset->n_open=%d, status=%s", dataset->n_open, dataset->status);
				pthread_mutex_unlock(&memory_protect);
				response_msg = MSG_OPEN_OP;
				ret = SendConfirmationMessage(arguments, response_msg);
				if (ret == 0)
				{
					perror("HERCULES_ERR_PUBLISH_DELETEMSG");
					slog_error("HERCULES_ERR_PUBLISH_DELETEMSG");
					return -1;
				}
			}
		}
		break;
		default:
			fprintf(stderr, "HERCULES_ERR_INVALID_GET_CASE\n");
			slog_error("HERCULES_ERR_INVALID_GET_CASE");
			break;
		}
		break;
	}
	// Write operations.
	case SET_OP:
	{
		size_t msg_length = 0;
		void *data_ref = NULL;
		SendConfirmationMessage(arguments, MSG_OK_OP);
		// fprintf(stderr, "Set request %s\n", key.c_str());
		switch (is_performance_operation)
		{
		case PERFORMANCE_OP:
		{
			// TODO: this function can be called on a thread.
			// This avoid blocking future operations.
			// fprintf(stderr, "Receiving performance, client=%lu\n", arguments->worker_uid);
			// NUMBER_OF_LAUNCHED_THREADS++;
			Malleability((void *)arguments);
			// NUMBER_OF_LAUNCHED_THREADS--;
			// fprintf(stderr, "Performance received, client=%lu\n", arguments->worker_uid);
		}
		break;
		default:
		{
			slog_debug("[SET_OP] Creating dataset %s", key.c_str());
			std::unique_lock<std::mutex> lck(operation_lock);
			pthread_mutex_lock(&memory_protect);
			// if (!TIMING(HierarchicalMapGet(hierarchical_map, key, &address_, &block_size_rtvd), "HierarchicalMapGet", int32_t, arguments->thread_id))
			if (!TIMING(hierarchical_map->HierarchicalMapGet(key, &address_, &block_size_rtvd), "HierarchicalMapGet", int32_t, arguments->thread_id))
			{ // If the record was not already stored, add the block.
				// fprintf(stderr, "%s not found", key.c_str());
				slog_debug("Recv dynamic buffer size %ld", block_size_recv);
				// Get the length of the message to be received.
				size_t length = 0;
				int32_t ret = -1;
				int reused_memory = 1;
				// pthread_mutex_lock(&lock_network);
				length = TIMING(get_recv_data_length(arguments->ucp_worker, arguments->worker_uid), "get_recv_data_length", size_t, arguments->thread_id);
				if (length == 0)
				{
					perror("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
					slog_error("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
					pthread_mutex_unlock(&memory_protect);
					return -1;
				}
				// Receive the block into the buffer.
				void *buffer = NULL;
				if (operation == IMSS_INFO)
				{ // Hercules instance case.
					buffer = (void *)TIMING(malloc(length * sizeof(char)), "malloc buffer for imss info", void *, arguments->thread_id);
				}
				else
				{
					// try to reutilizate memory from the memory pool.
					// if (TIMING(StsQueue.size(mem_pool), "mem_pool size", int, arguments->thread_id) > 0)
					// {
					// 	buffer = (void *)TIMING(StsQueue.pop(mem_pool), "mem_pool pop", void *, arguments->thread_id);
					// }
					// if (buffer == NULL)
					// {
					// 	// if there are not free block on the memory pool, then perform dynamic alloc.
					// 	buffer = (void *)TIMING(malloc(length * sizeof(char)), "malloc buffer", void *, arguments->thread_id);
					// 	reused_memory = 0;
					// }
					// buffer = calloc(1, sizeof(dataset_info));
					buffer = calloc(1, length);
				}
				if (buffer == NULL)
				{
					char err_msg[MAX_ERR_MSG_LEN] = {0};
					sprintf(err_msg, "HERCULES_ERR_STAT_SET_OP_MEMORY_ALLOC: %s", key.c_str());
					perror(err_msg);
					slog_error("%s", err_msg);
					pthread_mutex_lock(&memory_protect);
					return -1;
				}
				// INSERT THE ELEMENT IN THE MAP.
				int32_t insert_successful = -1;
				// Insert the received uri into the directory tree.
				// slog_debug("Inserting %s into directory tree", key.c_str());
				// GNode *new_node = NULL;
				// insert_successful = TIMING(GTree_insert((char *)key_for_tree.c_str(), &new_node), "*GTree_insert", int32_t, arguments->thread_id);
				// slog_debug("insert_successful=%d, new node add=%p", insert_successful, new_node);
				// if (insert_successful == -1)
				// {
				// 	slog_error("HERCULES_ERR_METADATA_WORKER_GTREEINSERT_SET_OP");
				// 	perror("HERCULES_ERR_METADATA_WORKER_GTREEINSERT_SET_OP");
				// 	free(buffer);
				// 	pthread_mutex_unlock(&memory_protect);
				// 	return -1;
				// }

				if (operation == IMSS_INFO)
				{ // Hercules instance case.
					ret = TIMING(recv_dynamic_stream(arguments->ucp_worker, arguments->server_ep, buffer, IMSS_INFO, arguments->worker_uid, length), "recv_dynamic_stream IMSS_INFO", int32_t, arguments->thread_id);
					// save the pointer to the hercules instance to be access on malleability.
					// arguments->hercules_info_struct = (imss_info *)buffer;
					curr_global_imss = (imss_info *)buffer;
					slog_debug("Hercules Instance received, num of initial servers = %d", curr_global_imss->num_storages);
				}
				else
				{
					ret = TIMING(recv_dynamic_stream(arguments->ucp_worker, arguments->server_ep, buffer, DATASET_INFO, arguments->worker_uid, length), "recv_dynamic_stream DATASET_INFO", int32_t, arguments->thread_id);
					// dataset_info *struct_ = (dataset_info *)buffer;
					slog_debug("END Recv dynamic, n_server_when_created=%d", ((dataset_info *)buffer)->n_servers_when_created);
				}

				insert_successful = TIMING(hierarchical_map->HierarchicalMapPut(key, buffer, length, reused_memory, NULL, 1), "HierarchicalMapPut", int, arguments->thread_id);
				slog_debug("map->put (key %s) err %d", key.c_str(), insert_successful);

				if (insert_successful != 0)
				{
					slog_error("HERCULES_ERR_METADATA_WORKER_MAPPUT_SET_OP");
					perror("HERCULES_ERR_METADATA_WORKER_MAPPUT_SET_OP");
					free(buffer);
					pthread_mutex_unlock(&memory_protect);
					return -1;
				}
				pthread_mutex_unlock(&memory_protect);

				if (ret < 0)
				{
					perror("HERCULES_ERR_STAT_WORKER_HELPER_RECV_DYNAMIC_STREAM");
					slog_error("HERCULES_ERR_STAT_WORKER_HELPER_RECV_DYNAMIC_STREAM");
					free(buffer);
					// pthread_mutex_unlock(&memory_protect);
					return -1;
				}

				// Update the pointer.
				// arguments->pt += block_size_recv;
				slog_debug("Dataset %s has been created.", key.c_str());
			}
			// If it was already stored:
			else
			{
				// fprintf(stderr, "%s found", key.c_str());
				// pthread_mutex_lock(&memory_protect);
				dataset = (dataset_info *)address_;
				if (!strncmp(dataset->status, STATUS_DIRTY, strlen(STATUS_DIRTY)))
				{ // dataset is dirty. We will delete from the garbage collector.
					slog_debug("Dirty dataset found %s on SET operation, recovering from the garbage collector.", key.c_str());
					// int ret = map->garbage_collector_pop(key);
					int ret = hierarchical_map->HierarchicalMapPopFromGarbageCollector(key);
					if (ret == 0)
					{
						slog_debug("%s has not been found on the garbage collector map, but it found as dirty.", key.c_str());
					}
					ClearIntervalsStructure(dataset);
					strncpy(dataset->status, STATUS_ATACH, strlen(STATUS_ATACH) + 1);
				}
				pthread_mutex_unlock(&memory_protect);

				// Follow a certain behavior if the received block was already stored.
				slog_debug("LOCAL DATASET_UPDATE %ld", block_size_recv);
				// switch (1) // TO CKECK!
				// {
				// // Update where the blocks of a LOCAL dataset have been stored.
				// case LOCAL_DATASET_UPDATE:
				// {
				// 	// pthread_mutex_lock(&lock_network);
				// 	msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
				// 	if (msg_length == 0)
				// 	{
				// 		perror("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
				// 		slog_error("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
				// 		// pthread_mutex_unlock(&lock_network);
				// 		return -1;
				// 	}
				// 	data_ref = malloc(msg_length);
				// 	if (data_ref == NULL)
				// 	{
				// 		perror("HERCULES_ERR_STAT_LOCAL_DATASET_UPDATE");
				// 		slog_error("HERCULES_ERR_STAT_LOCAL_DATASET_UPDATE");
				// 		// pthread_mutex_unlock(&lock_network);
				// 		return -1;
				// 	}
				// 	msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, data_ref, msg_length, arguments->worker_uid, 0);
				// 	if (msg_length == 0)
				// 	{
				// 		perror("HERCULES_ERR_METADATA_WORKER_RECV_DATA_SET_OP");
				// 		slog_error("HERCULES_ERR_METADATA_WORKER_RECV_DATA_SET_OP");
				// 		free(data_ref);
				// 		// pthread_mutex_unlock(&lock_network);
				// 		return -1;
				// 	}

				// 	uint32_t data_size = RESPONSE_SIZE; // MIRAR

				// 	// Value to be written in certain positions of the vector.
				// 	uint16_t *update_value = (uint16_t *)(data_size + (char *)data_ref - 8);
				// 	// Positions to be updated.
				// 	uint32_t *update_positions = (uint32_t *)data_ref;

				// 	// pthread_mutex_lock(&memory_protect);
				// 	// Set of positions that are going to be updated (those are just under the concerned dataset but not pointed by it).
				// 	uint16_t *data_locations = (uint16_t *)((char *)address_ + sizeof(dataset_info));

				// 	// Number of positions to be updated.
				// 	int32_t num_pos_toupdate = (data_size / sizeof(uint32_t)) - 2;

				// 	// Perform the update operation.
				// 	for (int32_t i = 0; i < num_pos_toupdate; i++)
				// 		data_locations[update_positions[i]] = *update_value;

				// 	// Answer the client with the update.
				// 	slog_debug("[STAT_WORKER] Updating existing dataset %s.", key.c_str());
				// 	// char answer[] = "UPDATED!\0";
				// 	response_msg = MSG_UPDATED_OP;
				// 	ret = SendConfirmationMessage(arguments, response_msg);
				// 	if (ret == 0)
				// 	{
				// 		slog_error("HERCULES_ERR_METADATA_WORKER_SEND_DATA_SET_OP");
				// 		perror("HERCULES_ERR_METADATA_WORKER_SEND_DATA_SET_OP");
				// 		free(data_ref);
				// 		// pthread_mutex_unlock(&lock_network);
				// 		return -1;
				// 	}
				// 	free(data_ref);

				// 	// pthread_mutex_unlock(&lock_network);
				// 	break;
				// }

				// default:
				// { // always executed.
				slog_debug("num_input_read=%d, operation=%d", num_input_read, operation);
				int new_server_status = 0;
				int flag = 0;
				switch (num_input_read)
				{
				case 4: // we expect to get 4 values in a normal case.
					// we look for extra information.
					if (req[num_characters_read] != '\0')
					{
						slog_live("Extra characters found after expected input: '%s'\n", &req[num_characters_read]);
						// get the server status to be set.
						sscanf(&req[num_characters_read], "%d", &new_server_status);
						flag = 1;
					}
					else
					{
						slog_live("No extra characters found.\n");
					}
					break;
				default:
					break;
				}

				if (flag)
				{
					unsigned long num_active_storages = atol(number);
					int delete_dataserver_indx = operation;
					slog_debug("[STAT_WORKER] Updating existing dataset %s, number_active_storage_servers=%lu.", key.c_str(), num_active_storages);
					pthread_mutex_lock(&memory_protect);
					char *address_aux = (char *)address_;
					// fprintf(stderr, "block_size_rtvd=%lu\n", block_size_rtvd);
					imss_info *imss_info_ = (imss_info *)malloc(block_size_rtvd * sizeof(imss_info));
					memcpy(imss_info_, address_aux, sizeof(imss_info));

					// imss_info_->num_active_storages--;
					imss_info_->num_active_storages = num_active_storages;
					memcpy(address_aux, imss_info_, sizeof(imss_info));

					// skip imss_info and num_storages.
					address_aux += sizeof(imss_info);
					address_aux += imss_info_->num_storages * LINE_LENGTH;
					// reserve memory to store the status list.
					// imss_info_->status = (int *)malloc(imss_info_->num_storages * sizeof(int));
					// copy the status list.
					// memcpy(imss_info_->status, address_aux, imss_info_->num_storages * sizeof(int));

					// int current_num_storages = imss_info_->num_storages;
					// slog_debug("[STAT_WORKER] prev. num data servers=%d", imss_info_->num_storages);
					// slog_debug("[STAT_WORKER] changing data server %d with status=%d to a new status=%d", delete_dataserver_indx, imss_info_->status[delete_dataserver_indx], new_server_status);
					// free(imss_info_->ips[delete_dataserver_indx]);
					// imss_info_->status[delete_dataserver_indx] = new_server_status;
					// slog_debug("[STAT_WORKER] new num data servers=%d, new status=%d", imss_info_->num_active_storages, imss_info_->status[delete_dataserver_indx]);
					// address_ += sizeof(imss_info);
					// address_ += imss_info_->num_storages * LINE_LENGTH;
					// memcpy(address_aux, imss_info_->status, imss_info_->num_storages * sizeof(int));
					// move the pointer to the arr_num_active_storages position.
					// address_aux += (imss_info_->num_storages * sizeof(int));

					// copy the arr_num_active_storages list.
					// imss_info_->arr_num_active_storages = (int *)malloc(imss_info_->num_storages * sizeof(int));
					// memcpy(imss_info_->arr_num_active_storages, address_aux, imss_info_->num_storages * sizeof(int));
					// set the value.
					// imss_info_->arr_num_active_storages[delete_dataserver_indx] = imss_info_->num_active_storages;
					// memcpy(address_aux, imss_info_->arr_num_active_storages, imss_info_->num_storages * sizeof(int));
					free(imss_info_);
					pthread_mutex_unlock(&memory_protect);
				}
				else
				{
					// pthread_mutex_lock(&lock_network);
					size_t length = -1;
					length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
					if (length == 0)
					{
						slog_error("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_UPDATE_DATASET");
						perror("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_UPDATE_DATASET");
						// pthread_mutex_unlock(&memory_protect);
						// pthread_mutex_unlock(&lock_network);
						return -1;
					}

					void *buffer = (void *)malloc(length);
					// buffer = address_;
					if (buffer == NULL)
					{
						perror("HERCULES_ERR_STAT_MEMORY_ALLOC");
						slog_error("HERCULES_ERR_STAT_MEMORY_ALLOC");
						// pthread_mutex_unlock(&memory_protect);
						// pthread_mutex_unlock(&lock_network);
						return -1;
					}
					// Receive the block into the buffer.
					ret = recv_dynamic_stream(arguments->ucp_worker, arguments->server_ep, buffer, DATASET_INFO, arguments->worker_uid, length);
					if (ret < 0)
					{
						perror("HERCULES_ERR_STAT_SET_OP_RECV_STREAM");
						slog_error("HERCULES_ERR_STAT_SET_OP_RECV_STREAM");
						// pthread_mutex_unlock(&memory_protect);
						// pthread_mutex_unlock(&lock_network);
						return -1;
					}

					// prev. dataset.
					slog_debug("Updating dataset,\n printing intervals of prev. dataset.");
#ifdef DPRINTF
					PrintIntervals(dataset);
#endif

					dataset_info *received_struct = (dataset_info *)buffer;

					slog_debug("printing intervals of received dataset.");
#ifdef DPRINTF
					PrintIntervals(received_struct);
#endif

					// check if the received interval is highest that the prev. one.
					// first check who has most intervals.
					// if (dataset->num_intervals <= received_struct->num_intervals && dataset->intervals != NULL)
					{
						// free the old intrvals.
						// if (dataset->intervals != NULL)
						// {
						// 	for (size_t i = 0; i < dataset->num_intervals; i++)
						// 	{
						// 		free(dataset->intervals[i]);
						// 	}
						// 	free(dataset->intervals);
						// }

						// memcpy(dataset, received_struct, sizeof(dataset_info));
						// dataset->intervals = (IntervalEntry **)malloc(sizeof(IntervalEntry *) * received_struct->num_intervals);
						pthread_mutex_lock(&memory_protect);
						for (size_t i = 0; i < received_struct->num_intervals; i++)
						{
							// Allocate memory for each individual IntervalEntry and copy the data.
							// dataset->intervals[i] = (IntervalEntry *)malloc(sizeof(IntervalEntry));
							// memcpy(dataset->intervals[i], received_struct->intervals[i], sizeof(IntervalEntry));
							SetInterval(dataset, received_struct->intervals[i]->value, received_struct->intervals[i]->left_interval, received_struct->intervals[i]->right_interval);
						}
						pthread_mutex_unlock(&memory_protect);

						slog_debug("Printing intervals of the updated dataset.");
#ifdef DPRINTF
						PrintIntervals((dataset_info *)dataset);
#endif

						// memcpy((dataset_info *)address_, (dataset_info *)buffer, sizeof(dataset_info));
						// memcpy(dataset->intervals, ((dataset_info *)buffer)->intervals, sizeof(dataset->intervals) * dataset->num_intervals);
					}
					// Free the received buffer.
					ClearIntervalsStructure(received_struct);
					// if (received_struct->intervals != NULL)
					// {
					// 	for (size_t i = 0; i < received_struct->num_intervals; i++)
					// 	{
					// 		free(received_struct->intervals[i]);
					// 	}
					// 	free(received_struct->intervals);
					// }
					free(received_struct);
					received_struct = NULL;
					//  slog_debug("[STAT_WORKER] End Updating existing dataset %s.", key.c_str());
				}

				slog_debug("[STAT_WORKER] End Updating existing dataset %s.", key.c_str());
				break;
			}
			// pthread_mutex_unlock(&memory_protect);
		}
		break;
		}
	}
	break;
	default:
		fprintf(stderr, "HERCULES_ERR_INVALID_SET_GET_CASE\n");
		slog_error("HERCULES_ERR_INVALID_SET_GET_CASE");
		break;
	}

	slog_debug(" Terminated meta helper");

	return 0;
}

/**
 * @brief Dispatcher thread method distributing clients among the pool of metadata server threads. It asign a thread of a server (metadata or data) to attend future
 * request of that client by sending its address to the client.
 *
 * @param th_argv arguments for this method.
 * @return void*
 */
void *Dispatcher(void *th_argv)
{
	// Enable thread cancellation
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

	// Set the cancellation type to deferred
	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

	// Cast from generic pointer type to p_argv struct type pointer.
	p_argv *arguments = (p_argv *)th_argv;

	// uint32_t client_id_ = 0;
	struct sockaddr_in server_addr;
	socklen_t addrlen = sizeof(server_addr);
	int ret = 0;
	// int listenfd = -1;
	int optval = 1;
	char *tmp_file_path = arguments->tmp_file_path;
	u_int16_t hercules_thread_pool_size = arguments->hercules_thread_pool_size;
	fprintf(stderr, "Thread pool size=%d\n", hercules_thread_pool_size);
	int client_id_counter = 0;

	// Get a socket file descriptor.
	global_server_fd_thread = socket(AF_INET, SOCK_STREAM, 0);
	if (global_server_fd_thread < 0)
	{
		perror("HERCULES_ERR_DISPATCHER_SOCKET");
		slog_fatal("HERCULES_ERR_DISPATCHER_SOCKET");
		ready(tmp_file_path, "ERROR");
		pthread_exit(NULL);
	}

	// To reuse the address and port.
	ret = setsockopt(global_server_fd_thread, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
	if (ret < 0)
	{
		perror("HERCULES_ERR_DISPATCHER_SET_SOCKET_OPT");
		slog_fatal("HERCULES_ERR_DISPATCHER_SET_SOCKET_OPT");
		ready(tmp_file_path, "ERROR");
		pthread_exit(NULL);
	}

	// Obtenemos la dirección del servidor
	bzero((char *)&server_addr, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = INADDR_ANY;
	server_addr.sin_port = htons(arguments->port);

	// Asociamos el socket a la dirección del servidor
	if (bind(global_server_fd_thread, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
	{
		perror("HERCULES_ERR_DISPATCHER_BIND");
		ready(tmp_file_path, "ERROR");
		pthread_exit(NULL);
	}

	// Prepare to accept connections.
	ret = listen(global_server_fd_thread, 100);
	if (ret < 0)
	{
		perror("HERCULES_ERR_DISPATCHER_LISTEN");
		ready(tmp_file_path, "ERROR");
		pthread_exit(NULL);
	}

	slog_info("global_server_fd_thread=%d", global_server_fd_thread);

	/* Accept next connection */
	int new_socket = -1;
	while (1)
	{
		ucs_status_t status;
		// slog_debug("[DISPATCHER] Waiting for connection requests.");
		// fprintf(stderr, "[DISPATCHER] Waiting for connection requests.\n");
		new_socket = accept(global_server_fd_thread, (struct sockaddr *)&server_addr, &addrlen);

		if (global_finish_dispatcher != RUNNING_SERVER_STATUS)
		{ // This server is shutting down. No more connections are allowed.
			slog_info("Shutdown received on dispatcher thread.");
			Shutdown_server();
			fprintf(stderr, "Ending dispatcher thread.\n");
			slog_info("Ending dispatcher thread.");
			pthread_exit(NULL);
		}

		if (new_socket < 0)
		{
			// slog_error("ERR_HERCULES_DISPATCHER_ACCEPT");
			continue;
		}

		// Allocate memory for arguments to pass to the new thread.
		client_handler_args *args = (client_handler_args *)malloc(sizeof(client_handler_args));
		if (args == NULL)
		{
			perror("HERCULES_ERR_DISPATCHER_MEMORY_ALLOC");
			slog_error("HERCULES_ERR_DISPATCHER_MEMORY_ALLOC");
			close(new_socket);
			continue;
		}

		args->client_socket = new_socket;
		args->client_id_counter = client_id_counter++;
		args->hercules_thread_pool_size = hercules_thread_pool_size;

		// new thread to attend this client connection.
		pthread_t client_thread;
		ret = pthread_create(&client_thread, NULL, HandleClient, (void *)args);
		if (ret != 0)
		{
			perror("HERCULES_ERR_PTHREAD_CREATE_HANDLE_CLIENT");
			slog_error("HERCULES_ERR_PTHREAD_CREATE_HANDLE_CLIENT");
			free(args);
			close(new_socket); // Close the socket if we can't handle it.
		}
		else
		{
			// Detach the thread. This means the resources of the thread
			// will be automatically released when it terminates.
			pthread_detach(client_thread);
		}
	}
	close(global_server_fd_thread);

	pthread_exit(NULL);
}

void *HandleClient(void *args)
{
	int ret = 0;

	client_handler_args *client_args = (client_handler_args *)args;
	int new_socket = client_args->client_socket;
	uint32_t current_client_id = client_args->client_id_counter;
	u_int16_t hercules_thread_pool_size = client_args->hercules_thread_pool_size;

	char req[REQUEST_SIZE] = {0};
	char mode[MODE_SIZE] = {0};
	uint32_t client_id_from_req = 0;
	slog_debug("\nClient %u connected, socket %d.", current_client_id, new_socket);

	ret = recv(new_socket, req, REQUEST_SIZE, MSG_WAITALL);
	if (ret < 0)
	{
		perror("HERCULES_ERR_HANDLE_CLIENT_DISPATCHER_RECV");
		slog_error("HERCULES_ERR_HANDLE_CLIENT_DISPATCHER_RECV");
		close(new_socket);
		free(client_args);
		pthread_exit(NULL);
	}
	if (ret == 0)
	{
		slog_debug("Client %u disconnected (recv returned 0) on socket %d.", current_client_id, new_socket);
		close(new_socket);
		free(client_args);
		pthread_exit(NULL);
	}
	sscanf(req, "%" PRIu32 " %s", &client_id_from_req, mode);

	char *req_content = strstr(req, mode);
	req_content += 4;

	slog_debug("Client %u, req=%s, req_content=%s", current_client_id, req, req_content);

	// Determine the index for local_addr and local_addr_len based on the client_id_counter.
	uint32_t resource_idx = current_client_id % hercules_thread_pool_size;

	// Check if the client is requesting connection resources.
	if (!strncmp(req_content, "HELLO!", 6))
	{
		// Case where a client (front-end) is connecting to the data server.
		ret = send(new_socket, &local_addr_len[resource_idx], sizeof(local_addr_len[resource_idx]), 0);
		if (ret == -1)
		{
			perror("HERCULES_ERR_DISPATCHER_HELLO_SEND1");
			slog_error("HERCULES_ERR_DISPATCHER_HELLO_SEND1 for client %u", current_client_id);
		}
		ret = send(new_socket, local_addr[resource_idx], local_addr_len[resource_idx], 0);
		if (ret == -1)
		{
			perror("HERCULES_ERR_DISPATCHER_HELLO_SEND2");
			slog_error("HERCULES_ERR_DISPATCHER_HELLO_SEND2 for client %u", current_client_id);
		}
		slog_debug("Replied client %u, hercules_thread_pool_size=%d, with resource_idx %u", current_client_id, hercules_thread_pool_size, resource_idx);
	}
	else if (!strncmp(req_content, "MAIN!", 5))
	{
		// Case where a data server is connecting to the metadata server.
		ret = send(new_socket, &local_addr_len[resource_idx], sizeof(local_addr_len[resource_idx]), 0);
		if (ret == -1)
		{
			perror("HERCULES_ERR_DISPATCHER_MAIN_SEND1");
			slog_error("HERCULES_ERR_DISPATCHER_MAIN_SEND1 for client %u", current_client_id);
		}
		ret = send(new_socket, local_addr[resource_idx], local_addr_len[resource_idx], 0);
		if (ret == -1)
		{
			perror("HERCULES_ERR_DISPATCHER_MAIN_SEND2");
			slog_error("HERCULES_ERR_DISPATCHER_MAIN_SEND2 for client %u", current_client_id);
		}
		slog_debug("Client %u, sent address %lu (%lu) to the client %d", current_client_id, local_addr[resource_idx], local_addr_len[resource_idx], current_client_id);
	}
	// Check if someone is requesting identity resources.
	else if (*((int32_t *)req) == WHO)
	{
		ret = send(new_socket, &local_addr_len[resource_idx], sizeof(local_addr_len[resource_idx]), 0);
		if (ret == -1)
		{
			slog_error("HERCULES_ERR_DISPATCHER_WHO_SEND1 for client %u", current_client_id);
		}
		ret = send(new_socket, local_addr[resource_idx], local_addr_len[resource_idx], 0);
		if (ret == -1)
		{
			slog_error("HERCULES_ERR_DISPATCHER_WHO_SEND2 (WHO) for client %u", current_client_id);
		}
		slog_debug("Replied to client for WHO request.", current_client_id);
	}
	else
	{
		slog_error("Client %u sent unknown request: %s", current_client_id, req);
		fprintf(stderr, "Client %u sent unknown request: %s\n", current_client_id, req);
	}

	// slog_debug("\n");

	// MIRAR ucp_worker_release_address(ucp_worker_threads[client_id_from_req % hercules_thread_pool_size], local_addr);
	close(new_socket);
	free(client_args);
	pthread_exit(NULL);
}

int ready(char *tmp_file_path, const char *msg)
{
	// fprintf(stderr, "Trying to create the file %s with the message %s\n", tmp_file_path, msg);
	char status[25] = {0};
	char err_msg[MAX_ERR_MSG_LEN] = {0};
	char cwd[PATH_MAX] = {0};
	FILE *tmp_file; // make the file pointer as temporary file.

	if (getcwd(cwd, sizeof(cwd)) == NULL)
	{
		perror("Error getting the current working directory.");
		return -1;
	}
	fprintf(stderr, "%s\n", tmp_file_path);
	tmp_file = fopen(tmp_file_path, "w+");
	if (tmp_file == NULL)
	{
		sprintf(err_msg, "Error in creating the temporary file: %s, current directory is: %s\n", tmp_file_path, cwd);
		perror(err_msg);
		return -1;
	}

	strcpy(status, "STATUS = ");
	strcat(status, msg);

	size_t written = fwrite(status, sizeof(char), strlen(status), tmp_file);
	if (written < strlen(status))
	{
		sprintf(err_msg, "Error writting in temporary file %s\n", tmp_file_path);
		perror(err_msg);
		fclose(tmp_file);
		return -1;
	}

	if (fclose(tmp_file) == EOF)
	{
		sprintf(err_msg, "Error closing the temporary file %s\n", tmp_file_path);
		perror(err_msg);
		return -1;
	}

	// fprintf(stderr, "Writting status %s (%zu bytes) file in: %s\n", msg, strlen(status), tmp_file_path);

	// if there was an error in the initialization of the server,
	// we kill the process.
	if (!strncmp(msg, "ERROR", sizeof("ERROR")))
	{
		exit(1);
	}
	return 0;
}
