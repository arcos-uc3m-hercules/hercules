#include <errno.h>
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
#include "imss.h"
#include "workers.h"
#include "directory.h"
#include "records.hpp"
#include "map_server_eps.hpp"

// Lock dealing when cleaning blocks
pthread_mutex_t mutex_garbage = PTHREAD_MUTEX_INITIALIZER;

// Initial buffer address.
char *buffer_address;
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

// URI of the attached deployment.
char att_imss_uri[URI_];

static long iov_cnt = 1;

// Map that stores server side endpoints
void *map_server_eps;

pthread_mutex_t tree_mut = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mp = PTHREAD_MUTEX_INITIALIZER;

ucp_worker_h *ucp_worker_threads;
ucp_address_t **local_addr;
size_t *local_addr_len;

int global_finish_threads = 0;
int global_finish_checkpoint = 0;
int global_finish_snapshot = 0;
int global_server_fd_thread = -1;
size_t global_offset = 0;

std::mutex mtx;
std::condition_variable cv;
int data_ready = 0;

#define GARBAGE_COLLECTOR_PERIOD 120
#define CKECKPOINT_PERIOD 10

int ready(char *tmp_file_path, const char *msg)
{
	// fprintf(stderr, "Trying to create the file %s with the message %s\n", tmp_file_path, msg);
	char status[25];
	char err_msg[MAX_ERR_MSG_LEN];
	char cwd[PATH_MAX];
	FILE *tmp_file; // = tmpfile(); // make the file pointer as temporary file.

	if (getcwd(cwd, sizeof(cwd)) == NULL)
	{
		perror("Error getting the current working directory.");
		return -1;
	}

	tmp_file = fopen(tmp_file_path, "w");
	if (tmp_file == NULL)
	{
		sprintf(err_msg, "Error in creating the temporary file: %s, current directory is: %s\n", tmp_file_path, cwd);
		perror(err_msg);
		return -1;
	}

	strcpy(status, "STATUS = ");
	strcat(status, msg);

	size_t written = fwrite(status, strlen(status), 1, tmp_file);
	if (written < 0)
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

	// fprintf(stderr, "Writting status file in: %s\n", tmp_file_path);

	// if there was an error in the initialization of the server,
	// we kill the process.
	if (!strncmp(msg, "ERROR", sizeof("ERROR")))
	{
		exit(1);
	}
	return 0;
}

// if malleability_on = 1, new requests will be not handled and server will
// respond with a "malleability" string.
// int malleability_on = 0;
#define MALLEABILITY_MESSAGE = "MALLEABILITY";

void handle_signal(int signal)
{
	if (signal == SIGUSR1)
	{
		fprintf(stderr, "*** Received SIGUSR1\n");
		global_finish_threads = 1;

		// To dispatcher thread.
		if (shutdown(global_server_fd_thread, SHUT_RD) == -1)
		{
			fprintf(stderr, "Error closing server_fd\n");
		}
	}
}

// Thread method attending client read-write data requests.
void *srv_worker(void *th_argv)
{

	// Enable thread cancellation
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

	// Set the cancellation type to deferred
	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

	ucp_ep_params_t ep_params;

	ucp_am_handler_param_t param;
	ucs_status_t status;
	int ret = 0;
	p_argv *arguments = (p_argv *)th_argv;

	ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
						   UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
						   UCP_EP_PARAM_FIELD_ERR_HANDLER |
						   UCP_EP_PARAM_FIELD_USER_DATA;
	ep_params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
	ep_params.err_handler.cb = err_cb_server;
	// ep_params.err_handler.arg = NULL;

	map_server_eps = map_server_eps_create();

	BLOCK_SIZE = arguments->blocksize * 1024;

	for (;;)
	{
		errno = 0;
		size_t peer_addr_len;
		ucp_address_t *peer_addr;
		ucs_status_t ep_status = UCS_OK;
		ucp_ep_h ep;
		struct ucx_context *request = NULL;
		char *req;
		ucp_tag_recv_info_t info_tag;
		ucp_tag_message_h msg_tag;
		msg_req_t *msg;
		ucp_request_param_t recv_param;

		clock_t t;
		double time_taken;
		t = clock();

		// Register signal handler
		// signal(SIGUSR1, handle_signal);

		do
		{
			/* Progressing before probe to update the state */
			ucp_worker_progress(arguments->ucp_worker);
			/* Probing incoming events in non-block mode */
			msg_tag = ucp_tag_probe_nb(arguments->ucp_worker, tag_req, tag_mask, 1, &info_tag);
			if (global_finish_threads == 1)
			{
				fprintf(stderr, "Ending data server thread.\n");
				pthread_exit(NULL);
			}
		} while (msg_tag == NULL);
		// ucs_status_t status;
		// /* Receive test string from server */
		// for (;;)
		// {
		// 	/* Probing incoming events in non-block mode */
		// 	msg_tag = ucp_tag_probe_nb(arguments->ucp_worker, tag_req, tag_mask, 1, &info_tag);
		// 	if (msg_tag != NULL)
		// 	{
		// 		/* Message arrived */
		// 		break;
		// 	}
		// 	else if (ucp_worker_progress(arguments->ucp_worker))
		// 	{
		// 		/* Some events were polled; try again without going to sleep */
		// 		continue;
		// 	}
		// 	/* If we got here, ucp_worker_progress() returned 0, so we can sleep.
		// 	 * Following blocked methods used to polling internal file descriptor
		// 	 * to make CPU idle and don't spin loop
		// 	 */
		// 	// if (ucp_test_mode == TEST_MODE_WAIT)
		// 	{
		// 		/* Polling incoming events*/
		// 		status = ucp_worker_wait(arguments->ucp_worker);
		// 		// CHKERR_JUMP(status != UCS_OK, "ucp_worker_wait\n", err_ep);
		// 	}
		// 	// else if (ucp_test_mode == TEST_MODE_EVENTFD)
		// 	// {
		// 	// 	status = test_poll_wait(ucp_worker);
		// 	// 	CHKERR_JUMP(status != UCS_OK, "test_poll_wait\n", err_ep);
		// 	// }
		// }

		slog_debug("[srv_worker] Message length=%ld bytes.", info_tag.length);
		msg = (msg_req_t *)malloc(info_tag.length);

		recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
								  UCP_OP_ATTR_FIELD_DATATYPE;

		recv_param.datatype = ucp_dt_make_contig(1);
		recv_param.cb.recv = recv_handler;

		request = (struct ucx_context *)ucp_tag_msg_recv_nbx(arguments->ucp_worker, msg, info_tag.length, msg_tag, &recv_param);

		status = ucx_wait(arguments->ucp_worker, request, "receive", "srv_worker");

		peer_addr_len = msg->addr_len;
		peer_addr = (ucp_address *)malloc(peer_addr_len);
		req = msg->request;

		memcpy(peer_addr, msg + 1, peer_addr_len);

		ucp_worker_address_attr_t attr;
		attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
		ucp_worker_address_query(peer_addr, &attr);
		slog_debug(" Receiving request from %" PRIu64 ".", attr.worker_uid);

		//  look for this peer_addr in the map and get the ep
		ret = map_server_eps_search(map_server_eps, attr.worker_uid, &ep);
		// create ep if it's not in the map
		if (ret < 0)
		{
			// ucp_ep_h new_ep;
			ep_params.address = peer_addr;
			ep_params.user_data = &ep_status;
			// struct worker_info *worker_info = (struct worker_info*)malloc(sizeof(struct worker_info));
			// worker_info->worker_uid = attr.worker_uid;
			// worker_info->server_type = 'd';
			// ep_params.err_handler.arg = &worker_info;
			ep_params.err_handler.arg = &attr.worker_uid;

			status = ucp_ep_create(arguments->ucp_worker, &ep_params, &ep);
			// add ep to the map
			map_server_eps_put(map_server_eps, attr.worker_uid, ep);
		}
		else
		{
			slog_debug("\t[srv_worker]['%" PRIu64 "] Endpoint already exist'", attr.worker_uid);
		}

		arguments->peer_address = peer_addr;
		arguments->server_ep = ep;
		arguments->worker_uid = attr.worker_uid;

		srv_worker_helper(arguments, req);
		t = clock() - t;

		time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
		slog_info("[srv_worker] Serving time %f s\n", time_taken);

		free(peer_addr);
		free(msg);
	}
}

int srv_worker_helper(p_argv *arguments, const char *req)
{

	ucs_status_t status;
	int ret = -1;

	// Cast from generic pointer type to p_argv struct type pointer.

	// Obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;

	// Resources specifying if the ZMQ_SNDMORE flag was set in the sender.
	int64_t more;
	size_t more_size = sizeof(more);
	int is_shared_memory = 0, snapshot_op = 0;

	// Code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";
	char mode[MODE_SIZE];

	// slog_debug(" Waiting for new request.");
	// Save the request to be served.
	slog_debug(" request to be served %s", req);

	// Elements conforming the request.
	uint32_t block_size_recv, block_offset;
	char uri_[URI_];
	size_t to_read = 0;
	int sender = 0;

	sscanf(req, "%s", mode);

	if (!strcmp(mode, "BROADCAST"))
	{
		sscanf(req, "%s %s %d", mode, uri_, &sender);
		slog_debug("BROADCAST condition, req=%s, mode=%s, uri_=%s, sender=%d", req, mode, uri_, sender);
		map->put_snapshot(uri_, sender);
		// nothing else to do.
		return 0;
	}

	sscanf(req, "%s %" PRIu32 " %" PRIu32 " %s %lu", mode, &block_size_recv, &block_offset, uri_, &to_read);
	if (!strcmp(mode, "GET"))
	{
		more = GET_OP;
	}
	if (!strcmp(mode, "SET"))
	{
		more = SET_OP;
	}
	if (!strcmp(mode, "LOCALGET"))
	{
		more = GET_OP;
		is_shared_memory = 1;
	}
	if (!strcmp(mode, "LOCALSET"))
	{
		more = SET_OP;
		is_shared_memory = 1;
	}
	if (!strcmp(mode, "SNAPSET"))
	{
		more = SET_OP;
		snapshot_op = 1;
	}

	slog_debug(" Request - mode '%s', block_size_recv '%" PRIu32 "', block_offset '%" PRIu32 "', uri_ '%s', more %ld", mode, block_size_recv, block_offset, uri_, more);

	// Create an std::string in order to be managed by the map structure.
	std::string key;
	key.assign((const char *)uri_);

	// Information associated to the arriving key.
	void *address_;
	uint64_t block_size_rtvd;

	// Differentiate between READ and WRITE operations.
	switch (more)
	{
	// No more messages will arrive to the socket.
	case READ_OP:
	{
		switch (block_size_recv)
		{
		case READ_OP:
		{
			int32_t ret = map->get(key, &address_, &block_size_rtvd);
			// Check if there was an associated block to the key.
			if (ret == 0)
			{
				// Send the error code block.
				ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid);
				if (ret < 0)
				{
					slog_error("ERR_HERCULES_WORKER_SEND_READ_OP");
					perror("ERR_HERCULES_WORKER_SEND_READ_OP");
					return -1;
				}
			}
			else
			{
				// ret = TIMING(send_data(arguments->ucp_worker, arguments->server_ep, address_, block_size_rtvd, arguments->worker_uid), "[READ_OP][READ_OP] Send the requested block");
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
				{
					// Send the requested block.

					// struct stat *stats;
					// stats = (struct stat *)address_;
					// slog_debug("[READ_OP][READ_OP] Send the requested block with key=%s, block_offset=%ld, block_size_rtvd=%ld kb, to_read=%ld kb, stat->st_nlink=%lu, is_shared_memory=%d", key.c_str(), block_offset, block_size_rtvd / 1024, to_read / 1024, stats->st_nlink, is_shared_memory);
					slog_debug("[READ_OP][READ_OP] Send the requested block with key=%s, block_offset=%ld, block_size_rtvd=%ld kb, to_read=%ld kb, is_shared_memory=%d", key.c_str(), block_offset, block_size_rtvd / 1024, to_read / 1024, is_shared_memory);
					size_t ret_send_data = 0;
					if (is_shared_memory)
					{
						ret_send_data = send_data(arguments->ucp_worker, arguments->server_ep, (char *)address_, block_size_rtvd, arguments->worker_uid);
					}
					else
					{
						ret_send_data = send_data(arguments->ucp_worker, arguments->server_ep, (char *)address_ + block_offset, to_read, arguments->worker_uid);
					}
					slog_debug("[READ_OP][READ_OP] send_data, ret_send_data=%lu", ret_send_data);
					if (ret_send_data == 0)
					{
						slog_error("HERCULES_ERR_WORKER_SENDBLOCK");
						perror("HERCULES_ERR_WORKER_SENDBLOCK");
						return -1;
					}
				}
			}
			break;
		}
		case RELEASE:
		{
			map_server_eps_erase(map_server_eps, arguments->worker_uid, arguments->ucp_worker);
			slog_debug("[READ_OP][RELEASE]");
			char release_msg[] = "RELEASE\0";

			ret = NETWORK_TIMING(send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid), "[READ_OP][RENAME_OP] Send release", int);
			if (ret == 0)
			{
				perror("ERR_HERCULES_SRV_SEND_DATA_RELEASE");
				slog_error("ERR_HERCULES_SRV_SEND_DATA_RELEASE");
				return -1;
			}
			break;
		}
		case DELETE_OP:
		{
			slog_debug("DELETE_OP");
			slog_debug("Cleaning %s", key.c_str());
			map->cleaning_specific(key);
			char release_msg[] = "DELETE\0";
			ret = send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid);
			if (ret == 0)
			{
				perror("ERR_HERCULES_PUBLISH_DELETEOP");
				slog_error("ERR_HERCULES_PUBLISH_DELETEOP");
				return -1;
			}
			break;
		}
		case RENAME_OP:
		{
			std::size_t found = key.find(',');
			slog_debug("[RENAME_OP], key=%s, found=%d", key.c_str(), found);
			if (found != std::string::npos)
			{
				slog_debug("[RENAME_OP], found != npos");
				std::string old_key = key.substr(0, found);
				std::string new_key = key.substr(found + 1, key.length());
				slog_debug("[RENAME_OP], old_key=%s, new_key=%s", old_key.c_str(), new_key.c_str());
				// RENAME MAP
				map->cleaning_specific(new_key);
				int32_t result = map->rename_data_srv_worker(old_key, new_key);
				if (result == 0)
				{
					break;
				}
			}
			else
			{
				slog_debug("[RENAME_OP], found == npos");
			}

			char release_msg[] = "RENAME\0";
			ret = NETWORK_TIMING(send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid), "[READ_OP][RENAME_OP] Send rename", int);
			if (ret == 0)
			{
				perror("ERR_HERCULES_PUBLISH_RENAMEMSG");
				slog_error("ERR_HERCULES_PUBLISH_RENAMEMSG");
				return -1;
			}
			break;
		}
		case RENAME_DIR_DIR_OP:
		{
			// printf("SRV_WORKER RENAME_DIR_DIR_OP");
			std::size_t found = key.find(' ');
			if (found != std::string::npos)
			{
				std::string old_dir = key.substr(0, found);
				std::string rdir_dest = key.substr(found + 1, key.length());

				// RENAME MAP
				map->rename_data_dir_srv_worker(old_dir, rdir_dest);
			}

			char release_msg[] = "RENAME\0";
			ret = NETWORK_TIMING(send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid), "[READ_OP][RENAME_DIR_DIR_OP] Send rename", int);
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
			ret = NETWORK_TIMING(send_data(arguments->ucp_worker, arguments->server_ep, arguments->my_uri, strlen(arguments->my_uri) + 1, arguments->worker_uid), ("[READ_OP][WHO] send uri: %s", arguments->my_uri), int);
			if (ret == 0)
			{
				perror("ERR_HERCULES_WHOREQUEST");
				slog_error("ERR_HERCULES_WHOREQUEST");
				return -1;
			}
			break;
		}
		default:
			break;
		}
		break;
	}
	// More messages will arrive to the socket.
	case WRITE_OP:
	{
		int op;
		std::size_t found = key.find(' ');
		std::size_t found2 = key.find("[OP]=");
		slog_debug("[WRITE_OP] found=%d, found2=%d", found, found2);
		if (found2 != std::string::npos)
		{
			slog_debug("[WRITE_OP] Entra en found2");
			op = stoi(key.substr(found2 + 5, (found - (found2 + 5))));
			key.erase(0, found + 1);
		}

		if (found != std::string::npos && found2 == std::string::npos)
		{
			std::string path = key.substr(0, found);
			key.erase(0, found + 1);
			// std::cout <<"path:" << key << '';

			std::size_t found = key.find(' ');
			int curr_blk = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int end_blk = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int start_offset = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int end_offset = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			found = key.find(' ');
			int IMSS_DATA_BSIZE = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			int size = stoi(key);

			size_t msg_length = 0;
			msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
			if (msg_length == 0)
			{
				slog_error("ERRIMSS_DATA_WORKER_WORKER_OP_1_INVALID_MSG_LENGTH");
				perror("ERRIMSS_DATA_WORKER_WORKER_OP_1_INVALID_MSG_LENGTH");
				return -1;
			}

			char *buf = (char *)malloc(msg_length * sizeof(char));
			// Receive all blocks into the buffer.
			msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)buf, msg_length, arguments->worker_uid, 0);
			// msg_length = recv_data_opt(arguments->ucp_worker, arguments->server_ep, (void **)&buf, msg_length, arguments->worker_uid, 0);
			if (msg_length == 0)
			{
				perror("ERRIMSS_DATA_WORKER_WORKER_OP_1_RECV_DATA");
				slog_error("ERRIMSS_DATA_WORKER_WORKER_OP_1_RECV_DATA");
				free(buf);
				return -1;
			}

			int pos = path.find('$');
			std::string first_element = path.substr(0, pos + 1);
			first_element = first_element + "0";
			map->get(first_element, &address_, &block_size_rtvd);
			// imss_info * data = (imss_info *) address_;
			// printf("READ_OP SEND data->type=%c",data->type);
			struct stat *stats = (struct stat *)address_;

			// Needed variables
			size_t byte_count = 0;
			int first = 0;
			int ds = 0;
			int64_t to_copy = 0;
			uint32_t filled = 0;
			void *aux = (void *)malloc(IMSS_DATA_BSIZE);
			int count = 0;
			// For the rest of blocks
			while (curr_blk <= end_blk)
			{
				// printf("Nodename    - %s current_block=%d", detect.nodename, curr_blk);
				count = count + 1;
				// printf("count=%d",count);
				pos = path.find('$');
				std::string element = path.substr(0, pos + 1);
				element = element + std::to_string(curr_blk);
				// std::cout <<"element:" << element << '';

				// First fragmented block
				if (first == 0 && start_offset && stats->st_size != 0)
				{
					// Get previous block
					map->get(element, &aux, &block_size_rtvd); // path por curr_block
															   // Bytes to write are the minimum between the size parameter and the remaining space in the block (BLOCKSIZE-start_offset)
					to_copy = (size < IMSS_DATA_BSIZE - start_offset) ? size : IMSS_DATA_BSIZE - start_offset;

					memcpy((char *)aux + start_offset, buf + byte_count, to_copy);
				}
				// Last Block
				else if (curr_blk == end_blk)
				{
					if (end_offset != 0)
					{
						to_copy = end_offset;
					}
					else
					{
						to_copy = IMSS_DATA_BSIZE;
					}
					// Only if last block has contents
					if (curr_blk <= stats->st_blocks && start_offset)
					{
						map->get(element, &aux, &block_size_rtvd); // path por curr_block
					}
					else
					{
						memset(aux, 0, IMSS_DATA_BSIZE);
					}
					if (byte_count == size)
					{
						to_copy = 0;
					}
					// printf("curr_block=%d, end_block=%d, byte_count=%d",curr_blk, end_blk, byte_count);
					memcpy(aux, buf + byte_count, to_copy);
				}
				// middle block
				else
				{
					to_copy = IMSS_DATA_BSIZE;
					memcpy(aux, buf + byte_count, to_copy);
				}

				// Write and update variables
				if (!map->get(element, &address_, &block_size_rtvd))
				{
					map->put(element, aux, block_size_rtvd);

					// printf("Nodename    - %s after put", detect.nodename);
				}
				else
				{
					memcpy(address_, aux, block_size_rtvd);
				}
				// printf("currblock=%d, byte_count=%d",curr_blk, byte_count);
				byte_count += to_copy;
				++curr_blk;
				++first;
			}
			int16_t off = (end_blk * IMSS_DATA_BSIZE) - 1 - size;
			if (size + off > stats->st_size)
			{
				stats->st_size = size + off;
				stats->st_blocks = curr_blk - 1;
			}

			free(buf);
		}
		else if (found != std::string::npos && op == 2)
		{
			std::string path;
			std::size_t found = key.find(' ');
			// printf("Nodename	-%s SPLIT WRITEV",detect.nodename);

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

			found = key.find('$');
			int amount = stoi(key.substr(0, found));
			int size = amount * blocksize;
			key.erase(0, found + 1);

			slog_debug("amount=%d", amount);
			slog_debug("path=%s", path.c_str());
			slog_debug("blocksize=%d", blocksize);
			slog_debug("start_offset=%d", start_offset);
			slog_debug("size=%d", size);
			slog_debug("rest=%s", key.c_str());

			// Receive all blocks into the buffer.
			size_t msg_length = 0;
			msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
			if (msg_length == 0)
			{
				perror("ERRIMSS_WORKER_DATA_WRITE_OP_2_INVALID_MSG_LENGTH");
				slog_error("ERRIMSS_WORKER_DATA_WRITE_OP_2_INVALID_MSG_LENGTH");
				return -1;
			}

			void *buf = malloc(msg_length);

			msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, buf, msg_length, arguments->worker_uid, 0);
			// msg_length = recv_data_opt(arguments->ucp_worker, arguments->server_ep, &buf, msg_length, arguments->worker_uid, 0);
			if (msg_length == 0)
			{
				perror("ERRIMSS_WORKER_DATA_WRITE_OP_2_RECV_DATA");
				slog_error("ERRIMSS_WORKER_DATA_WRITE_OP_2_RECV_DATA");
				free(buf);
				return -1;
			}

			// size_recv = size; // MIRAR
			int32_t insert_successful;

			// printf("Nodename	-%s size_recv=%d",detect.nodename,size_recv);
			// printf("Salida buf full=%c",buf[100]);

			int32_t byte_count = 0;
			for (int i = 0; i < amount; i++)
			{
				// substract current block
				found = key.find('$');
				int curr_blk = stoi(key.substr(0, found));
				key.erase(0, found + 1);

				std::string element = path;
				element = element + '$' + std::to_string(curr_blk);
				// printf(" element=%s",element.c_str());

				if (map->get(element, &address_, &block_size_rtvd) == 0)
				{
					// If don't exist
					char *buffer = (char *)aligned_alloc(1024, blocksize);
					memcpy(buffer, (char *)buf + byte_count, blocksize);
					// printf("Salida buffer part=%c",buffer[100]);
					insert_successful = map->put(element, buffer, block_size_recv);
					if (insert_successful != 0)
					{
						perror("ERRIMSS_WORKER_MAPPUT");
						return -1;
					}
				}
				else
				{
					// If already exits
					memcpy(address_, (char *)buf + byte_count, blocksize);
				}
				byte_count = byte_count + blocksize;
			}
			free(buf);
		}
		else
		{
			slog_debug("[WRITE_OP] WRITE NORMAL CASE. Size %ld, offset=%ld", block_size_recv, block_offset);
			// search for the block to know if it was previously stored.
			int ret = 0;
			// Checks if it is data for the Snapshot operation or regular data.
			if (snapshot_op)
			{
				// Nothing to do.
			}
			else
			{
				ret = map->get(key, &address_, &block_size_rtvd);
			}

			// if the block was not already stored:
			if (ret == 0)
			{
				slog_debug("[WRITE_OP] NO key find %s", key.c_str());
				clock_t tp;
				tp = clock();
				void *buffer = NULL;
				tp = clock() - tp;
				double time_taken2 = ((double)tp) / CLOCKS_PER_SEC; // in seconds
				//  Receive the block into the buffer.
				clock_t tr;
				// get the buffer data length.
				size_t msg_length = 0;
				int size_asigned_to_block = 0;
				// If data is stored in shared memory due LOCAL policy, the server does not need to receive the data.
				if (!is_shared_memory)
				{
					msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
					if (msg_length == 0)
					{
						perror("HERCULES_ERR_DATA_WORKER_WRITE_NEW_BLOCK_INVALID_MSG_LENGTH");
						slog_error("HERCULES_ERR_DATA_WORKER_WRITE_NEW_BLOCK_INVALID_MSG_LENGTH");
						return -1;
					}

					buffer = (void *)StsQueue.pop(mem_pool);
					if (buffer == NULL)
					{
						buffer = (void *)malloc(BLOCK_SIZE * sizeof(char));
					}

					if (buffer == NULL)
					{
						perror("HERCULES_ERR_MEMORY_ALLOCATION");
						slog_error("HERCULES_ERR_MEMORY_ALLOCATION");
						return -1;
					}
					size_asigned_to_block = BLOCK_SIZE;

					// Receive the data from the front end.
					msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)buffer + block_offset, msg_length, arguments->worker_uid, 1);
					if (msg_length == 0)
					{
						perror("HERCULES_ERR_DATA_WORKER_WRITE_NEW_BLOCK_RECV_DATA");
						slog_error("HERCULES_ERR_DATA_WORKER_WRITE_NEW_BLOCK_RECV_DATA");
						return -1;
					}
				}
				else // Data in shared memory.
				{
					//  Tell the client this is a new block to be copy in the shared memory with an offset "global_offset".
					char answer[RESPONSE_SIZE];
					sprintf(answer, "NEW %ld", global_offset);
					slog_info("Answer=%s", answer);
					ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, answer, STRING, arguments->worker_uid);
					if (ret < 0)
					{
						perror("ERR_HERCULES_WORKER_SEND_DYNAMIC_SHM_LOCAL");
						slog_error("ERR_HERCULES_WORKER_SEND_DYNAMIC_SHM_LOCAL");
						return -1;
					}

					// Find the length of the string required to store the number, including the null terminator
					int length_number = snprintf(NULL, 0, "%lu %d", global_offset, block_size_recv) + 1;
					buffer = (void *)calloc(length_number, sizeof(char));
					if (buffer == NULL)
					{
						perror("HERCULES_ERR_MEMORY_ALLOCATION_SHM");
						slog_error("HERCULES_ERR_MEMORY_ALLOCATION_SHM");
						return -1;
					}

					// When using shared memory, "buffer" will store the offset and the block size.
					ret = snprintf((char *)buffer, length_number, "%lu %d", global_offset, block_size_recv);
					if (ret < 0)
					{
						perror("HERCULES_ERR_ENCODING");
						slog_error("HERCULES_ERR_ENCODING");
						return -1;
					}

					slog_info("buffer=%s, length_number=%d, global_offset=%lu", buffer, length_number, global_offset);

					size_asigned_to_block = length_number;
					global_offset += block_size_recv;
				}

				int32_t insert_successful;

				// Include the new record in the tracking structure.
				tr = clock();
				// fprintf(stderr,"[WRITE_OP] ****[PUT]********* key=%s\n",  key.c_str());
				slog_debug("[WRITE_OP] ****[PUT, block_size_recv=%ld, BLOCK_SIZE=%lu, msg_length=%lu]********* key=%s", block_size_recv, BLOCK_SIZE, msg_length, key.c_str());
				// TODO: should this be block_size_recv or a different size? block_size_recv might not be the full block size
				if (snapshot_op)
				{
					// Get the origin data server id from the received key.
					// int origin_server_id = 0;
					// std::string data_uri;
					// std::string file_name;
					// getBlockInformation(key, &origin_server_id, &data_uri, &file_name);
					// slog_debug("key: %s, origin_server_id: %d, data_uri: %s, file_name: %s", key.c_str(), origin_server_id, data_uri.c_str(), file_name.c_str());
					// Fill buffer_broadcast with the data received from the other servers.
					// buffer_broadcast[];
					slog_debug("Snapshot opration, origin server=%s", key.c_str());
					insert_successful = map->put_broadcast(key, buffer, msg_length);
				}
				else
				{
					insert_successful = map->put(key, buffer, size_asigned_to_block);
				}
				slog_debug("[WRITE_OP] insert_successful=%d, key=%s, size_asigned_to_block=%d", insert_successful, key.c_str(), size_asigned_to_block);
				tr = clock() - tr;
				double time_taken = ((double)tr) / CLOCKS_PER_SEC; // in seconds

				// Include the new record in the tracking structure.
				if (insert_successful != 0)
				{
					perror("HERCULES_ERR_WORKER_MAPPUT");
					slog_error("HERCULES_ERR_WORKER_MAPPUT");
					free(buffer);
					return -1;
				}

				// // Only when there is not a snapshot operation.
				// if (!snapshot_op)
				// {
				// 	// The following buffer is used for Sanpshot.
				// 	// key is the uri, and value is: 0 data will not be copy to disk, and 1 data will be copy to disk. By default, when an element is inserted, value is 1 and it will be set to 0 when the corresponding Snapshot thread copy the data to disk.
				// 	// fprintf(stderr, "Inserting key = %s\n", key.c_str());

				std::size_t found = key.find("$0");
				if (found != std::string::npos) // block 0.
				{
						insert_successful = map->put_snapshot(key, -1);
						// Include the new record in the tracking structure.
						if (insert_successful != 0)
						{
							perror("HERCULES_ERR_WORKER_SEC_MAP_PUT");
							slog_error("HERCULES_ERR_WORKER_SEC_MAP_PUT");
							return -1;
						}
				}

				// Update the pointer.
				arguments->pt += block_size_recv;
				// }
			}
			// if the block was already stored:
			else
			{
				slog_debug("[WRITE_OP] Key find %s", key.c_str());
				// Receive the block into the buffer.
				std::size_t found = key.find("$0");
				if (found != std::string::npos)
				{ // block 0.
					slog_debug("[WRITE_OP] Updating block $0 (%d)", block_size_rtvd);
					struct stat *old, *latest;
					size_t msg_length = 0;
					// If data is stored in shared memory due LOCAL policy, the server does not need to receive the data.
					if (!is_shared_memory)
					{ // non shared memory method.
						msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
						if (msg_length == 0)
						{
							perror("HERCULES_ERR_DATA_WORKER_WRITE_BLOCK_0_INVALID_MSG_LENGTH");
							slog_error("HERCULES_ERR_DATA_WORKER_WRITE_BLOCK_0_INVALID_MSG_LENGTH");
							return -1;
						}
						slog_live("msg_length=%lu", msg_length);
						// void *buffer = malloc(block_size_recv);
						void *buffer = (void *)malloc(msg_length * sizeof(char));
						msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, buffer, msg_length, arguments->worker_uid, 0);
						if (msg_length == 0)
						{
							perror("HERCULES_ERR_DATA_WORKER_WRITE_BLOCK_0_RECV_DATA");
							slog_error("HERCULES_ERR_DATA_WORKER_WRITE_BLOCK_0_RECV_DATA");
							free(buffer);
							return -1;
						}

						old = (struct stat *)address_;
						latest = (struct stat *)buffer;
						slog_debug(" File size new %ld old %ld", latest->st_size, old->st_size);
						latest->st_size = std::max(latest->st_size, old->st_size);
						// slog_debug(" buffer->st_size: %ld, block_offset=%ld", latest->st_size, block_offset);
						slog_debug(" buffer->st_size: %ld, block_offset=%ld, old->st_nlink: %ld, new->st_nlink: %ld", latest->st_size, block_offset, old->st_nlink, latest->st_nlink);
						// Overwrite block 0 data.
						memcpy((char *)address_ + block_offset, buffer, msg_length);

						// TODO: should we update this block's size in the map?
						// map->update(key, address_, msg_length);
						// Updates the second map to update the data in disk.
						// map->update_simple(key, 1);

						free(buffer);
					}
					else
					{ // data is in shared memory.
						//  Tell the client to update the shared memory.
						char answer[RESPONSE_SIZE];
						// "address_" is the shared memory offset.
						sprintf(answer, "TOUPDATE %s", (char *)address_);
						ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, answer, STRING, arguments->worker_uid);
						if (ret < 0)
						{
							slog_error("ERR_HERCULES_WORKER_SEND_DYNAMIC_BLOCK_0_WRITE_OP");
							perror("ERR_HERCULES_WORKER_SEND_DYNAMIC_BLOCK_0_WRITE_OP");
							return -1;
						}
					}
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
							return -1;
						}
						// Verify if the new size (msg_length + block_offset) is greater than the old size (block_size_rtvd).
						slog_debug("msg_length=%lu, block_offset=%d, msg_length=%d", msg_length, block_offset, msg_length);
						msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)address_ + block_offset, msg_length, arguments->worker_uid, 1);
						// msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)buffer + block_offset, msg_length, arguments->worker_uid, 1);
						if (msg_length == 0)
						{
							slog_error("HERCULES_ERR_DATA_WORKER_WRITE_NON_BLOCK_0_RECV_DATA");
							perror("HERCULES_ERR_DATA_WORKER_WRITE_NON_BLOCK_0_RECV_DATA");
							return -1;
						}

						// Updates the second map to update the data in disk.
						map->update_simple(key, 1);
					}
					else
					{ // Data is in shared memory.
						//  Tell the client to update the shared memory.
						char answer[RESPONSE_SIZE];
						// "address_" is the shared memory offset.
						sprintf(answer, "TOUPDATE %s", (char *)address_);
						ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, answer, STRING, arguments->worker_uid);
						if (ret < 0)
						{
							slog_error("ERR_HERCULES_WORKER_SEND_DYNAMIC_NON_BLOCK_0_WRITE_OP");
							perror("ERR_HERCULES_WORKER_SEND_DYNAMIC_NON_BLOCK_0_WRITE_OP");
							return -1;
						}
					}
				}
				// slog_debug("address_=%x", address_);
			}
		}
		break;
	}
	default:
		break;
	}

	slog_debug(" Terminated data helper");
	return 0;
}

// Thread method searching and cleaning nodes with st_nlink=0
void *garbage_collector(void *th_argv)
{
	// fprintf(stderr, "Init garbage collector\n");
	slog_debug("Init garbage collector");
	// Obtain the current map class element from the set of arguments.
	map_records *map = (map_records *)th_argv;

	for (;;)
	{
		// Gnodetraverse_garbage_collector(map);//Future
		sleep(GARBAGE_COLLECTOR_PERIOD);
		pthread_mutex_lock(&mutex_garbage);
		map->cleaning();
		pthread_mutex_unlock(&mutex_garbage);
	}
	pthread_exit(NULL);
}

// Thread method to copy datasets from Hercules to Disk.
void *Checkpoint(void *th_argv)
{
	slog_debug("Init checkpoint writter");

	clock_t t;
	double time_taken;
	t = clock();

	p_argv *arguments = (p_argv *)th_argv;
	BLOCK_SIZE = arguments->blocksize * 1024;
	sleep(1);
	// Obtain the current map class element from the set of arguments.
	// map_records *map =(map_records *) arguments->map; // (map_records *)th_argv;
	std::shared_ptr<map_records> map = arguments->map;

	// fprintf(stderr, "Checkpoint path = %s, len=%lu\n", arguments->args.hercules_checkpoint_path, strlen(arguments->args.hercules_checkpoint_path));
	const char *checkpoint_dir = arguments->args.hercules_checkpoint_path;
	const int server_id = arguments->args.id;
	const char *POLICY = arguments->args.policy;

	if (strlen(checkpoint_dir) == 0)
	{
		printf("Checkpointing path has not been provided.\tHERCULES_CHECKPOINT_PATH = /home/user/checkpointing_path/\n");
		fflush(stdout);
		global_finish_checkpoint = 1;
		pthread_exit(NULL);
	}

	// if (!arguments->args.id)
	// { // only one server creates the checkpoint directory.
	// 	Make_directory(checkpoint_dir);
	// }

	for (;;)
	{
		sleep(CKECKPOINT_PERIOD);
		// fprintf(stderr, "Running Checkpointing, global_finish_checkpoint=%d\n", global_finish_checkpoint);
		pthread_mutex_lock(&mutex_garbage);
		// TIMING_NO_RETURN(map->Checkpoint(BLOCK_SIZE, checkpoint_dir, global_finish_checkpoint, arguments->args.id, arguments->args.data_hostname, arguments->args), "Checkpoint");
		// To stop this thread we will wait for "hercules stop".
		if (global_finish_checkpoint == 1)
		{
			// Call a barrier to stop all servers.
			sleep(30);
			global_finish_threads = 1;
			pthread_mutex_unlock(&mutex_garbage);
			fprintf(stderr, "Ending checkponting thread.\n");
			break;
		}
		pthread_mutex_unlock(&mutex_garbage);
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

	const char *snapshot_dir = arguments->args.hercules_snapshot_path;
	const int server_id = arguments->args.id;
	const char *POLICY = arguments->args.policy;

	if (strlen(snapshot_dir) == 0)
	{
		printf("Snapshot path has not been provided.\tHERCULES_SNAPSHOT_PATH = /home/user/snapshot_path/\n");
		fflush(stdout);
		global_finish_snapshot = 1;
		pthread_exit(NULL);
	}

	// if (!arguments->args.id)
	// { // only one server creates the snapshot directory.
	// 	Make_directory(snapshot_dir);
	// }
	fprintf(stderr, "Running Snapshot in %s\n", snapshot_dir);
	for (;;)
	{
		sleep(CKECKPOINT_PERIOD);
		slog_debug("Running Snapshot in %s", snapshot_dir);
		pthread_mutex_lock(&mutex_garbage);
		TIMING_NO_RETURN(map->Snapshot(BLOCK_SIZE, snapshot_dir, global_finish_snapshot, arguments->args.id, arguments->args.data_hostname, arguments->args), "Snapshot");
		// To stop this thread we will wait for "hercules stop".
		if (global_finish_snapshot == 1)
		{
			// TODO: Call a barrier to stop all servers.
			sleep(30);
			global_finish_threads = 1;
			pthread_mutex_unlock(&mutex_garbage);
			fprintf(stderr, "Ending checkponting thread.\n");
			break;
		}
		pthread_mutex_unlock(&mutex_garbage);
	}

	t = clock() - t;
	time_taken = ((double)t) / (CLOCKS_PER_SEC);

	pthread_exit(NULL);
}

// Thread method attending client read-write metadata requests.
void *stat_worker(void *th_argv)
{

	// Enable thread cancellation
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

	// Set the cancellation type to deferred
	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

	ucp_am_handler_param_t param;
	ucs_status_t status;
	int ret = 0;

	p_argv *arguments = (p_argv *)th_argv;

	map_server_eps = map_server_eps_create();

	ucp_ep_params_t ep_params;

	// ep_params = (ucp_ep_params_t *)malloc(sizeof(ucp_ep_params_t));
	ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS |
						   UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
						   UCP_EP_PARAM_FIELD_ERR_HANDLER |
						   UCP_EP_PARAM_FIELD_USER_DATA;
	ep_params.err_mode = UCP_ERR_HANDLING_MODE_PEER;
	ep_params.err_handler.cb = err_cb_server;

	for (;;)
	{
		mtrace();

		size_t peer_addr_len;
		ucp_address_t *peer_addr;
		ucs_status_t ep_status = UCS_OK;
		ucp_ep_h ep;
		struct ucx_context *request = NULL;
		char *req;
		ucp_tag_recv_info_t info_tag;
		ucp_tag_message_h msg_tag;
		msg_req_t *msg;
		ucp_request_param_t recv_param;

		// Register signal handler
		// signal(SIGUSR1, handle_signal);

		ucs_status_t status;
		/* Receive test string from server */
		do
		{
			/* Progressing before probe to update the state */
			ucp_worker_progress(arguments->ucp_worker);
			/* Probing incoming events in non-block mode */
			msg_tag = ucp_tag_probe_nb(arguments->ucp_worker, tag_req, tag_mask, 1, &info_tag);
			if (global_finish_threads == 1)
			{
				fprintf(stderr, "Ending metadata thread.\n");
				pthread_exit(NULL);
			}
		} while (msg_tag == NULL);

		// for (;;)
		// {
		/* Probing incoming events in non-block mode */
		// msg_tag = ucp_tag_probe_nb(arguments->ucp_worker, tag_req, tag_mask, 1, &info_tag);
		// if (msg_tag != NULL)
		// {
		// 	/* Message arrived */
		// 	break;
		// }
		// else if (ucp_worker_progress(arguments->ucp_worker))
		// {
		// 	/* Some events were polled; try again without going to sleep */
		// 	continue;
		// }
		// /* If we got here, ucp_worker_progress() returned 0, so we can sleep.
		//  * Following blocked methods used to polling internal file descriptor
		//  * to make CPU idle and don't spin loop
		//  */
		// // if (ucp_test_mode == TEST_MODE_WAIT)
		// {
		// 	/* Polling incoming events*/
		// 	status = ucp_worker_wait(arguments->ucp_worker);
		// 	// CHKERR_JUMP(status != UCS_OK, "ucp_worker_wait\n", err_ep);
		// }
		// else if (ucp_test_mode == TEST_MODE_EVENTFD)
		// {
		// 	status = test_poll_wait(ucp_worker);
		// 	CHKERR_JUMP(status != UCS_OK, "test_poll_wait\n", err_ep);
		// }
		// }

		msg = (msg_req_t *)malloc(info_tag.length);
		memset(msg, 0, info_tag.length);

		recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
								  UCP_OP_ATTR_FIELD_DATATYPE |
								  UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
		recv_param.datatype = ucp_dt_make_contig(1);
		recv_param.cb.recv = recv_handler;

		request = (struct ucx_context *)ucp_tag_msg_recv_nbx(arguments->ucp_worker, msg, info_tag.length, msg_tag, &recv_param);

		status = ucx_wait(arguments->ucp_worker, request, "receive", "stat_worker");

		peer_addr_len = msg->addr_len;
		peer_addr = (ucp_address *)malloc(peer_addr_len);
		req = msg->request;

		memcpy(peer_addr, msg + 1, peer_addr_len);

		ucp_worker_address_attr_t attr;
		attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
		ucp_worker_address_query(peer_addr, &attr);
		slog_debug("[stat_worker] Receiving request from %" PRIu64 ".", attr.worker_uid);

		//  look for this peer_addr in the map and get the ep
		ret = map_server_eps_search(map_server_eps, attr.worker_uid, &ep);
		// create ep if it's not in the map
		if (ret < 0)
		{

			// ep_params->err_handler.arg = NULL;

			// struct worker_info *worker_info = (struct worker_info*)malloc(sizeof(struct worker_info));
			// worker_info->worker_uid = attr.worker_uid;
			// worker_info->server_type = 'm';
			// ep_params->err_handler.arg = &worker_info;
			ep_params.err_handler.arg = &attr.worker_uid;

			// ucp_ep_h new_ep;
			ep_params.address = peer_addr;
			ep_params.user_data = &ep_status;
			status = ucp_ep_create(arguments->ucp_worker, &ep_params, &ep);
			// ucp_ep_print_info(ep, stderr);
			//  add ep to the map
			map_server_eps_put(map_server_eps, attr.worker_uid, ep);
		}
		else
		{
			// fprintf(stderr, "\t[m]['%" PRIu64 "'] Endpoint already exist\n", attr.worker_uid);
			slog_debug("\t[stat_worker]['%" PRIu64 "'] Endpoint already exist", attr.worker_uid);
		}

		arguments->peer_address = peer_addr;
		arguments->server_ep = ep;
		arguments->worker_uid = attr.worker_uid;
		muntrace();

		// arguments->worker_uid = attr.worker_uid;
		stat_worker_helper(arguments, req);

		// ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FORCE);
		free(peer_addr);
		free(msg);

		// ep_close(arguments->ucp_worker, arguments->server_ep, UCP_EP_CLOSE_MODE_FLUSH);
	}
}

int stat_worker_helper(p_argv *arguments, char *req)
{
	ucs_status_t status;
	int ret = 0;

	// Obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;

	uint16_t current_offset = 0;

	// Resources specifying if the ZMQ_SNDMORE flag was set in the sender.
	int64_t more;
	size_t more_size = sizeof(more);

	// Code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";

	/*struct timeval start, end;
	  long delta_us;*/

	int operation = 0; // server id.
	char mode[MODE_SIZE];
	int32_t req_size = 0;
	char raw_msg[req_size + 1];
	char number[16];
	// char *uri_;
	char uri_[URI_];
	int extra_info = 0;
	int num_characters_read = 0;
	int num_input_read = 0;

	// slog_debug("[STAT WORKER] Waiting for new request.");
	// Save the request to be served.
	// recv_data(arguments->ucp_worker, arguments->server_ep, req);
	slog_info("[STAT WORKER] Request - '%s'", req);
	// 	sscanf(req, "%" PRIu32 " %s", &operation, mode);
	num_input_read = sscanf(req, "%" PRIu32 " %s %s %s %n", &operation, mode, number, uri_, &num_characters_read);

	// char *req_content = strstr(req, mode);
	// req_content += 4;

	if (!strcmp(mode, "GET"))
		more = GET_OP;
	else
		more = SET_OP;

	// Expeted incomming message format: "SIZE_IN_KB KEY"
	// int32_t req_size = strlen(req_content);

	// memcpy((void *)raw_msg, req_content, req_size);
	// raw_msg[req_size] = '\0';

	// printf("*********worker_metadata raw_msg %s",raw_msg);
	slog_info("[workers][stat_worker_helper] request received=%s", req);

	// Reference to the client request.
	// sscanf(raw_msg, "%s", number);
	// int32_t number_length = (int32_t)strlen(number);
	// Elements conforming the request.
	// char *uri_ = raw_msg + number_length + 1;
	// raw_msg += number_length + 1;
	// memcpy(uri, raw_msg, number_length + 1);
	uint64_t block_size_recv = (uint64_t)atoi(number);

	// req_content += req_size;

	// slog_info("[workers][stat_worker_helper] operation=%d, number=%s, number_length=%d, uri=%s, block_size_recv=%ld", operation, number, number_length, uri_, block_size_recv);
	slog_info("[workers][stat_worker_helper] operation=%d, number=%s, uri=%s, block_size_recv=%ld", operation, number, uri_, block_size_recv);

	// Create an std::string in order to be managed by the map structure.
	std::string key;
	key.assign((const char *)uri_);

	// Information associated to the arriving key.
	void *address_;
	uint64_t block_size_rtvd;
	dataset_info *dataset;

	// printf("stat_worker RECV more=%ld, blocksss=%ld",more, block_size_recv);
	// Differentiate between READ and WRITE operations.
	switch (more)
	{
	// No more messages will arrive to the socket.
	case GET_OP:
	{
		switch (block_size_recv)
		{
		case GETDIR:
		{
			char *buffer;
			int32_t numelems_indir;
			// Retrieve all elements inside the requested directory.
			pthread_mutex_lock(&tree_mut);
			slog_info("[workers][stat_worker_helper] Calling GTree_getdir, key=%s", key.c_str());
			buffer = GTree_getdir((char *)key.c_str(), &numelems_indir);
			slog_info("[workers][stat_worker_helper] Ending GTree_getdir, key=%s, numelems_indir=%d", key.c_str(), numelems_indir);
			pthread_mutex_unlock(&tree_mut);
			if (buffer == NULL)
			{
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("HERCULES_ERR_STATWORKER_NODIR");
					return -1;
				}
				break;
			}

			// Send the serialized set of elements within the requested directory.
			msg_t m;
			m.size = numelems_indir * URI_;
			m.data = buffer;

			slog_info("[workers][stat_worker_helper] MSG, numelems_indir=%ld, size=%ld", numelems_indir, m.size);

			if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, (char *)&m, MSG, arguments->worker_uid) < 0)
			{
				perror("ERRIMSS_WORKER_SENDBLOCK");
				return -1;
			}

			slog_debug("[workers][stat_worker_helper] buffer=%s", buffer);

			char *curr = buffer;
			// char *item = (char *)malloc(URI_ * sizeof(char));
			// for (int32_t i = 0; i < numelems_indir; i++)
			// {
			// 	memcpy(item, curr, URI_);
			// 	//(*items)[i] = elements;
			// 	slog_debug("[IMSS][get_dir] item %d: %s", i, item);
			// 	curr += URI_;
			// }
			// free(item);
			free(buffer);
			break;
		}
		case READ_OP:
		{
			// Check if there was an associated block to the key.
			int err = map->get(key, &address_, &block_size_rtvd);
			slog_debug("[STAT WORKER] map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// Send the error code block.
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("HERCULES_ERR_STAT_WORKER_READ_OP_SEND_ERR");
					return -1;
				}
			}
			else
			{
				// dataset_info *dataset = (dataset_info*) address_;
				// printf("[STAT_SERVER] dataset.original=%s",dataset->original_name);
				// imss_info * data = (imss_info *) address_;
				// printf("READ_OP SEND data->type=%c",data->type);
				// Send the requested block.
				dataset = (dataset_info *)address_;
				slog_debug("Before dataset->n_open=%d", dataset->n_open);
				// Checks if the clients wants to open the file.
				switch (operation)
				{
				case 1: // file opened.
					dataset->n_open += 1;
					slog_debug("File opened");
					break;
				default:
					break;
				}

				slog_debug("After dataset->n_open=%d", dataset->n_open);
				msg_t m;
				m.data = address_;
				m.size = block_size_rtvd;
				err = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, (char *)&m, MSG, arguments->worker_uid);
				if (err < 0)
				{
					perror("HERCULES_ERR_STAT_WORKER_READ_OP_SEND_STREAM");
					return -1;
				}
			}
			break;
		}
		case RELEASE:
		{
			slog_debug("[stat_worker_thread][READ_OP][RELEASE] Deleting endpoint with %" PRIu64 "", arguments->worker_uid);
			map_server_eps_erase(map_server_eps, arguments->worker_uid, arguments->ucp_worker);
			// ucp_destroy(arguments->ucp_context);
			slog_debug("[stat_worker_thread][READ_OP][RELEASE] Endpoints deleted ");
			break;
			// return 0;
		}
		case DELETE_OP:
		{
			slog_debug("DELETE_OP");
			int err = map->get(key, &address_, &block_size_rtvd);
			slog_debug("[STAT WORKER] map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// Send the error code block.
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("ERRIMSS_WORKER_SENDERR");
					return -1;
				}
			}
			else
			{
				dataset = (dataset_info *)address_;

				// Checks if the clients wants to unlink the file.
				switch (operation)
				{
				case 4: // unlink.
					strncpy(dataset->status, "dest", strlen("dest"));
					slog_debug("Dataset mark as dest");
					break;
				default:
					break;
				}

				slog_debug("dataset->n_open=%d, dataset->status=%s", dataset->n_open, dataset->status);
				char release_msg[10];	  //= "DELETE\0";
				if (dataset->n_open == 0) // if no more process has the file opened.
				{
					int32_t result = map->delete_metadata_stat_worker(key);
					slog_debug("[stat_worker_thread][READ_OP][DELETE_OP] delete_metadata_stat_worker=%d", result);
					GTree_delete((char *)key.c_str());
					strncpy(release_msg, "DELETE\0", strlen("DELETE\0") + 1);
				}
				else
				{
					strncpy(release_msg, "NODELETE\0", strlen("NODELETE") + 1);
				}

				// char release_msg[] = "DELETE\0";
				slog_debug("release_msg=%s", release_msg);
				if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid) == 0)
				{
					perror("ERR_HERCULES_PUBLISH_DELETEMSG");
					slog_error("ERR_HERCULES_PUBLISH_DELETEMSG");
					return -1;
				}
			}
			break;
		}
		case RENAME_OP:
		{
			std::size_t found = key.find(' ');
			if (found != std::string::npos)
			{
				std::string old_key = key.substr(0, found);
				std::string new_key = key.substr(found + 1, key.length());

				slog_debug("[stat_worker_helper][RENAME] old_key=%s, new_key=%s\n", old_key.c_str(), new_key.c_str());

				// RENAME MAP
				int32_t result = map->rename_metadata_stat_worker(old_key, new_key);
				slog_live("rename metadata stat worker=%d", result);
				if (result == 0)
				{
					// printf("0 elements rename from stat_worker");
					slog_warn("[stat_worker_helper][RENAME] 0 elements rename from stat_worker");
					break;
				}

				// RENAME TREE
				int ret = GTree_rename((char *)old_key.c_str(), (char *)new_key.c_str());
				slog_debug("[stat_worker_helper][RENAME] GTree_rename=%d", ret);
			}

			char release_msg[] = "RENAME\0";

			if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid) == 0)
			{
				perror("ERR_HERCULES_PUBLISH_RENAMEMSG");
				perror("ERR_HERCULES_PUBLISH_RENAMEMSG");
				return -1;
			}
			break;
		}
		case RENAME_DIR_DIR_OP:
		{
			std::size_t found = key.find(' ');
			if (found != std::string::npos)
			{
				std::string old_dir = key.substr(0, found);
				std::string rdir_dest = key.substr(found + 1, key.length());

				// RENAME MAP
				map->rename_metadata_dir_stat_worker(old_dir, rdir_dest);

				// RENAME TREE
				GTree_rename_dir_dir((char *)old_dir.c_str(), (char *)rdir_dest.c_str());
			}

			char release_msg[] = "RENAME\0";

			if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid) == 0)
			{
				perror("ERR_HERCULES_PUBLISH_RENAMEMSG");
				slog_error("ERR_HERCULES_PUBLISH_RENAMEMSG");
				return -1;
			}
			break;
		}
		case CLOSE_OP:
		{
			slog_debug("CLOSE_OP");
			int err = map->get(key, &address_, &block_size_rtvd);
			slog_debug("[STAT WORKER] map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// Send the error code block.
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("ERRIMSS_WORKER_SENDERR");
					return -1;
				}
			}
			else
			{
				dataset = (dataset_info *)address_;
				// Checks if the clients wants to open the file.
				// switch (operation)
				// {
				// case 1: // file opened.
				slog_debug("Closing file, dataset->n_open=%d", dataset->n_open);
				if (dataset->n_open > 0)
				{
					dataset->n_open -= 1;
				}

				slog_debug("After dataset->n_open=%d, status=%s", dataset->n_open, dataset->status);
				char release_msg[10]; //= "DELETE\0";
									  // if file status is marked as "dest", it is delete after close.
				if (!strncmp(dataset->status, "dest", strlen("dest")) && dataset->n_open == 0)
				{
					slog_debug("Deleting %s", key.c_str());
					int32_t result = map->delete_metadata_stat_worker(key);
					slog_debug("[stat_worker_thread][READ_OP][DELETE_OP] delete_metadata_stat_worker=%d", result);
					GTree_delete((char *)key.c_str());
					strcpy(release_msg, "DELETE\0");
				}
				else
				{
					strcpy(release_msg, "CLOSE\0");
				}
				// int32_t result = map->delete_metadata_stat_worker(key);
				// slog_debug("[stat_worker_thread][READ_OP][DELETE_OP] delete_metadata_stat_worker=%d", result);
				// GTree_delete((char *)key.c_str());
				// char release_msg[] = "CLOSE\0";
				// if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, RESPONSE_SIZE, arguments->worker_uid) < 0)
				if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid) == 0)
				{
					perror("ERR_HERCULES_PUBLISH_DELETEMSG");
					slog_error("ERR_HERCULES_PUBLISH_DELETEMSG");
					return -1;
				}
			}
			break;
		}
		case OPEN_OP:
		{
			slog_debug("OPEN_OP");
			int err = map->get(key, &address_, &block_size_rtvd);
			slog_debug("[STAT WORKER] map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// Send the error code block.
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("ERRIMSS_WORKER_SENDERR");
					return -1;
				}
			}
			else
			{
				dataset = (dataset_info *)address_;
				// Checks if the clients wants to open the file.
				// switch (operation)
				// {
				// case 1: // file opened.
				slog_debug("Before dataset->n_open=%d", dataset->n_open);
				dataset->n_open += 1;
				slog_debug("File closed");
				// break;
				// default:
				// 	break;
				// }
				slog_debug("After dataset->n_open=%d, status=%s", dataset->n_open, dataset->status);
				char release_msg[10]; //= "DELETE\0";
				strncpy(release_msg, "OPEN", strlen("OPEN") + 1);
				// int32_t result = map->delete_metadata_stat_worker(key);
				// slog_debug("[stat_worker_thread][READ_OP][DELETE_OP] delete_metadata_stat_worker=%d", result);
				// GTree_delete((char *)key.c_str());
				// char release_msg[] = "CLOSE\0";
				if (send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid) == 0)
				{
					perror("ERR_HERCULES_PUBLISH_DELETEMSG");
					slog_error("ERR_HERCULES_PUBLISH_DELETEMSG");
					return -1;
				}
			}
			break;
		}
		default:
			break;
		}
		break;
	}
		// More messages will arrive to the socket.
	case SET_OP:
	{
		slog_debug("SET_OP");
		slog_debug("[STAT WORKER] Creating dataset %s.", key.c_str());
		pthread_mutex_lock(&mp);
		// If the record was not already stored, add the block.
		if (!map->get(key, &address_, &block_size_rtvd))
		{
			pthread_mutex_unlock(&mp);
			// slog_debug("[STAT WORKER] Adding new block %p", &address_);

			slog_debug("[STAT WORKER] Recv dynamic buffer size %ld", block_size_recv);
			// Get the length of the message to be received.
			size_t length = 0;
			int32_t ret = -1;
			length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
			if (length == 0)
			{
				perror("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
				slog_error("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
				return -1;
			}
			// Receive the block into the buffer.
			void *buffer = (void *)malloc(length * sizeof(char));
			ret = recv_dynamic_stream(arguments->ucp_worker, arguments->server_ep, buffer, BUFFER, arguments->worker_uid, length);

			dataset_info *struct_ = (dataset_info *)buffer;

			// ret = recv_dynamic_stream(arguments->ucp_worker, arguments->server_ep, buffer, BUFFER, arguments->worker_uid, length);
			// length = recv_dynamic_stream_opt(arguments->ucp_worker, arguments->server_ep, &buffer, BUFFER, arguments->worker_uid, length);
			slog_debug("[STAT WORKER] END Recv dynamic, n_server_when_created=%d", struct_->n_servers_when_created);

			if (ret < 0)
			{
				perror("HERCULES_ERR_STAT_WORKER_HELPER_RECV_DYNAMIC_STREAM");
				slog_error("HERCULES_ERR_STAT_WORKER_HELPER_RECV_DYNAMIC_STREAM");
				free(buffer);
				return -1;
			}

			int32_t insert_successful = -1;
			// insert_successful = map->put(key, buffer, block_size_recv);
			insert_successful = map->put(key, buffer, length);
			slog_debug("[STAT WORKER] map->put (key %s) err %d", key.c_str(), insert_successful);

			if (insert_successful != 0)
			{
				slog_error("HERCULES_ERR_METADATA_WORKER_MAPPUT_SET_OP");
				perror("HERCULES_ERR_METADATA_WORKER_MAPPUT_SET_OP");
				free(buffer);
				return -1;
			}

			// Insert the received uri into the directory tree.
			pthread_mutex_lock(&tree_mut);
			// slog_debug("[STAT WORKER] Inserting %s into directory tree", key.c_str());
			insert_successful = GTree_insert((char *)key.c_str());
			pthread_mutex_unlock(&tree_mut);

			if (insert_successful == -1)
			{
				slog_error("HERCULES_ERR_METADATA_WORKER_GTREEINSERT_SET_OP");
				perror("HERCULES_ERR_METADATA_WORKER_GTREEINSERT_SET_OP");
				free(buffer);
				// perror("ERRIMSS_STATWORKER_GTREEINSERT");
				return -1;
			}

			// Update the pointer.
			arguments->pt += block_size_recv;
			slog_debug("[STAT WORKER] Dataset %s has been created.", key.c_str());
		}
		// If was already stored:
		else
		{
			// Follow a certain behavior if the received block was already stored.
			slog_debug("[STAT WORKER] LOCAL DATASET_UPDATE %ld", block_size_recv);
			switch (1) // TO CKECK!
			{
			// Update where the blocks of a LOCAL dataset have been stored.
			case LOCAL_DATASET_UPDATE:
			{
				size_t msg_length = 0;
				msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
				if (msg_length == 0)
				{
					perror("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
					slog_error("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
					pthread_mutex_unlock(&mp);
					// perror("ERRIMSS_METADATA_LOCAL_DATASET_UPDATE_INVALID_MSG_LENGTH");
					return -1;
				}
				// // void data_ref[msg_length];
				void *data_ref = malloc(msg_length);
				// void *data_ref;
				// char data_ref[REQUEST_SIZE];
				msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, data_ref, msg_length, arguments->worker_uid, 0);
				if (msg_length == 0)
				{
					perror("HERCULES_ERR_METADATA_WORKER_RECV_DATA_SET_OP");
					slog_error("HERCULES_ERR_METADATA_WORKER_RECV_DATA_SET_OP");
					free(data_ref);
					pthread_mutex_unlock(&mp);
					// perror("ERRIMSS_METADATA_LOCAL_DATASET_UPDATE_RECV_DATA");
					return -1;
				}

				uint32_t data_size = RESPONSE_SIZE; // MIRAR

				// Value to be written in certain positions of the vector.
				uint16_t *update_value = (uint16_t *)(data_size + (char *)data_ref - 8);
				// Positions to be updated.
				uint32_t *update_positions = (uint32_t *)data_ref;

				// Set of positions that are going to be updated (those are just under the concerned dataset but not pointed by it).
				uint16_t *data_locations = (uint16_t *)((char *)address_ + sizeof(dataset_info));

				// Number of positions to be updated.
				int32_t num_pos_toupdate = (data_size / sizeof(uint32_t)) - 2;

				// Perform the update operation.
				for (int32_t i = 0; i < num_pos_toupdate; i++)
					data_locations[update_positions[i]] = *update_value;

				// Answer the client with the update.
				slog_debug("[STAT_WORKER] Updating existing dataset %s.", key.c_str());
				char answer[] = "UPDATED!\0";
				if (send_data(arguments->ucp_worker, arguments->server_ep, answer, strlen(answer) + 1, arguments->worker_uid) == 0)
				{
					slog_error("HERCULES_ERR_METADATA_WORKER_SEND_DATA_SET_OP");
					perror("HERCULES_ERR_METADATA_WORKER_SEND_DATA_SET_OP");
					// perror("ERRIMSS_WORKER_DATALOCATANSWER2");
					free(data_ref);
					pthread_mutex_unlock(&mp);
					return -1;
				}
				free(data_ref);

				break;
			}

			default:
			{
				// fprintf(stderr, "num_input_read=%d\n", num_input_read);
				int new_server_status = 0;
				int flag = 0;
				switch (num_input_read)
				{
				case 4: // we expect to get 4 values in a normal case.
					// we look for extra information.
					if (req[num_characters_read] != '\0')
					{
						slog_live("Extra characters found after expected input: '%s'\n", &req[num_characters_read]);
						// fprintf(stderr, "Extra characters found after expected input: '%s'\n", &req[num_characters_read]);
						// get the server status to be set.
						sscanf(&req[num_characters_read], "%d", &new_server_status);
						flag = 1;
					}
					else
					{
						slog_live("No extra characters found.\n");
						// fprintf(stderr, "No extra characters found.\n");
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

					char *address_aux = (char *)address_;
					// fprintf(stderr, "block_size_rtvd=%lu\n", block_size_rtvd);
					imss_info *imss_info_ = (imss_info *)malloc(block_size_rtvd * sizeof(imss_info));
					// = (imss_info *)address_aux;
					memcpy(imss_info_, address_aux, sizeof(imss_info));

					// imss_info_->num_active_storages--;
					imss_info_->num_active_storages = num_active_storages;
					memcpy(address_aux, imss_info_, sizeof(imss_info));

					// skip imss_info and num_storages.
					address_aux += sizeof(imss_info);
					address_aux += imss_info_->num_storages * LINE_LENGTH;
					// reserve memory to store the status list.
					imss_info_->status = (int *)malloc(imss_info_->num_storages * sizeof(int));
					// copy the status list.
					memcpy(imss_info_->status, address_aux, imss_info_->num_storages * sizeof(int));

					// int current_num_storages = imss_info_->num_storages;
					// slog_debug("[STAT_WORKER] prev. num data servers=%d", imss_info_->num_storages);
					slog_debug("[STAT_WORKER] changing data server %d with status=%d to a new status=%d", delete_dataserver_indx, imss_info_->status[delete_dataserver_indx], new_server_status);
					// free(imss_info_->ips[delete_dataserver_indx]);
					imss_info_->status[delete_dataserver_indx] = new_server_status;
					slog_debug("[STAT_WORKER] new num data servers=%d, new status=%d", imss_info_->num_active_storages, imss_info_->status[delete_dataserver_indx]);
					// address_ += sizeof(imss_info);
					// address_ += imss_info_->num_storages * LINE_LENGTH;
					memcpy(address_aux, imss_info_->status, imss_info_->num_storages * sizeof(int));
					// move the pointer to the arr_num_active_storages position.
					address_aux += (imss_info_->num_storages * sizeof(int));

					// copy the arr_num_active_storages list.
					imss_info_->arr_num_active_storages = (int *)malloc(imss_info_->num_storages * sizeof(int));
					memcpy(imss_info_->arr_num_active_storages, address_aux, imss_info_->num_storages * sizeof(int));
					// set the value.
					imss_info_->arr_num_active_storages[delete_dataserver_indx] = imss_info_->num_active_storages;
					memcpy(address_aux, imss_info_->arr_num_active_storages, imss_info_->num_storages * sizeof(int));
					free(imss_info_);
				}
				else
				{
					size_t length = -1;
					length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
					if (length == 0)
					{
						slog_error("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_UPDATE_DATASET");
						perror("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_UPDATE_DATASET");
						return -1;
					}
					// Clear the corresponding memory region.
					void *buffer = (void *)malloc(block_size_recv);
					// Receive the block into the buffer.
					recv_dynamic_stream(arguments->ucp_worker, arguments->server_ep, buffer, BUFFER, arguments->worker_uid, length);
					free(buffer);
					slog_debug("[STAT_WORKER] End Updating existing dataset %s.", key.c_str());
				}

				slog_debug("[STAT_WORKER] End Updating existing dataset %s.", key.c_str());
				break;
			}
			}
			pthread_mutex_unlock(&mp);
		}
		break;
	}
	default:
		break;
	}
	// pthread_mutex_unlock(&mp);

	slog_debug(" Terminated meta helper\n");

	return 0;
}

// Server dispatcher thread method.
void *srv_attached_dispatcher(void *th_argv)
{
	// Cast from generic pointer type to p_argv struct type pointer.
	p_argv *arguments = (p_argv *)th_argv;

	ucx_server_ctx_t context;
	ucp_worker_h ucp_data_worker;
	ucp_am_handler_param_t param;
	ucp_ep_h server_ep;
	ucs_status_t status;
	int ret;

	// Variable specifying the ID that will be granted to the next client.
	uint32_t client_id_ = 0;
	// char req[256];
	char *req;

	ret = init_worker(arguments->ucp_context, &ucp_data_worker);
	if (ret != 0)
	{
		perror("ERRIMSS_INIT_WORKER");
		pthread_exit(NULL);
	}

	/* Initialize the server's context. */
	context.conn_request = StsQueue.create();

	// Register signal handler
	// signal(SIGUSR1, handle_signal);

	for (;;)
	{
		ucp_conn_request_h conn_req;
		slog_debug("[DATA DISPATCHER] Waiting for connection requests.");

		while (StsQueue.size(context.conn_request) == 0)
		{
			ucp_worker_progress(arguments->ucp_worker);
			if (global_finish_threads == 1)
			{
				fprintf(stderr, "Ending srv_attached_dispatcher thread\n");
				pthread_exit(NULL);
			}
		}

		conn_req = (ucp_conn_request_h)StsQueue.pop(context.conn_request);

		status = server_create_ep(ucp_data_worker, conn_req, &server_ep);
		if (status != UCS_OK)
		{
			perror("ERRIMSS_SERVER_CREATE_EP");
			// ep_flush(server_ep, ucp_data_worker);
			ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
			pthread_exit(NULL);
		}

		// Save the identity of the requesting client.
		size_t msg_length = 0;
		msg_length = get_recv_data_length(ucp_data_worker, arguments->worker_uid);
		if (msg_length == 0)
		{
			perror("ERRIMSS_SRV_DISPATCHER_INVALID_MSG_LENGTH");
			slog_error("ERRIMSS_SRV_DISPATCHER_INVALID_MSG_LENGTH");
			pthread_exit(NULL);
		}

		// char mode[msg_length];
		req = (char *)malloc(msg_length * sizeof(char));
		// char mode[MODE_SIZE];
		msg_length = recv_data(ucp_data_worker, server_ep, (char *)req, msg_length, arguments->worker_uid, 0);
		// msg_length = recv_data_opt(ucp_data_worker, server_ep, (void **)&req, msg_length, arguments->worker_uid, 0);
		if (msg_length == 0)
		{
			perror("HERCULES_ERR_SRV_DISPATCHER_DATA_RECV_DATA");
			slog_error("HERCULES_ERR_SRV_DISPATCHER_DATA_RECV_DATA");
			// free(mode);
			free(req);
			pthread_exit(NULL);
		}

		char *mode = (char *)malloc(msg_length * sizeof(char));
		slog_info("Req=%s", req);

		sscanf(req, "%" PRIu32 " %s", &client_id_, mode);
		char *req_content = strstr(req, mode);
		req_content += 4;

		uint32_t c_id = client_id_;
		slog_info("req_content=%s", req_content);
		// Check if the client is requesting connection resources.
		if (!strncmp(req_content, "HELLO!", 6))
		{
			slog_info("Requesting resources, req=%s", req);
			if (strncmp(req_content, "HELLO!JOIN", 10) != 0)
			{
				// Retrieve the buffer size that will be asigned to the current server process.
				char buff[6];
				sscanf(req, "%s %ld %s", buff, &buffer_KB, att_imss_uri);
				strcpy(arguments->my_uri, att_imss_uri);

				// printf("MU URI: %s", att_imss_uri);

				// Notify that the value has been received.
				pthread_mutex_lock(&buff_size_mut);
				copied = 1;
				pthread_cond_signal(&buff_size_cond);
				pthread_mutex_unlock(&buff_size_mut);
			}

			// Message containing the client's communication ID plus its connection port.
			char response_[RESPONSE_SIZE];
			memset(response_, '\0', RESPONSE_SIZE);
			// Port that the new client will be forwarded to.
			int32_t port_ = arguments->port + 1 + (client_id_ % IMSS_THREAD_POOL);
			// Wrap the previous info into the ZMQ message.
			sprintf(response_, "%d%c%d", port_, '-', client_id_++);
			slog_info("Seding response_=%s", response_);
			// Send communication specifications.
			if (send_data(ucp_data_worker, server_ep, response_, strlen(response_) + 1, arguments->worker_uid) == 0)
			{
				perror("HERCULES_ERR_SRVDISP_SENDBLOCK");
				slog_error("HERCULES_ERR_SRVDISP_SENDBLOCK");
				// ep_flush(server_ep, ucp_data_worker);
				ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
				free(mode);
				free(req);
				pthread_exit(NULL);
			}

			slog_debug("[DATA DISPATCHER] Replied client %s.", response_);
			free(mode);
			free(req);
			continue;
		}
		// Check if someone is requesting identity resources.
		else if (*((int32_t *)req) == WHO) // MIRAR
		{
			// Provide the uri of this instance.
			if (send_data(ucp_data_worker, server_ep, arguments->my_uri, strlen(arguments->my_uri) + 1, arguments->worker_uid) == 0) // MIRAR
			{
				perror("ERR_HERCULES_WHOREQUEST");
				slog_error("ERR_HERCULES_WHOREQUEST");
				// ep_flush(server_ep, ucp_data_worker);
				ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
				free(mode);
				free(req);
				pthread_exit(NULL);
			}
		}
		// context.conn_request = NULL;
		ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_MODE_FLUSH);
		free(mode);
		free(req);
	}
	pthread_exit(NULL);
}

// Metadata dispatcher thread method.
void *dispatcher(void *th_argv)
{
	// Enable thread cancellation
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

	// Set the cancellation type to deferred
	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

	// Cast from generic pointer type to p_argv struct type pointer.
	p_argv *arguments = (p_argv *)th_argv;

	uint32_t client_id_ = 0;
	char req[REQUEST_SIZE];
	struct sockaddr_in server_addr;
	socklen_t addrlen = sizeof(server_addr);
	int ret;
	int listenfd = -1;
	int optval = 1;
	// char service[8];
	char *tmp_file_path = arguments->tmp_file_path;
	int client = 0;

	// Get a socket file descriptor.
	global_server_fd_thread = socket(AF_INET, SOCK_STREAM, 0);
	if (global_server_fd_thread < 0)
	{
		perror("ERR_HERCULES_DISPATCHER_SOCKET");
		ready(tmp_file_path, "ERROR");
		pthread_exit(NULL);
	}

	// To reuse the address and port.
	ret = setsockopt(global_server_fd_thread, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
	if (ret < 0)
	{
		perror("ERR_HERCULES_DISPATCHER_SET_SOCKET_OPT");
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
		perror("ERR_HERCULES_DISPATCHER_BIND");
		ready(tmp_file_path, "ERROR");
		pthread_exit(NULL);
	}

	// Prepare to accept connections.
	ret = listen(global_server_fd_thread, 100);
	if (ret < 0)
	{
		perror("ERR_HERCULES_DISPATCHER_LISTEN");
		ready(tmp_file_path, "ERROR");
		pthread_exit(NULL);
	}

	/* Accept next connection */
	// listenfd = sockfd;
	int new_socket = -1;
	while (1)
	{
		ucs_status_t status;
		char mode[MODE_SIZE];

		// slog_debug("[DISPATCHER] Waiting for connection requests.");
		// fprintf(stderr, "[DISPATCHER] Waiting for connection requests.\n");
		new_socket = accept(global_server_fd_thread, (struct sockaddr *)&server_addr, &addrlen);

		if (global_finish_threads == 1)
		{
			fprintf(stderr, "Ending dispatcher thread.\n");
			pthread_exit(NULL);
		}

		if (new_socket < 0)
		{
			// slog_error("ERR_HERCULES_DISPATCHER_ACCEPT");
			continue;
		}
		ret = recv(new_socket, req, REQUEST_SIZE, MSG_WAITALL);
		if (ret < 0)
		{
			slog_error("ERR_HERCULES_DISPATCHER_RECV");
		}

		sscanf(req, "%" PRIu32 " %s", &client_id_, mode);

		char *req_content = strstr(req, mode);
		req_content += 4;

		slog_debug("[DISPATCHER] req=%s, req_content=%s", req, req_content);

		// Check if the client is requesting connection resources.
		if (!strncmp(req_content, "HELLO!", 6))
		{
			ret = send(new_socket, &local_addr_len[(client % IMSS_THREAD_POOL)], sizeof(local_addr_len[(client % IMSS_THREAD_POOL)]), 0);
			if (ret == -1)
			{
				slog_error("ERR_HERCULES_DISPATCHER_SEND1");
			}
			ret = send(new_socket, local_addr[(client % IMSS_THREAD_POOL)], local_addr_len[(client % IMSS_THREAD_POOL)], 0);
			if (ret == -1)
			{
				slog_error("ERR_HERCULES_DISPATCHER_SEND2");
			}
			client++;
			slog_debug("[DISPATCHER] Replied client.");
		}
		else if (!strncmp(req_content, "MAIN!", 5))
		{
			// TO FIX: 0 must be a dynamic value depending on the number of
			// metadata servers.
			ret = send(new_socket, &local_addr_len[0], sizeof(local_addr_len[0]), 0);
			ret = send(new_socket, local_addr[0], local_addr_len[0], 0);
		}
		// Check if someone is requesting identity resources.
		else if (*((int32_t *)req) == WHO)
		{
			ret = send(new_socket, &local_addr_len[client], sizeof(local_addr_len[client]), 0);
			ret = send(new_socket, local_addr[client], local_addr_len[client], 0);
			slog_debug("[DISPATCHER] Replied client %s.", arguments->my_uri);
		}

		// MIRAR ucp_worker_release_address(ucp_worker_threads[client_id_ % IMSS_THREAD_POOL], local_addr);
		close(new_socket);
	}
	close(global_server_fd_thread);

	pthread_exit(NULL);
}
