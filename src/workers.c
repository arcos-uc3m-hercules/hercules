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
#include "redis.h"
#include "imss.h"
#include "workers.h"
#include "directory.h"
#include "records.hpp"
#include "map_server_eps.hpp"

// Lock dealing when cleaning blocks
pthread_mutex_t mutex_garbage = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_snapshot = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_checkpoint = PTHREAD_MUTEX_INITIALIZER;

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

extern pthread_mutex_t hiredis_mut;
pthread_mutex_t mp = PTHREAD_MUTEX_INITIALIZER;

extern redisContext *hiredis_context;

ucp_worker_h *ucp_worker_threads;
ucp_address_t **local_addr;
size_t *local_addr_len;

int global_finish_threads = 0;
int global_finish_checkpoint = 0;
int global_finish_snapshot = 0;
int global_server_fd_thread = -1;
pthread_cond_t global_broadcast_cond;
pthread_cond_t global_finish_cond;
pthread_cond_t global_run_snapshot_cond;
pthread_cond_t global_run_checkpoint_cond;
pthread_mutex_t global_finish_mut = PTHREAD_MUTEX_INITIALIZER;

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

	//fprintf(stderr, "Writting status %s (%zu bytes) file in: %s\n", msg, strlen(status), tmp_file_path);

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

// void handle_signal(int signal)
// {
// 	if (signal == SIGUSR1)
// 	{
// 		fprintf(stderr, "*** Received SIGUSR1\n");
// 		global_finish_threads = 1;

// 		// To dispatcher thread.
// 		if (shutdown(global_server_fd_thread, SHUT_RD) == -1)
// 		{
// 			fprintf(stderr, "Error closing server_fd\n");
// 		}
// 	}
// }

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
		// errno = 0;
		size_t peer_addr_len = 0;
		ucp_address_t *peer_addr;
		ucs_status_t ep_status = UCS_OK;
		ucp_ep_h ep = NULL;
		struct ucx_context *request = NULL;
		char *req;
		ucp_tag_recv_info_t info_tag;
		ucp_tag_message_h msg_tag;
		msg_req_t *msg;
		ucp_request_param_t recv_param;

		clock_t t;
		double time_taken;
		t = clock();

		// register signal handler
		// signal(sigusr1, handle_signal);

		do
		{
			/* progressing before probe to update the state */
			ucp_worker_progress(arguments->ucp_worker);
			/* probing incoming events in non-block mode */
			msg_tag = ucp_tag_probe_nb(arguments->ucp_worker, tag_req, tag_mask, 1, &info_tag);
			if (global_finish_threads == 1)
			{
				fprintf(stderr, "ending data server thread.\n");
				pthread_exit(null);
			}
		} while (msg_tag == null);
		// ucs_status_t status;
		// /* receive test string from server */
		// for (;;)
		// {
		// 	/* probing incoming events in non-block mode */
		// 	msg_tag = ucp_tag_probe_nb(arguments->ucp_worker, tag_req, tag_mask, 1, &info_tag);
		// 	if (msg_tag != null)
		// 	{
		// 		/* message arrived */
		// 		break;
		// 	}
		// 	else if (ucp_worker_progress(arguments->ucp_worker))
		// 	{
		// 		/* some events were polled; try again without going to sleep */
		// 		continue;
		// 	}
		// 	/* if we got here, ucp_worker_progress() returned 0, so we can sleep.
		// 	 * following blocked methods used to polling internal file descriptor
		// 	 * to make cpu idle and don't spin loop
		// 	 */
		// 	// if (ucp_test_mode == test_mode_wait)
		// 	{
		// 		/* polling incoming events*/
		// 		status = ucp_worker_wait(arguments->ucp_worker);
		// 		// chkerr_jump(status != ucs_ok, "ucp_worker_wait\n", err_ep);
		// 	}
		// 	// else if (ucp_test_mode == test_mode_eventfd)
		// 	// {
		// 	// 	status = test_poll_wait(ucp_worker);
		// 	// 	chkerr_jump(status != ucs_ok, "test_poll_wait\n", err_ep);
		// 	// }
		// }

		slog_debug("[srv_worker] message length=%ld bytes.", info_tag.length);
		msg = (msg_req_t *)malloc(info_tag.length);

		recv_param.op_attr_mask = ucp_op_attr_field_callback |
								  ucp_op_attr_field_datatype;

		recv_param.datatype = ucp_dt_make_contig(1);
		recv_param.cb.recv = recv_handler;

		request = (struct ucx_context *)ucp_tag_msg_recv_nbx(arguments->ucp_worker, msg, info_tag.length, msg_tag, &recv_param);

		status = ucx_wait(arguments->ucp_worker, request, "receive", "srv_worker");

		peer_addr_len = msg->addr_len;
		peer_addr = (ucp_address *)malloc(peer_addr_len);
		req = msg->request;

		memcpy(peer_addr, msg + 1, peer_addr_len);

		ucp_worker_address_attr_t attr;
		attr.field_mask = ucp_worker_address_attr_field_uid;
		ucp_worker_address_query(peer_addr, &attr);
		slog_debug(" receiving request from %" priu64 ".", attr.worker_uid);

		//  look for this peer_addr in the map and get the ep
		ret = map_server_eps_search(map_server_eps, attr.worker_uid, &ep);
		// create ep if it's not in the map
		if (ret < 0)
		{
			// ucp_ep_h new_ep;
			ep_params.address = peer_addr;
			ep_params.err_mode = ucp_err_handling_mode_peer;
			// ep_params.err_mode = ucp_err_handling_mode_none;
			// ep_params.err_handler.cb = err_cb_server;
			ep_params.err_handler.cb = failure_handler;
			ep_params.err_handler.arg = null;
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
			slog_debug("\t[srv_worker]['%" priu64 "] endpoint already exist'", attr.worker_uid);
		}

		arguments->peer_address = peer_addr;
		arguments->server_ep = ep;
		arguments->worker_uid = attr.worker_uid;

		srv_worker_helper(arguments, req);
		t = clock() - t;

		time_taken = ((double)t) / clocks_per_sec; // in seconds
		slog_info("[srv_worker] serving time %f s\n", time_taken);
		// status = flush_ep(arguments->ucp_worker, ep);
		// slog_debug("flush_ep completed with status %d (%s)\n", status, ucs_status_string(status));

		free(msg);
		free(peer_addr);
	}
}

int srv_worker_helper(p_argv *arguments, const char *req)
{

	ucs_status_t status;
	int ret = -1;

	// cast from generic pointer type to p_argv struct type pointer.

	// obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;

	// resources specifying if the request set in the sender.
	int64_t more = 0;
	size_t more_size = sizeof(more);
	int is_shared_memory = 0, snapshot_op = 0;

	// code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$errimss_no_key_avail$";
	char mode[mode_size];

	// slog_debug(" waiting for new request.");
	// save the request to be served.
	slog_debug(" request to be served %s", req);

	// elements conforming the request.
	uint32_t block_size_recv, block_offset;
	char uri_[uri_];
	size_t to_read = 0;
	int sender = 0;

	sscanf(req, "%s", mode);

	if (!strcmp(mode, "broadcast"))
	{
		sscanf(req, "%s %s %d", mode, uri_, &sender);
		slog_debug("broadcast condition, req=%s, mode=%s, uri_=%s, sender=%d", req, mode, uri_, sender);
		map->put_snapshot(uri_, sender);
		fprintf(stderr, "sending signal to do snapshot in server %d\n", arguments->args.id);
		pthread_cond_signal(&global_run_snapshot_cond);
		pthread_cond_signal(&global_run_checkpoint_cond);
		// nothing else to do.
		return 0;
	}

	sscanf(req, "%s %" priu32 " %" priu32 " %s %lu", mode, &block_size_recv, &block_offset, uri_, &to_read);
	if (!strcmp(mode, "get"))
	{
		more = get_op;
	}
	if (!strcmp(mode, "set"))
	{
		more = set_op;
	}
	if (!strcmp(mode, "localget"))
	{
		more = get_op;
		is_shared_memory = 1;
	}
	if (!strcmp(mode, "localset"))
	{
		more = set_op;
		is_shared_memory = 1;
	}
	if (!strcmp(mode, "snapset"))
	{
		more = set_op;
		snapshot_op = 1;
	}

	slog_debug(" request - mode '%s', block_size_recv '%" priu32 "', block_offset '%" priu32 "', uri_ '%s', more %ld", mode, block_size_recv, block_offset, uri_, more);

	// create an std::string in order to be managed by the map structure.
	std::string key;
	key.assign((const char *)uri_);

	// information associated to the arriving key.
	void *address_;
	uint64_t block_size_rtvd;

	// differentiate between read and write operations.
	switch (more)
	{
	// no more messages will arrive to the socket.
	case read_op:
	{
		switch (block_size_recv)
		{
		case read_op:
		{
			int32_t ret = map->get(key, &address_, &block_size_rtvd);
			// check if there was an associated block to the key.
			if (ret == 0)
			{
				// send the error code block.
				ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, string, arguments->worker_uid);
				if (ret < 0)
				{
					slog_error("err_hercules_worker_send_read_op");
					perror("err_hercules_worker_send_read_op");
					return -1;
				}
			}
			else
			{
				// ret = timing(send_data(arguments->ucp_worker, arguments->server_ep, address_, block_size_rtvd, arguments->worker_uid), "[read_op][read_op] send the requested block");
				if (to_read <= 0)
				{
					to_read = block_size_rtvd;
				}
				// if (is_shared_memory)
				// {
				// 	slog_debug("[read_op][read_op] send requested block size, key=%s", key.c_str());
				// 	// send the size of the block, without any data.
				// 	char size_of_block[10];
				// 	sprintf(size_of_block, "%lu", to_read);
				// 	ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, size_of_block, string, arguments->worker_uid);
				// 	if (ret < 0)
				// 	{
				// 		slog_error("err_hercules_worker_send_read_op");
				// 		perror("err_hercules_worker_send_read_op");
				// 		return -1;
				// 	}
				// }
				// else
				{
					// send the requested block.

					// struct stat *stats;
					// stats = (struct stat *)address_;
					// slog_debug("[read_op][read_op] send the requested block with key=%s, block_offset=%ld, block_size_rtvd=%ld kb, to_read=%ld kb, stat->st_nlink=%lu, is_shared_memory=%d", key.c_str(), block_offset, block_size_rtvd / 1024, to_read / 1024, stats->st_nlink, is_shared_memory);
					slog_debug("[read_op][read_op] send the requested block with key=%s, block_offset=%ld, block_size_rtvd=%ld kb, to_read=%ld kb, is_shared_memory=%d", key.c_str(), block_offset, block_size_rtvd / 1024, to_read / 1024, is_shared_memory);
					size_t ret_send_data = 0;
					if (is_shared_memory)
					{
						ret_send_data = send_data(arguments->ucp_worker, arguments->server_ep, (char *)address_, block_size_rtvd, arguments->worker_uid);
					}
					else
					{
						ret_send_data = send_data(arguments->ucp_worker, arguments->server_ep, (char *)address_ + block_offset, to_read, arguments->worker_uid);
					}
					slog_debug("[read_op][read_op] send_data, ret_send_data=%lu", ret_send_data);
					if (ret_send_data == 0)
					{
						slog_error("hercules_err_worker_sendblock");
						perror("hercules_err_worker_sendblock");
						return -1;
					}
				}
			}
			break;
		}
		case release:
		{
			map_server_eps_erase(map_server_eps, arguments->worker_uid, arguments->ucp_worker);
			slog_debug("[read_op][release]");
			/*
			response_msg = msg_release_op;
			ret = sendconfirmationmessage(arguments, response_msg);
			if (ret == 0)
			{
				perror("err_hercules_srv_send_data_release");
				slog_error("err_hercules_srv_send_data_release");
				return -1;
			}
				*/
			break;
		}
		case delete_op:
		{
			slog_debug("delete_op");
			slog_debug("cleaning %s", key.c_str());
			map->cleaning_specific(key);
			char release_msg[] = "delete\0";
			ret = send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid);
			if (ret == 0)
			{
				perror("err_hercules_publish_deleteop");
				slog_error("err_hercules_publish_deleteop");
				return -1;
			}
			break;
		}
		case rename_op:
		{
			std::size_t found = key.find(',');
			slog_debug("[rename_op], key=%s, found=%d", key.c_str(), found);
			if (found != std::string::npos)
			{
				slog_debug("[rename_op], found != npos");
				std::string old_key = key.substr(0, found);
				std::string new_key = key.substr(found + 1, key.length());
				slog_debug("[rename_op], old_key=%s, new_key=%s", old_key.c_str(), new_key.c_str());
				// rename map
				map->cleaning_specific(new_key);
				int32_t result = map->rename_data_srv_worker(old_key, new_key);
				if (result == 0)
				{
					break;
				}
			}
			else
			{
				slog_debug("[rename_op], found == npos");
			}

			char release_msg[] = "rename\0";
			ret = network_timing(send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid), "[read_op][rename_op] send rename", int);
			if (ret == 0)
			{
				perror("err_hercules_publish_renamemsg");
				slog_error("err_hercules_publish_renamemsg");
				return -1;
			}
			break;
		}
		case rename_dir_dir_op:
		{
			// printf("srv_worker rename_dir_dir_op");
			std::size_t found = key.find(' ');
			if (found != std::string::npos)
			{
				std::string old_dir = key.substr(0, found);
				std::string rdir_dest = key.substr(found + 1, key.length());

				// rename map
				map->rename_data_dir_srv_worker(old_dir, rdir_dest);
			}

			char release_msg[] = "rename\0";
			ret = network_timing(send_data(arguments->ucp_worker, arguments->server_ep, release_msg, strlen(release_msg) + 1, arguments->worker_uid), "[read_op][rename_dir_dir_op] send rename", int);
			if (ret == 0)
			{
				perror("err_hercules_publish_renamemsg");
				slog_error("err_hercules_publish_renamemsg");
				return 1;
			}
			break;
		}
		case readv: // only 1 server work
		{
			// printf("readv case");
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

				// needed variables
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
					// std::cout <<"server readv element:" << element << '';
					if (map->get(element, &address_, &block_size_rtvd) == 0)
					{ // if dont exist
					  // send the error code block.
					  // std::cout <<"server readv no existe element:" << element << '';
						ret = network_timing(send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, string, arguments->worker_uid), "[read_op][readv] send_dynamic_stream", int);
						if (ret < 0)
						{
							perror("errimss_worker_senderr");
							free(buf);
							return -1;
						}
					} // if was already stored:
					else
					{
						// first block case
						if (first == 0)
						{
							if (size < stats->st_size - start_offset)
							{
								// to_read = size;
								to_read = blocksize * kb - start_offset;
							}
							else
							{
								if (stats->st_size < blocksize * kb)
								{
									to_read = stats->st_size - start_offset;
								}
								else
								{
									to_read = blocksize * kb - start_offset;
								}
							}
							// check if offset is bigger than filled, return 0 because is eof case
							if (start_offset > stats->st_size)
								return 0;
							memcpy(buf, (char *)address_ + start_offset, to_read);
							byte_count += to_read;
							++first;

							// middle block case
						}
						else if (curr_blk != end_blk)
						{
							memcpy((char *)buf + byte_count, address_, blocksize * kb);
							byte_count += blocksize * kb;
							// end block case
						}
						else
						{
							// read the minimum between end_offset and filled (read_ = min(end_offset, filled))
							int64_t pending = size - byte_count;
							memcpy((char *)buf + byte_count, address_, pending);
							byte_count += pending;
						}
					}
					++curr_blk;
				}
				ret = network_timing(send_data(arguments->ucp_worker, arguments->server_ep, buf, size, arguments->worker_uid), "[read_op][readv] send", int);
				// send the requested block.
				if (ret == 0)
				{
					perror("err_hercules_worker_sendblock");
					slog_error("err_hercules_worker_sendblock");
					free(buf);
					return -1;
				}
				free(buf);
			}
			break;
		}
		case split_readv:
		{
			// printf("split_readv case");
			slog_debug("key=%s", key.c_str());
			std::size_t found = key.find(' ');
			std::string path;
			if (found != std::string::npos)
			{
				path = key.substr(0, found);
				key.erase(0, found + 1);

				found = key.find(' ');
				int blocksize = stoi(key.substr(0, found)) * kb;
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
					perror("errimss_data_worker_invalid_msg_length");
					slog_error("errimss_data_worker_invalid_msg_length");
					return -1;
				}
				void *msg = malloc(msg_length);

				msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, msg, msg_length, arguments->worker_uid, 0);
				// msg_length = recv_data_opt(arguments->ucp_worker, arguments->server_ep, &msg, msg_length, arguments->worker_uid, 0);
				// send the requested block.
				if (msg_length == 0)
				{
					perror("errimss_data_worker_recv_data");
					slog_error("errimss_data_worker_recv_data");
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
				// needed variables
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
					{ // if dont exist
					  // send the error code block.
						if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, string, arguments->worker_uid) < 0)
						{
							free(msg);
							return -1;
							pthread_exit(null);
						}
					} // if was already stored:

					memcpy(buf + byte_count, address_, blocksize);
					byte_count += blocksize;
				}
				// send the requested block.
				ret = network_timing(send_data(arguments->ucp_worker, arguments->server_ep, buf, byte_count, arguments->worker_uid), "[read_op][readv] send buf", int);
				if (ret == 0)
				{
					free(msg);
					free(buf);
					perror("err_hercules_worker_sendblock");
					slog_error("err_hercules_worker_sendblock");
					return -1;
				}
				free(msg);
				free(buf);
			}
			break;
		}
		case who:
		{
			// provide the uri of this instance.
			ret = network_timing(send_data(arguments->ucp_worker, arguments->server_ep, arguments->my_uri, strlen(arguments->my_uri) + 1, arguments->worker_uid), ("[read_op][who] send uri: %s", arguments->my_uri), int);
			if (ret == 0)
			{
				perror("err_hercules_whorequest");
				slog_error("err_hercules_whorequest");
				return -1;
			}
			break;
		}
		default:
			break;
		}
		break;
	}
	// more messages will arrive to the socket.
	case write_op:
	{
		int op;
		std::size_t found = key.find(' ');
		std::size_t found2 = key.find("[op]=");
		slog_debug("[write_op] found=%d, found2=%d", found, found2);
		if (found2 != std::string::npos)
		{
			slog_debug("[write_op] entra en found2");
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
			int imss_data_bsize = stoi(key.substr(0, found));
			key.erase(0, found + 1);

			int size = stoi(key);

			size_t msg_length = 0;
			msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
			if (msg_length == 0)
			{
				slog_error("errimss_data_worker_worker_op_1_invalid_msg_length");
				perror("errimss_data_worker_worker_op_1_invalid_msg_length");
				return -1;
			}

			char *buf = (char *)malloc(msg_length * sizeof(char));
			// receive all blocks into the buffer.
			msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)buf, msg_length, arguments->worker_uid, 0);
			// msg_length = recv_data_opt(arguments->ucp_worker, arguments->server_ep, (void **)&buf, msg_length, arguments->worker_uid, 0);
			if (msg_length == 0)
			{
				perror("errimss_data_worker_worker_op_1_recv_data");
				slog_error("errimss_data_worker_worker_op_1_recv_data");
				free(buf);
				return -1;
			}

			int pos = path.find('$');
			std::string first_element = path.substr(0, pos + 1);
			first_element = first_element + "0";
			map->get(first_element, &address_, &block_size_rtvd);
			// imss_info * data = (imss_info *) address_;
			// printf("read_op send data->type=%c",data->type);
			struct stat *stats = (struct stat *)address_;

			// needed variables
			size_t byte_count = 0;
			int first = 0;
			int ds = 0;
			int64_t to_copy = 0;
			uint32_t filled = 0;
			void *aux = (void *)malloc(imss_data_bsize);
			int count = 0;
			// for the rest of blocks
			while (curr_blk <= end_blk)
			{
				// printf("nodename    - %s current_block=%d", detect.nodename, curr_blk);
				count = count + 1;
				// printf("count=%d",count);
				pos = path.find('$');
				std::string element = path.substr(0, pos + 1);
				element = element + std::to_string(curr_blk);
				// std::cout <<"element:" << element << '';

				// first fragmented block
				if (first == 0 && start_offset && stats->st_size != 0)
				{
					// get previous block
					map->get(element, &aux, &block_size_rtvd); // path por curr_block
															   // bytes to write are the minimum between the size parameter and the remaining space in the block (blocksize-start_offset)
					to_copy = (size < imss_data_bsize - start_offset) ? size : imss_data_bsize - start_offset;

					memcpy((char *)aux + start_offset, buf + byte_count, to_copy);
				}
				// last block
				else if (curr_blk == end_blk)
				{
					if (end_offset != 0)
					{
						to_copy = end_offset;
					}
					else
					{
						to_copy = imss_data_bsize;
					}
					// only if last block has contents
					if (curr_blk <= stats->st_blocks && start_offset)
					{
						map->get(element, &aux, &block_size_rtvd); // path por curr_block
					}
					else
					{
						memset(aux, 0, imss_data_bsize);
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
					to_copy = imss_data_bsize;
					memcpy(aux, buf + byte_count, to_copy);
				}

				// write and update variables
				if (!map->get(element, &address_, &block_size_rtvd))
				{
					map->put(element, aux, block_size_rtvd);

					// printf("nodename    - %s after put", detect.nodename);
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
			int16_t off = (end_blk * imss_data_bsize) - 1 - size;
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
			// printf("nodename	-%s split writev",detect.nodename);

			path = key.substr(0, found);
			key.erase(0, found + 1);

			found = key.find(' ');
			int blocksize = stoi(key.substr(0, found)) * kb;
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

			// receive all blocks into the buffer.
			size_t msg_length = 0;
			msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
			if (msg_length == 0)
			{
				perror("errimss_worker_data_write_op_2_invalid_msg_length");
				slog_error("errimss_worker_data_write_op_2_invalid_msg_length");
				return -1;
			}

			void *buf = malloc(msg_length);

			msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, buf, msg_length, arguments->worker_uid, 0);
			// msg_length = recv_data_opt(arguments->ucp_worker, arguments->server_ep, &buf, msg_length, arguments->worker_uid, 0);
			if (msg_length == 0)
			{
				perror("errimss_worker_data_write_op_2_recv_data");
				slog_error("errimss_worker_data_write_op_2_recv_data");
				free(buf);
				return -1;
			}

			// size_recv = size; // mirar
			int32_t insert_successful;

			// printf("nodename	-%s size_recv=%d",detect.nodename,size_recv);
			// printf("salida buf full=%c",buf[100]);

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
					// if don't exist
					char *buffer = (char *)aligned_alloc(1024, blocksize);
					memcpy(buffer, (char *)buf + byte_count, blocksize);
					// printf("salida buffer part=%c",buffer[100]);
					insert_successful = map->put(element, buffer, block_size_recv);
					if (insert_successful != 0)
					{
						perror("errimss_worker_mapput");
						return -1;
					}
				}
				else
				{
					// if already exits
					memcpy(address_, (char *)buf + byte_count, blocksize);
				}
				byte_count = byte_count + blocksize;
			}
			free(buf);
		}
		else
		{
			slog_debug("[write_op] write normal case. size %ld, offset=%ld", block_size_recv, block_offset);
			// search for the block to know if it was previously stored.
			int ret = 0;
			// checks if it is data for the snapshot operation or regular data.
			if (snapshot_op)
			{
				// nothing to do.
			}
			else
			{
				ret = map->get(key, &address_, &block_size_rtvd);
			}

			// if the block was not already stored:
			if (ret == 0)
			{
				slog_debug("[write_op] no key find %s", key.c_str());
				clock_t tp;
				tp = clock();
				void *buffer = null;
				tp = clock() - tp;
				double time_taken2 = ((double)tp) / clocks_per_sec; // in seconds
				//  receive the block into the buffer.
				clock_t tr;
				// get the buffer data length.
				size_t msg_length = 0;
				int size_asigned_to_block = 0;
				// if data is stored in shared memory due local policy, the server does not need to receive the data.
				if (!is_shared_memory)
				{
					slog_debug("[write_op] is_shared_memory=%d", is_shared_memory);
					// get the length of the data to be received.
					msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
					if (msg_length == 0)
					{
						perror("hercules_err_data_worker_write_new_block_invalid_msg_length");
						slog_error("hercules_err_data_worker_write_new_block_invalid_msg_length");
						return -1;
					}
					slog_debug("[write_op] msg_length=%lu", msg_length);
					// buffer = (void *)stsqueue.pop(mem_pool);
					if (buffer == null)
					{
						slog_debug("allocating buffer");
						if (snapshot_op)
						{ // snapshot operation sends data bigger than block_size.
							// buffer = timing((void *)mem_type_malloc(msg_length * sizeof(char));, "[write] mem_type_malloc snapshot", void *, arguments->thread_id);
							buffer = (void *)malloc(msg_length * sizeof(char));
							slog_debug("allocating buffer of size %lu, snapshot_op=%d", msg_length, snapshot_op);
						}
						else
						{
							// buffer = timing((void *)mem_type_malloc(block_size * sizeof(char));, "[write] mem_type_malloc", void *, arguments->thread_id);
							buffer = (void *)malloc(block_size * sizeof(char));
							slog_debug("allocating buffer of size %lu", block_size);
						}
					}
					else
					{
						slog_debug("reusing buffer");
					}

					if (buffer == null)
					{
						perror("hercules_err_memory_allocation");
						slog_error("hercules_err_memory_allocation");
						return -1;
					}
					size_asigned_to_block = block_size;

					// receive the data from the front end.
					msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)buffer + block_offset, msg_length, arguments->worker_uid, 1);
					if (msg_length == 0)
					{
						perror("hercules_err_data_worker_write_new_block_recv_data");
						slog_error("hercules_err_data_worker_write_new_block_recv_data");
						return -1;
					}
				}
				else // data in shared memory.
				{
					//  tell the client this is a new block to be copy in the shared memory with an offset "global_offset".
					char answer[response_size];
					sprintf(answer, "new %ld", global_offset);
					slog_info("answer=%s", answer);
					ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, answer, string, arguments->worker_uid);
					if (ret < 0)
					{
						perror("err_hercules_worker_send_dynamic_shm_local");
						slog_error("err_hercules_worker_send_dynamic_shm_local");
						return -1;
					}

					// find the length of the string required to store the number, including the null terminator
					int length_number = snprintf(null, 0, "%lu %d", global_offset, block_size_recv) + 1;
					buffer = (void *)calloc(length_number, sizeof(char));
					if (buffer == null)
					{
						perror("hercules_err_memory_allocation_shm");
						slog_error("hercules_err_memory_allocation_shm");
						return -1;
					}

					// when using shared memory, "buffer" will store the offset and the block size.
					ret = snprintf((char *)buffer, length_number, "%lu %d", global_offset, block_size_recv);
					if (ret < 0)
					{
						perror("hercules_err_encoding");
						slog_error("hercules_err_encoding");
						return -1;
					}

					slog_info("buffer=%s, length_number=%d, global_offset=%lu", buffer, length_number, global_offset);

					size_asigned_to_block = length_number;
					global_offset += block_size_recv;
				}

				int32_t insert_successful;

				// include the new record in the tracking structure.
				tr = clock();
				// fprintf(stderr,"[write_op] ****[put]********* key=%s\n",  key.c_str());
				slog_debug("[write_op] ****[put, block_size_recv=%ld, block_size=%lu, msg_length=%lu]********* key=%s", block_size_recv, block_size, msg_length, key.c_str());
				// todo: should this be block_size_recv or a different size? block_size_recv might not be the full block size
				if (snapshot_op)
				{
					// get the origin data server id from the received key.
					// int origin_server_id = 0;
					// std::string data_uri;
					// std::string file_name;
					// getblockinformation(key, &origin_server_id, &data_uri, &file_name);
					// slog_debug("key: %s, origin_server_id: %d, data_uri: %s, file_name: %s", key.c_str(), origin_server_id, data_uri.c_str(), file_name.c_str());
					// fill buffer_broadcast with the data received from the other servers.
					// buffer_broadcast[];
					slog_debug("snapshot operation, origin server=%s", key.c_str());
					insert_successful = map->put_broadcast(key, buffer, msg_length);
				}
				else
				{
					insert_successful = map->put(key, buffer, size_asigned_to_block);
				}
				slog_debug("[write_op] insert_successful=%d, key=%s, size_asigned_to_block=%d", insert_successful, key.c_str(), size_asigned_to_block);
				tr = clock() - tr;
				double time_taken = ((double)tr) / clocks_per_sec; // in seconds

				// include the new record in the tracking structure.
				if (insert_successful != 0)
				{
					perror("hercules_err_worker_mapput");
					slog_error("hercules_err_worker_mapput");
					free(buffer);
					return -1;
				}

				// // only when there is not a snapshot operation.
				// if (!snapshot_op)
				// {
				// 	// the following buffer is used for sanpshot.
				// 	// key is the uri, and value is: 0 data will not be copy to disk, and 1 data will be copy to disk. by default, when an element is inserted, value is 1 and it will be set to 0 when the corresponding snapshot thread copy the data to disk.
				// 	// fprintf(stderr, "inserting key = %s\n", key.c_str());

				std::size_t found = key.find("$0");
				if (found != std::string::npos) // block 0.
				{
					insert_successful = map->put_snapshot(key, -1);
					// include the new record in the tracking structure.
					if (insert_successful != 0)
					{
						perror("hercules_err_worker_sec_map_put");
						slog_error("hercules_err_worker_sec_map_put");
						return -1;
					}
				}

				// update the pointer.
				arguments->pt += block_size_recv;
				// }
			}
			// if the block was already stored:
			else
			{
				slog_debug("[write_op] key find %s", key.c_str());
				// receive the block into the buffer.
				std::size_t found = key.find("$0");
				if (found != std::string::npos)
				{ // block 0.
					slog_debug("[write_op] updating block $0 (%d)", block_size_rtvd);
					struct stat *old, *latest;
					size_t msg_length = 0;
					// if data is stored in shared memory due local policy, the server does not need to receive the data.
					if (!is_shared_memory)
					{ // non shared memory method.
						ucp_tag_recv_info_t info_tag;
						ucp_tag_message_h msg_tag = null;
						// pthread_mutex_lock(&lock_network);
						//  msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
						msg_length = timing(get_recv_data_length_2(arguments->ucp_worker, arguments->worker_uid, &info_tag, &msg_tag), "[write] get_recv_data_length_2", size_t, arguments->thread_id);
						// pthread_mutex_unlock(&lock_network);
						if (msg_length == 0)
						{
							perror("hercules_err_data_worker_write_block_0_invalid_msg_length");
							slog_error("hercules_err_data_worker_write_block_0_invalid_msg_length");
							return -1;
						}
						slog_live("msg_length=%lu", msg_length);
						// void *buffer = malloc(block_size_recv);
						void *buffer = (void *)malloc(msg_length * sizeof(char));
						msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, buffer, msg_length, arguments->worker_uid, 0);
						if (msg_length == 0)
						{
							perror("hercules_err_data_worker_write_block_0_recv_data");
							slog_error("hercules_err_data_worker_write_block_0_recv_data");
							free(buffer);
							return -1;
						}

						old = (struct stat *)address_;
						latest = (struct stat *)buffer;
						slog_debug(" file size new %ld old %ld", latest->st_size, old->st_size);
						latest->st_size = std::max(latest->st_size, old->st_size);
						// slog_debug(" buffer->st_size: %ld, block_offset=%ld", latest->st_size, block_offset);
						slog_debug(" buffer->st_size: %ld, block_offset=%ld, old->st_nlink: %ld, new->st_nlink: %ld", latest->st_size, block_offset, old->st_nlink, latest->st_nlink);
						// overwrite block 0 data.
						memcpy((char *)address_ + block_offset, buffer, msg_length);

						// todo: should we update this block's size in the map?
						// map->update(key, address_, msg_length);
						// updates the second map to update the data in disk.
						// map->update_simple(key, 1);

						free(buffer);
					}
					else
					{ // data is in shared memory.
						//  tell the client to update the shared memory.
						char answer[response_size];
						// "address_" is the shared memory offset.
						sprintf(answer, "toupdate %s", (char *)address_);
						ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, answer, string, arguments->worker_uid);
						if (ret < 0)
						{
							slog_error("err_hercules_worker_send_dynamic_block_0_write_op");
							perror("err_hercules_worker_send_dynamic_block_0_write_op");
							return -1;
						}
					}
				}
				else
				{ // non block 0.
					slog_debug("[write_op] updated non 0 existing block, key.c_str(): %s", key.c_str());
					size_t msg_length = 0;
					if (!is_shared_memory)
					{ // non shared memory.
						msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
						if (msg_length == 0)
						{
							slog_error("hercules_err_data_worker_write_non_block_0_invalid_msg_length");
							perror("hercules_err_data_worker_write_non_block_0_invalid_msg_length");
							return -1;
						}
						// verify if the new size (msg_length + block_offset) is greater than the old size (block_size_rtvd).
						slog_debug("msg_length=%lu, block_offset=%d, msg_length=%d", msg_length, block_offset, msg_length);
						msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)address_ + block_offset, msg_length, arguments->worker_uid, 1);
						// msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, (char *)buffer + block_offset, msg_length, arguments->worker_uid, 1);
						if (msg_length == 0)
						{
							slog_error("hercules_err_data_worker_write_non_block_0_recv_data");
							perror("hercules_err_data_worker_write_non_block_0_recv_data");
							return -1;
						}

						// updates the second map to update the data in disk.
						map->update_simple(key, 1);
					}
					else
					{ // data is in shared memory.
						//  tell the client to update the shared memory.
						char answer[response_size];
						// "address_" is the shared memory offset.
						sprintf(answer, "toupdate %s", (char *)address_);
						ret = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, answer, string, arguments->worker_uid);
						if (ret < 0)
						{
							slog_error("err_hercules_worker_send_dynamic_non_block_0_write_op");
							perror("err_hercules_worker_send_dynamic_non_block_0_write_op");
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

	slog_debug(" terminated data helper");
	return 0;
}

// thread method searching and cleaning nodes with st_nlink=0
void *garbage_collector(void *th_argv)
{
	// fprintf(stderr, "init garbage collector\n");
	slog_debug("init garbage collector");
	// obtain the current map class element from the set of arguments.
	map_records *map = (map_records *)th_argv;

	for (;;)
	{
		// gnodetraverse_garbage_collector(map);//future
		sleep(garbage_collector_period);
		pthread_mutex_lock(&mutex_garbage);
		map->cleaning();
		pthread_mutex_unlock(&mutex_garbage);
	}
	pthread_exit(null);
}

// thread method to copy datasets from hercules to disk.
void *checkpoint(void *th_argv)
{
	slog_debug("init snapshot");

	clock_t t;
	double time_taken;
	t = clock();

	p_argv *arguments = (p_argv *)th_argv;
	block_size = arguments->blocksize * 1024;
	sleep(1);
	// obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;

	const char *checkpoint_dir = arguments->args.hercules_snapshot_path;
	const int server_id = arguments->args.id;
	const char *policy = arguments->args.policy;

	if (strlen(checkpoint_dir) == 0)
	{
		printf("snapshot path has not been provided.\thercules_snapshot_path = /home/user/snapshot_path/\n");
		fflush(stdout);
		global_finish_snapshot = 1;
		pthread_exit(null);
	}

	// if (!arguments->args.id)
	// { // only one server creates the snapshot directory.
	// 	make_directory(checkpoint_dir);
	// }
	fprintf(stderr, "running checkpoint in %s\n", checkpoint_dir);
	int ret = 1;
	for (;;)
	{
		// sleep(ckeckpoint_period);
		pthread_mutex_lock(&mutex_checkpoint);

		slog_debug("running checkpoint in %s", checkpoint_dir);

		timing_no_return(
			ret = map->checkpoint(block_size, checkpoint_dir, global_finish_snapshot, arguments->args.id, arguments->args.data_hostname, arguments->args), "checkpoint");

		if (ret != 1)
		{
			fprintf(stderr, "waiting for signal to unlock checkpoint in server %d\n", server_id);
			pthread_cond_wait(&global_run_checkpoint_cond, &mutex_checkpoint);
			pthread_mutex_unlock(&mutex_checkpoint);
			if (map->get_buffer_size() == 0) { // if there is no data to copy to disk, we will finish the snapshot.
				break;
			}
			continue;
		}

		pthread_mutex_unlock(&mutex_checkpoint);
		// to stop this thread we will wait for "hercules stop".
		if (global_finish_snapshot == 1)
		{
			// todo: call a barrier to stop all servers.
			slog_debug("waiting to finish snapshot thread.");
			global_finish_threads = 1;
			pthread_cond_signal(&global_finish_cond);
			fprintf(stderr, "ending checkponting thread.\n");
			break;
		}
	}

	t = clock() - t;
	time_taken = ((double)t) / (clocks_per_sec);

	pthread_exit(null);
}

// thread method to copy datasets from hercules to disk.
void *snapshot(void *th_argv)
{
	slog_debug("init snapshot");

	clock_t t;
	double time_taken;
	t = clock();

	p_argv *arguments = (p_argv *)th_argv;
	block_size = arguments->blocksize * 1024;
	sleep(1);
	// obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;

	const char *snapshot_dir = arguments->args.hercules_snapshot_path;
	const int server_id = arguments->args.id;
	const char *policy = arguments->args.policy;

	if (strlen(snapshot_dir) == 0)
	{
		printf("snapshot path has not been provided.\thercules_snapshot_path = /home/user/snapshot_path/\n");
		fflush(stdout);
		global_finish_snapshot = 1;
		pthread_exit(null);
	}

	// if (!arguments->args.id)
	// { // only one server creates the snapshot directory.
	// 	make_directory(snapshot_dir);
	// }
	fprintf(stderr, "running snapshot in %s\n", snapshot_dir);
	int ret = 1;
	for (;;)
	{
		// sleep(ckeckpoint_period);
		pthread_mutex_lock(&mutex_snapshot);

		slog_debug("running snapshot in %s", snapshot_dir);

		timing_no_return(
			ret = map->snapshot(block_size, snapshot_dir, global_finish_snapshot, arguments->args.id, arguments->args.data_hostname, arguments->args), "snapshot");

		if (ret != 1)
		{
			fprintf(stderr, "waiting for signal to unlock snapshot in server %d\n", server_id);
			pthread_cond_wait(&global_run_snapshot_cond, &mutex_snapshot);
			pthread_mutex_unlock(&mutex_snapshot);
			if (map->get_buffer_size() == 0) { // if there is no data to copy to disk, we will finish the snapshot.
				break;
			}
			continue;
		}

		pthread_mutex_unlock(&mutex_snapshot);
		// to stop this thread we will wait for "hercules stop".
		if (global_finish_snapshot == 1)
		{
			// todo: call a barrier to stop all servers.
			slog_debug("waiting to finish snapshot thread.");
			global_finish_threads = 1;
			pthread_cond_signal(&global_finish_cond);
			fprintf(stderr, "ending checkponting thread.\n");
			break;
		}
	}

	t = clock() - t;
	time_taken = ((double)t) / (clocks_per_sec);

	pthread_exit(null);
}

int stat_worker_helper(p_argv *arguments, char *req, void *map_server_eps)
{
	ucs_status_t status;
	int ret = 0;
	const char *response_msg = null;

	// obtain the current map class element from the set of arguments.
	std::shared_ptr<map_records> map = arguments->map;

	uint16_t current_offset = 0;

	// resources specifying if the zmq_sndmore flag was set in the sender.
	int64_t more = -1;
	size_t more_size = sizeof(more);

	// code to be sent if the requested to-be-read key does not exist.
	char err_code[] = "$errimss_no_key_avail$";

	uint32_t operation = 0; // server id.
	char mode[mode_size] = {'\0'};
	int32_t req_size = 0;
	char raw_msg[req_size + 1] = {'\0'};
	char number[16] = {'\0'};
	char uri_[uri_] = {'\0'};
	int extra_info = 0;
	int num_characters_read = 0;
	int num_input_read = 0;

	// save the request to be served.
	slog_info("request - '%s'", req);
	num_input_read = sscanf(req, "%" priu32 " %s %s %s %n", &operation, mode, number, uri_, &num_characters_read);

	if (!strcmp(mode, "get"))
		more = get_op;
	else
		more = set_op;

	uint64_t block_size_recv = (uint64_t)atoi(number);

	slog_info("mode=%s, operation=%d, number=%s, uri=%s, block_size_recv=%ld, num_characters_read=%d", mode, operation, number, uri_, block_size_recv, num_characters_read);

	// create an std::string in order to be managed by the map structure.
	std::string key;
	key.assign((const char *)uri_);

	// information associated to the arriving key.
	void *address_ = null;
	uint64_t block_size_rtvd = 0;
	dataset_info *dataset;

	// differentiate between read and write operations.
	switch (more)
	{
	// read operations.
	case get_op:
	{
		switch (block_size_recv)
		{
		case getdir:
		{
			char *buffer = null;
			int32_t numelems_indir = -1;
			// retrieve all elements inside the requested directory.
			pthread_mutex_lock(&hiredis_mut);
			slog_info("[workers][stat_worker_helper] calling redis_getdir, key=%s", key.c_str());
			buffer = redis_getdir(hiredis_context, ( char const *)key.c_str(), (int32_t *)&numelems_indir);
			slog_info("[workers][stat_worker_helper] ending redis_getdir, key=%s, numelems_indir=%d", key.c_str(), numelems_indir);
			pthread_mutex_unlock(&hiredis_mut);
			if (numelems_indir == -1)
			{
				// add an extra slash to check if it a directory.
				if (!key.empty() && key.back() != '/')
				{
					key += '/';

                    pthread_mutex_lock(&hiredis_mut);
                    slog_info("[workers][stat_worker_helper] Calling redis_getdir, key=%s", key.c_str());
                    buffer = redis_getdir(hiredis_context, ( char const *)key.c_str(), (int32_t *)&numelems_indir);
                    slog_info("[workers][stat_worker_helper] Ending redis_getdir, key=%s, numelems_indir=%d", key.c_str(), numelems_indir);
                    pthread_mutex_unlock(&hiredis_mut);
				}
			}
			slog_info("[workers] Ending GTree_getdir, key=%s, numelems_indir=%d", key.c_str(), numelems_indir);

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

			// char *curr = buffer;
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
		{ // case 0.
			slog_debug("[READ_OP]");
			// Check if there was an associated block to the key.
			int err = map->get(key, &address_, &block_size_rtvd);
			if (err == 0)
			{
				if (!key.empty() && key.back() != '/')
				{
					key += '/';
					err = map->get(key, &address_, &block_size_rtvd);
				}
			}

			slog_debug("map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// Send the error code block.
				// pthread_mutex_lock(&lock_network);
				if (send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, err_code, STRING, arguments->worker_uid) < 0)
				{
					perror("HERCULES_ERR_STAT_WORKER_READ_OP_SEND_ERR");
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				// pthread_mutex_unlock(&lock_network);
			}
			else
			{
				// dataset_info *dataset = (dataset_info*) address_;
				// printf("[STAT_SERVER] dataset.original=%s",dataset->original_name);
				// imss_info * data = (imss_info *) address_;
				// printf("READ_OP SEND data->type=%c",data->type);
				// Send the requested block.
				dataset = (dataset_info *)address_;
				// TODO: check why dataset->n_open is 21893 for imss://
				slog_debug("Before dataset->n_open=%d", dataset->n_open);
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
				pthread_mutex_lock(&memory_protect);
				msg_t m;
				m.data = address_;
				m.size = block_size_rtvd;
				// pthread_mutex_lock(&lock_network);
				err = send_dynamic_stream(arguments->ucp_worker, arguments->server_ep, (char *)&m, MSG, arguments->worker_uid);
				pthread_mutex_unlock(&memory_protect);

				if (err < 0)
				{
					perror("HERCULES_ERR_STAT_WORKER_READ_OP_SEND_STREAM");
					slog_error("HERCULES_ERR_STAT_WORKER_READ_OP_SEND_STREAM");
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				// pthread_mutex_unlock(&lock_network);
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
		}
		case DELETE_OP:
		{
			slog_debug("DELETE_OP");
			int err = map->get(key, &address_, &block_size_rtvd);
			slog_debug("map->get (key %s, block_size_rtvd %ld) get res %d", key.c_str(), block_size_rtvd, err);
			if (err == 0)
			{
				// const char *last = key.c_str() + strlen(key.c_str()) - 1;
				// if (last[0] != '/')
				if (!key.empty() && key.back() != '/')
				{
					key += '/';
					err = map->get(key, &address_, &block_size_rtvd);
				}
			}

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
					strncpy(dataset->status, "dest", strlen("dest") + 1);
					slog_debug("Dataset mark as dest");
					break;
				default:
					break;
				}

				slog_debug("dataset->n_open=%d, dataset->status=%s", dataset->n_open, dataset->status);
				int32_t ret_map = 0, ret_tree = 0;
				if (dataset->n_open == 0) // if no more process has the file opened.
				{
					// TODO: before delete, it's better to check if the file is on the structures.
					ret_map = map->delete_metadata_stat_worker(key);
                    if (hiredis_context == NULL)
					{
						perror("HERCULES_ERR_STATWORKER_NOCONN");
						return -1;
					}
					pthread_mutex_lock(&hiredis_mut);
                    ret_tree = redis_delete_data(hiredis_context, (char *)key.c_str());
					pthread_mutex_unlock(&hiredis_mut);
					slog_debug("delete_metadata_stat_worker=%d, redis_delete=%d", ret_map, ret_tree);
				}
				pthread_mutex_unlock(&memory_protect);

				if (ret_map && ret_tree)
				{
					response_msg = MSG_DELETE_OP;
				}
				else
				{
					response_msg = MSG_NODELETE_OP;
				}

				slog_debug("response_msg=%s", response_msg);
				// pthread_mutex_lock(&lock_network);
				ret = SendConfirmationMessage(arguments, response_msg);
				if (ret == 0)
				{
					perror("ERR_HERCULES_PUBLISH_DELETEMSG");
					slog_error("ERR_HERCULES_PUBLISH_DELETEMSG");
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				// pthread_mutex_unlock(&lock_network);
			}
			break;
		}
		case RENAME_OP:
		{
			int ret = -1;
			std::size_t found = key.find(',');
			if (found != std::string::npos)
			{
				std::string old_key = key.substr(0, found);
				// std::string new_key = key.substr(found + 1, key.length());
				std::string new_key = key.substr(found + 1);

				slog_debug("[RENAME] old_key=%s, new_key=%s\n", old_key.c_str(), new_key.c_str());

				// RENAME MAP
				ret = map->rename_metadata_stat_worker(old_key, new_key);
				slog_live("rename metadata stat worker=%d", ret);

				// RENAME TREE
				if (ret != -1)
				{
                    if (hiredis_context == NULL)
                    {
                        perror("HERCULES_ERR_STATWORKER_NOCONN");
                        return -1;
                    }

					pthread_mutex_lock(&hiredis_mut);
                    ret = redis_rename(hiredis_context, (char *)old_key.c_str(), (char *)new_key.c_str());
					pthread_mutex_unlock(&hiredis_mut);
					slog_debug("[RENAME] redis_rename=%d", ret);
				}
			}

			if (ret == -1)
			{
				response_msg = MSG_ERROR_OP;
			}
			else
			{
				response_msg = MSG_RENAME_OP;
			}

			// pthread_mutex_lock(&lock_network);
			ret = SendConfirmationMessage(arguments, response_msg);
			if (ret == 0)
			{
				perror("HERCULES_ERR_PUBLISH_RENAMEMSG");
				slog_error("HERCULES_ERR_PUBLISH_RENAMEMSG");
				// pthread_mutex_unlock(&lock_network);
				return -1;
			}
			// pthread_mutex_unlock(&lock_network);
			break;
		}
		case RENAME_DIR_DIR_OP:
		{
			int ret = -1;
			std::size_t found = key.find(',');
			if (found != std::string::npos)
			{
				std::string old_dir = key.substr(0, found);
				std::string rdir_dest = key.substr(found + 1);

				slog_debug("[RENAME_DIR_DIR_OP] old_key=%s, new_key=%s\n", old_dir.c_str(), rdir_dest.c_str());

				// RENAME MAP
				ret = map->rename_metadata_dir_stat_worker(old_dir, rdir_dest);

				// RENAME TREE
				if (ret == 0)
				{
                    if (hiredis_context == NULL)
                    {
                        perror("HERCULES_ERR_STATWORKER_NOCONN");
                        return -1;
                    }
					pthread_mutex_lock(&hiredis_mut);
                    ret = redis_rename_dir_dir(hiredis_context, (char *)old_dir.c_str(), (char *)rdir_dest.c_str());
					pthread_mutex_unlock(&hiredis_mut);
				}

				slog_debug("old_dir=%s, rdir_dest=%s, ret=%d", old_dir.c_str(), rdir_dest.c_str(), ret);
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
			}
			else
			{
				response_msg = MSG_RENAME_OP;
			}
			// pthread_mutex_lock(&lock_network);
			ret = SendConfirmationMessage(arguments, response_msg);
			if (ret == 0)
			{
				perror("HERCULES_ERR_PUBLISH_RENAMEMSG");
				slog_error("HERCULES_ERR_PUBLISH_RENAMEMSG");
				// pthread_mutex_unlock(&lock_network);
				return -1;
			}
			// pthread_mutex_unlock(&lock_network);
			break;
		}
		case CLOSE_OP:
		{
			slog_debug("CLOSE_OP");
			int err = map->get(key, &address_, &block_size_rtvd);
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
				// switch (operation)
				// {
				// case 1: // file opened.
				slog_debug("Closing file, dataset->n_open=%d", dataset->n_open);
				if (dataset->n_open > 0)
				{
					dataset->n_open -= 1;
				}

				slog_debug("After dataset->n_open=%d, status=%s", dataset->n_open, dataset->status);
				// if file status is marked as "dest", it is delete after close.
				if (!strncmp(dataset->status, "dest", strlen("dest")) && dataset->n_open == 0)
				{
					slog_debug("Deleting %s", key.c_str());
					int32_t result = map->delete_metadata_stat_worker(key);
                    if (hiredis_context == NULL)
					{
						perror("HERCULES_ERR_STATWORKER_NOCONN");
						return -1;
				    }	
					slog_debug("[READ_OP][DELETE_OP] delete_metadata_stat_worker=%d", result);
					pthread_mutex_lock(&hiredis_mut);
					redis_delete_data(hiredis_context, (char *)key.c_str());
					pthread_mutex_unlock(&hiredis_mut);
					response_msg = MSG_DELETE_OP;
				}
				else
				{
					response_msg = MSG_CLOSE_OP;
				}
				pthread_mutex_unlock(&memory_protect);

				// int32_t result = map->delete_metadata_stat_worker(key);
				// pthread_mutex_lock(&lock_network);
				ret = SendConfirmationMessage(arguments, response_msg);
				if (ret == 0)
				{
					perror("HERCULES_ERR_PUBLISH_DELETEMSG");
					slog_error("HERCULES_ERR_PUBLISH_DELETEMSG");
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				// pthread_mutex_unlock(&lock_network);
			}
			break;
		}
		case OPEN_OP:
		{
			slog_debug("OPEN_OP");
			int err = map->get(key, &address_, &block_size_rtvd);
			if (err == 0)
			{
				if (!key.empty() && key.back() != '/')
				{
					//  Adds an extra slash to check if it is a directory.
					key += '/';
					err = map->get(key, &address_, &block_size_rtvd);
				}
			}

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
			break;
		}
		default:
			break;
		}
		break;
	}
	// Write operations.
	case SET_OP:
	{
		// switch (operation)
		// {
		// case INSTANCE_OP:
		// {
		// 	slog_debug("NEW INSTANCE OPERATION");
		// 	slog_debug("[SET_OP] Creating dataset %s.", key.c_str());
		// 	/* code */
		// 	break;
		// }
		// default:
		// 	break;
		// }

		slog_debug("[SET_OP] Creating dataset %s.", key.c_str());
		// TO CHECK: this mutex can be removed cause' map->get has another mutex.
		// pthread_mutex_lock(&memory_protect);
		// If the record was not already stored, add the block.
		if (!map->get(key, &address_, &block_size_rtvd))
		{
			// pthread_mutex_unlock(&memory_protect);
			slog_debug("Recv dynamic buffer size %ld", block_size_recv);
			// Get the length of the message to be received.
			size_t length = 0;
			int32_t ret = -1;
			// pthread_mutex_lock(&lock_network);
			length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
			if (length == 0)
			{
				perror("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
				slog_error("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
				// pthread_mutex_unlock(&memory_protect);
				// pthread_mutex_unlock(&lock_network);
				return -1;
			}
			// Receive the block into the buffer.
			void *buffer = (void *)malloc(length * sizeof(char));
			if (buffer == NULL)
			{
				perror("HERCULES_ERR_STAT_SET_OP_MEMORY_ALLOC");
				slog_error("HERCULES_ERR_STAT_SET_OP_MEMORY_ALLOC");
				// pthread_mutex_unlock(&memory_protect);
				// pthread_mutex_unlock(&lock_network);
				return -1;
			}
			ret = recv_dynamic_stream(arguments->ucp_worker, arguments->server_ep, buffer, BUFFER, arguments->worker_uid, length);
			// pthread_mutex_unlock(&lock_network);

			dataset_info *struct_ = (dataset_info *)buffer;

			slog_debug("END Recv dynamic, n_server_when_created=%d", struct_->n_servers_when_created);

			if (ret < 0)
			{
				perror("HERCULES_ERR_STAT_WORKER_HELPER_RECV_DYNAMIC_STREAM");
				slog_error("HERCULES_ERR_STAT_WORKER_HELPER_RECV_DYNAMIC_STREAM");
				free(buffer);
				// pthread_mutex_unlock(&memory_protect);
				return -1;
			}

			int32_t insert_successful = -1;
			insert_successful = map->put(key, buffer, length);
			slog_debug("map->put (key %s) err %d", key.c_str(), insert_successful);

			if (insert_successful != 0)
			{
				slog_error("HERCULES_ERR_METADATA_WORKER_MAPPUT_SET_OP");
				perror("HERCULES_ERR_METADATA_WORKER_MAPPUT_SET_OP");
				free(buffer);
				// pthread_mutex_unlock(&memory_protect);
				return -1;
			}

			// Insert the received uri into the directory tree.
             if (hiredis_context == NULL)
			{
				perror("HERCULES_ERR_STATWORKER_NOCONN");
				return -1;
			}
			pthread_mutex_lock(&hiredis_mut);
			insert_successful = redis_insert_data(hiredis_context, (char *)key.c_str());
			pthread_mutex_unlock(&hiredis_mut);			
            slog_debug("insert_successful=%d", insert_successful);
			if (insert_successful == -1)
			{
				slog_error("HERCULES_ERR_METADATA_WORKER_GTREEINSERT_SET_OP");
				perror("HERCULES_ERR_METADATA_WORKER_GTREEINSERT_SET_OP");
				free(buffer);
				// pthread_mutex_unlock(&memory_protect);
				return -1;
			}

			// Update the pointer.
			arguments->pt += block_size_recv;
			slog_debug("Dataset %s has been created.", key.c_str());
		}
		// If was already stored:
		else
		{
			// Follow a certain behavior if the received block was already stored.
			slog_debug("LOCAL DATASET_UPDATE %ld", block_size_recv);
			switch (1) // TO CKECK!
			{
			// Update where the blocks of a LOCAL dataset have been stored.
			case LOCAL_DATASET_UPDATE:
			{
				// pthread_mutex_lock(&lock_network);
				size_t msg_length = 0;
				msg_length = get_recv_data_length(arguments->ucp_worker, arguments->worker_uid);
				if (msg_length == 0)
				{
					perror("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
					slog_error("HERCULES_ERR_METADATA_WORKER_GET_RECV_DATA_LENGTH_SET_OP");
					// pthread_mutex_unlock(&memory_protect);
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				// // void data_ref[msg_length];
				void *data_ref = malloc(msg_length);
				if (data_ref == NULL)
				{
					perror("HERCULES_ERR_STAT_LOCAL_DATASET_UPDATE");
					slog_error("HERCULES_ERR_STAT_LOCAL_DATASET_UPDATE");
					// pthread_mutex_unlock(&memory_protect);
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				// void *data_ref;
				// char data_ref[REQUEST_SIZE];
				msg_length = recv_data(arguments->ucp_worker, arguments->server_ep, data_ref, msg_length, arguments->worker_uid, 0);
				if (msg_length == 0)
				{
					perror("HERCULES_ERR_METADATA_WORKER_RECV_DATA_SET_OP");
					slog_error("HERCULES_ERR_METADATA_WORKER_RECV_DATA_SET_OP");
					free(data_ref);
					// pthread_mutex_unlock(&memory_protect);
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}

				uint32_t data_size = RESPONSE_SIZE; // MIRAR

				// Value to be written in certain positions of the vector.
				uint16_t *update_value = (uint16_t *)(data_size + (char *)data_ref - 8);
				// Positions to be updated.
				uint32_t *update_positions = (uint32_t *)data_ref;

				pthread_mutex_lock(&memory_protect);
				// Set of positions that are going to be updated (those are just under the concerned dataset but not pointed by it).
				uint16_t *data_locations = (uint16_t *)((char *)address_ + sizeof(dataset_info));

				// Number of positions to be updated.
				int32_t num_pos_toupdate = (data_size / sizeof(uint32_t)) - 2;

				// Perform the update operation.
				for (int32_t i = 0; i < num_pos_toupdate; i++)
					data_locations[update_positions[i]] = *update_value;

				// Answer the client with the update.
				slog_debug("[STAT_WORKER] Updating existing dataset %s.", key.c_str());
				// char answer[] = "UPDATED!\0";
				response_msg = MSG_UPDATED_OP;
				ret = SendConfirmationMessage(arguments, response_msg);
				if (ret == 0)
				{
					slog_error("HERCULES_ERR_METADATA_WORKER_SEND_DATA_SET_OP");
					perror("HERCULES_ERR_METADATA_WORKER_SEND_DATA_SET_OP");
					free(data_ref);
					// pthread_mutex_unlock(&lock_network);
					return -1;
				}
				free(data_ref);

				// pthread_mutex_unlock(&lock_network);
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
					// Clear the corresponding memory region.
					void *buffer = (void *)malloc(block_size_recv);
					if (buffer == NULL)
					{
						perror("HERCULES_ERR_STAT_MEMORY_ALLOC");
						slog_error("HERCULES_ERR_STAT_MEMORY_ALLOC");
						// pthread_mutex_unlock(&memory_protect);
						// pthread_mutex_unlock(&lock_network);
						return -1;
					}
					// Receive the block into the buffer.
					ret = recv_dynamic_stream(arguments->ucp_worker, arguments->server_ep, buffer, BUFFER, arguments->worker_uid, length);
					if (ret == -1)
					{
						perror("HERCULES_ERR_STAT_SET_OP_RECV_STREAM");
						slog_error("HERCULES_ERR_STAT_SET_OP_RECV_STREAM");
						// pthread_mutex_unlock(&memory_protect);
						// pthread_mutex_unlock(&lock_network);
						return -1;
					}

					// pthread_mutex_unlock(&lock_network);
					free(buffer);
					// slog_debug("[STAT_WORKER] End Updating existing dataset %s.", key.c_str());
				}

				slog_debug("[STAT_WORKER] End Updating existing dataset %s.", key.c_str());
				break;
			}
			}
		}
		// pthread_mutex_unlock(&memory_protect);
		break;
	}
	default:
		break;
	}

	slog_debug(" Terminated meta helper\n");

	return 0;
}

/**
 * @brief Obsolete: Server dispatcher thread method.
 *
 * */
void *srv_attached_dispatcher(void *th_argv)
{
	// Cast from generic pointer type to p_argv struct type pointer.
	p_argv *arguments = (p_argv *)th_argv;

	ucx_server_ctx_t context;
	ucp_worker_h ucp_data_worker;
	ucp_am_handler_param_t param;
	ucp_ep_h server_ep;
	ucs_status_t status;
	u_int16_t hercules_thread_pool_size = arguments->hercules_thread_pool_size;
	int ret;

	// Variable specifying the ID that will be granted to the next client.
	uint32_t client_id_ = 0;
	// char req[256];
	char *req;

	ret = init_worker(arguments->ucp_context, &ucp_data_worker);
	if (ret != 0)
	{
		perror("HERCULES_ERR_INIT_SRV_DISPATCHER");
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
		if (req == NULL)
		{
			perror("HERCULES_ERR_DISPATCHER_MEMORY_ALLOC_MSG_LEN");
			slog_error("HERCULES_ERR_DISPATCHER_MEMORY_ALLOC_MSG_LEN");
			pthread_exit(NULL);
		}

		// char mode[MODE_SIZE];
		msg_length = recv_data(ucp_data_worker, server_ep, (char *)req, msg_length, arguments->worker_uid, 0);
		// msg_length = recv_data_opt(ucp_data_worker, server_ep, (void **)&req, msg_length, arguments->worker_uid, 0);
		if (msg_length == 0)
		{
			perror("HERCULES_ERR_SRV_DISPATCHER_DATA_RECV_DATA");
			slog_error("HERCULES_ERR_SRV_DISPATCHER_DATA_RECV_DATA");
			free(req);
			pthread_exit(NULL);
		}

		char *mode = (char *)malloc(msg_length * sizeof(char));
		if (mode == NULL)
		{
			perror("HERCULES_ERR_DISPATCHER_MEMORY_ALLOC_MODE");
			slog_error("HERCULES_ERR_DISPATCHER_MEMORY_ALLOC_MODE");
			free(req);
			pthread_exit(NULL);
		}

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
			int32_t port_ = arguments->port + 1 + (client_id_ % hercules_thread_pool_size);
			// Wrap the previous info into the UCX message.
			sprintf(response_, "%d%c%d", port_, '-', client_id_++);
			slog_info("Seding response_=%s", response_);
			// Send communication specifications.
			ret = SendConfirmationMessage(arguments, response_);
			if (ret == 0)
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
			ret = SendConfirmationMessage(arguments, arguments->my_uri);
			// Provide the uri of this instance.
			if (ret == 0)
			{
				perror("HERCULES_ERR_WHOREQUEST");
				slog_error("HERCULES_ERR_WHOREQUEST");
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
	char *tmp_file_path = arguments->tmp_file_path;
	u_int16_t hercules_thread_pool_size = arguments->hercules_thread_pool_size;
	int client = 0;

	// Get a socket file descriptor.
	global_server_fd_thread = socket(AF_INET, SOCK_STREAM, 0);
	if (global_server_fd_thread < 0)
	{
		perror("HERCULES_ERR_DISPATCHER_SOCKET");
		ready(tmp_file_path, "ERROR");
		pthread_exit(NULL);
	}

	// To reuse the address and port.
	ret = setsockopt(global_server_fd_thread, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
	if (ret < 0)
	{
		perror("HERCULES_ERR_DISPATCHER_SET_SOCKET_OPT");
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
			slog_error("HERCULES_ERR_DISPATCHER_RECV");
		}

		sscanf(req, "%" PRIu32 " %s", &client_id_, mode);

		char *req_content = strstr(req, mode);
		req_content += 4;

		slog_debug("req=%s, req_content=%s", req, req_content);

		// Check if the client is requesting connection resources.
		if (!strncmp(req_content, "HELLO!", 6))
		{
			// Case where a client (front-end) is connecting to the data server.
			// fprintf(stderr, "HERCULES_THREAD_POOL_SIZE = %d, client=%d\n", hercules_thread_pool_size, client);
			ret = send(new_socket, &local_addr_len[(client % hercules_thread_pool_size)], sizeof(local_addr_len[(client % hercules_thread_pool_size)]), 0);
			if (ret == -1)
			{
				slog_error("HERCULES_ERR_DISPATCHER_SEND1");
			}
			ret = send(new_socket, local_addr[(client % hercules_thread_pool_size)], local_addr_len[(client % hercules_thread_pool_size)], 0);
			if (ret == -1)
			{
				slog_error("HERCULES_ERR_DISPATCHER_SEND2");
			}
			client++;
			slog_debug("Replied client %d, hercules_thread_pool_size=%d.", client, hercules_thread_pool_size);
		}
		else if (!strncmp(req_content, "MAIN!", 5))
		{
			// Case where a data server is connecting to the metadata server.
			// TO FIX: 0 must be a dynamic value depending on the number of
			// metadata servers.
			ret = send(new_socket, &local_addr_len[(client % hercules_thread_pool_size)], sizeof(local_addr_len[(client % hercules_thread_pool_size)]), 0);
			ret = send(new_socket, local_addr[(client % hercules_thread_pool_size)], local_addr_len[(client % hercules_thread_pool_size)], 0);
			slog_debug("Sent address %lu (%lu) to the client %d", local_addr[(client % hercules_thread_pool_size)], local_addr_len[(client % hercules_thread_pool_size)], client);
		}
		// Check if someone is requesting identity resources.
		else if (*((int32_t *)req) == WHO)
		{
			ret = send(new_socket, &local_addr_len[client], sizeof(local_addr_len[client]), 0);
			ret = send(new_socket, local_addr[client], local_addr_len[client], 0);
			slog_debug("Replied client %s.", arguments->my_uri);
		}

		slog_debug("\n");

		// MIRAR ucp_worker_release_address(ucp_worker_threads[client_id_ % hercules_thread_pool_size], local_addr);
		close(new_socket);
	}
	close(global_server_fd_thread);

	pthread_exit(NULL);
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

	// Map that stores server side endpoints
	void *map_server_eps = NULL;

	// if (!arguments->thread_id)
	{ // thread 0.
		map_server_eps = map_server_eps_create();
		BLOCK_SIZE = arguments->blocksize * 1024;
	}

	for (;;)
	{
		// errno = 0;
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

		clock_t t;
		double time_taken;
		t = clock();

		slog_debug("Message length=%ld bytes.", info_tag.length);
		msg = (msg_req_t *)malloc(info_tag.length);

		recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
								  UCP_OP_ATTR_FIELD_DATATYPE |
								  UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
		recv_param.datatype = ucp_dt_make_contig(1);
		recv_param.cb.recv = recv_handler;

		request = (struct ucx_context *)ucp_tag_msg_recv_nbx(arguments->ucp_worker, msg, info_tag.length, msg_tag, &recv_param);

		status = ucx_wait(arguments->ucp_worker, request, "receive", "srv_worker");
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
			// fprintf(stderr, "unable to allocate memory for peer address\n");
			perror("unable to allocate memory for peer address\n");
			slog_error("unable to allocate memory for peer address\n");
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
			slog_debug("\t['%" PRIu64 "] Endpoint already exist'", attr.worker_uid);
		}

		arguments->peer_address = peer_addr;
		arguments->server_ep = ep;
		arguments->worker_uid = attr.worker_uid;

		TIMING_NO_RETURN(srv_worker_helper(arguments, req, map_server_eps), "srv_worker_helper %d", arguments->thread_id);
		t = clock() - t;

		time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
		slog_info("Serving time %f s\n", time_taken);

		free(peer_addr);
		free(msg);
	}
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

