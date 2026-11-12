#include "arg_parser.h"
#include "comms.h"
#include "imss.h"
#include "policies.h"
#include "slog.h"
#include "workers.h"
#include <cstring>
#include <ctime>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

extern struct arguments args;

// Queue of client reads held during decommissioning migration.
std::vector<PendingMigratingRead> pending_migrating_reads;
std::mutex mutex_pending_migrating_reads;
std::atomic<bool> decommissioning_active{false};
std::atomic<int32_t> decom_target_id{-1};
std::atomic<int32_t> decom_prev_view_id{-1};
std::atomic<int32_t> decom_new_view_id{-1};

// Code to be sent if the requested to-be-read key does not exist.
char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";

/**
 * @brief Sends MSG_SHUTDOWN_SERVER to a single data server endpoint with a bounded timeout
 * and waits for its immediate MSG_OK_OP confirmation.
 *
 * @param ucp_worker Metadata server UCX worker handle.
 * @param ep Target data server UCX endpoint.
 * @param addr Local worker address.
 * @param addr_len Local worker address length.
 * @param meta_worker_uid Local worker UID used for probing the confirmation reply.
 * @return int 0 if the data server received and confirmed the shutdown message, -1 if unreachable.
 */
static int send_shutdown_req(ucp_worker_h ucp_worker, ucp_ep_h ep, ucp_address_t *addr, size_t addr_len, uint64_t meta_worker_uid)
{
	if (ep == NULL || ucp_worker == NULL || addr == NULL || addr_len == 0)
	{
		slog_warn("HERCULES_ERR_SEND_SHUTDOWN_REQ: Invalid parameters for send_shutdown_req");
		fprintf(stderr, "[%s] HERCULES_ERR_SEND_SHUTDOWN_REQ: Invalid parameters for send_shutdown_req\n", args.data_hostname);
		return -1;
	}

	size_t msg_len = sizeof(msg_req_t) + addr_len;
	msg_req_t *msg = (msg_req_t *)calloc(1, msg_len);
	if (msg == NULL)
	{
		slog_error("HERCULES_ERR_SEND_SHUTDOWN_REQ: Failed to allocate memory for shutdown request");
		fprintf(stderr, "[%s] HERCULES_ERR_SEND_SHUTDOWN_REQ: Failed to allocate memory for shutdown request\n", args.data_hostname);
		return -1;
	}

	msg->addr_len = addr_len;
	memcpy(msg->request, MSG_SHUTDOWN_SERVER, strlen(MSG_SHUTDOWN_SERVER) + 1);
	memcpy(msg + 1, addr, addr_len);

	send_req_t ctx;
	ctx.complete = 0;
	ctx.buffer = (void *)msg;

	ucp_request_param_t send_param;
	memset(&send_param, 0, sizeof(send_param));
	send_param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE |
				  UCP_OP_ATTR_FIELD_CALLBACK |
				  UCP_OP_ATTR_FLAG_NO_IMM_CMPL |
				  UCP_OP_ATTR_FIELD_USER_DATA;
	send_param.datatype = ucp_dt_make_contig(1);
	send_param.cb.send = send_handler_req;
	send_param.user_data = &ctx;

	struct ucx_context *request = (struct ucx_context *)ucp_tag_send_nbx(ep, msg, msg_len, tag_req, &send_param);
	const double send_timeout_sec = 2.0;
	ucs_status_t status = ucx_wait_timeout(ucp_worker, request, "send", MSG_SHUTDOWN_SERVER, send_timeout_sec);
	free(msg);
	if (status != UCS_OK)
	{
		return -1;
	}

	// Wait up to 2 seconds for immediate MSG_OK_OP confirmation from the data server.
	if (wait_ack_timeout(ucp_worker, meta_worker_uid, ep, SYNC, send_timeout_sec) != 0)
	{
		return -1;
	}
	return 0;
}

/**
 * @brief Sends MSG_SHUTDOWN_SERVER to all connected data servers on the metadata server's UCX worker.
 * @param arguments Pointer to metadata server worker thread arguments.
 */
void send_shutdown_to_all_data_servers(p_argv *arguments)
{
	int32_t total_storages = 0;
	if (curr_global_imss_info != NULL && curr_global_imss_info->num_storages > 0)
	{
		total_storages = curr_global_imss_info->num_storages;
	}
	else if (number_storage_servers.load() > 0)
	{
		total_storages = (int32_t)number_storage_servers.load();
	}

	if (arguments == NULL)
	{
		slog_error("HERCULES_ERR_SEND_SHUTDOWN_REQ: Invalid arguments");
		fprintf(stderr, "[%s] HERCULES_ERR_SEND_SHUTDOWN_REQ: Invalid arguments\n", args.data_hostname);
		pthread_mutex_lock(&stop_server_mutex);
		expected_shutdown_servers.store(0);
		all_data_servers_stopped = true;
		pthread_cond_broadcast(&all_data_servers_stopped_cond);
		pthread_mutex_unlock(&stop_server_mutex);
		return;
	}

	if (total_storages <= 0)
	{
		slog_warn("HERCULES_ERR_SEND_SHUTDOWN_REQ: No data servers to shut down");
		fprintf(stderr, "[%s] HERCULES_ERR_SEND_SHUTDOWN_REQ: No data servers to shut down\n", args.data_hostname);
		pthread_mutex_lock(&stop_server_mutex);
		expected_shutdown_servers.store(0);
		all_data_servers_stopped = true;
		pthread_cond_broadcast(&all_data_servers_stopped_cond);
		pthread_mutex_unlock(&stop_server_mutex);
		return;
	}

	if (total_storages > MAX_SERVERS_PER_VIEW)
	{
		slog_warn("HERCULES_WARN_SEND_SHUTDOWN_REQ: Total storages exceeds MAX_SERVERS_PER_VIEW");
		fprintf(stderr, "[%s] HERCULES_WARN_SEND_SHUTDOWN_REQ: Total storages exceeds MAX_SERVERS_PER_VIEW\n", args.data_hostname);
		total_storages = MAX_SERVERS_PER_VIEW;
	}

	ucp_worker_address_attr_t attr;
	memset(&attr, 0, sizeof(attr));
	attr.field_mask = UCP_WORKER_ADDRESS_ATTR_FIELD_UID;
	ucp_worker_address_query(local_addr[arguments->thread_id], &attr);
	uint64_t meta_worker_uid = attr.worker_uid;

	int32_t reachable_count = 0;
	for (int32_t i = 0; i < total_storages; i++)
	{
		const char *srv_host = (curr_global_imss_info != NULL && curr_global_imss_info->ips != NULL && curr_global_imss_info->ips[i] != NULL) ? curr_global_imss_info->ips[i] : "unknown";
		if (data_endpoints[i] == NULL || (curr_global_imss_info != NULL && curr_global_imss_info->server_states[i] == SERVER_DECOMMISSIONED))
		{
			slog_info("[Metadata Server] Skipping data server %d (%s) during shutdown (decommissioned or NULL endpoint).", i, srv_host);
			fprintf(stderr, "[Metadata Server] Skipping data server %d (%s) during shutdown (decommissioned or NULL endpoint).\n", i, srv_host);
			continue;
		}

		slog_info("[Metadata Server] Sending %s to data server %d (%s)...", MSG_SHUTDOWN_SERVER, i, srv_host);
		fprintf(stderr, "[Metadata Server] Sending %s to data server %d (%s)...\n", MSG_SHUTDOWN_SERVER, i, srv_host);

		if (send_shutdown_req(arguments->ucp_worker, data_endpoints[i], local_addr[arguments->thread_id], local_addr_len[arguments->thread_id], meta_worker_uid) == 0)
		{
			reachable_count++;
			slog_info("[Metadata Server] Data server %d (%s) confirmed %s.", i, srv_host, MSG_SHUTDOWN_SERVER);
			fprintf(stderr, "[Metadata Server] Data server %d (%s) confirmed %s.\n", i, srv_host, MSG_SHUTDOWN_SERVER);
		}
		else
		{
			slog_warn("[Metadata Server] Unable to send %s to data server %d (%s), discarding error and assuming server is no longer running.", MSG_SHUTDOWN_SERVER, i, srv_host);
			fprintf(stderr, "[Metadata Server] Unable to send %s to data server %d (%s), discarding error and assuming server is no longer running.\n", MSG_SHUTDOWN_SERVER, i, srv_host);
		}
	}

	pthread_mutex_lock(&stop_server_mutex);
	expected_shutdown_servers.store(reachable_count);
	slog_info("[Metadata Server] %d/%d data servers confirmed %s.", reachable_count, total_storages, MSG_SHUTDOWN_SERVER);
	fprintf(stderr, "[Metadata Server] %d/%d data servers confirmed %s.\n", reachable_count, total_storages, MSG_SHUTDOWN_SERVER);

	if (reachable_count > 0 && !stop_server_pending_requests.empty() && (int)stop_server_pending_requests.size() >= reachable_count)
	{
		for (const auto &entry : stop_server_pending_requests)
		{
			p_argv pending_argument;
			pending_argument.ucp_worker = entry.second.ucp_worker;
			pending_argument.server_ep = entry.second.server_ep;
			pending_argument.worker_uid = entry.second.worker_uid;
			SendConfirmationMessage(&pending_argument, MSG_OK_OP);
		}
		stop_server_pending_requests.clear();
		stop_server_signal_timestamps.clear();
	}

	if (reachable_count <= 0 || (int)released_data_servers.size() >= reachable_count)
	{
		all_data_servers_stopped = true;
		pthread_cond_broadcast(&all_data_servers_stopped_cond);
	}
	pthread_mutex_unlock(&stop_server_mutex);
}

/**
 * @brief Coordinates graceful shutdown of all connected data servers from the metadata server.
 *
 * @return int 0 on completion.
 */
int shutdown_connected_data_servers(void)
{
	int32_t total_storages = 0;
	if (curr_global_imss_info != NULL && curr_global_imss_info->num_storages > 0)
	{
		total_storages = curr_global_imss_info->num_storages;
	}
	else if (number_storage_servers.load() > 0)
	{
		total_storages = (int32_t)number_storage_servers.load();
	}

	if (total_storages <= 0)
	{
		slog_info("[Metadata Server] No connected data servers registered, skipping data server shutdown broadcast.");
		return 0;
	}

	pthread_mutex_lock(&stop_server_mutex);
	released_data_servers.clear();
	all_data_servers_stopped = false;
	expected_shutdown_servers.store(total_storages);
	metadata_shutdown_active.store(1, std::memory_order_release);
	metadata_shutdown_pending.store(1, std::memory_order_release);

	struct timespec timeout_ts;
	clock_gettime(CLOCK_REALTIME, &timeout_ts);
	timeout_ts.tv_sec += 60;

	while (!all_data_servers_stopped)
	{
		int rc = pthread_cond_timedwait(&all_data_servers_stopped_cond, &stop_server_mutex, &timeout_ts);
		if (rc == ETIMEDOUT)
		{
			slog_warn("[Metadata Server] Timed out waiting for all data servers to complete shutdown (%zu/%d released). Proceeding with metadata server shutdown.", released_data_servers.size(), expected_shutdown_servers.load());
			fprintf(stderr, "[Metadata Server] Timed out waiting for all data servers to complete shutdown (%zu/%d released). Proceeding with metadata server shutdown.\n", released_data_servers.size(), expected_shutdown_servers.load());
			if (!stop_server_pending_requests.empty())
			{
				for (const auto &entry : stop_server_pending_requests)
				{
					p_argv pending_argument;
					pending_argument.ucp_worker = entry.second.ucp_worker;
					pending_argument.server_ep = entry.second.server_ep;
					pending_argument.worker_uid = entry.second.worker_uid;
					SendConfirmationMessage(&pending_argument, MSG_OK_OP);
				}
				stop_server_pending_requests.clear();
				stop_server_signal_timestamps.clear();
			}
			break;
		}
	}

	metadata_shutdown_active.store(0, std::memory_order_release);
	pthread_mutex_unlock(&stop_server_mutex);
	return 0;
}

/**
 * @brief Fulfills all client read requests held while waiting for blocks to migrate from a decommissioned server.
 *
 * @param arguments Thread arguments containing hierarchical map pointer and worker context.
 */
void attend_pending_migrating_reads(p_argv *arguments)
{
	if (arguments == NULL)
	{
		fprintf(stderr, "attend_pending_migrating_reads: arguments pointer is NULL.\n");
		slog_error("attend_pending_migrating_reads: arguments pointer is NULL.");
		return;
	}

	std::vector<PendingMigratingRead> reads_to_attend;
	{
		std::lock_guard<std::mutex> lock(mutex_pending_migrating_reads);
		// Atomically extracts queued reads and clears decommission hold state.
		reads_to_attend.swap(pending_migrating_reads);
		decommissioning_active.store(false, std::memory_order_release);
	}

	if (reads_to_attend.empty())
	{
		fprintf(stderr, "No pending migrating reads to attend on decommissioning completion.\n");
		slog_malleability("No pending migrating reads to attend on decommissioning completion.");
		return;
	}

	slog_malleability("Fulfilling %zu pending migrating reads on decommissioning completion.", reads_to_attend.size());

	HierarchicalRecords *hierarchical_map = (arguments->hierarchical_map != NULL) ? arguments->hierarchical_map : global_hierarchical_map;
	if (hierarchical_map == NULL)
	{
		fprintf(stderr, "attend_pending_migrating_reads: hierarchical_map pointer is NULL.\n");
		slog_error("attend_pending_migrating_reads: hierarchical_map pointer is NULL.");
		for (const auto &req : reads_to_attend)
		{
			send_dynamic_stream(req.ucp_worker, req.server_ep, err_code, STRING, req.worker_uid);
		}
		return;
	}

	for (const auto &req : reads_to_attend)
	{
		void *address_ = NULL;
		uint64_t block_size_rtvd = 0;
		int32_t ret = hierarchical_map->HierarchicalMapGet(req.key, &address_, &block_size_rtvd);

		if (ret != 0 && address_ != NULL)
		{
			// Clamps read size to avoid out-of-bounds access past the retrieved block.
			uint64_t available_bytes = (block_size_rtvd > req.block_offset) ? (block_size_rtvd - req.block_offset) : 0;
			size_t to_read = (req.to_read <= 0 || req.to_read > available_bytes) ? available_bytes : req.to_read;
			if (req.is_shared_memory)
			{ // shared memory.
				pthread_mutex_lock(&memory_protect);
				void *content = setContentSMByID(req.shm_key, to_read, (char *)address_ + req.block_offset);
				pthread_mutex_unlock(&memory_protect);
				const char *resp = (content != NULL) ? MSG_OK_OP : MSG_ERROR_OP;
				if (content != NULL)
				{
					unlinkSM(content);
				}
				p_argv pending_arg;
				memset(&pending_arg, 0, sizeof(pending_arg));
				pending_arg.ucp_worker = req.ucp_worker;
				pending_arg.server_ep = req.server_ep;
				pending_arg.worker_uid = req.worker_uid;
				SendConfirmationMessage(&pending_arg, resp);
			}
			else
			{ // non shared memory.
				pthread_mutex_lock(&memory_protect);
				send_data(req.ucp_worker, req.server_ep, (char *)address_ + req.block_offset, to_read, req.worker_uid);
				pthread_mutex_unlock(&memory_protect);
			}
		}
		else
		{
			send_dynamic_stream(req.ucp_worker, req.server_ep, err_code, STRING, req.worker_uid);
		}
	}
}

/**
 * @brief Checks if a missing block is currently in transit from a decommissioned server to this node.
 *
 * @param key Block URI
 * @param current_server_id Current server's ID.
 * @return true if the block belonged to the retiring server in prev_view and routes to this server in new_view.
 */
bool is_block_in_flight(const std::string &key, int32_t current_server_id)
{
	if (!decommissioning_active.load(std::memory_order_acquire))
	{
		return false;
	}

	size_t pos = key.find('$');
	if (pos == std::string::npos)
	{
		return false;
	}

	std::string dataset_uri = key.substr(0, pos);
	std::string block_str = key.substr(pos + 1);
	long block_number = 0;
	if (!is_valid_long(block_str.c_str(), &block_number))
	{
		return false;
	}

	int32_t prev_vid = decom_prev_view_id.load(std::memory_order_acquire);
	int32_t new_vid = decom_new_view_id.load(std::memory_order_acquire);
	int32_t target_decom_id = decom_target_id.load(std::memory_order_acquire);

	if (target_decom_id < 0)
	{
		return false;
	}

	if (curr_global_imss_info == NULL)
	{
		imss *local_imss_ = NULL;
		const char *target_uri = resolve_imss_uri(NULL);
		if (find_imss_pointer(target_uri, &local_imss_) != -1 && local_imss_ != NULL)
		{
			curr_global_imss_info = &local_imss_->info;
		}
	}

	if (curr_global_imss_info == NULL)
	{
		slog_error("[%s] HERCULES_ERR_BLOCK_IN_FLIGHT: IMSS info is not available", args.data_hostname);
		fprintf(stderr, "[%s] HERCULES_ERR_BLOCK_IN_FLIGHT: IMSS info is not available\n", args.data_hostname);
		return false;
	}

	if (prev_vid < 0 || prev_vid >= curr_global_imss_info->num_views ||
	    new_vid < 0 || new_vid >= curr_global_imss_info->num_views)
	{
		slog_error("[%s] HERCULES_ERR_BLOCK_IN_FLIGHT: Invalid view IDs", args.data_hostname);
		fprintf(stderr, "[%s] HERCULES_ERR_BLOCK_IN_FLIGHT: Invalid view IDs\n", args.data_hostname);
		return false;
	}

	TopologyView *prev_view = &curr_global_imss_info->views[prev_vid];
	TopologyView *new_view = &curr_global_imss_info->views[new_vid];

	if (prev_view->num_servers <= 0 || new_view->num_servers <= 0)
	{
		slog_error("[%s] HERCULES_ERR_BLOCK_IN_FLIGHT: Invalid server count in views", args.data_hostname);
		fprintf(stderr, "[%s] HERCULES_ERR_BLOCK_IN_FLIGHT: Invalid server count in views\n", args.data_hostname);
		return false;
	}

	int32_t orig_logical = find_server(prev_view->num_servers, block_number, dataset_uri.c_str(), GET, TYPE_DATA_SERVER, curr_global_imss_info->session_plcy);
	if (orig_logical < 0 || orig_logical >= prev_view->num_servers)
	{
		slog_error("[%s] HERCULES_ERR_BLOCK_IN_FLIGHT: Invalid original logical server", args.data_hostname);
		fprintf(stderr, "[%s] HERCULES_ERR_BLOCK_IN_FLIGHT: Invalid original logical server\n", args.data_hostname);
		return false;
	}
	int32_t orig_server = prev_view->server_ids[orig_logical];

	int32_t new_logical = find_server(new_view->num_servers, block_number, dataset_uri.c_str(), GET, TYPE_DATA_SERVER, curr_global_imss_info->session_plcy);
	if (new_logical < 0 || new_logical >= new_view->num_servers)
	{
		slog_error("[%s] HERCULES_ERR_BLOCK_IN_FLIGHT: Invalid new logical server", args.data_hostname);
		fprintf(stderr, "[%s] HERCULES_ERR_BLOCK_IN_FLIGHT: Invalid new logical server\n", args.data_hostname);
		return false;
	}
	int32_t new_server = new_view->server_ids[new_logical];

	if (new_server != current_server_id)
	{
		return false;
	}

	bool is_from_target = (orig_server == target_decom_id);
	if (!is_from_target)
	{
		// Checks if the block was written under an earlier topology view before scale-up.
		for (int32_t v = 0; v < prev_vid; v++)
		{
			TopologyView *v_prior = &curr_global_imss_info->views[v];
			if (v_prior->num_servers > 0)
			{
				int32_t log_srv = find_server(v_prior->num_servers, block_number, dataset_uri.c_str(), GET, TYPE_DATA_SERVER, curr_global_imss_info->session_plcy);
				if (log_srv >= 0 && log_srv < v_prior->num_servers && v_prior->server_ids[log_srv] == target_decom_id)
				{
					is_from_target = true;
					break;
				}
			}
		}
	}

	return is_from_target;
}

/**
 * @brief Checks if the incoming write operation is a data migration transfer from a retiring server.
 *
 * @param arguments Thread arguments containing current request buffer.
 * @param transfer_mode Communication mode of the transfer.
 * @return true if write operation is a migration transfer, false otherwise.
 */
bool is_migration_write_request(const p_argv *arguments, CommunicationMode transfer_mode)
{
	if (arguments == NULL || arguments->curr_req == NULL)
	{
		return false;
	}
	return (strncmp(arguments->curr_req, "SETNETWORK", 10) == 0 ||
		transfer_mode == CommunicationMode::MODE_BUFF_NETWORK ||
		(transfer_mode == CommunicationMode::MODE_DISK && strncmp(arguments->curr_req, "SETDISK", 7) == 0));
}

/**
 * @brief Handles stale migration writes when the target key already exists on the server.
 *
 * If the incoming request is a migration transfer for an existing key, the data is drained
 * or skipped and an OK confirmation is returned without overwriting newer data.
 *
 * @param arguments Thread arguments.
 * @param transfer_mode Communication mode.
 * @param key Block key URI.
 * @param msg_length Length of the incoming network message or disk block size to drain.
 * @return true if request was handled as a stale migration write, false otherwise.
 */
bool handle_stale_migration_write(p_argv *arguments, CommunicationMode transfer_mode, const std::string &key, size_t msg_length)
{
	if (!is_migration_write_request(arguments, transfer_mode))
	{
		return false;
	}

	if (transfer_mode == CommunicationMode::MODE_NETWORK)
	{
		slog_debug("[WRITE_OP] Key %s already exists on server. Draining stale migration write without overwrite.", key.c_str());
		// Drains incoming network payload to consume the socket stream without overwriting client data.
		void *drain_buf = malloc(msg_length);
		if (drain_buf != NULL)
		{
			recv_data(arguments->ucp_worker, arguments->server_ep, drain_buf, msg_length, arguments->worker_uid, 0);
			free(drain_buf);
		}
	}
	else
	{
		slog_debug("[WRITE_OP] Key %s already exists on server. Skipping stale disk migration overwrite.", key.c_str());
	}

	return true;
}
