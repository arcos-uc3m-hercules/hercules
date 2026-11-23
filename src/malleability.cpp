#include "arg_parser.h"
#include "comms.h"
#include "hercules.hpp"
#include "imss.h"
#include "performance_records.hpp"
#include "policies.h"
#include "slog.h"
#include "workers.h"
#include <algorithm>
#include <cctype>
#include <cinttypes>
#include <cstring>
#include <ctime>
#include <errno.h>
#include <pthread.h>
#include <set>
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

// Shared bandwidth history and current interval byte buckets across worker and monitor modules.
std::map<std::string, std::vector<ElasticityMetric>> elasticity_records_history;
std::map<std::string, PerformanceBucket> pending_perf_bucket;
std::atomic<bool> discard_transition_interval{false};

// Shared state for application profile-driven malleability.
std::vector<ApplicationProfileEvent> application_profile_events;
std::atomic<bool> application_profile_active{false};
std::atomic<bool> application_profile_record_active{false};
std::atomic<bool> application_profile_t0_initialized{false};
std::chrono::steady_clock::time_point application_profile_t0;

// Synchronization for the periodic elasticity monitor thread.
pthread_mutex_t mutex_elasticity_monitor = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_elasticity_monitor = PTHREAD_COND_INITIALIZER;

// Per-sender completion and error counters for MODE_NETWORK_ASYNC migration barriers.
AsyncMigrationTracker global_async_migration_tracker = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, {}};

// Code to be sent if the requested to-be-read key does not exist.
char err_code[] = "$ERRIMSS_NO_KEY_AVAIL$";

const char *communication_mode_to_str(CommunicationMode mode)
{
	switch (mode)
	{
	case CommunicationMode::MODE_NETWORK:
		return "NETWORK";
	case CommunicationMode::MODE_DISK:
		return "DISK";
	case CommunicationMode::MODE_SHM:
		return "SHM";
	case CommunicationMode::MODE_BUFF_NETWORK:
		return "BUFF_NETWORK";
	case CommunicationMode::MODE_NETWORK_ASYNC:
		return "NETWORK_ASYNC";
	default:
		return "UNDEFINED";
	}
}

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
	return (strncmp(arguments->curr_req, MSG_SETNETWORK_ASYNC, strlen(MSG_SETNETWORK_ASYNC)) == 0 ||
		strncmp(arguments->curr_req, "SETNETWORK", 10) == 0 ||
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
 * @param recv_tag Optional UCX receive tag overriding arguments->worker_uid for async migration.
 * @return true if request was handled as a stale migration write, false otherwise.
 */
bool handle_stale_migration_write(p_argv *arguments, CommunicationMode transfer_mode, const std::string &key, size_t msg_length, uint64_t recv_tag)
{
	if (!is_migration_write_request(arguments, transfer_mode))
	{
		return false;
	}

	uint64_t data_recv_tag = arguments->worker_uid;
	if (recv_tag != 0)
	{
		data_recv_tag = recv_tag;
	}

	if (transfer_mode == CommunicationMode::MODE_NETWORK)
	{
		slog_debug("[WRITE_OP] Key %s already exists on server. Draining stale migration write without overwrite.", key.c_str());
		// Drains incoming network payload to consume the socket stream without overwriting client data.
		void *drain_buf = malloc(msg_length);
		if (drain_buf != NULL)
		{
			recv_data(arguments->ucp_worker, arguments->server_ep, drain_buf, msg_length, data_recv_tag, 0);
			free(drain_buf);
		}
	}
	else
	{
		slog_debug("[WRITE_OP] Key %s already exists on server. Skipping stale disk migration overwrite.", key.c_str());
	}

	return true;
}

/**
 * @brief Removes the decommissioned peer and re-indexes local IDs on surviving data servers.
 *
 * @param arguments Worker thread arguments containing server ID and hostname configuration.
 */
void finalize_decommission_on_surviving_server(p_argv *arguments)
{
	if (!arguments || !arguments->args)
	{
		slog_error("HERCULES_ERR_END_DECOM_NULL_ARGS: Invalid arguments or arguments->args in finalize_decommission_on_surviving_server");
		fprintf(stderr, "HERCULES_ERR_END_DECOM_NULL_ARGS: Invalid arguments or arguments->args in finalize_decommission_on_surviving_server\n");
		return;
	}

	int32_t target_id = decom_target_id.exchange(-1, std::memory_order_acq_rel);
	if (target_id < 0)
	{
		return;
	}

	const char *imss_uri = resolve_imss_uri(arguments);
	imss *imss_ = NULL;
	int32_t target_num_servers = 0;

	std::unique_lock<std::mutex> lock(mutex_hercules_struct);
	int32_t imss_found_in = find_imss_pointer(imss_uri, &imss_);
	if (imss_found_in == -1 || imss_ == NULL || imss_->info.num_storages <= 0)
	{
		slog_warn("HERCULES_WARN_END_DECOM_INVALID_IMSS: cannot finalize decommission for target %d (uri=%s)", target_id, imss_uri ? imss_uri : "null");
		fprintf(stderr, "HERCULES_WARN_END_DECOM_INVALID_IMSS: cannot finalize decommission for target %d (uri=%s)\n", target_id, imss_uri ? imss_uri : "null");
		return;
	}

	if (target_id >= imss_->info.num_storages)
	{
		slog_debug("finalize_decommission_on_surviving_server: target_id=%d already outside num_storages=%d", target_id, imss_->info.num_storages);
		return;
	}

	// Never remove this surviving server's own hostname slot.
	if (arguments->args->data_hostname[0] != '\0' && imss_->info.ips != NULL && imss_->info.ips[target_id] != NULL && strcmp(imss_->info.ips[target_id], arguments->args->data_hostname) == 0)
	{
		slog_error("HERCULES_ERR_END_DECOM_SELF_REMOVAL: target_id=%d matches self hostname '%s', aborting cleanup.", target_id, arguments->args->data_hostname);
		fprintf(stderr, "HERCULES_ERR_END_DECOM_SELF_REMOVAL: target_id=%d matches self hostname '%s', aborting cleanup.\n", target_id, arguments->args->data_hostname);
		return;
	}

	target_num_servers = imss_->info.num_storages - 1;
	// ReleaseSpecificDataServerNetworkResources acquires mutex_hercules_struct internally.
	lock.unlock();

	int32_t release_ret = ReleaseSpecificDataServerNetworkResources(imss_, 1, target_id, target_num_servers);

	lock.lock();
	if (release_ret != 0)
	{
		slog_error("HERCULES_ERR_END_DECOM_RELEASE: failed to release resources for target_id=%d (ret=%d)", target_id, release_ret);
		return;
	}

	// Remap surviving server's local TopologyViews before reusing the active view so slots stay aligned.
	if (imss_->info.num_views > 0)
	{
		RemapTopologyViewsOnSlotRemoval(&imss_->info, target_id, imss_->info.current_view_id);
	}

	number_storage_servers.store((uint32_t)imss_->info.num_storages);
	size_t num_elements_to_shift = (imss_->info.num_storages > target_id) ? (size_t)(imss_->info.num_storages - target_id) : 0;
	Update_data_endpoint_list(target_id, num_elements_to_shift);

	// Shift local server ID down to stay aligned with metadata server and client slot indices.
	if (arguments->args->id > target_id)
	{
		arguments->args->id--;
	}
	if (arguments->args != &args && args.id > target_id)
	{
		args.id--;
	}
	arguments->args->num_data_servers = (uint32_t)imss_->info.num_storages;
	args.num_data_servers = (uint32_t)imss_->info.num_storages;

	int32_t active_ids[MAX_SERVERS_PER_VIEW];
	int32_t active_count = collect_active_rw_servers(&imss_->info, active_ids);
	if (active_count > 0)
	{
		AddTopologyView(&imss_->info, active_count, active_ids);
	}
	curr_global_imss_info = &imss_->info;

	slog_malleability("[END_DECOMMISSION] Removed server slot %d, new server_id=%d, num_storages=%d, current_view_id=%d", target_id, arguments->args->id, imss_->info.num_storages, imss_->info.current_view_id);
}

/**
 * @brief Lazily resolves curr_global_imss_info from the global hierarchical map.
 *
 * @param imss_uri URI of the Hercules IMSS metadata instance to look up.
 * @return Pointer to the resolved imss_info structure, or NULL if lookup fails.
 */
imss_info *resolve_global_imss_info(const char *imss_uri)
{
	if (curr_global_imss_info == NULL)
	{
		if (global_hierarchical_map == nullptr || imss_uri == NULL)
		{
			slog_error("HERCULES_ERR_RESOLVE_IMSS_INFO_INVALID_ARGUMENTS");
			fprintf(stderr, "HERCULES_ERR_RESOLVE_IMSS_INFO_INVALID_ARGUMENTS\n");
			return NULL;
		}
		void *address_ = NULL;
		uint64_t block_size_rtvd = 0;
		slog_debug("curr_global_imss_info is NULL, looking for the value.");
		if (global_hierarchical_map->HierarchicalMapGet(imss_uri, &address_, &block_size_rtvd))
		{
			curr_global_imss_info = (imss_info *)address_;
			slog_live("Hercules instance found: %s", imss_uri);
		}
		else
		{
			slog_error("HERCULES_ERR_RESOLVE_IMSS_INF_NOT_FOUND");
			fprintf(stderr, "HERCULES_ERR_RESOLVE_IMSS_INF_NOT_FOUND\n");
			return NULL;
		}
	}
	return curr_global_imss_info;
}

/**
 * @brief Computes overall server bandwidth from write and read rates, filtering minor noise below 5% of the dominant metric.
 *
 * @param write_bw Measured interval write throughput in bytes per second.
 * @param read_bw Measured interval read throughput in bytes per second.
 * @return Combined average throughput when both directions are significant (>= 5%), otherwise the dominant rate.
 */
static double compute_overall_bandwidth(double write_bw, double read_bw)
{
	const double min_relative_ratio = 0.05;
	if (write_bw > 0.0 && read_bw > 0.0)
	{
		double dominant = (write_bw > read_bw) ? write_bw : read_bw;
		double minor = (write_bw > read_bw) ? read_bw : write_bw;
		if (minor >= min_relative_ratio * dominant)
		{
			return (write_bw + read_bw) / 2.0;
		}
		else
		{
			return dominant;
		}
	}
	else if (write_bw > 0.0)
	{
		return write_bw;
	}
	else if (read_bw > 0.0)
	{
		return read_bw;
	}
	else
	{
		return 0.0;
	}
}

/**
 * @brief Computes the cluster-wide maximum parallel client active write and read durations across all server buckets.
 */
static void compute_cluster_active_durations(const std::map<std::string, PerformanceBucket> &buckets, double *out_write_sec, double *out_read_sec)
{
	std::map<uint64_t, double> total_client_write;
	std::map<uint64_t, double> total_client_read;
	double max_write = 0.0;
	double max_read = 0.0;

	for (const auto &pair : buckets)
	{
		for (const auto &cw : pair.second.client_write_time_sec)
		{
			double total = (total_client_write[cw.first] += cw.second);
			if (total > max_write)
			{
				max_write = total;
			}
		}
		for (const auto &cr : pair.second.client_read_time_sec)
		{
			double total = (total_client_read[cr.first] += cr.second);
			if (total > max_read)
			{
				max_read = total;
			}
		}
		if (pair.second.client_write_time_sec.empty() && pair.second.max_write_time_sec > max_write)
		{
			max_write = pair.second.max_write_time_sec;
		}
		if (pair.second.client_read_time_sec.empty() && pair.second.max_read_time_sec > max_read)
		{
			max_read = pair.second.max_read_time_sec;
		}
	}

	*out_write_sec = max_write;
	*out_read_sec = max_read;
}

/**
 * @brief Flushes pending interval byte buckets into aligned per-server bandwidth history.
 *
 * @param interval_ms Monitoring interval length in milliseconds.
 * @param analysis_window_size Maximum number of historical intervals retained per server.
 * @param imss_uri URI of the Hercules IMSS metadata instance used to enumerate active servers.
 * @return 1 if active I/O traffic was recorded in this interval, 0 if skipped due to idle traffic or unavailable metadata.
 */
int flush_performance_interval(int32_t interval_ms, int32_t analysis_window_size, const char *imss_uri)
{
	if (resolve_global_imss_info(imss_uri) == NULL || curr_global_imss_info->ips == NULL)
	{
		slog_debug("[ElasticityMonitor] IMSS metadata is not available yet, skipping interval flush.");
		return 0;
	}

	pthread_mutex_lock(&mutext_performance_metrics);
	// Drop accumulated transition metrics on the first tick after a scaling operation finishes.
	if (discard_transition_interval.exchange(false, std::memory_order_acq_rel))
	{
		pending_perf_bucket.clear();
		pthread_mutex_unlock(&mutext_performance_metrics);
		return 0;
	}

	std::set<std::string> active_rw_hosts;
	std::map<std::string, int> active_rw_ids;
	for (int32_t i = 0; i < curr_global_imss_info->num_storages; ++i)
	{
		if (curr_global_imss_info->ips[i] != NULL && curr_global_imss_info->ips[i][0] != '\0' && curr_global_imss_info->server_states[i] == SERVER_ACTIVE_RW)
		{
			std::string srv_host = curr_global_imss_info->ips[i];
			active_rw_hosts.insert(srv_host);
			active_rw_ids[srv_host] = i;
		}
	}

	// Prune late reports from decommissioned or inactive servers so they never re-enter elasticity history.
	for (auto it = pending_perf_bucket.begin(); it != pending_perf_bucket.end();)
	{
		if (active_rw_hosts.find(it->first) == active_rw_hosts.end())
		{
			it = pending_perf_bucket.erase(it);
		}
		else
		{
			++it;
		}
	}

	long long total_interval_bytes = 0;
	for (const auto &pair : pending_perf_bucket)
	{
		total_interval_bytes += pair.second.write_bytes + pair.second.read_bytes;
	}

	// Skip idle intervals so compute pauses between I/O bursts do not pull down the moving average.
	if (total_interval_bytes <= 0)
	{
		pending_perf_bucket.clear();
		pthread_mutex_unlock(&mutext_performance_metrics);
		return 0;
	}

	for (const auto &kv : active_rw_ids)
	{
		// Seed a zero-byte bucket for active RW servers with no traffic in this interval so per-server history vectors remain index-aligned.
		if (pending_perf_bucket.find(kv.first) == pending_perf_bucket.end())
		{
			PerformanceBucket zero_bucket;
			zero_bucket.server_id = kv.second;
			pending_perf_bucket[kv.first] = zero_bucket;
		}
	}

	double interval_sec = (double)interval_ms / (double)MS_PER_SEC;
	double cluster_write_sec = 0.0;
	double cluster_read_sec = 0.0;
	compute_cluster_active_durations(pending_perf_bucket, &cluster_write_sec, &cluster_read_sec);
	double write_denom = (cluster_write_sec > 0.0) ? cluster_write_sec : interval_sec;
	double read_denom = (cluster_read_sec > 0.0) ? cluster_read_sec : interval_sec;
	// Divide each server's interval bytes by the cluster-wide maximum parallel client transfer duration (falling back to interval_sec when duration is zero), then bound per-server history to analysis_window_size.
	for (const auto &pair : pending_perf_bucket)
	{
		ElasticityMetric metric;
		metric.server_hostname = pair.first;
		metric.server_id = pair.second.server_id;
		metric.write_performance = (pair.second.write_bytes > 0) ? ((double)pair.second.write_bytes / write_denom) : 0.0;
		metric.read_performance = (pair.second.read_bytes > 0) ? ((double)pair.second.read_bytes / read_denom) : 0.0;
		metric.overall_performance = compute_overall_bandwidth(metric.write_performance, metric.read_performance);

		std::vector<ElasticityMetric> &server_hist = elasticity_records_history[pair.first];
		server_hist.push_back(metric);
		// Bound per-server history to the configured analysis window size.
		if ((int32_t)server_hist.size() > analysis_window_size)
		{
			size_t excess = server_hist.size() - (size_t)analysis_window_size;
			server_hist.erase(server_hist.begin(), server_hist.begin() + excess);
		}
		slog_debug("[ElasticityMonitor] %s (id=%d): W=%.2f MB/s, R=%.2f MB/s, overall=%.2f MB/s, hist=%zu", pair.first.c_str(), metric.server_id, metric.write_performance / MB, metric.read_performance / MB, metric.overall_performance / MB, server_hist.size());
	}

	pending_perf_bucket.clear();
	pthread_mutex_unlock(&mutext_performance_metrics);
	return 1;
}

/**
 * @brief Ensures arguments->ucp_worker is bound to the primary metadata UCX worker before malleability is executed.
 *
 * @param arguments Monitor thread arguments whose ucp_worker and thread_id are initialized lazily.
 * @return true if a valid UCX worker is bound, false if pool initialization is not ready yet.
 */
static bool ensure_monitor_ucp_worker(p_argv *arguments)
{
	if (arguments == NULL)
	{
		slog_error("HERCULES_ERR_ELASTICITY_MONITOR_NULL_ARGS");
		fprintf(stderr, "HERCULES_ERR_ELASTICITY_MONITOR_NULL_ARGS\n");
		return false;
	}

	// Bind the primary metadata UCX worker once pool initialization is live so decommissioning operations can send/receive.
	if (arguments->ucp_worker == NULL)
	{
		if (arguments->hercules_thread_pool_size > 0 && ucp_worker_threads != NULL && ucp_worker_threads[0] != NULL)
		{
			// TODO: improve this.
			arguments->ucp_worker = ucp_worker_threads[0];
			arguments->thread_id = 0;
		}
		else
		{
			slog_warn("HERCULES_ERR_ELASTICITY_MONITOR_NULL_WORKER: Metadata UCX worker is not initialized.");
			// fprintf(stderr, "HERCULES_ERR_ELASTICITY_MONITOR_NULL_WORKER: Metadata UCX worker is not initialized.\n");
			return false;
		}
	}

	return true;
}

/**
 * @brief Takes a snapshot of elasticity history and evaluates/prints current cluster bandwidth.
 */
static scaling_action evaluate_performance_snapshot(p_argv *arguments, bool lock_malleability, bool update_counters)
{
	std::map<std::string, std::vector<ElasticityMetric>> history_snapshot;
	ElasticityMetric slowest_server;

	pthread_mutex_lock(&mutext_performance_metrics);
	history_snapshot = elasticity_records_history;
	pthread_mutex_unlock(&mutext_performance_metrics);

	if (lock_malleability)
	{
		pthread_mutex_lock(&mutext_malleability);
	}
	scaling_action action = make_scaling_decision(history_snapshot, arguments->args->malleability_windows_size, arguments->args->malleability_performance_threshold, slowest_server, update_counters);
	if (lock_malleability)
	{
		pthread_mutex_unlock(&mutext_malleability);
	}
	return action;
}

double get_application_profile_elapsed_sec()
{
	if (!application_profile_t0_initialized.load(std::memory_order_acquire))
	{
		return 0.0;
	}
	return std::chrono::duration<double>(std::chrono::steady_clock::now() - application_profile_t0).count();
}

bool has_due_application_profile_event()
{
	if (!application_profile_active.load(std::memory_order_acquire) || !application_profile_t0_initialized.load(std::memory_order_acquire))
	{
		return false;
	}

	double elapsed_sec = get_application_profile_elapsed_sec();
	bool has_due = false;

	pthread_mutex_lock(&mutext_malleability);
	for (const auto &ev : application_profile_events)
	{
		if (!ev.executed && elapsed_sec >= ev.trigger_sec)
		{
			has_due = true;
			break;
		}
	}
	pthread_mutex_unlock(&mutext_malleability);
	return has_due;
}

/**
 * @brief Background thread that periodically flushes interval byte buckets and triggers bandwidth elasticity evaluation.
 *
 * @param th_argv Worker thread arguments containing malleability mode, metric, interval, and IMSS configuration.
 * @return Always returns NULL upon thread exit.
 */
void *ElasticityMonitor(void *th_argv)
{
	p_argv *arguments = (p_argv *)th_argv;
	if (arguments == NULL || arguments->args == NULL)
	{
		slog_error("HERCULES_ERR_ELASTICITY_MONITOR_NULL_ARGS");
		fprintf(stderr, "HERCULES_ERR_ELASTICITY_MONITOR_NULL_ARGS\n");
		pthread_exit(NULL);
	}

	int32_t mall_mode = arguments->args->malleability;
	int32_t mall_metric = arguments->args->malleability_metric;
	ApplicationProfileMode prof_mode = arguments->args->application_profile_mode;
	if (prof_mode == APP_PROFILE_MODE_OFF && arguments->args->application_profile_path[0] != '\0')
	{
		prof_mode = APP_PROFILE_MODE_PLAY;
		arguments->args->application_profile_mode = APP_PROFILE_MODE_PLAY;
	}
	bool has_app_profile = (prof_mode != APP_PROFILE_MODE_OFF);
	bool monitor_enabled = (mall_mode == MALLEABILITY_CONF_PERF) || (mall_mode == MALLEABILITY_CONF_ENABLED && (mall_metric == MALLEABILITY_METRIC_BANDWIDTH || mall_metric == MALLEABILITY_METRIC_HYBRID || has_app_profile));
	if (!monitor_enabled)
	{
		slog_debug("ElasticityMonitor exiting: malleability=%d, metric=%d", mall_mode, mall_metric);
		pthread_exit(NULL);
	}

	if (has_app_profile && mall_mode == MALLEABILITY_CONF_ENABLED)
	{
		pthread_mutex_lock(&mutext_malleability);
		application_profile_active.store(false, std::memory_order_release);
		application_profile_record_active.store(false, std::memory_order_release);

		if (resolve_application_profile_path(arguments->args, NULL) == 0)
		{
			if (prof_mode == APP_PROFILE_MODE_PLAY)
			{ // Load the application profile.
				if (load_application_profile(arguments->args->application_profile_path, application_profile_events) == 0 && !application_profile_events.empty())
				{
					application_profile_active.store(true, std::memory_order_release);
				}
				else
				{
					slog_warn("HERCULES_WARN_APP_PROFILE_EMPTY: Application profile '%s' has no valid events. Fallback to threshold-based malleability.", arguments->args->application_profile_path);
					fprintf(stderr, "WARNING: Application profile '%s' has no valid events. Fallback to threshold-based malleability.\n", arguments->args->application_profile_path);
				}
			}
			else if (prof_mode == APP_PROFILE_MODE_RECORD)
			{ // Records the application profile.
				if (init_application_profile_recording(arguments->args) == 0)
				{
					application_profile_record_active.store(true, std::memory_order_release);
				}
			}
			else if (prof_mode == APP_PROFILE_MODE_AUTO)
			{ // Load the application profile or record a new one if does not exist.
				if (access(arguments->args->application_profile_path, R_OK) == 0 && load_application_profile(arguments->args->application_profile_path, application_profile_events) == 0 && !application_profile_events.empty())
				{
					application_profile_active.store(true, std::memory_order_release);
					slog_info("[ApplicationProfile] AUTO mode matched existing profile '%s' (%zu events), entering PLAY mode.", arguments->args->application_profile_path, application_profile_events.size());
					fprintf(stderr, "[ApplicationProfile] AUTO mode matched existing profile '%s' (%zu events), entering PLAY mode.\n", arguments->args->application_profile_path, application_profile_events.size());
				}
				else if (init_application_profile_recording(arguments->args) == 0)
				{
					application_profile_record_active.store(true, std::memory_order_release);
					slog_info("[ApplicationProfile] AUTO mode found no existing profile at '%s' -> entering RECORD mode.", arguments->args->application_profile_path);
					fprintf(stderr, "[ApplicationProfile] AUTO mode found no existing profile at '%s' -> entering RECORD mode.\n", arguments->args->application_profile_path);
				}
			}
		}
		pthread_mutex_unlock(&mutext_malleability);
	}

	int32_t interval_ms = arguments->args->malleability_interval_ms;
	// Precompute the relative timespec offset once.
	struct timespec interval_ts;
	interval_ts.tv_sec = interval_ms / MS_PER_SEC;
	interval_ts.tv_nsec = (long)(interval_ms % MS_PER_SEC) * NS_PER_MS;
	slog_info("ElasticityMonitor started (interval_ms=%" PRId32 ", mode=%d, metric=%d)", interval_ms, mall_mode, mall_metric);
	fprintf(stderr, "ElasticityMonitor started (interval_ms=%" PRId32 ", mode=%d, metric=%d)\n", interval_ms, mall_mode, mall_metric);

	for (;;)
	{
		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);
		// Add the precomputed interval to current CLOCK_REALTIME
		ts.tv_sec += interval_ts.tv_sec;
		ts.tv_nsec += interval_ts.tv_nsec;
		// Carry nanosecond overflow into tv_sec to prevent EINVAL.
		if (ts.tv_nsec >= NS_PER_SEC)
		{
			ts.tv_sec += 1;
			ts.tv_nsec -= NS_PER_SEC;
		}

		// Wait until the next interval boundary or until shutdown signals termination.
		pthread_mutex_lock(&mutex_elasticity_monitor);
		if (global_finish_threads == 1)
		{
			slog_info("ElasticityMonitor exiting: global_finish_threads == 1");
			fprintf(stderr, "ElasticityMonitor exiting: global_finish_threads == 1\n");
			pthread_mutex_unlock(&mutex_elasticity_monitor);
			break;
		}
		pthread_cond_timedwait(&cond_elasticity_monitor, &mutex_elasticity_monitor, &ts);
		bool should_stop = (global_finish_threads == 1);
		pthread_mutex_unlock(&mutex_elasticity_monitor);

		if (should_stop)
		{
			break;
		}

		// Flush accumulated interval bytes; in profile mode log active intervals and check scheduled timestamps even on idle ticks.
		int had_traffic = flush_performance_interval(interval_ms, arguments->args->malleability_windows_size, arguments->args->imss_uri);
		if (application_profile_active.load(std::memory_order_acquire))
		{
			if (had_traffic)
			{
				evaluate_performance_snapshot(arguments, false, false);
			}
			if (!has_due_application_profile_event())
			{
				continue;
			}
		}
		else if (!had_traffic)
		{
			continue;
		}

		if (!ensure_monitor_ucp_worker(arguments))
		{
			continue;
		}

		if (arguments->args->malleability == MALLEABILITY_CONF_ENABLED)
		{
			// Guard against overlapping malleability workflows already in flight while still reporting current bandwidth without updating signal counters.
			if (active_commissioning_pid.load() > 0 || comissioning_on.load(std::memory_order_acquire))
			{
				slog_debug("Malleability skipped: another malleability task is already active (commissioning_on=%d, active_pid=%d).", comissioning_on.load(std::memory_order_relaxed), active_commissioning_pid.load());
				evaluate_performance_snapshot(arguments, false, false);
				continue;
			}

			bool expected = false;
			if (!comissioning_on.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
			{
				slog_debug("Malleability skipped: another thread claimed malleability evaluation.");
				evaluate_performance_snapshot(arguments, false, false);
				continue;
			}

			MalleabilityArgs *malleability_args = fill_malleability_args(arguments);
			if (malleability_args == NULL)
			{
				slog_error("HERCULES_ERR_ELASTICITY_MONITOR_NULL_MALL_ARGS");
				fprintf(stderr, "HERCULES_ERR_ELASTICITY_MONITOR_NULL_MALL_ARGS\n");
				comissioning_on.store(false, std::memory_order_release);
				continue;
			}

			pthread_t malleability_stage_thread;
			if (pthread_create(&malleability_stage_thread, NULL, run_malleability, (void *)malleability_args) != 0)
			{
				perror("HERCULES_ERR_MALLEABILITY_COMISSIONING_THREAD_CREATE");
				slog_error("HERCULES_ERR_MALLEABILITY_COMISSIONING_THREAD_CREATE");
				fprintf(stderr, "HERCULES_ERR_MALLEABILITY_COMISSIONING_THREAD_CREATE\n");
				comissioning_on.store(false, std::memory_order_release);
				free(malleability_args);
				continue;
			}
			if (pthread_detach(malleability_stage_thread) != 0)
			{
				perror("HERCULES_ERR_MALLEABILITY_COMISSIONING_THREAD_DETACH");
				slog_error("HERCULES_ERR_MALLEABILITY_COMISSIONING_THREAD_DETACH");
				fprintf(stderr, "HERCULES_ERR_MALLEABILITY_COMISSIONING_THREAD_DETACH\n");
			}
		}
		else if (arguments->args->malleability == MALLEABILITY_CONF_PERF)
		{
			scaling_action action = evaluate_performance_snapshot(arguments, true, true);

			// Discard in-flight transition metrics accumulated during a scaling operation so the next interval starts cleanly on the new topology.
			if (action != scaling_action::HOLD)
			{
				discard_transition_interval.store(true, std::memory_order_release);
				pthread_mutex_lock(&mutext_performance_metrics);
				pending_perf_bucket.clear();
				pthread_mutex_unlock(&mutext_performance_metrics);
			}
		}
	}

	slog_info("Ending ElasticityMonitor thread.");
	pthread_exit(NULL);
}

/**
 * @brief Trims leading and trailing ASCII whitespace in place.
 *
 * @param str Mutable string buffer to trim.
 * @return char* Pointer to the first non-whitespace character in str.
 */
static char *trim_whitespace(char *str)
{
	if (str == NULL)
	{
		return NULL;
	}

	while (*str != '\0' && isspace((unsigned char)*str))
	{
		str++;
	}

	size_t len = strlen(str);
	while (len > 0 && isspace((unsigned char)str[len - 1]))
	{
		str[len - 1] = '\0';
		len--;
	}

	return str;
}

int load_application_profile(const char *path, std::vector<ApplicationProfileEvent> &events)
{
	events.clear();

	if (path == NULL || path[0] == '\0')
	{
		slog_error("HERCULES_ERR_APP_PROFILE_PATH: Empty or NULL application profile path.");
		fprintf(stderr, "HERCULES_ERR_APP_PROFILE_PATH: Empty or NULL application profile path.\n");
		return -1;
	}

	FILE *fp = fopen(path, "r");
	if (fp == NULL)
	{
		slog_error("HERCULES_ERR_APP_PROFILE_OPEN: Failed to open application profile '%s' (%s).", path, strerror(errno));
		fprintf(stderr, "HERCULES_ERR_APP_PROFILE_OPEN: Failed to open application profile '%s' (%s).\n", path, strerror(errno));
		return -1;
	}

	char line_buf[1024];
	int line_num = 0;
	bool seen_non_empty = false;

	while (fgets(line_buf, sizeof(line_buf), fp) != NULL)
	{
		line_num++;

		// Strip inline comments starting with '#'.
		char *comment = strchr(line_buf, '#');
		if (comment != NULL)
		{
			*comment = '\0';
		}

		char *trimmed = trim_whitespace(line_buf);
		if (trimmed[0] == '\0')
		{
			continue;
		}

		char *saveptr = NULL;
		char *tok_sec = strtok_r(trimmed, ",", &saveptr);
		char *tok_target = strtok_r(NULL, ",", &saveptr);
		char *tok_extra = strtok_r(NULL, ",", &saveptr);

		tok_sec = trim_whitespace(tok_sec);
		tok_target = trim_whitespace(tok_target);

		// Treat an alphabetic first token on the first non-empty line as a CSV header.
		if (!seen_non_empty)
		{
			seen_non_empty = true;
			if (tok_sec != NULL && isalpha((unsigned char)tok_sec[0]))
			{
				continue;
			}
		}

		if (tok_sec == NULL || tok_sec[0] == '\0' || tok_target == NULL || tok_target[0] == '\0' || tok_extra != NULL)
		{
			slog_warn("HERCULES_WARN_APP_PROFILE_SYNTAX: Malformed application profile entry at %s:%d, skipping.", path, line_num);
			fprintf(stderr, "WARNING: Malformed application profile entry at %s:%d, skipping.\n", path, line_num);
			continue;
		}

		double trigger_sec = 0.0;
		if (!is_valid_double(tok_sec, &trigger_sec) || !isfinite(trigger_sec) || trigger_sec < 0.0)
		{
			slog_warn("HERCULES_WARN_APP_PROFILE_TIME: Invalid timestamp '%s' at %s:%d, skipping.", tok_sec, path, line_num);
			fprintf(stderr, "WARNING: Invalid application profile timestamp '%s' at %s:%d, skipping.\n", tok_sec, path, line_num);
			continue;
		}

		int parsed_target = 0;
		if (!is_valid_integer(tok_target, &parsed_target) || parsed_target < 1)
		{
			slog_warn("HERCULES_WARN_APP_PROFILE_TARGET: Invalid target_servers '%s' at %s:%d, skipping.", tok_target, path, line_num);
			fprintf(stderr, "WARNING: Invalid application profile target_servers '%s' at %s:%d, skipping.\n", tok_target, path, line_num);
			continue;
		}

		ApplicationProfileEvent ev;
		ev.trigger_sec = trigger_sec;
		ev.target_servers = (int32_t)parsed_target;
		ev.executed = false;
		events.push_back(ev);
	}

	fclose(fp);

	std::stable_sort(events.begin(), events.end(), [](const ApplicationProfileEvent &a, const ApplicationProfileEvent &b)
			 { return a.trigger_sec < b.trigger_sec; });

	slog_info("[ApplicationProfile] Loaded %zu scaling events from '%s'.", events.size(), path);
	fprintf(stderr, "[ApplicationProfile] Loaded %zu scaling events from '%s'.\n", events.size(), path);
	return 0;
}

/**
 * @brief Computes a 64-bit FNV-1a hash over a NUL-terminated string.
 *
 * @param str Input string to hash.
 * @return uint64_t 64-bit FNV-1a digest.
 */
static uint64_t fnv1a_64(const char *str)
{
	uint64_t hash = 14695981039346656037ULL;
	if (str == NULL)
	{
		return hash;
	}

	const unsigned char *p = (const unsigned char *)str;
	while (*p != '\0')
	{
		hash ^= (uint64_t)(*p);
		hash *= 1099511628211ULL;
		p++;
	}
	return hash;
}

/**
 * @brief Extracts the basename of raw_name and sanitizes it for safe profile filenames.
 *
 * @param raw_name Input application name or path.
 * @param out_name Output buffer receiving the sanitized token.
 * @param out_size Size of out_name buffer in bytes.
 */
static void sanitize_app_name(const char *raw_name, char *out_name, size_t out_size)
{
	if (out_name == NULL || out_size == 0)
	{
		slog_error("HERCULES_ERR_APP_PROFILE_SANITIZE_BUF: Invalid output buffer in sanitize_app_name.");
		fprintf(stderr, "HERCULES_ERR_APP_PROFILE_SANITIZE_BUF: Invalid output buffer in sanitize_app_name.\n");
		return;
	}

	if (raw_name == NULL || raw_name[0] == '\0')
	{
		snprintf(out_name, out_size, "app");
		return;
	}

	const char *base = strrchr(raw_name, '/');
	const char *src = (base != NULL && base[1] != '\0') ? (base + 1) : raw_name;

	size_t idx = 0;
	while (*src != '\0' && idx + 1 < out_size)
	{
		unsigned char ch = (unsigned char)*src;
		// verify is the string has only alphanumeric characters.
		if (isalnum(ch) || ch == '-' || ch == '_')
		{
			out_name[idx++] = (char)ch;
		}
		else
		{
			out_name[idx++] = '_';
		}
		src++;
	}
	out_name[idx] = '\0';

	if (idx == 0)
	{
		snprintf(out_name, out_size, "app");
	}
}

uint64_t compute_application_profile_hash(const struct arguments *cfg, char *out_signature, size_t sig_size)
{
	if (cfg == NULL)
	{
		slog_error("HERCULES_ERR_APP_PROFILE_HASH_NULL_CFG: NULL arguments pointer in compute_application_profile_hash.");
		fprintf(stderr, "HERCULES_ERR_APP_PROFILE_HASH_NULL_CFG: NULL arguments pointer in compute_application_profile_hash.\n");
		if (out_signature != NULL && sig_size > 0)
		{
			out_signature[0] = '\0';
		}
		return 0;
	}

	char clean_app[128];
	sanitize_app_name(cfg->application_name, clean_app, sizeof(clean_app));

	int32_t clients = (cfg->num_clients > 0) ? cfg->num_clients : 1;
	const char *policy = cfg->policy;

	char sig_buf[1024];
	snprintf(sig_buf, sizeof(sig_buf), "app=%s;clients=%" PRId32 ";init_servers=%" PRId64 ";min_servers=%" PRId32 ";max_servers=%" PRId32 ";block_kb=%zu;policy=%s;async=%" PRId32 ";attached=%d", clean_app, clients, cfg->num_data_servers, cfg->lower_bound_servers, cfg->upper_bound_servers, cfg->block_size, policy, cfg->async_io, (int)cfg->attached);

	if (out_signature != NULL && sig_size > 0)
	{
		snprintf(out_signature, sig_size, "%s", sig_buf);
	}

	return fnv1a_64(sig_buf);
}

int resolve_application_profile_path(struct arguments *cfg, uint64_t *out_hash)
{
	if (cfg == NULL)
	{
		slog_error("HERCULES_ERR_APP_PROFILE_RESOLVE: NULL arguments pointer.");
		fprintf(stderr, "HERCULES_ERR_APP_PROFILE_RESOLVE: NULL arguments pointer.\n");
		return -1;
	}

	uint64_t hash = compute_application_profile_hash(cfg, NULL, 0);
	if (out_hash != NULL)
	{
		*out_hash = hash;
	}

	// Preserve an explicitly configured profile path when provided.
	if (cfg->application_profile_path[0] != '\0')
	{
		return 0;
	}

	char clean_app[128];
	sanitize_app_name(cfg->application_name, clean_app, sizeof(clean_app));

	char dir_buf[PATH_MAX];
	const char *raw_dir = (cfg->application_profile_dir[0] != '\0') ? cfg->application_profile_dir : ".";
	snprintf(dir_buf, sizeof(dir_buf), "%s", raw_dir);
	size_t dir_len = strlen(dir_buf);
	while (dir_len > 1 && dir_buf[dir_len - 1] == '/')
	{
		dir_buf[dir_len - 1] = '\0';
		dir_len--;
	}

	int32_t clients = (cfg->num_clients > 0) ? cfg->num_clients : 1;
	char file_name[384];
	int fname_len = snprintf(file_name, sizeof(file_name), "%s_%" PRId32 "c_%" PRId64 "s_%016" PRIx64 ".csv", clean_app, clients, cfg->num_data_servers, hash);
	if (fname_len < 0 || (size_t)fname_len >= sizeof(file_name))
	{
		slog_error("HERCULES_ERR_APP_PROFILE_FILENAME: Generated profile filename exceeds buffer limit.");
		fprintf(stderr, "HERCULES_ERR_APP_PROFILE_FILENAME: Generated profile filename exceeds buffer limit.\n");
		return -1;
	}

	bool has_trailing_slash = (dir_len > 0 && dir_buf[dir_len - 1] == '/');
	size_t sep_len = has_trailing_slash ? 0 : 1;
	if (dir_len + sep_len + (size_t)fname_len >= sizeof(cfg->application_profile_path))
	{
		slog_error("HERCULES_ERR_APP_PROFILE_PATH_LEN: Resolved profile path exceeds PATH_MAX.");
		fprintf(stderr, "HERCULES_ERR_APP_PROFILE_PATH_LEN: Resolved profile path exceeds PATH_MAX.\n");
		return -1;
	}

	memcpy(cfg->application_profile_path, dir_buf, dir_len);
	size_t pos = dir_len;
	if (!has_trailing_slash)
	{
		cfg->application_profile_path[pos++] = '/';
	}
	memcpy(cfg->application_profile_path + pos, file_name, (size_t)fname_len + 1);
	return 0;
}

int init_application_profile_recording(const struct arguments *cfg)
{
	if (cfg == NULL || cfg->application_profile_path[0] == '\0')
	{
		slog_error("HERCULES_ERR_APP_PROFILE_RECORD_INIT: Invalid configuration or empty profile path.");
		fprintf(stderr, "HERCULES_ERR_APP_PROFILE_RECORD_INIT: Invalid configuration or empty profile path.\n");
		return -1;
	}

	char sig_buf[1024];
	uint64_t hash = compute_application_profile_hash(cfg, sig_buf, sizeof(sig_buf));

	FILE *fp = fopen(cfg->application_profile_path, "w");
	if (fp == NULL)
	{
		slog_error("HERCULES_ERR_APP_PROFILE_RECORD_OPEN: Failed to create profile '%s' (%s).", cfg->application_profile_path, strerror(errno));
		fprintf(stderr, "HERCULES_ERR_APP_PROFILE_RECORD_OPEN: Failed to create profile '%s' (%s).\n", cfg->application_profile_path, strerror(errno));
		return -1;
	}

	fprintf(fp, "# app_signature: %s\n", sig_buf);
	fprintf(fp, "# signature_hash: %016" PRIx64 "\n", hash);
	fprintf(fp, "elapsed_sec, target_servers\n");
	fflush(fp);
	fclose(fp);

	slog_info("[ApplicationProfile] Initialized profile recording at '%s' (hash=%016" PRIx64 ").", cfg->application_profile_path, hash);
	fprintf(stderr, "[ApplicationProfile] Initialized profile recording at '%s' (hash=%016" PRIx64 ").\n", cfg->application_profile_path, hash);
	return 0;
}

int record_application_profile_event(const char *path, double elapsed_sec, int32_t active_servers)
{
	if (path == NULL || path[0] == '\0' || !isfinite(elapsed_sec) || elapsed_sec < 0.0 || active_servers < 1)
	{
		slog_warn("HERCULES_WARN_APP_PROFILE_RECORD_ARGS: Invalid record parameters (elapsed=%.2f, servers=%" PRId32 ").", elapsed_sec, active_servers);
		fprintf(stderr, "WARNING: Invalid application profile record parameters (elapsed=%.2f, servers=%" PRId32 ").\n", elapsed_sec, active_servers);
		return -1;
	}

	FILE *fp = fopen(path, "a");
	if (fp == NULL)
	{
		slog_error("HERCULES_ERR_APP_PROFILE_APPEND: Failed to append event to '%s' (%s).", path, strerror(errno));
		fprintf(stderr, "HERCULES_ERR_APP_PROFILE_APPEND: Failed to append event to '%s' (%s).\n", path, strerror(errno));
		return -1;
	}

	fprintf(fp, "%.2f, %" PRId32 "\n", elapsed_sec, active_servers);
	fflush(fp);
	fclose(fp);

	slog_info("[ApplicationProfile] Recorded scaling event (t=%.2fs, active_servers=%" PRId32 ") to '%s'.", elapsed_sec, active_servers, path);
	fprintf(stderr, "[ApplicationProfile] Recorded scaling event (t=%.2fs, active_servers=%" PRId32 ") to '%s'.\n", elapsed_sec, active_servers, path);
	return 0;
}

/**
 * @brief Discards transition-period performance buckets and resets elasticity history after a scaling operation.
 *
 * @param erased_host Optional decommissioned server hostname to remove from elasticity history.
 */
void clear_transition_performance_metrics(const std::string *erased_host)
{
	bool had_entry = false;
	size_t erased_elements = 0;

	discard_transition_interval.store(true, std::memory_order_release);
	pthread_mutex_lock(&mutext_performance_metrics);
	if (erased_host != NULL && !erased_host->empty())
	{
		had_entry = (elasticity_records_history.find(*erased_host) != elasticity_records_history.end());
		erased_elements = elasticity_records_history.erase(*erased_host);
	}
	pending_perf_bucket.clear();
	for (auto &pair : elasticity_records_history)
	{
		pair.second.clear();
	}
	pthread_mutex_unlock(&mutext_performance_metrics);

	if (had_entry)
	{
		if (erased_elements > 0)
		{
			slog_malleability("Server %s has been deleted from the elasticity records history.", erased_host->c_str());
		}
		else
		{
			slog_error("Server %s has NOT been deleted from the elasticity records history.", erased_host->c_str());
		}
	}
}

/**
 * @brief Selects the data server ID and hostname to decommission for an application profile scale-down step.
 *
 * @param history_snapshot Current per-server elasticity history snapshot.
 * @param current_servers Number of currently active data servers.
 * @param out_key Output hostname of the chosen server when available.
 * @return int Target server ID to decommission, or -1 if no candidate exists.
 */
int select_profile_decommission_candidate(const std::map<std::string, std::vector<ElasticityMetric>> &history_snapshot, int32_t current_servers, std::string &out_key)
{
	int target_id = -1;
	double min_perf = 0.0;
	out_key.clear();

	for (const auto &pair : history_snapshot)
	{
		if (pair.second.empty())
		{
			continue;
		}
		const ElasticityMetric &latest = pair.second.back();
		if (latest.server_id < 0 || latest.server_id >= current_servers)
		{
			continue;
		}
		if (target_id == -1 || latest.overall_performance < min_perf)
		{
			target_id = latest.server_id;
			min_perf = latest.overall_performance;
			out_key = pair.first;
		}
	}

	// Fall back to the highest active server ID when no recent I/O metric exists.
	if (target_id == -1 && current_servers > 0)
	{
		target_id = current_servers - 1;
		if (curr_global_imss_info != NULL && target_id >= 0 && target_id < curr_global_imss_info->num_storages && curr_global_imss_info->ips != NULL && curr_global_imss_info->ips[target_id] != NULL)
		{
			out_key = curr_global_imss_info->ips[target_id];
		}
	}

	return target_id;
}

/**
 * @brief Executes manual server decommissioning on a dedicated worker thread so metadata request workers remain non-blocking.
 */
void *run_manual_decommission(void *th_argv)
{
	ManualDecommissionArgs *manual_args = (ManualDecommissionArgs *)th_argv;
	if (manual_args == NULL || manual_args->malleability_args == NULL)
	{
		slog_error("HERCULES_ERR_MANUAL_DECOM_NULL_ARGS: invalid manual decommission thread arguments");
		fprintf(stderr, "HERCULES_ERR_MANUAL_DECOM_NULL_ARGS: invalid manual decommission thread arguments\n");
		comissioning_on.store(false, std::memory_order_release);
		if (manual_args != NULL)
		{
			free(manual_args->malleability_args);
			free(manual_args);
		}
		pthread_exit(NULL);
	}

	pthread_mutex_lock(&mutext_malleability);
	comissioning_on.store(true, std::memory_order_release);
	decomissioning_stage(manual_args->malleability_args, manual_args->target_server_id);
	comissioning_on.store(false, std::memory_order_release);
	pthread_mutex_unlock(&mutext_malleability);

	free(manual_args->malleability_args);
	free(manual_args);
	pthread_exit(NULL);
}

static void *migration_pipeline_worker(void *arg)
{
	MigrationFlushPipeline *pipeline = static_cast<MigrationFlushPipeline *>(arg);
	if (pipeline == NULL)
	{
		pthread_exit(NULL);
	}

	while (true)
	{
		pthread_mutex_lock(&pipeline->mutex);
		while (pipeline->queue.empty() && !pipeline->stop_requested && !pipeline->failed)
		{
			pthread_cond_wait(&pipeline->cond_not_empty, &pipeline->mutex);
		}

		if (pipeline->failed || (pipeline->stop_requested && pipeline->queue.empty()))
		{
			pthread_mutex_unlock(&pipeline->mutex);
			break;
		}

		MigrationFlushTask task = std::move(pipeline->queue.front());
		pipeline->queue.pop_front();
		pipeline->inflight_count++;
		pthread_mutex_unlock(&pipeline->mutex);

		int32_t rc = pipeline->flush_cb(task.target_conn_idx, task.buffer, task.batch_seq, pipeline->user_ctx);

		pthread_mutex_lock(&pipeline->mutex);
		pipeline->inflight_count--;
		if (rc < 0)
		{
			pipeline->failed = true;
			slog_error("HERCULES_ERR_MIGRATION_PIPELINE_FLUSH: flush callback failed for conn_idx=%d batch_seq=%" PRIu64 " (rc=%" PRId32 ")", task.target_conn_idx, task.batch_seq, rc);
			fprintf(stderr, "HERCULES_ERR_MIGRATION_PIPELINE_FLUSH: flush callback failed for conn_idx=%d batch_seq=%" PRIu64 " (rc=%" PRId32 ")\n", task.target_conn_idx, task.batch_seq, rc);
		}

		reset_server_buffer(task.buffer);
		if (!pipeline->failed && pipeline->free_buffers.size() < pipeline->max_inflight)
		{
			pipeline->free_buffers.push_back(std::move(task.buffer));
		}

		pthread_cond_broadcast(&pipeline->cond_not_full);
		if (pipeline->failed)
		{
			pthread_cond_broadcast(&pipeline->cond_not_empty);
		}
		pthread_mutex_unlock(&pipeline->mutex);
	}

	pthread_exit(NULL);
}

/**
 * @brief Initializes a bounded producer-consumer flush pipeline for double-buffered block migration.
 */
int migration_pipeline_init(MigrationFlushPipeline *pipeline, int num_workers, size_t max_inflight, MigrationFlushCallback flush_cb, void *user_ctx)
{
	if (pipeline == NULL)
	{
		slog_error("HERCULES_ERR_MIGRATION_PIPELINE_INIT: pipeline pointer is NULL");
		fprintf(stderr, "HERCULES_ERR_MIGRATION_PIPELINE_INIT: pipeline pointer is NULL\n");
		return -1;
	}

	pipeline->initialized = false;
	if (num_workers <= 0 || max_inflight == 0 || flush_cb == NULL)
	{
		slog_error("HERCULES_ERR_MIGRATION_PIPELINE_INIT: invalid arguments (num_workers=%d, max_inflight=%zu, flush_cb=%p)", num_workers, max_inflight, (const void *)flush_cb);
		fprintf(stderr, "HERCULES_ERR_MIGRATION_PIPELINE_INIT: invalid arguments (num_workers=%d, max_inflight=%zu, flush_cb=%p)\n", num_workers, max_inflight, (const void *)flush_cb);
		return -1;
	}

	if (pthread_mutex_init(&pipeline->mutex, NULL) != 0)
	{
		slog_error("HERCULES_ERR_MIGRATION_PIPELINE_INIT: pthread_mutex_init failed");
		fprintf(stderr, "HERCULES_ERR_MIGRATION_PIPELINE_INIT: pthread_mutex_init failed\n");
		return -1;
	}
	if (pthread_cond_init(&pipeline->cond_not_empty, NULL) != 0)
	{
		pthread_mutex_destroy(&pipeline->mutex);
		slog_error("HERCULES_ERR_MIGRATION_PIPELINE_INIT: cond_not_empty init failed");
		fprintf(stderr, "HERCULES_ERR_MIGRATION_PIPELINE_INIT: cond_not_empty init failed\n");
		return -1;
	}
	if (pthread_cond_init(&pipeline->cond_not_full, NULL) != 0)
	{
		pthread_cond_destroy(&pipeline->cond_not_empty);
		pthread_mutex_destroy(&pipeline->mutex);
		slog_error("HERCULES_ERR_MIGRATION_PIPELINE_INIT: cond_not_full init failed");
		fprintf(stderr, "HERCULES_ERR_MIGRATION_PIPELINE_INIT: cond_not_full init failed\n");
		return -1;
	}

	pipeline->queue.clear();
	pipeline->free_buffers.clear();
	pipeline->workers.clear();
	pipeline->max_inflight = max_inflight;
	pipeline->inflight_count = 0;
	pipeline->next_batch_seq = 0;
	pipeline->stop_requested = false;
	pipeline->failed = false;
	pipeline->flush_cb = flush_cb;
	pipeline->user_ctx = user_ctx;
	pipeline->initialized = true;

	for (int i = 0; i < num_workers; i++)
	{
		pthread_t tid;
		if (pthread_create(&tid, NULL, migration_pipeline_worker, static_cast<void *>(pipeline)) != 0)
		{
			slog_error("HERCULES_ERR_MIGRATION_PIPELINE_THREAD_CREATE: failed to spawn worker %d/%d", i, num_workers);
			fprintf(stderr, "HERCULES_ERR_MIGRATION_PIPELINE_THREAD_CREATE: failed to spawn worker %d/%d\n", i, num_workers);
			migration_pipeline_finish(pipeline, true);
			return -1;
		}
		pipeline->workers.push_back(tid);
	}

	return 0;
}

/**
 * @brief Acquires a recycled ServerBuffer from the pipeline pool or initializes a fresh one.
 */
ServerBuffer migration_pipeline_acquire_buffer(MigrationFlushPipeline *pipeline, size_t max_size, size_t initial_cap_hint)
{
	if (pipeline != NULL && pipeline->initialized)
	{
		pthread_mutex_lock(&pipeline->mutex);
		if (!pipeline->free_buffers.empty())
		{
			ServerBuffer recycled = std::move(pipeline->free_buffers.back());
			pipeline->free_buffers.pop_back();
			pthread_mutex_unlock(&pipeline->mutex);
			recycled.max_size = max_size;
			reset_server_buffer(recycled);
			return recycled;
		}
		pthread_mutex_unlock(&pipeline->mutex);
	}

	ServerBuffer fresh;
	fresh.max_size = max_size;
	init_server_buffer(fresh, initial_cap_hint);
	return fresh;
}

/**
 * @brief Submits a filled ServerBuffer to the asynchronous flush pipeline with bounded backpressure.
 */
int migration_pipeline_submit(MigrationFlushPipeline *pipeline, int target_conn_idx, ServerBuffer &&srv_buf)
{
	if (pipeline == NULL || !pipeline->initialized)
	{
		slog_error("HERCULES_ERR_MIGRATION_PIPELINE_SUBMIT: pipeline not initialized");
		fprintf(stderr, "HERCULES_ERR_MIGRATION_PIPELINE_SUBMIT: pipeline not initialized\n");
		return -1;
	}

	if (srv_buf.offset == 0 || srv_buf.block_count == 0)
	{
		return 0;
	}

	pthread_mutex_lock(&pipeline->mutex);
	while ((pipeline->queue.size() + pipeline->inflight_count) >= pipeline->max_inflight && !pipeline->failed && !pipeline->stop_requested)
	{
		pthread_cond_wait(&pipeline->cond_not_full, &pipeline->mutex);
	}

	if (pipeline->failed || pipeline->stop_requested)
	{
		pthread_mutex_unlock(&pipeline->mutex);
		return -1;
	}

	MigrationFlushTask task;
	task.target_conn_idx = target_conn_idx;
	task.batch_seq = pipeline->next_batch_seq++;
	task.buffer = std::move(srv_buf);
	pipeline->queue.push_back(std::move(task));
	pthread_cond_signal(&pipeline->cond_not_empty);
	pthread_mutex_unlock(&pipeline->mutex);
	return 0;
}

/**
 * @brief Abords pending flush tasks, joins all worker threads, and releases pipeline resources.
 */
int migration_pipeline_finish(MigrationFlushPipeline *pipeline, bool abort_pending)
{
	if (pipeline == NULL || !pipeline->initialized)
	{
		return 0;
	}

	pthread_mutex_lock(&pipeline->mutex);
	pipeline->stop_requested = true;
	if (abort_pending)
	{
		pipeline->failed = true;
	}
	pthread_cond_broadcast(&pipeline->cond_not_empty);
	pthread_cond_broadcast(&pipeline->cond_not_full);
	pthread_mutex_unlock(&pipeline->mutex);

	for (pthread_t tid : pipeline->workers)
	{
		pthread_join(tid, NULL);
	}

	bool had_error = pipeline->failed;
	pipeline->queue.clear();
	pipeline->free_buffers.clear();
	pipeline->workers.clear();
	pipeline->inflight_count = 0;
	pipeline->initialized = false;

	pthread_cond_destroy(&pipeline->cond_not_full);
	pthread_cond_destroy(&pipeline->cond_not_empty);
	pthread_mutex_destroy(&pipeline->mutex);

	return had_error ? -1 : 0;
}

/**
 * @brief Initializes a standalone AsyncMigrationTracker instance.
 * @param tracker Tracker instance to initialize.
 */
void async_migration_tracker_init(AsyncMigrationTracker *tracker)
{
	if (tracker == NULL)
	{
		return;
	}
	pthread_mutex_init(&tracker->mutex, NULL);
	pthread_cond_init(&tracker->cond, NULL);
	tracker->senders.clear();
}

/**
 * @brief Releases synchronization primitives owned by a standalone AsyncMigrationTracker instance.
 * @param tracker Tracker instance to destroy.
 */
void async_migration_tracker_destroy(AsyncMigrationTracker *tracker)
{
	if (tracker == NULL)
	{
		return;
	}
	pthread_mutex_lock(&tracker->mutex);
	tracker->senders.clear();
	pthread_mutex_unlock(&tracker->mutex);
	pthread_cond_destroy(&tracker->cond);
	pthread_mutex_destroy(&tracker->mutex);
}

/**
 * @brief Clears per-sender asynchronous migration counters at the start of a decommission cycle.
 * @param tracker Tracker instance to reset.
 */
void async_migration_tracker_reset(AsyncMigrationTracker *tracker)
{
	if (tracker == NULL)
	{
		return;
	}
	pthread_mutex_lock(&tracker->mutex);
	tracker->senders.clear();
	pthread_cond_broadcast(&tracker->cond);
	pthread_mutex_unlock(&tracker->mutex);
}

/**
 * @brief Records the completion status of one SETNETWORK_ASYNC block for sender_uid and wakes waiting barriers.
 * @param tracker Tracker instance.
 * @param sender_uid Source server UCX worker UID.
 * @param success True if the block was stored or safely drained as stale; false on write/protocol error.
 */
void async_migration_tracker_record(AsyncMigrationTracker *tracker, uint64_t sender_uid, bool success)
{
	if (tracker == NULL)
	{
		return;
	}
	pthread_mutex_lock(&tracker->mutex);
	AsyncMigrationSenderCounts &counts = tracker->senders[sender_uid];
	if (success)
	{
		counts.completed++;
	}
	else
	{
		counts.errors++;
	}
	pthread_cond_broadcast(&tracker->cond);
	pthread_mutex_unlock(&tracker->mutex);
}

/**
 * @brief Waits until sender_uid reaches expected_blocks completed/errored async writes or timeout_sec expires.
 * Cleans up the sender entry upon return so subsequent migrations start with fresh counters.
 * @param tracker Tracker instance.
 * @param sender_uid Source server UCX worker UID.
 * @param expected_blocks Number of blocks dispatched by sender_uid to this destination.
 * @param timeout_sec Maximum wait duration in seconds (minimum 1).
 * @param out_completed Optional output pointer for completed block count.
 * @param out_errors Optional output pointer for errored block count.
 * @return 0 if completed == expected_blocks with zero errors, -1 on storage/count error, -2 on timeout.
 */
int async_migration_tracker_wait(AsyncMigrationTracker *tracker, uint64_t sender_uid, uint64_t expected_blocks, int timeout_sec, uint64_t *out_completed, uint64_t *out_errors)
{
	if (tracker == NULL)
	{
		if (out_completed != NULL)
		{
			*out_completed = 0;
		}
		if (out_errors != NULL)
		{
			*out_errors = 0;
		}
		return -1;
	}

	int effective_timeout = timeout_sec;
	if (effective_timeout < 1)
	{
		effective_timeout = 1;
	}

	struct timespec deadline;
	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += effective_timeout;

	pthread_mutex_lock(&tracker->mutex);
	bool timed_out = false;
	while (true)
	{
		uint64_t completed = 0;
		uint64_t errors = 0;
		auto it = tracker->senders.find(sender_uid);
		if (it != tracker->senders.end())
		{
			completed = it->second.completed;
			errors = it->second.errors;
		}

		if (completed + errors >= expected_blocks)
		{
			if (out_completed != NULL)
			{
				*out_completed = completed;
			}
			if (out_errors != NULL)
			{
				*out_errors = errors;
			}
			tracker->senders.erase(sender_uid);
			pthread_mutex_unlock(&tracker->mutex);
			if (errors == 0 && completed == expected_blocks)
			{
				return 0;
			}
			return -1;
		}

		if (timed_out)
		{
			if (out_completed != NULL)
			{
				*out_completed = completed;
			}
			if (out_errors != NULL)
			{
				*out_errors = errors;
			}
			tracker->senders.erase(sender_uid);
			pthread_mutex_unlock(&tracker->mutex);
			return -2;
		}

		int rc = pthread_cond_timedwait(&tracker->cond, &tracker->mutex, &deadline);
		if (rc == ETIMEDOUT)
		{
			timed_out = true;
		}
	}
}
