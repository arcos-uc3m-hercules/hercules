#include "workers.h"
#include "arg_parser.h"
#include "comms.h"
#include "imss.h"
#include "slog.h"
#include <cstring>
#include <ctime>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

extern struct arguments args;

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
