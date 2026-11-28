#include "comms.h"
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <sys/stat.h>
#include <vector>

#include <ucp/api/ucp.h>
// #include <ucx/ucx.h>
#include "map_server_eps.hpp"
// to manage logs.
#include "slog.h"

void *map_server_eps_create()
{
	return reinterpret_cast<void *>(new map_server_eps_t);
}

void map_server_eps_destroy(void *map, ucp_worker_h ucp_worker)
{
	if (map)
	{
		map_server_eps_t *m = reinterpret_cast<map_server_eps_t *>(map);
		std::vector<ServerEpEntry> entries;
		{
			std::unique_lock<std::mutex> lock(mut_eps);
			entries.reserve(m->size());
			for (const auto &entry : *m)
			{
				entries.push_back(entry.second);
			}
			m->clear();
		}
		delete m;

		for (const auto &entry : entries)
		{
			int close_rc = 0;
			if (entry.ep != NULL)
			{
				close_rc = (ucp_worker != NULL) ? close_ucx_endpoint(ucp_worker, entry.ep) : -1;
			}
			if (entry.ctx != NULL && close_rc == 0)
			{
				free(entry.ctx);
			}
		}
	}
}

void map_server_eps_put_with_ctx(void *map, uint64_t uuid, ucp_ep_h ep, client_ep_context_t *ctx, ucp_worker_h ucp_worker)
{
	map_server_eps_t *m = reinterpret_cast<map_server_eps_t *>(map);
	ucp_ep_h old_ep = NULL;
	client_ep_context_t *old_ctx = NULL;
	size_t map_size = 0;
	{
		std::unique_lock<std::mutex> lock(mut_eps);
		auto it = m->find(uuid);
		if (it != m->end())
		{
			old_ep = it->second.ep;
			old_ctx = it->second.ctx;
			it->second.ep = ep;
			it->second.ctx = ctx;
		}
		else
		{
			ServerEpEntry entry = {ep, ctx};
			m->insert(std::pair<uint64_t, ServerEpEntry>(uuid, entry));
		}
		map_size = m->size();
	}

	// Close any replaced UCX endpoint before freeing its context 
	// so err_cb_client cannot dereference freed memory.
	int close_rc = 0;
	if (old_ep != NULL && old_ep != ep)
	{
		close_rc = (ucp_worker != NULL) ? close_ucx_endpoint(ucp_worker, old_ep) : -1;
	}
	// Free the previous context only if it differs from the new one
	// and its endpoint is already closed (or NULL/unchanged).
	if (old_ctx != NULL && old_ctx != ctx && close_rc == 0)
	{
		free(old_ctx);
	}

	slog_debug("\t['%" PRIu64 "'] Adding new connection, #%zu", uuid, map_size);
	// fprintf(stderr, "\t[%c]['%" PRIu64 "'] Adding new connection, #%zu\n", server_type, uuid, map_size);
}

void map_server_eps_put(void *map, uint64_t uuid, ucp_ep_h ep)
{
	map_server_eps_put_with_ctx(map, uuid, ep, NULL, NULL);
}

void map_server_eps_erase(void *map, uint64_t uuid, ucp_worker_h ucp_worker)
{
	map_server_eps_t *m = reinterpret_cast<map_server_eps_t *>(map);
	ucp_ep_h ep_to_close = NULL;
	client_ep_context_t *ctx_to_free = NULL;
	{
		std::unique_lock<std::mutex> lock(mut_eps);
		auto search = m->find(uuid);
		if (search != m->end())
		{
			ep_to_close = search->second.ep;
			ctx_to_free = search->second.ctx;
			m->erase(search);
		}
	}
	int close_rc = 0;
	if (ep_to_close != NULL)
	{
		close_rc = (ucp_worker != NULL) ? close_ucx_endpoint(ucp_worker, ep_to_close) : -1;
	}
	if (ctx_to_free != NULL && close_rc == 0)
	{
		free(ctx_to_free);
	}
}

int map_server_eps_search_with_ctx(void *map, uint64_t uuid, ucp_ep_h *ep, client_ep_context_t **ctx)
{
	map_server_eps_t *m = reinterpret_cast<map_server_eps_t *>(map);
	// slog_debug("Locking");
	std::unique_lock<std::mutex> lock(mut_eps);
	// slog_debug("Unlocking");

	auto search = m->find(uuid);

	if (search != m->end())
	{
		if (ep != NULL)
		{
			*ep = search->second.ep;
		}
		if (ctx != NULL)
		{
			*ctx = search->second.ctx;
		}
		return 1;
	}
	else
	{
		if (ctx != NULL)
		{
			*ctx = NULL;
		}
		return -1;
	}
}

int map_server_eps_search(void *map, uint64_t uuid, ucp_ep_h *ep)
{
	return map_server_eps_search_with_ctx(map, uuid, ep, NULL);
}

size_t map_server_eps_get_size(void *map)
{
	return reinterpret_cast<map_server_eps_t *>(map)->size();
}
