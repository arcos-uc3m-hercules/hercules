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

void map_server_eps_destroy(void *map)
{
	if (map)
	{
		delete reinterpret_cast<map_server_eps_t *>(map);
	}
}

void map_server_eps_put(void *map, uint64_t uuid, ucp_ep_h ep)
{
	map_server_eps_t *m = reinterpret_cast<map_server_eps_t *>(map);
	std::unique_lock<std::mutex> lock(mut_eps);

	m->insert(std::pair<uint64_t, ucp_ep_h>(uuid, ep));

	slog_debug("\t['%" PRIu64 "'] Adding new connection, #%ld", uuid, m->size());
	// fprintf(stderr, "\t[%c]['%" PRIu64 "'] Adding new connection, #%ld\n", server_type, uuid, m->size());
}

void map_server_eps_erase(void *map, uint64_t uuid, ucp_worker_h ucp_worker)
{
	map_server_eps_t *m = reinterpret_cast<map_server_eps_t *>(map);
	ucp_ep_h ep_to_close = NULL;
	{
		std::unique_lock<std::mutex> lock(mut_eps);
		auto search = m->find(uuid);
		if (search != m->end())
		{
			ep_to_close = search->second;
			m->erase(search);
		}
	}
	if (ep_to_close != NULL)
	{
		close_ucx_endpoint(ucp_worker, ep_to_close);
	}
}

int map_server_eps_search(void *map, uint64_t uuid, ucp_ep_h *ep)
{
	map_server_eps_t *m = reinterpret_cast<map_server_eps_t *>(map);
	// slog_debug("Locking");
	std::unique_lock<std::mutex> lock(mut_eps);
	// slog_debug("Unlocking");

	auto search = m->find(uuid);

	if (search != m->end())
	{
		*ep = (search->second);
		return 1;
	}
	else
	{
		return -1;
	}
}

size_t map_server_eps_get_size(void *map)
{
	return reinterpret_cast<map_server_eps_t *>(map)->size();
}
