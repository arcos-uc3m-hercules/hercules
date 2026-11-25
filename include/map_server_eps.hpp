#ifndef MAP_SERVER_EPS_H
#define MAP_SERVER_EPS_H

#include "comms.h"
#include <map>
#include <ucp/api/ucp.h>
#include <inttypes.h>
#include <mutex>

struct ServerEpEntry
{
	ucp_ep_h ep;
	client_ep_context_t *ctx;
};

typedef std::map<uint64_t, ServerEpEntry> map_server_eps_t;
static std::mutex mut_eps;

void *  map_server_eps_create();
void map_server_eps_destroy(void *map, ucp_worker_h ucp_worker);
void    map_server_eps_put(void * map, uint64_t uuid, ucp_ep_h ep);
void    map_server_eps_put_with_ctx(void *map, uint64_t uuid, ucp_ep_h ep, client_ep_context_t *ctx, ucp_worker_h ucp_worker);
void    map_server_eps_erase(void * map, uint64_t uuid, ucp_worker_h ucp_worker);
int     map_server_eps_search(void * map, uint64_t uuid, ucp_ep_h *ep);
int     map_server_eps_search_with_ctx(void *map, uint64_t uuid, ucp_ep_h *ep, client_ep_context_t **ctx);
size_t map_server_eps_get_size(void *map);

#endif
