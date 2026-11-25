#ifndef MAP_EP_H
#define MAP_EP_H

#include <map>
#include <ucp/api/ucp.h>
#include "queue.h"
#include <atomic>
#include <cstdint>

// TODO: move this to the correct header.
extern std::atomic<size_t> outstanding_sends;
extern std::atomic<uint64_t> async_send_errors;

/**
 * @brief Atomically decrements outstanding_sends.
 */
static inline void decrement_outstanding_sends()
{
	size_t current = outstanding_sends.load(std::memory_order_relaxed);
	while (current > 0 && !outstanding_sends.compare_exchange_weak(current, current - 1, std::memory_order_relaxed))
	{
	}
}

typedef std::map<ucp_ep_h, StsHeader*> map_ep_t;


void *  map_ep_create();
void    map_ep_put(void * map, ucp_ep_h ep, StsHeader * req_queue);
void    map_ep_erase(void * map, ucp_ep_h ep);
int     map_ep_search(void * map, const ucp_ep_h ep, StsHeader ** req_queue);

#endif
