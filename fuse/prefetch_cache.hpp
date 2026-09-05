#ifndef PREFETCH_CACHE_HPP
#define PREFETCH_CACHE_HPP

#include "imss.h"
#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <utility>

extern uint64_t IMSS_DATA_BSIZE;

/**
 * @brief Node managing the lifetime of a raw prefetch buffer received from a server.
 */
struct PrefetchBufferNode
{
	char *raw_buffer;
	size_t length;
	PrefetchBufferNode(char *buf, size_t len) : raw_buffer(buf), length(len) {}
	~PrefetchBufferNode()
	{
		if (raw_buffer != nullptr)
		{
			free(raw_buffer);
			raw_buffer = nullptr;
		}
	}
};

/**
 * @brief Entry for an individual prefetched block pointing inside a PrefetchBufferNode.
 */
struct PrefetchBlockEntry
{
	std::shared_ptr<PrefetchBufferNode> buffer_ref;
	const char *data;
};

/**
 * @brief Work item representing a background prefetch request for a block range.
 */
struct PrefetchTask
{
	std::string path;
	int32_t dataset_id;
	uint32_t start_block_id;
	size_t num_blocks;
	size_t total_read_size;
};

/**
 * @brief Thread-safe client-side cache storing prefetched block records for open files,
 *        with an integrated background request queue and worker loop.
 */
class PrefetchCacheV2
{
      private:
	std::mutex mtx;
	std::map<std::string, std::map<uint32_t, PrefetchBlockEntry>> file_cache;

	std::mutex queue_mtx;
	std::condition_variable queue_cv;
	std::queue<PrefetchTask> tasks;
	std::set<std::pair<std::string, uint32_t>> in_flight_tasks;
	bool stop_requested{false};

      public:
	PrefetchCacheV2() : stop_requested(false) {}
	~PrefetchCacheV2()
	{
		stop_worker();
		clear();
	}

	int has_block(const char *path, uint32_t block_id)
	{
		if (path == nullptr)
		{
			return 0;
		}
		std::unique_lock<std::mutex> lck(mtx);
		auto it_file = file_cache.find(std::string(path));
		if (it_file == file_cache.end())
		{
			return 0;
		}
		return (it_file->second.find(block_id) != it_file->second.end()) ? 1 : 0;
	}

	int get_data(const char *path, uint32_t block_id, void *dst, size_t offset_in_block, size_t bytes_to_read)
	{
		if (path == nullptr || dst == nullptr)
		{
			return 0;
		}
		std::unique_lock<std::mutex> lck(mtx);
		auto it_file = file_cache.find(std::string(path));
		if (it_file == file_cache.end())
		{
			return 0;
		}
		auto it_block = it_file->second.find(block_id);
		if (it_block == it_file->second.end())
		{
			return 0;
		}
		memcpy(dst, it_block->second.data + offset_in_block, bytes_to_read);
		return 1;
	}

	void put_buffer(const char *path, void *received_buffer, size_t received_length, size_t block_data_size)
	{
		if (path == nullptr || received_buffer == nullptr || received_length == 0)
		{
			return;
		}

		const uint32_t BLOCK_ID_SIZE = sizeof(uint32_t);
		const size_t RECORD_SIZE = BLOCK_ID_SIZE + block_data_size;

		auto buf_node = std::make_shared<PrefetchBufferNode>((char *)received_buffer, received_length);

		std::unique_lock<std::mutex> lck(mtx);
		auto &block_map = file_cache[std::string(path)];

		char *current_ptr = (char *)received_buffer;
		char *end_ptr = current_ptr + received_length;

		while (current_ptr + RECORD_SIZE <= end_ptr)
		{
			uint32_t block_id = *(uint32_t *)current_ptr;
			const char *data_ptr = current_ptr + BLOCK_ID_SIZE;

			PrefetchBlockEntry entry;
			entry.buffer_ref = buf_node;
			entry.data = data_ptr;

			block_map[block_id] = entry;
			current_ptr += RECORD_SIZE;
		}
	}

	int submit_request(const char *path, int32_t dataset_id, uint32_t start_block_id, size_t num_blocks, size_t total_read_size)
	{
		if (path == nullptr || num_blocks == 0)
		{
			return 0;
		}
		std::string path_str(path);
		{
			std::unique_lock<std::mutex> lck(queue_mtx);
			if (stop_requested)
			{
				return 0;
			}
			auto key = std::make_pair(path_str, start_block_id);
			if (in_flight_tasks.find(key) != in_flight_tasks.end())
			{
				return 0; // Already queued or in flight
			}
			in_flight_tasks.insert(key);
			tasks.push({path_str, dataset_id, start_block_id, num_blocks, total_read_size});
		}
		queue_cv.notify_one();
		return 1;
	}

	void stop_worker()
	{
		{
			std::unique_lock<std::mutex> lck(queue_mtx);
			stop_requested = true;
		}
		queue_cv.notify_all();
	}

	void worker_loop()
	{
		while (true)
		{
			PrefetchTask task;
			{
				std::unique_lock<std::mutex> lck(queue_mtx);
				while (!stop_requested && tasks.empty())
				{
					queue_cv.wait(lck);
				}
				if (stop_requested && tasks.empty())
				{
					break;
				}
				if (!tasks.empty())
				{
					task = tasks.front();
					tasks.pop();
				}
				else
				{
					continue;
				}
			}

			if (!has_block(task.path.c_str(), task.start_block_id))
			{
				void *prefetch_buf = nullptr;
				ssize_t been_read = get_ndata_prefetch((char *)task.path.c_str(), task.dataset_id, (int32_t)task.start_block_id, &prefetch_buf, task.total_read_size, task.num_blocks);
				if (been_read > 0 && prefetch_buf != nullptr)
				{
					size_t blk_size = (IMSS_DATA_BSIZE > 0) ? (size_t)IMSS_DATA_BSIZE : (512 * 1024);
					put_buffer(task.path.c_str(), prefetch_buf, (size_t)been_read, blk_size);
				}
				else if (prefetch_buf != nullptr)
				{
					free(prefetch_buf);
				}
			}

			{
				std::unique_lock<std::mutex> lck(queue_mtx);
				in_flight_tasks.erase(std::make_pair(task.path, task.start_block_id));
			}
		}
	}

	void invalidate(const char *path)
	{
		if (path == nullptr)
		{
			return;
		}
		std::unique_lock<std::mutex> lck(mtx);
		file_cache.erase(std::string(path));
	}

	void clear()
	{
		std::unique_lock<std::mutex> lck(mtx);
		file_cache.clear();
	}
};

#endif // PREFETCH_CACHE_HPP
