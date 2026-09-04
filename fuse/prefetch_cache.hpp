#ifndef PREFETCH_CACHE_HPP
#define PREFETCH_CACHE_HPP

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>

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
 * @brief Thread-safe client-side cache storing prefetched block records for open files.
 */
class PrefetchCacheV2
{
private:
	std::mutex mtx;
	std::map<std::string, std::map<uint32_t, PrefetchBlockEntry>> file_cache;

public:
	PrefetchCacheV2() {}
	~PrefetchCacheV2() { clear(); }

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
