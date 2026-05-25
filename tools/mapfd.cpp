#include <map>
#include <iostream>
#include <vector>
#include <memory>
#include <cstddef>
#include <cstring>
#include <climits>
#include <sys/stat.h>
#include <fcntl.h>
#include <mutex>

#include "mapfd.hpp"
#include "slog.h"

using std::string;

// Structure representing the open file description state
struct FileDescription
{
	std::string pathname;
	unsigned long offset;

	FileDescription(std::string path, unsigned long off)
	    : pathname(std::move(path)), offset(off) {}
};

// Map file descriptors to a shared pointer of the file description
typedef std::map<int, std::shared_ptr<FileDescription>> Map;

std::mutex fdlock;

extern "C"
{
	void *map_fd_create()
	{
		std::unique_lock<std::mutex> lck(fdlock);
		return reinterpret_cast<void *>(new Map());
	}

	void map_fd_destroy(void *map)
	{
		if (map)
		{
			std::unique_lock<std::mutex> lck(fdlock);
			Map *m = reinterpret_cast<Map *>(map);
			m->clear();
			delete m;
		}
	}

	int map_fd_put(void *map, const char *pathname, const int fd, unsigned long offset)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);

		auto value = std::make_shared<FileDescription>(pathname, offset);
		auto ret = m->insert({fd, value});

		return ret.second ? 1 : -1;
	}

	int map_fd_dup(void *map, const int old_fd, const int new_fd)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);

		auto search = m->find(old_fd);
		if (search == m->end())
		{
			return -1;
		}

		// Map the new_fd to point to the exact same tracking instance
		(*m)[new_fd] = search->second;
		return 1;
	}

	void map_fd_update_value(void *map, const char *pathname, const int fd, unsigned long offset)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		auto search = m->find(fd);

		if (search != m->end())
		{
			search->second->pathname = pathname;
			search->second->offset = offset;
		}
	}

	void map_fd_update_fd(void *map, const char *pathname, const int fd, const int new_fd, unsigned long offset)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		auto node = m->extract(fd);

		if (!node.empty())
		{
			node.key() = new_fd;
			node.mapped()->pathname = pathname;
			node.mapped()->offset = offset;
			m->insert(std::move(node));
		}
	}

	void map_fd_erase(void *map, const int fd)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		m->erase(fd);
	}

	int map_fd_erase_by_pathname(void *map, const char *pathname)
	{
		slog_debug("Erasing %s from the map", pathname);
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		char aux_path[PATH_MAX] = {0};

		size_t len = strlen(pathname);
		if (len > 0 && pathname[len - 1] != '/')
		{
			strncpy(aux_path, pathname, PATH_MAX - 2);
			strcat(aux_path, "/");
		}

		for (auto it = m->begin(); it != m->end();)
		{
			const char *val = it->second->pathname.c_str();
			slog_debug("pathname=%s, aux_path=%s", pathname, aux_path);
			if (strcmp(val, pathname) == 0 || (aux_path[0] != '\0' && strcmp(val, aux_path) == 0))
			{
				it = m->erase(it);
				return 1;
			}
			else
			{
				++it;
			}
		}

		return -1;
	}

	int map_fd_search(void *map, const char *pathname, const int fd, unsigned long *offset)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		auto search = m->find(fd);

		if (search != m->end())
		{
			if (offset)
				*offset = search->second->offset;
			return 1;
		}
		return -1;
	}

	std::string map_fd_search_by_val(void *map, const int fd)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		if (!map)
			return "";

		Map *m = reinterpret_cast<Map *>(map);
		auto search = m->find(fd);

		if (search != m->end())
		{
			return search->second->pathname;
		}
		return "";
	}

	int map_fd_search_by_val_close(void *map, int fd)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		auto search = m->find(fd);

		if (search != m->end())
		{
			m->erase(search);
			return 0;
		}
		return 1;
	}

	int map_fd_search_by_pathname(void *map, const char *pathname, int *fd, long *offset)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		// Traverse the map
		for (auto &it : *m)
		{
			if (strcmp(it.second->pathname.c_str(), pathname) == 0)
			{
				if (fd)
					*fd = it.first;
				if (offset)
					*offset = static_cast<long>(it.second->offset);
				return 1;
			}
		}
		return -1;
	}


	int map_fd_serialize(void *map, void **out_buf, size_t *out_size)
	{
		if (!map || !out_buf || !out_size)
			return -1;

		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);

		std::vector<char> buffer;

		auto append_bytes = [&buffer](const void *data, size_t size)
		{
			const char *ptr = reinterpret_cast<const char *>(data);
			buffer.insert(buffer.end(), ptr, ptr + size);
		};

		// identify unique FileDescription instances to preserve shared_ptr mapping (dup tracking)
		std::vector<std::shared_ptr<FileDescription>> unique_descs;
		std::map<std::shared_ptr<FileDescription>, uint32_t> desc_to_index;

		for (const auto &pair : *m)
		{
			if (desc_to_index.find(pair.second) == desc_to_index.end())
			{
				uint32_t idx = unique_descs.size();
				desc_to_index[pair.second] = idx;
				unique_descs.push_back(pair.second);
			}
		}

		// write metadata header
		uint32_t num_descriptions = unique_descs.size();
		uint32_t num_entries = m->size();
		append_bytes(&num_descriptions, sizeof(num_descriptions));
		append_bytes(&num_entries, sizeof(num_entries));

		// serialize unique FileDescription configurations
		for (const auto &desc : unique_descs)
		{
			append_bytes(&desc->offset, sizeof(desc->offset));

			uint32_t path_len = desc->pathname.size();
			append_bytes(&path_len, sizeof(path_len));
			if (path_len > 0)
			{
				append_bytes(desc->pathname.data(), path_len);
			}
		}

		// serialize the map layout matching FDs to their Description Index
		for (const auto &pair : *m)
		{
			int fd = pair.first;
			uint32_t idx = desc_to_index[pair.second];
			append_bytes(&fd, sizeof(fd));
			append_bytes(&idx, sizeof(idx));
		}

		*out_size = buffer.size();
		*out_buf = malloc(*out_size);
		if (!*out_buf)
			return -1;

		std::memcpy(*out_buf, buffer.data(), *out_size);
		return 0;
	}

	int map_fd_deserialize(void *map, const void *buf, size_t size)
	{
		if (!map || !buf || size < (sizeof(uint32_t) * 2))
			return -1;

		std::unique_lock<std::mutex> lck(fdlock);
		Map *m = reinterpret_cast<Map *>(map);
		m->clear();

		const char *ptr = reinterpret_cast<const char *>(buf);

		auto read_bytes = [&ptr](void *dest, size_t num_bytes)
		{
			std::memcpy(dest, ptr, num_bytes);
			ptr += num_bytes;
		};

		// read Header metadata
		uint32_t num_descriptions = 0;
		uint32_t num_entries = 0;
		read_bytes(&num_descriptions, sizeof(num_descriptions));
		read_bytes(&num_entries, sizeof(num_entries));

		// reconstruct unique FileDescriptions in matching order
		std::vector<std::shared_ptr<FileDescription>> unique_descs(num_descriptions);
		for (uint32_t i = 0; i < num_descriptions; ++i)
		{
			unsigned long offset = 0;
			read_bytes(&offset, sizeof(offset));

			uint32_t path_len = 0;
			read_bytes(&path_len, sizeof(path_len));

			std::string pathname;
			if (path_len > 0)
			{
				pathname.resize(path_len);
				read_bytes(&pathname[0], path_len);
			}

			unique_descs[i] = std::make_shared<FileDescription>(pathname, offset);
		}

		// populate the Map tying indices back to their proper FDs
		for (uint32_t i = 0; i < num_entries; ++i)
		{
			int fd = 0;
			uint32_t idx = 0;
			read_bytes(&fd, sizeof(fd));
			read_bytes(&idx, sizeof(idx));

			if (idx >= num_descriptions)
				return -1; // Out-of-bounds corruption guard
			m->insert({fd, unique_descs[idx]});
		}

		return 0;
	}

} // extern "C"
