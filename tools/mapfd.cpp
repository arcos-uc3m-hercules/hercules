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
struct FileDescription {
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

		for (auto it = m->begin(); it != m->end(); )
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
			if (offset) *offset = search->second->offset;
			return 1;
		}
		return -1;
	}

	std::string map_fd_search_by_val(void *map, const int fd)
	{
		std::unique_lock<std::mutex> lck(fdlock);
		if (!map) return "";
		
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
				if (fd) *fd = it.first;
				if (offset) *offset = static_cast<long>(it.second->offset);
				return 1;
			}
		}
		return -1;
	}

} // extern "C"
