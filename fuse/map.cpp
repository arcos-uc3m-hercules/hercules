
#include "map.hpp"

// to manage logs.
#include "slog.h"

extern uint64_t IMSS_BLKSIZE;
#define KB 1024
#define GB 1073741824

using std::string;

// for sincronization in the map.
std::mutex map_lock;

extern "C"
{

	void *map_create()
	{
		return reinterpret_cast<void *>(new Map);
	}

	void map_destroy(void *map)
	{
		if (map)
		{
			delete reinterpret_cast<Map *>(map);
		}
	}

	// Insert the element "p = {v,stat,aux}" with key "k" to the map "map".
	// This info is used by the "fd_lookup" function to avoid extra calls for the block 0.
	// void map_put(void *map, char *k, int v, struct stat stat, char *aux)
	void map_put(void *map, const char *k, int v, struct stat stat, char *aux)
	{
		std::unique_lock<std::mutex> lck(map_lock);
		std::pair<std::map<std::string, struct elements>::iterator, bool> ret;
		Map *m = reinterpret_cast<Map *>(map);
		struct elements p = {v, stat, aux};
		ret = m->insert(std::pair<std::string, struct elements>(std::string(k), p));
		if (ret.second == false)
		{
			slog_debug("Element %s already existed.", k);
		}
	}

	void map_hierarchical_put(void *map, char *k, int v, struct stat stat, char *aux)
	{
		std::unique_lock<std::mutex> lck(map_lock);
		std::pair<std::map<std::string, struct elements>::iterator, bool> ret;
		Map *m = reinterpret_cast<Map *>(map);
		struct elements p = {v, stat, aux};
		ret = m->insert(std::pair<std::string, struct elements>(std::string(k), p));
		if (ret.second == false)
		{
			slog_debug("Element %s already existed.", k);
		}
	}

	void map_update(void *map, const char *k, int v, struct stat stat)
	{
		std::unique_lock<std::mutex> lck(map_lock);
		Map *m = reinterpret_cast<Map *>(map);
		auto search = m->find(std::string(k));
		search->second.stat = stat;
	}

	// Removes the element with key "k" from the map "map".
	void map_erase(void *map, const char *k)
	{
		std::unique_lock<std::mutex> lck(map_lock);
		Map *m = reinterpret_cast<Map *>(map);
		// int ret = 0;
		auto search = m->find(std::string(k));

		if (search != m->end())
		{
			free(search->second.aux);
			slog_debug("erasing element with key %s", k);
		}
		else
		{
			slog_debug("element with key %s was not find", k);
		}
		int num_elements_erased = m->erase(std::string(k));
		slog_debug("finish map_erase, num_elements_erased=%d", num_elements_erased);
		// return ret;
	}

	int map_search(void *map, const char *k, int *v, struct stat *stat, char **aux)
	{
		std::unique_lock<std::mutex> lck(map_lock);
		Map *m = reinterpret_cast<Map *>(map);
		auto search = m->find(std::string(k));

		if (search != m->end())
		{
			*v = search->second.fd;
			*stat = search->second.stat;
			// printf("map_search: %p\n", search->second.aux);
			*aux = search->second.aux;
			// printf("map_search aux: %p\n", *aux);
			return 1;
		}
		else
		{
			return -1;
		}
	}

	/**
	 * @brief Rename the name of a regular file on the local map.
	 * @return On success, 1 is returned.
	 */
	int map_rename(void *map, const char *oldname, const char *newname)
	{
		std::unique_lock<std::mutex> lck(map_lock);
		Map *m = reinterpret_cast<Map *>(map);
		auto node = m->extract(oldname);
		node.key() = newname;
		m->insert(std::move(node));
		// TODO: add error handling.
		return 1;
	}

	/**
	 * @brief Rename the name of all the entries of a directory on the local map.
	 * @return On success, 1 is returned. -1 if there are no elements to rename.
	 */
	// int map_rename_dir_dir(void *old_map, void *new_map, const char *old_dir, const char *rdir_dest)
	int map_rename_dir_dir(void *old_map, const char *old_dir, const char *rdir_dest)
	{
		std::unique_lock<std::mutex> lck(map_lock);
		Map *m = reinterpret_cast<Map *>(old_map);
		// Map *new_m = reinterpret_cast<Map *>(new_map);

		// TODO: check for map size. If zero then return inmediatly.

		std::vector<string> vec;

		size_t len_old_dir = strlen(old_dir);
		size_t len_curr_key = 0;
		string key;
		int found = 0;
		for (auto it = m->cbegin(); it != m->cend(); ++it)
		{
			key = it->first;
			found = key.find(old_dir);
			if (found != std::string::npos)
			{
				len_curr_key = key.length();
				slog_debug("len_old_dir=%lu, len_curr_key=%lu, old_dir %s found in %s", len_old_dir, len_curr_key, old_dir, key.c_str());
				if (len_old_dir < len_curr_key)
				{
					if (key[len_old_dir] != '/')
					{
						slog_debug("Skipping %s", key.c_str());
						continue;
					}
				}
				// free(it->second.aux);
				vec.insert(vec.begin(), key);
			}
		}
		slog_debug("Renaming %lu/%lu elements", vec.size(), m->size());

		if (vec.size() == 0)
		{
			// NO elements to rename.
			slog_debug("No elements to rename.");
			return -1;
		}


		std::vector<string>::iterator i;
		for (i = vec.begin(); i < vec.end(); i++)
		{
			// m->erase(*i);
			string key = *i;
			key.erase(0, strlen(old_dir));

			string new_path = rdir_dest;
			new_path.append(key);

			auto node = m->extract(*i);
			node.key() = new_path;
			slog_debug("New path %s", new_path.c_str());
			m->insert(std::move(node));
		}

		return 1;
	}

	void map_free(void *map)
	{
		std::unique_lock<std::mutex> lck(map_lock);
		Map *m = reinterpret_cast<Map *>(map);
		// auto search = m->find(std::string(k));

		for (auto it = m->cbegin(); it != m->cend(); ++it)
		{
			free(it->second.aux);
		}

		m->clear();

		// if (search != m->end())
		// {
		// 	free(search->second.aux);
		// 	slog_debug("[map] erasing element with key %s", k);
		// }
		// else
		// {
		// 	slog_debug("[map] element with key %s was not find", k);
		// }
		// int num_elements_erased = m->erase(std::string(k));
		// slog_debug("[map] finish map_erase, num_elements_erased=%d", num_elements_erased);
		// return ret;
	}

} // extern "C"
