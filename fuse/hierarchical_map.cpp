#include "hierarchical_map.hpp"
#include "imss.h"
#include "map.hpp"

// to manage logs.
#include "slog.h"
#include <sys/mman.h>

// extern uint64_t IMSS_BLKSIZE;
// #define KB 1024
// #define GB 1073741824

// using std::string;

// for sincronization in the map.
std::mutex hierarchical_map_lock;

extern "C"
{

	void *HierarchicalMapCreate(std::string root)
	{
		HierarchicalMap *hmap = new HierarchicalMap;
		Map *root_map = new Map;
		(*hmap)[root] = root_map;

		char shm_name_hiermap[256];
		snprintf(shm_name_hiermap, sizeof(shm_name_hiermap), "/hercules_state_hiermap_%d", getpid());
		slog_debug("shm_name_hiermap=%s", shm_name_hiermap);

		int shm_fd_hiermap = shm_open(shm_name_hiermap, O_RDONLY, 0600);
		if (shm_fd_hiermap >= 0)
		{
			struct stat st;
			slog_debug("running fstat for shm_name_hiermap");
			if (fstat(shm_fd_hiermap, &st) != -1)
			{
				slog_debug("st.st_size=%ld", st.st_size);
				if (st.st_size > 0)
				{
					slog_debug("Ok, the size of the shared memory block is %ld", st.st_size);
					slog_debug("Discovered inherited hierarchical map state. Deserializing...");

					lseek(shm_fd_hiermap, 0, SEEK_SET);

					HierarchicalMapDeserialize(hmap, shm_fd_hiermap);
				}
				else
				{
					slog_debug("shm_name_hiermap, Invalid size");
				}
			}
			else
			{
				slog_debug("Stat error of shm_name_hiermap, fd=%d", shm_fd_hiermap);
				exit(-1);
			}
			close(shm_fd_hiermap);

			shm_unlink(shm_name_hiermap);
		}
		else
		{
			slog_debug("No inherited state found. Running with a fresh hierarchical map.");
		}

		return reinterpret_cast<void *>(hmap);
	}

	void HierarchicalMapDestroy(void *hierarchical_map)
	{
		if (hierarchical_map)
		{
			delete reinterpret_cast<HierarchicalMap *>(hierarchical_map);
		}
	}

	size_t HierarchicalMapGetSize(void *hierarchical_map)
	{
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);
		return hiermap->size();
	}

	int HierarchicalMapPut(void *hierarchical_map, const char *k, int v, struct stat stat_info, char *aux)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);
		slog_debug("Putting %s on the hierarchical map.", k);
		char first_parent_dir[PATH_MAX] = {0};
		find_last_parent_dir(k, first_parent_dir);

		// Retrieve the map for the parent directory's children from 'hierarchical_map'.
		auto it = hiermap->find(first_parent_dir);
		Map *parent_map = NULL;
		if (it == hiermap->end())
		{ // parent map does not exists locally, we add it to the hierarchical map.
			slog_warn("Parent directory %s does not exist or has not been added as a directory element. Creating it.", first_parent_dir);
			parent_map = new Map();
			(*hiermap)[first_parent_dir] = parent_map;
		}
		else
		{ // parent map exists locally, we get the pointer.
			parent_map = it->second;
		}

		// check the size of the parent map.
		// if(parent_map->size() > 100) {
		// 	return -1;
		// }

		map_put(parent_map, k, v, stat_info, aux);
		slog_debug("Element %s inserted on the directory map %s", k, first_parent_dir);
		print_file_type(stat_info, k);

		// If the newly added element is a directory, a new std::map for its children
		// must also be created and stored in 'HierarchicalMap'.
		if (S_ISDIR(stat_info.st_mode))
		{
			slog_debug("%s is a directory.", k);
			if (hiermap->find(k) == hiermap->end())
			{
				// std::map<std::string, struct stat> *new_directory_children_map = new std::map<std::string, struct stat>();
				Map *new_directory_children_map = new Map();
				(*hiermap)[k] = new_directory_children_map;
				slog_debug("Created children map for new directory: %s", k);
			}
			else
			{
				slog_debug("Children map for directory %s already exists", k);
			}
		}
		return 0;
	}

	Map *HierarchicalMapGetDir(void *hierarchical_map, const char *k)
	{
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

		// Look up the parent directory's children map in 'HierarchicalMap'.
		slog_debug("Seeking for directory on the directory table %s", k);
		auto parent_map = hiermap->find(std::string(k));

		if (parent_map != hiermap->end())
		{
			// Parent Map found.
			// Map *parent_children_map = parent_map->second;
			slog_debug("%s map found, requested=%s", parent_map->first.c_str(), k);
			return parent_map->second;
		}
		else
		{
			slog_debug("Parent map %s not found.", k);
			return NULL;
		}
	}

	int HierarchicalMapRenameKey(void *hierarchical_map, const char *old_dir, const char *new_dir)
	{
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

		// Look up the parent directory's children map in 'HierarchicalMap'.
		slog_debug("Looking for the parent of %s", old_dir);
		// auto parent_map = hiermap->find(std::string(old_dir));

		char first_parent_dir[PATH_MAX] = {0};
		find_last_parent_dir(old_dir, first_parent_dir);
		auto parent_map = hiermap->find(std::string(first_parent_dir));

		if (parent_map != hiermap->end())
		{
			// Get the value associated with the parent map.
			Map *parent_children_map = parent_map->second;

			// Insert the new key with the old value.
			(*hiermap)[new_dir] = parent_children_map;

			// Erase the old entry.
			hiermap->erase(parent_map);

			slog_debug("Parent %s map changed to %s", old_dir, new_dir);

			// parent_map = hiermap->find(std::string(new_dir));
			// if (parent_map != hiermap->end())
			// {
			// 	slog_debug("New dir %s is on the hierarchical map", new_dir);
			// }

			return 1;
		}
		else
		{
			slog_debug("Parent map %s not found.", old_dir);
			return 0;
		}
	}

	Map *HierarchicalMapGetChild(void *hierarchical_map, const char *k)
	{
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

		char first_parent_dir[PATH_MAX] = {0};
		find_last_parent_dir(k, first_parent_dir);

		// Look up the parent directory's children map in 'HierarchicalMap'.
		auto parent_map = hiermap->find(std::string(first_parent_dir));

		if (parent_map != hiermap->end())
		{
			// Parent Map found.
			// Map *parent_children_map = parent_map->second;
			slog_debug("Parent %s map found for %s", parent_map->first.c_str(), k);
			return parent_map->second;
		}
		else
		{
			slog_debug("Parent map %s of %s not found.", first_parent_dir, k);
			return NULL;
		}
	}

	int HierarchicalMapSearch(void *hierarchical_map, const char *k, struct elements *elem)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);
		// Look up the parent directory's children map in 'hierarchical_map'.
		Map *parent_children_map = HierarchicalMapGetChild(hierarchical_map, k);
		if (parent_children_map != NULL)
		{ // Parent Map found.
			slog_debug("looking up %s in the parent map", k);
			// Look up the stat_info on the parent's children map.
			return map_search(parent_children_map, k, elem);
		}
		else
		{
			slog_debug("%s not found in the map", k);
			return -1;
		}
	}

	void HierarchicalMapUpdate(void *hierarchical_map, const char *k, int v, struct stat *stats, char *aux)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);

		// Look up the parent directory's children map in 'hierarchical_map'.
		Map *parent_children_map = HierarchicalMapGetChild(hierarchical_map, k);
		if (parent_children_map != NULL)
		{
			map_update(parent_children_map, k, v, stats, aux);
		}
		else
		{
			slog_debug("%s not found in the map", k);
		}
	}

	/**
	 * @brief Removes the element with key "k" from the local front-end hierarchical map.
	 * @return void
	 */
	void HierarchicalMapErase(void *hierarchical_map, const char *k)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);

		// Look up the parent directory's children map in 'hierarchical_map'.
		Map *parent_children_map = HierarchicalMapGetChild(hierarchical_map, k);
		if (parent_children_map != NULL)
		{
			map_erase(parent_children_map, k);
		}
		else
		{
			slog_debug("%s not found in the map", k);
		}
	}

	/**
	 * @brief Rename the name of a regular file on the local hierarchical_map.
	 * @return On success, 1 is returned.
	 */
	int HierarchicalMapRename(void *hierarchical_map, const char *oldname, const char *newname)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);

		// Look up the parent directory's children map in 'hierarchical_map'.
		Map *parent_children_map = HierarchicalMapGetChild(hierarchical_map, oldname);
		if (parent_children_map != NULL)
		{
			// map_erase(parent_children_map, k);
			return map_rename(parent_children_map, oldname, newname);
		}
		else
		{
			slog_debug("%s not found in the map", oldname);
			return -1;
		}
	}

	/**
	 * @brief Rename the name of a directory (and all its entries) on the local hierarchical_map.
	 * @return On success, 1 is returned. -1 if there are no elements to rename.
	 */
	int HierarchicalMapRenameDirDir(void *hierarchical_map, const char *old_dir, const char *rdir_dest)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);
		// Look up the map of the old directory.
		auto it_old = hiermap->find(old_dir);
		if (it_old == hiermap->end())
		{
			slog_debug("%s not found in the map", old_dir);
			return -1;
		}

		Map *old_map = it_old->second;
		slog_debug("Renaming entries of %s to %s", old_dir, rdir_dest);
		Map *new_map = new Map();

		for (auto it = old_map->cbegin(); it != old_map->cend(); ++it)
		{
			std::string key = it->first;
			// Find the first occurrence of oldSubstring
			size_t pos = key.find(old_dir);

			// If found, replace it
			if (pos != std::string::npos)
			{
				key.replace(pos, strlen(old_dir), rdir_dest);
			}
			new_map->insert({key, it->second});
		}

		// Removes the node from the main hmap.
		hiermap->erase(it_old);
		delete old_map;

		// Includes the new node from into the main hmap.
		(*hiermap)[std::string(rdir_dest)] = new_map;

		Map *parent_map = HierarchicalMapGetChild(hierarchical_map, old_dir);
		if (parent_map == NULL)
		{
			slog_warn("Parent map for %s not found while renaming", old_dir);
			return -1;
		}
		map_rename_key(parent_map, old_dir, rdir_dest);

		return 1;
	}

	void HierarchicalMapFree(void *hierarchical_map)
	{
		if (!hierarchical_map)
			return;

		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		slog_debug("Calling HierarchicalMapFree for %p", hierarchical_map);

		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

		// Iterates all the hierarchical map to free the child maps.
		for (auto it = hiermap->cbegin(); it != hiermap->cend(); ++it)
		{
			map_free(it->second);
		}
		slog_debug("Ending HierarchicalMapFree");
	}

	void HierarchicalMapSerialize(void *hierarchical_map, int shm_fd)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

		if (!hiermap)
		{
			slog_warn("Hierarchical map is null. Nothing to serialize.");
			return;
		}

		slog_debug("[Serialization] Starting HierarchicalMap dump...");

		// Write the total number of directories (outer map size)
		uint32_t dir_count = hiermap->size();
		write(shm_fd, &dir_count, sizeof(dir_count));

		for (auto const &[dir_key, inner_map] : *hiermap)
		{
			slog_debug("Serializing dir %s", dir_key.c_str());
			// Serialize directory key
			uint32_t dir_key_len = dir_key.length();
			write(shm_fd, &dir_key_len, sizeof(dir_key_len));
			write(shm_fd, dir_key.c_str(), dir_key_len);

			// Write the total number of elements in the inner map
			uint32_t item_count = inner_map->size();
			write(shm_fd, &item_count, sizeof(item_count));

			for (auto const &[item_key, elem] : *inner_map)
			{
				// Serialize item key
				slog_debug("Serializing child %s", item_key.c_str());
				uint32_t item_key_len = item_key.length();
				write(shm_fd, &item_key_len, sizeof(item_key_len));
				write(shm_fd, item_key.c_str(), item_key_len);

				// Serialize struct elements standard fields
				write(shm_fd, &elem.fd, sizeof(elem.fd));
				write(shm_fd, &elem.stats, sizeof(elem.stats));
				
				print_file_type(elem.stats, item_key.c_str());
				// Serialize dynamic aux string
				uint32_t aux_len = 0;
				// = elem.aux ? strlen(elem.aux) : 0;
				if (elem.aux != NULL) {
					aux_len = sizeof(struct stat);
				}

				slog_debug("Aux value has a len of %d", aux_len);
				write(shm_fd, &aux_len, sizeof(aux_len));
				if (aux_len > 0)
				{
					write(shm_fd, elem.aux, aux_len);
				}
			}
		}

		slog_debug("[Serialization] Successfully dumped %u directories.", dir_count);
	}

	void HierarchicalMapDeserialize(void *hierarchical_map, int shm_fd)
	{
		// std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		// HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

		// if (!hiermap)
		// {
		// 	slog_error("Hierarchical map pointer is null during deserialization.");
		// 	return;
		// }

		slog_debug("[Deserialization] Rebuilding HierarchicalMap from shared memory...");

		uint32_t dir_count = 0;
		if (read(shm_fd, &dir_count, sizeof(dir_count)) != sizeof(dir_count))
		{
			slog_error("Failed to read directory count.");
			return;
		}

		slog_debug("dir count = %u", dir_count);

		for (uint32_t i = 0; i < dir_count; i++)
		{
			// Deserialize directory key
			uint32_t dir_key_len = 0;
			if (read(shm_fd, &dir_key_len, sizeof(dir_key_len)) != sizeof(dir_key_len))
				break;

			std::string dir_key(dir_key_len, '\0');
			if (read(shm_fd, &dir_key[0], dir_key_len) != dir_key_len)
				break;

			slog_debug("Deserializing directory [%u/%u]: '%s'", i + 1, dir_count, dir_key.c_str());

			// Reconstruct the inner map and insert into hierarchy

			uint32_t item_count = 0;
			if (read(shm_fd, &item_count, sizeof(item_count)) != sizeof(item_count))
				break;

			slog_debug("Directory '%s' contains %u items", dir_key.c_str(), item_count);

			for (uint32_t j = 0; j < item_count; j++)
			{
				// Deserialize item key
				uint32_t item_key_len = 0;
				if (read(shm_fd, &item_key_len, sizeof(item_key_len)) != sizeof(item_key_len))
					break;

				std::string item_key(item_key_len, '\0');
				if (read(shm_fd, &item_key[0], item_key_len) != item_key_len)
					break;

				// Deserialize struct elements standard fields
				struct elements elem;
				if (read(shm_fd, &elem.fd, sizeof(elem.fd)) != sizeof(elem.fd))
					break;
				if (read(shm_fd, &elem.stats, sizeof(elem.stats)) != sizeof(elem.stats))
					break;

				// Deserialize dynamic aux string
				uint32_t aux_len = 0;
				if (read(shm_fd, &aux_len, sizeof(aux_len)) != sizeof(aux_len))
					break;

				if (aux_len > 0)
				{
					slog_debug("Getting aux value");
					elem.aux = (char *)malloc(aux_len + 1);// new char[aux_len + 1];
					if (read(shm_fd, elem.aux, aux_len) == aux_len)
					{
						elem.aux[aux_len] = '\0';
					}
					else
					{
						slog_error("Truncated read while parsing aux string for item '%s'.", item_key.c_str());
					}
				}
				else
				{
					slog_debug("Aux value is null");
					elem.aux = NULL;
				}

				slog_debug("Restored item [%u/%u] in '%s': key='%s', fd=%d, aux='%s'",
					   j + 1, item_count, dir_key.c_str(), item_key.c_str(), elem.fd, elem.aux ? elem.aux : "NULL");

				// Insert fully restored item into the inner map
				HierarchicalMapPut(hierarchical_map, item_key.c_str(), elem.fd, elem.stats, elem.aux);
			}
		}

		slog_debug("[Deserialization] Hierarchy fully restored.");
	}

} // extern "C"
