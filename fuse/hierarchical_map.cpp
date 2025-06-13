#include "map.hpp"
#include "hierarchical_map.hpp"
#include "imss.h"

// to manage logs.
#include "slog.h"

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
		return reinterpret_cast<void *>(hmap);
	}

	void HierarchicalMapDestroy(void *hierarchical_map)
	{
		if (hierarchical_map)
		{
			delete reinterpret_cast<HierarchicalMap *>(hierarchical_map);
		}
	}

	int HierarchicalMapPut(void *hierarchical_map, const char *k, int v, struct stat stat_info, char *aux)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		std::pair<std::map<std::string, struct elements>::iterator, bool> ret;
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

		char first_parent_dir[PATH_MAX] = {0};
		find_last_parent_dir(k, first_parent_dir);

		// Retrieve the map for the parent directory's children from 'hierarchical_map'.
		auto it = hiermap->find(first_parent_dir);
		if (it == hiermap->end())
		{
			slog_debug("Error: Parent directory %s does not exist or has not been added as a directory element.", k);
			return -1;
		}

		Map *parent_children_map = it->second;

		map_put(parent_children_map, k, v, stat_info, aux);

		// If the newly added element is a directory, a new std::map for its children
		// must also be created and stored in 'HierarchicalMap'.
		if (S_ISDIR(stat_info.st_mode))
		{
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
			return parent_map->second;
		}
		else
		{
			slog_debug("Parent map %s of %s not found.", first_parent_dir, k);
			return NULL;
		}
	}

	int HierarchicalMapSearch(void *hierarchical_map, const char *k, int *v, struct stat *stat_info, char **aux)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);
		// Look up the parent directory's children map in 'hierarchical_map'.
		Map *parent_children_map = HierarchicalMapGetChild(hierarchical_map, k);
		if (parent_children_map != NULL)
		{ // Parent Map found.
			// Look up the stat_info on the parent's children map.
			return map_search(parent_children_map, k, v, stat_info, aux);
		}
		else
		{
			slog_debug("%s not found in the map", k);
			return -1;
		}
	}

	void HierarchicalMapUpdate(void *hierarchical_map, const char *k, int v, struct stat stat_info)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);

		// Look up the parent directory's children map in 'hierarchical_map'.
		Map *parent_children_map = HierarchicalMapGetChild(hierarchical_map, k);
		if (parent_children_map != NULL)
		{
			map_update(parent_children_map, k, v, stat_info);
		}
		else
		{
			slog_debug("%s not found in the map", k);
		}
	}

	// // Removes the element with key "k" from the map "map".
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

		// Look up the parent directory's children map in 'hierarchical_map'.
		Map *parent_children_map = HierarchicalMapGetChild(hierarchical_map, old_dir);
		if (parent_children_map != NULL)
		{
			return map_rename_dir_dir(parent_children_map, old_dir, rdir_dest);
		}
		else
		{
			slog_debug("%s not found in the map", old_dir);
			return -1;
		}
	}

	void HierarchicalMapFree(void *hierarchical_map)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);

		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

		// Iterates all the hierarchical map to free the child maps.
		for (auto it = hiermap->cbegin(); it != hiermap->cend(); ++it)
		{
			map_free(it->second);
		}
	}

} // extern "C"
