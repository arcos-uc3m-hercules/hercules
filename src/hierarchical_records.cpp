// #include "records.hpp"
#include "hierarchical_records.hpp"
#include "imss.h"

// to manage logs.
#include "slog.h"

// using std::string;

// for sincronization in the map.
std::mutex hierarchical_map_lock;
std::mutex hierarchical_map_lock_aux;

extern "C"
{
	char SERVER_TYPE;
	uint64_t max_storage_size;
	int64_t quantity_occupied;

	void *HierarchicalMapCreate(std::string root)
	{
		slog_debug("root=%s", root.c_str());
		HierarchicalMap *hmap = new HierarchicalMap;
		std::shared_ptr<map_records> root_map = std::make_shared<map_records>(max_storage_size);
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

	size_t HierarchicalMapGetSize(void *hierarchical_map)
	{
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);
		return hiermap->size();
	}

	int HierarchicalMapPut(void *hierarchical_map, std::string key, void *address, uint64_t length, int reused_buffer, GNode *gnode, int is_zero_block)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		// std::pair<std::map<std::string, BufferValue>::iterator, bool> ret;
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);
		const char *k = key.c_str();
		slog_debug("Putting %s on the hierarchical map.", k);
		char first_parent_dir[PATH_MAX] = {0};
		find_last_parent_dir(k, first_parent_dir);

		// Retrieve the map for the parent directory's children from 'hierarchical_map'.
		auto it = hiermap->find(first_parent_dir);
		std::shared_ptr<map_records> parent_map = nullptr;
		if (it == hiermap->end())
		{ // parent map does not exists locally, we add it to the hierarchical map.
			slog_warn("Parent directory %s does not exist or has not been added as a directory element. Creating it.", first_parent_dir);
			parent_map = std::make_shared<map_records>(max_storage_size);
			(*hiermap)[first_parent_dir] = parent_map;
		}
		else
		{ // parent map exists locally, we get the pointer.
			parent_map = it->second;
		}

		parent_map->put(key, address, length, reused_buffer, gnode);
		slog_debug("Element %s inserted on the directory map %s", k, first_parent_dir);

		// if this is the zero block, check if the dataset is a directory.
		if (is_zero_block)
		{
			// TODO: check this for data or metadata, data have stats in block 0, but metadata has
			// a struct dataset only.
			slog_debug("Server type=%c", SERVER_TYPE);
			int is_directory = 0;
			switch (SERVER_TYPE)
			{
			case TYPE_DATA_SERVER:
			{
				struct stat *stat_info;
				stat_info = (struct stat *)address;
				if (S_ISDIR(stat_info->st_mode))
				{
					is_directory = 1;
				}
			}
			break;
			case TYPE_METADATA_SERVER:
			{
				dataset_info *dataset;
				dataset = (dataset_info *)address;
				if (dataset->type == TYPE_DIRECTORY)
				{
					is_directory = 1;
				}
			}
			break;
			default:
				fprintf(stderr, "HERCULES_ERR_HIERARCHICAL_MAP_PUT_INVALID_SERVER_TYPE: %c\n", SERVER_TYPE);
				slog_error("HERCULES_ERR_HIERARCHICAL_MAP_PUT_INVALID_SERVER_TYPE: %c\n", SERVER_TYPE);
				return -1;
			}
			slog_debug("is directory=%d", is_directory);
			// If the newly added element is a directory, a new std::map for its children
			// must also be created and stored in 'HierarchicalMap'.
			if (is_directory)
			{
				std::string path_basename = key.substr(0, key.find("$"));
				slog_debug("%s is a directory.", path_basename.c_str());
				if (hiermap->find(path_basename) == hiermap->end())
				{
					// Map *new_directory_children_map = new Map();
					std::shared_ptr<map_records> new_directory_children_map = std::make_shared<map_records>(max_storage_size);
					(*hiermap)[path_basename] = new_directory_children_map;
					slog_debug("Created children map for new directory: %s", k);
				}
				else
				{
					slog_debug("Children map for directory %s already exists", k);
				}
			}
		}

		return 0;
	}

	std::shared_ptr<map_records> HierarchicalMapGetDir(void *hierarchical_map, const char *k)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock_aux);
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
			return nullptr;
		}
	}

	int HierarchicalMapRenameKey(void *hierarchical_map, const char *old_dir, const char *new_dir)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

		// Look up the parent directory's children map in 'HierarchicalMap'.
		slog_debug("Looking for %s on the dir map", old_dir);
		auto parent_map = hiermap->find(std::string(old_dir));

		if (parent_map != hiermap->end())
		{
			// Get the value associated with the parent map.
			std::shared_ptr<map_records> parent_children_map = parent_map->second;

			// Insert the new key with the old value.
			(*hiermap)[new_dir] = parent_children_map;

			// Erase the old entry.
			hiermap->erase(parent_map);

			slog_debug("Parent %s map changed to %s", old_dir, new_dir);

			parent_map = hiermap->find(std::string(new_dir));
			if (parent_map != hiermap->end())
			{
				slog_debug("New dir %s is on the hierarchical map", new_dir);
			}

			return 0;
		}
		else
		{
			slog_debug("Parent map %s not found.", old_dir);
			return -1;
		}
	}

	std::shared_ptr<map_records> HierarchicalMapGetChild(void *hierarchical_map, const char *k)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock_aux);
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

	int32_t HierarchicalMapGet(void *hierarchical_map, std::string k, void **add_, uint64_t *size_)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		// HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);
		// Look up the parent directory's children map in 'hierarchical_map'.
		std::shared_ptr<map_records> parent_children_map = HierarchicalMapGetChild(hierarchical_map, k.c_str());
		if (parent_children_map != NULL)
		{ // Parent Map found.
			slog_debug("looking up %s in the parent map", k.c_str());
			// Look up the stat_info on the parent's children map.
			return parent_children_map->get(k, add_, size_);
		}
		else
		{
			slog_debug("%s not found in the map", k.c_str());
			return 0;
		}
	}

	ssize_t HierarchicalMapGetPrefetch(void *hierarchical_map,
									   const std::string &base_key,
									   uint32_t start_block_id,
									   int num_data_servers,
									   char *prefetch_buffer,
									   size_t prefetch_size)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);

		// Find the parent map just ONCE.
		// We construct a dummy full key to find the correct parent map.
		std::string initial_key = base_key + "$" + std::to_string(start_block_id);
		std::shared_ptr<map_records> children_map = HierarchicalMapGetChild(hierarchical_map, initial_key.c_str());

		if (children_map == nullptr)
		{
			slog_warn("Parent map for base key '%s' not found.", base_key.c_str());
			return 0;
		}

		// Define the format constants inside the loop.
		const uint32_t BLOCK_ID_SIZE = sizeof(uint32_t);
		const size_t BLOCK_DATA_SIZE = BLOCK_SIZE;
		const size_t RECORD_SIZE = BLOCK_ID_SIZE + BLOCK_DATA_SIZE;
		slog_debug("BLOCK_ID_SIZE=%lu, BLOCK_DATA_SIZE=%lu, RECORD_SIZE=%lu, prefetch_size=%lu", BLOCK_ID_SIZE, BLOCK_DATA_SIZE, RECORD_SIZE, prefetch_size);

		size_t buffer_offset = 0;
		uint32_t current_block_id = start_block_id;

		// The main prefetch loop is now INSIDE the locked function.
		while (buffer_offset + RECORD_SIZE <= prefetch_size)
		{
			std::string current_key = base_key + "$" + std::to_string(current_block_id);

			void *address = nullptr;
			uint64_t block_size_rtvd = 0;

			// Perform the inner lookup directly on the children_map. This is much faster.
			if (children_map->get(current_key, &address, &block_size_rtvd) == 0)
			{
				slog_debug("Prefetch loop ended: block '%s' not found.", current_key.c_str());
				break; // Block not found, end of data.
			}

			if (block_size_rtvd != BLOCK_DATA_SIZE)
			{
				slog_warn("Prefetch stop: block size mismatch for '%s'. Expected %lu, got %lu.",
						  current_key.c_str(), BLOCK_DATA_SIZE, block_size_rtvd);
				break;
			}

			// Write the block ID and data into the buffer.
			*(uint32_t *)(prefetch_buffer + buffer_offset) = current_block_id;
			memcpy(prefetch_buffer + buffer_offset + BLOCK_ID_SIZE, address, BLOCK_DATA_SIZE);

			// Update offsets and the next block ID.
			buffer_offset += RECORD_SIZE;
			current_block_id += num_data_servers;
		}

		// The mutex is automatically unlocked here when 'lck' goes out of scope.
		return buffer_offset;
	}

	// void HierarchicalMapUpdate(void *hierarchical_map, const char *k, int v, struct stat stat_info)
	// {
	// 	std::unique_lock<std::mutex> lck(hierarchical_map_lock);

	// 	// Look up the parent directory's children map in 'hierarchical_map'.
	// 	Map *parent_children_map = HierarchicalMapGetChild(hierarchical_map, k);
	// 	if (parent_children_map != NULL)
	// 	{
	// 		map_update(parent_children_map, k, v, stat_info);
	// 	}
	// 	else
	// 	{
	// 		slog_debug("%s not found in the map", k);
	// 	}
	// }

	/**
	 * @brief Rename the name of a regular file on the local hierarchical_map.
	 * @return On success, 1 is returned.
	 */
	int32_t HierarchicalMapRenameRegularFile(void *hierarchical_map, const std::string &oldname, const std::string &newname)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);

		// Look up the parent directory's children map in 'hierarchical_map'.
		std::shared_ptr<map_records> parent_children_map = HierarchicalMapGetChild(hierarchical_map, oldname.c_str());
		if (parent_children_map != NULL)
		{
			// map_erase(parent_children_map, k);
			// return map_rename(parent_children_map, oldname, newname);
			return parent_children_map->rename_data_srv_worker(oldname, newname);
		}
		else
		{
			slog_debug("%s not found in the map", oldname.c_str());
			return -1;
		}
	}

	/**
	 * @brief Rename a directory (and all its entries) on the local front-end hierarchical_map.
	 * @return On success, 1 is returned. -1 if there are no elements to rename.
	 */
	int32_t BackEndHierarchicalMapRenameDirDir(void *hierarchical_map, std::string old_dir, std::string rdir_dest, GNode **gnode)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		// HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

		// Find the hash map of this dir.
		std::shared_ptr<map_records> map = TIMING(HierarchicalMapGetDir(hierarchical_map, old_dir.c_str()), "BackEndHierarchicalMapRenameDirDir,HierarchicalMapGetDir", std::shared_ptr<map_records>, 0);

		// Look up the parent directory's children map in 'hierarchical_map'.
		// Map *parent_children_map = HierarchicalMapGetChild(hierarchical_map, old_dir);
		if (map != nullptr)
		{

			slog_debug("Renaming entries of %s to %s in %c", old_dir.c_str(), rdir_dest.c_str(), SERVER_TYPE);
			int32_t ret = -1;
			// Rename all entries on the old directory.
			char msg[PATH_MAX] = {0};
			char first_parent_dir[PATH_MAX] = {0};
			find_last_parent_dir(old_dir.c_str(), first_parent_dir);
			std::shared_ptr<map_records> parent_map = HierarchicalMapGetDir(hierarchical_map, first_parent_dir);
			std::string aux_old_dir;
			std::string aux_rdir_dest;
			switch (SERVER_TYPE)
			{
			case TYPE_DATA_SERVER:
			{
				// Data server function.
				aux_old_dir = old_dir.append("$0");
				aux_rdir_dest = rdir_dest.append("$0");
				// lck.unlock();
				sprintf(msg, "BackEndHierarchicalMapRenameDirDir,rename_metadata_dir_stat_worker %ld", map->get_buffer_size());
				ret = TIMING(map->rename_data_dir_srv_worker(old_dir, rdir_dest), "BackEndHierarchicalMapRenameDirDir,rename_data_dir_srv_worker", int32_t, 0);
			}
			break;
			case TYPE_METADATA_SERVER:
			{
				// Metadata server function.
				aux_old_dir = old_dir;
				aux_rdir_dest = rdir_dest;
				sprintf(msg, "BackEndHierarchicalMapRenameDirDir,rename_metadata_dir_stat_worker %ld", map->get_buffer_size());
				ret = TIMING(map->rename_metadata_dir_stat_worker(old_dir, rdir_dest, gnode), msg, int, 0);
				// (*gnode) = map->find(old_dir).second.gnode;
				// BufferValue *entry = map->find(first_parent_dir);
				BufferValue *entry = parent_map->find(old_dir);
				if (entry != nullptr)
					*gnode = entry->gnode;
				else
					slog_debug("entry is null");
			}
			break;
			default:
				perror("HERCULES_ERR_HIERARCHICAL_MAP_RENAME_DIR_DIR_INVALID_SERVER_TYPE");
				slog_error("HERCULES_ERR_HIERARCHICAL_MAP_RENAME_DIR_DIR_INVALID_SERVER_TYPE: %c", SERVER_TYPE);
				break;
			}

			if (ret != 0)
			{
				slog_error("HERCULES_ERR_HIERARCHICAL_MAP_RENAME_DIR_DIR_RENAMING_DATA_ENTRIES_DIR");
				return -1;
			}

			// Rename the old directory key on the parent map.
			slog_debug("Renaming entry %s to %s in %s", old_dir.c_str(), rdir_dest.c_str(), first_parent_dir);
			// map_rename_dir_dir(old_map, old_dir, rdir_dest);
			// parent_map->rename_data_dir_srv_worker(old_dir, rdir_dest);
			parent_map->update_key(aux_old_dir, aux_rdir_dest);

			return 0;
		}
		else
		{
			slog_debug("%s not found in the map", old_dir.c_str());
			return -1;
		}
	}

	// void HierarchicalMapFree(void *hierarchical_map)
	// {
	// 	std::unique_lock<std::mutex> lck(hierarchical_map_lock);

	// 	HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

	// 	// Iterates all the hierarchical map to free the child maps.
	// 	for (auto it = hiermap->cbegin(); it != hiermap->cend(); ++it)
	// 	{
	// 		map_free(it->second);
	// 	}
	// }

	// ********** Garbage collector methods. **********
	/**
	 * @brief Adds an entry to the corresponding garbage collector vector.
	 * @return 0 on success, on error -1 is returned.
	 */
	int32_t HierarchicalMapPutInGarbageCollector(void *hierarchical_map, const std::string &key)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		// Find the hash map of this dir.
		char first_parent_dir[PATH_MAX] = {0};
		find_last_parent_dir(key.c_str(), first_parent_dir);
		std::shared_ptr<map_records> map = TIMING(HierarchicalMapGetDir(hierarchical_map, first_parent_dir), "HierarchicalMapPutInGarbageCollector,HierarchicalMapGetDir", std::shared_ptr<map_records>, 0);
		if (map != NULL)
		{
			slog_debug("Putting %s on the garbage collector of %s", key.c_str(), first_parent_dir);
			// Add the key to the garbage collector map.
			return map->put_garbage_collector(key);
		}
		else
		{
			slog_debug("%s not found in the map", key.c_str());
			return -1;
		}
	}

	/**
	 * @brief Method deleting a record from the garbage collector vector.
	 * @return 1 if the "key" was find in the garbage collector vector,
	 * 0 if the "key" was NOT find in the garbage collector vector, or
	 * -1 if the "key" was NOT find in the Hierarchical Map.
	 * */
	int32_t HierarchicalMapPopFromGarbageCollector(void *hierarchical_map, const std::string &key)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		// Find the hash map of this dir.
		char first_parent_dir[PATH_MAX] = {0};
		find_last_parent_dir(key.c_str(), first_parent_dir);
		std::shared_ptr<map_records> map = TIMING(HierarchicalMapGetDir(hierarchical_map, first_parent_dir), "HierarchicalMapPutInGarbageCollector,HierarchicalMapGetDir", std::shared_ptr<map_records>, 0);
		if (map != NULL)
		{
			// Pop the key from the garbage collector vector.
			slog_debug("Pop %s fron the garbage collector of %s", key.c_str(), first_parent_dir);
			return map->garbage_collector_pop(key);
		}
		else
		{
			// key was not find in the hierarchical map.
			slog_debug("%s not found in the map", key.c_str());
			return -1;
		}
	}

	/**
	 * @brief Method searching a record in the garbage collector vector.
	 * @return 1 if the "key" was find in the garbage collectorvector,
	 * 0 was NOT find in the garbage collectorvector, or
	 * -1 if the "key" was NOT find in the Hierarchical Map.
	 * */
	int32_t HierarchicalMapSearchInGarbageCollector(void *hierarchical_map, const std::string &key)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		// Find the hash map of this dir.
		char first_parent_dir[PATH_MAX] = {0};
		find_last_parent_dir(key.c_str(), first_parent_dir);
		std::shared_ptr<map_records> map = TIMING(HierarchicalMapGetDir(hierarchical_map, first_parent_dir), "HierarchicalMapPutInGarbageCollector,HierarchicalMapGetDir", std::shared_ptr<map_records>, 0);
		if (map != NULL)
		{
			// Search the key in the garbage collector vector.
			slog_debug("Searching %s on the garbage collector of %s", key.c_str(), first_parent_dir);
			return map->garbage_collector_search(key);
		}
		else
		{
			// key was not find in the hierarchical map.
			slog_debug("%s not found in the map", key.c_str());
			return -1;
		}
	}

	int32_t HierarchicalMapCleanGarbageCollector(void *hierarchical_map)
	{
		std::unique_lock<std::mutex> lck(hierarchical_map_lock);
		HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

		slog_debug("Running HierarchicalMapCleanGarbageCollector");
		for (const auto &pair : *hiermap)
		{
			const std::string &key = pair.first;
			slog_debug("Running garbage collector for %s", key.c_str());
			const std::shared_ptr<map_records> &map = pair.second;
			// std::cout << "Key: " << key << ", Value Data: " << record_ptr->data << std::endl;
			map->cleaning(SERVER_TYPE);
			slog_debug("---\n");
		}
		return 0;
	}

} // extern "C"
