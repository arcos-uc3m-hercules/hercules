/**
 * @file hierarchical_records.hpp
 * @author Genaro Sánchez-Gallegos, Javier Garcia-Blas, Jesus Carretero
 * @brief 
 * @version 0.2
 * @date 2025-08-01
 * 
 * @copyright Copyright (c) 2025
 * 
 */
#include "hierarchical_records.hpp"
#include "imss.h"

// to manage logs.
#include "slog.h"

int SERVER_ID;
// for sincronization in the map.
std::mutex hierarchical_map_lock;
// for garbage collector.
extern pthread_mutex_t mutex_garbage;
extern pthread_cond_t global_run_garbage_collector_cond;
extern pthread_cond_t global_free_space_cond;
pthread_mutex_t mutex_quantity_occupied = PTHREAD_MUTEX_INITIALIZER;

HierarchicalRecords::HierarchicalRecords(const std::string &root)
{
	slog_debug("root=%s", root.c_str());
	HierarchicalMap *hmap = new HierarchicalMap;
	std::shared_ptr<map_records> root_map = std::make_shared<map_records>(max_storage_size);
	(*hmap)[root] = root_map;
	// return reinterpret_cast<void *>(hmap);
	this->hiermap = hmap;
}

HierarchicalRecords::~HierarchicalRecords()
{
	slog_debug("Llamando al destructor de HierarchicalRecords");
	if (this->hiermap)
	{
		delete this->hiermap;
		this->hiermap = nullptr;
	}
}

size_t HierarchicalRecords::HierarchicalMapGetSize()
{
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);
	// HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);
	return this->hiermap->size();
}

int HierarchicalRecords::InsertDirectory(const std::string &key)
{
	const char *k = key.c_str();
	std::string path_basename = key.substr(0, key.find("$"));
	slog_debug("%s is a directory.", path_basename.c_str());
	if (this->hiermap->find(path_basename) == this->hiermap->end())
	{
		// Map *new_directory_children_map = new Map();
		std::shared_ptr<map_records> new_directory_children_map = std::make_shared<map_records>(max_storage_size);
		(*this->hiermap)[path_basename] = new_directory_children_map;
		slog_debug("Created children map for new directory: %s", k);
	}
	else
	{
		slog_debug("Children map for directory %s already exists", k);
	}
	return 1;
}

int HierarchicalRecords::CheckIfDirectory(const std::string& key, void *address)
{
	const char *k = key.c_str();
	// TODO: check this for data or metadata, data have stats in block 0, but metadata has
	// a struct dataset only.
	slog_debug("Server type=%c", SERVER_TYPE);
	int is_directory = 0;
	int ret = 0;
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
		ret = InsertDirectory(key);
	}
	return ret;
}

int HierarchicalRecords::HierarchicalMapPut(std::string key, void *address, uint64_t length, int reused_buffer, GNode *gnode, int is_zero_block)
{
	// check if there are space to alloc this block.
	double hercules_usage_percentage = get_storage_usage_percentage();
	// fprintf(stderr, "Memory used: %.2f%%\n", hercules_usage_percentage);
	slog_debug("Memory used: %.2f%%, reused_buffer=%d", hercules_usage_percentage, reused_buffer);
	if (hercules_usage_percentage >= 80.0)
	{
		pthread_mutex_lock(&mutex_garbage);
		fprintf(stderr, "[Server %d] Hercules has reached the %.2f%% of the maximum data storage capacity. Calling garbage collector.\n", SERVER_ID, hercules_usage_percentage);
		slog_debug("[Server %d] Hercules has reached the %.2f%% of the maximum data storage capacity. Calling garbage collector.\n", SERVER_ID, hercules_usage_percentage);
		// unlock garbage collector.
		// pthread_mutex_lock(&mutex_garbage);
		slog_debug("garbage collector mutex adquire");
#ifdef DPRINTF
		fprintf(stderr, "Sending signal to gargabe collector.\n");
#endif
		// Unlock the garbage collector.
		pthread_cond_signal(&global_run_garbage_collector_cond);
		// Simulated full capacity reached.
		// return 2;
		pthread_mutex_unlock(&mutex_garbage);
	}

	if (!reused_buffer)
	{
		// Increase only when a malloc was perform.
		IncreaseMemoryOccupied(length);
	}

	std::unique_lock<std::mutex> lck(hierarchical_map_lock);
	// HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);
	const char *k = key.c_str();
	slog_debug("Putting %s on the hierarchical map.", k);
	char first_parent_dir[PATH_MAX] = {0};
	find_last_parent_dir(k, first_parent_dir);
	// fprintf(stderr, "Putting %s on the hierarchical map %s.\n", k, first_parent_dir);

	// // Retrieve the map for the parent directory's children from 'hierarchical_map'.
	auto it = this->hiermap->find(first_parent_dir);
	std::shared_ptr<map_records> parent_map = nullptr;
	if (it == this->hiermap->end())
	{ // parent map does not exists locally, we add it to the hierarchical map.
		slog_warn("Parent directory %s does not exist or has not been added as a directory element. Creating it.", first_parent_dir);
		parent_map = std::make_shared<map_records>(max_storage_size);
		(*this->hiermap)[first_parent_dir] = parent_map;
	}
	else
	{ // parent map exists locally, we get the pointer.
		parent_map = it->second;
	}

	int ret = parent_map->put(key, address, length, reused_buffer, gnode);
	slog_debug("Element %s inserted on the directory map %s", key.c_str(), first_parent_dir);

	// if this is the zero block, check if the dataset is a directory.
	if (is_zero_block)
	{
		CheckIfDirectory(key, address);
	}

	// return ret;
	return 0;
}

std::shared_ptr<map_records> HierarchicalRecords::HierarchicalMapGetDir(const char *k)
{
	// std::unique_lock<std::mutex> lck(hierarchical_map_lock_aux);
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);
	// HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

	return get_dir_unsafe(k);
}

char *HierarchicalRecords::HierarchicalMapListDir(const char *desired_dir, int32_t *numdir_elems)
{
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);
	// HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

	slog_debug("Looking up directory %s in HierarchicalMap", desired_dir);
	auto it = this->hiermap->find(desired_dir);

	if (it == this->hiermap->end())
	{
		slog_debug("Directory %s not found in map", desired_dir);
		*numdir_elems = -1;
		return NULL;
	}

	// Get pointer to the children map (map_records)
	std::shared_ptr<map_records> children_map = it->second;

	if (children_map == nullptr)
	{
		// Should not happen if logic is consistent, but safety first
		*numdir_elems = -1;
		return NULL;
	}

	// total children including dirty/garbage ones.
	uint32_t num_children = children_map->size();

	slog_debug("Found directory. Total entries (including garbage): %d", num_children);

	if (num_children <= 0)
	{
		*numdir_elems = 0;
		return NULL;
	}

	char *dir_elements_buff = (char *)calloc(num_children + 1, URI_);
	if (dir_elements_buff == NULL)
	{
		perror("HERCULES_ERR_GTREE_GETDIR_ALLOC_MEMORY");
		slog_fatal("HERCULES_ERR_GTREE_GETDIR_ALLOC_MEMORY");
		exit(-1);
	}

	char *aux_dir_elem = dir_elements_buff;
	int32_t valid_elements_count = 0;
	int32_t num_dirty_child = 0;

	// Iterate and serialize
	// We iterate over the map_records.
	for (auto const &[filename, record_value] : *children_map)
	{
		slog_debug("filename.c_str()=%s, desired_dir=%s", filename.c_str(), desired_dir);
		if (filename == desired_dir)
		{
			continue;
		}
		// Lock garbage mutex before checking
		// pthread_mutex_lock(&mutex_garbage);
		// int found = HierarchicalMapSearchInGarbageCollector(hierarchical_map, (char *)filename.c_str());

		// if (!found)
		// {
		// Not garbage: Add to buffer
		// Ensure we don't overflow URI_ size, though URI_ implies fixed block size
		memcpy(aux_dir_elem, filename.c_str(), filename.length()); // or URI_ if you want to zero-pad immediately
		aux_dir_elem += URI_;
		valid_elements_count++;
		// }
		// else
		// {
		// 	num_dirty_child++;
		// }

		// pthread_mutex_unlock(&mutex_garbage);
	}

	// Release the main map lock now that we are done reading
	// lck.unlock();

	slog_debug("Directory %s has %d total elements, %d valid, %d dirty",
			   desired_dir, num_children, valid_elements_count, num_dirty_child);

	*numdir_elems = valid_elements_count;

	if (*numdir_elems == 0)
	{
		free(dir_elements_buff);
		return NULL;
	}

	return dir_elements_buff;
}

int HierarchicalRecords::HierarchicalMapRenameKey(const char *old_dir, const char *new_dir)
{
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);
	// HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

	// Look up the parent directory's children map in 'HierarchicalMap'.
	slog_debug("Looking for %s on the dir map", old_dir);
	auto parent_map = this->hiermap->find(std::string(old_dir));

	if (parent_map != this->hiermap->end())
	{
		// Get the value associated with the parent map.
		std::shared_ptr<map_records> parent_children_map = parent_map->second;

		// Insert the new key with the old value.
		(*this->hiermap)[new_dir] = parent_children_map;

		// Erase the old entry.
		this->hiermap->erase(parent_map);

		slog_debug("Parent %s map changed to %s", old_dir, new_dir);

		// parent_map = hiermap->find(std::string(new_dir));
		// if (parent_map != hiermap->end())
		// {
		// 	slog_debug("New dir %s is on the hierarchical map", new_dir);
		// }

		return 0;
	}
	else
	{
		slog_debug("Parent map %s not found.", old_dir);
		return -1;
	}
}

std::shared_ptr<map_records> HierarchicalRecords::HierarchicalMapGetChild(const char *k)
{
	// std::unique_lock<std::mutex> lck(hierarchical_map_lock_aux);
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);
	// HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

	return get_child_unsafe(k);
}

int32_t HierarchicalRecords::HierarchicalMapGet(std::string k, void **add_, uint64_t *size_)
{
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);
	// Look up the parent directory's children map in 'hierarchical_map'.
	std::shared_ptr<map_records> parent_children_map = get_child_unsafe(k.c_str());
	if (parent_children_map != nullptr)
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

ssize_t HierarchicalRecords::HierarchicalMapGetPrefetch(
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
	std::shared_ptr<map_records> children_map = get_child_unsafe(initial_key.c_str());

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
int32_t HierarchicalRecords::HierarchicalMapRenameRegularFile(const std::string &oldname, const std::string &newname)
{
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);
	// HierarchicalMap *hiermap = reinterpret_cast<HierarchicalMap *>(hierarchical_map);

	// Look up the parent directory's children map in 'hierarchical_map'.
	// std::shared_ptr<map_records> parent_children_map = HierarchicalMapGetChild(hierarchical_map, oldname.c_str());
	std::shared_ptr<map_records> parent_children_map = get_child_unsafe(oldname.c_str());
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
int32_t HierarchicalRecords::BackEndHierarchicalMapRenameDirDir(std::string old_dir, std::string rdir_dest, GNode **gnode)
{
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);

	// Find the hash map of this dir.
	std::shared_ptr<map_records> map = TIMING(get_dir_unsafe(old_dir.c_str()), "BackEndHierarchicalMapRenameDirDir,get_dir_unsafe", std::shared_ptr<map_records>, 0);

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
		// std::shared_ptr<map_records> parent_map = HierarchicalMapGetDir(hierarchical_map, first_parent_dir);
		std::shared_ptr<map_records> parent_map = get_dir_unsafe(first_parent_dir);
		if (!parent_map)
			return -1;

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

// ********** Garbage collector methods. **********
/**
 * @brief Adds an entry to the corresponding garbage collector vector.
 * @return 0 on success, on error -1 is returned.
 */
int32_t HierarchicalRecords::HierarchicalMapPutInGarbageCollector(const std::string &key)
{
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);

	// Find the hash map of this dir.
	char first_parent_dir[PATH_MAX] = {0};
	find_last_parent_dir(key.c_str(), first_parent_dir);
	std::shared_ptr<map_records> map = get_dir_unsafe(first_parent_dir);
	if (map != NULL)
	{
		slog_debug("Putting %s on the garbage collector of %s", key.c_str(), first_parent_dir);
		// Add the key to the garbage collector map.
		return map->put_garbage_collector(key);
	}
	slog_debug("%s not found in the map", key.c_str());
	return -1;
}

int32_t HierarchicalRecords::HierarchicalMapDeleteEntry(const std::string &key)
{
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);
	// Find the hash map of this dir.
	char first_parent_dir[PATH_MAX] = {0};
	find_last_parent_dir(key.c_str(), first_parent_dir);
	std::shared_ptr<map_records> map = get_dir_unsafe(first_parent_dir);
	if (map != NULL)
	{
		// Pop the key from the garbage collector vector.
		slog_debug("Deleting key %s from the metadata map of %s", key.c_str(), first_parent_dir);
		return map->delete_metadata_stat_worker(key);
	}
	// key was not find in the hierarchical map.
	slog_debug("%s not found in the map", key.c_str());
	return -1;
}

/**
 * @brief Method deleting a record from the garbage collector vector.
 * @return 1 if the "key" was find in the garbage collector vector,
 * 0 if the "key" was NOT find in the garbage collector vector, or
 * -1 if the "key" was NOT find in the Hierarchical Map.
 * */
int32_t HierarchicalRecords::HierarchicalMapPopFromGarbageCollector(const std::string &key)
{
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);
	// Find the hash map of this dir.
	char first_parent_dir[PATH_MAX] = {0};
	find_last_parent_dir(key.c_str(), first_parent_dir);
	std::shared_ptr<map_records> map = get_dir_unsafe(first_parent_dir);
	if (map != NULL)
	{
		// Pop the key from the garbage collector vector.
		slog_debug("Pop %s from the garbage collector of %s", key.c_str(), first_parent_dir);
		return map->garbage_collector_pop(key);
	}
	// key was not find in the hierarchical map.
	slog_debug("%s not found in the map", key.c_str());
	return -1;
}

/**
 * @brief Method searching a record in the garbage collector vector.
 * @return 1 if the "key" was find in the garbage collectorvector,
 * 0 was NOT find in the garbage collectorvector, or
 * -1 if the "key" was NOT find in the Hierarchical Map.
 * */
int32_t HierarchicalRecords::HierarchicalMapSearchInGarbageCollector(const std::string &key)
{
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);
	// Find the hash map of this dir.
	char first_parent_dir[PATH_MAX] = {0};
	find_last_parent_dir(key.c_str(), first_parent_dir);
	// std::shared_ptr<map_records> map = TIMING(HierarchicalMapGetDir(hierarchical_map, first_parent_dir), "HierarchicalMapPutInGarbageCollector,HierarchicalMapGetDir", std::shared_ptr<map_records>, 0);
	std::shared_ptr<map_records> map = get_dir_unsafe(first_parent_dir);
	if (map != NULL)
	{
		// Search the key in the garbage collector vector.
		slog_debug("Searching %s on the garbage collector of %s", key.c_str(), first_parent_dir);
		return map->garbage_collector_search(key);
	}
	// key was not find in the hierarchical map.
	slog_debug("%s not found in the map", key.c_str());
	return -1;
}

int32_t HierarchicalRecords::HierarchicalMapCleanGarbageCollector()
{
	std::unique_lock<std::mutex> lck(hierarchical_map_lock);

	slog_debug("Running HierarchicalMapCleanGarbageCollector");
	fprintf(stderr, "Running HierarchicalMapCleanGarbageCollector\n");
	double old_hercules_usage_percentage = get_storage_usage_percentage();
	uint64_t total_freed_memory = 0;
	for (const auto &pair : *this->hiermap)
	{
		const std::string &key = pair.first;
		slog_debug("Running garbage collector for %s", key.c_str());
		fprintf(stderr, "Running garbage collector for %s\n", key.c_str());
		const std::shared_ptr<map_records> &map = pair.second;
		// std::cout << "Key: " << key << ", Value Data: " << record_ptr->data << std::endl;
		total_freed_memory += map->cleaning(SERVER_TYPE);
		slog_debug("---\n");
	}
	fprintf(stderr, "[HierarchicalMapCleanGarbageCollector] Freed memory: %lu bytes (%lu MB)\n", total_freed_memory, total_freed_memory / MB);
	DecreaseMemoryOccupied(total_freed_memory);
	double new_hercules_usage_percentage = get_storage_usage_percentage();
	fprintf(stderr, "Memory freed: %.2f%%\n", new_hercules_usage_percentage - old_hercules_usage_percentage);
	return 0;
}

// Non-blocking functions.
std::shared_ptr<map_records> HierarchicalRecords::get_child_unsafe(const char *k)
{
	char first_parent_dir[PATH_MAX] = {0};
	find_last_parent_dir(k, first_parent_dir);

	// Buscar en el mapa
	auto parent_map = this->hiermap->find(std::string(first_parent_dir));
	if (parent_map != this->hiermap->end())
	{
		slog_debug("Parent %s map found for %s", parent_map->first.c_str(), k);
		return parent_map->second;
	}
	else
	{
		slog_debug("Parent map %s of %s not found.", first_parent_dir, k);
		fprintf(stderr, "Parent map %s of %s not found.\n", first_parent_dir, k);
		return nullptr;
	}
}

std::shared_ptr<map_records> HierarchicalRecords::get_dir_unsafe(const char *k)
{
	slog_debug("Seeking for directory on the directory table %s", k);
	auto parent_map = this->hiermap->find(std::string(k));

	if (parent_map != this->hiermap->end())
	{
		slog_debug("%s map found, requested=%s", parent_map->first.c_str(), k);
		return parent_map->second;
	}
	else
	{
		slog_debug("Parent map %s not found.", k);
		return nullptr;
	}
}
// End of Non-blocking functions.

extern "C"
{
	char SERVER_TYPE;
	uint64_t max_storage_size = 0;
	int64_t quantity_occupied = 0;

	int64_t get_size()
	{
		return max_storage_size;
	}

	/**
	 * @brief Checks if there are enough memory for "required space".
	 * @param required_space Requested memory.
	 * @return 1 if there are enough memory, 0 on other case.
	 */
	int CheckForMemorySpace(int64_t required_space)
	{
		int ret = 1;
		pthread_mutex_lock(&mutex_quantity_occupied);
		if (quantity_occupied + required_space > max_storage_size)
		{
			ret = 0;
		}
		pthread_mutex_unlock(&mutex_quantity_occupied);
		return ret;
	}

	/**
	 * @brief Increase the counter of how much memory is in use.
	 * Also checks if there are enough memory to alloc the "required space".
	 * @param required_space Space to be used by the new block.
	 * @return 1 if there are enough memory, 0 on other case.
	 */
	int IncreaseMemoryOccupied(int64_t required_space)
	{
		int ret = 1;
		pthread_mutex_lock(&mutex_quantity_occupied);
		if (quantity_occupied + required_space > max_storage_size)
		{
			slog_warn("memory occupied=%ld/%ld, required_space=%lu", quantity_occupied, max_storage_size, required_space);
			ret = 0;
		}
		else
		{
			quantity_occupied = quantity_occupied + required_space;
			slog_debug("memory occupied=%ld/%ld, required_space=%lu", quantity_occupied, max_storage_size, required_space);
		}
		pthread_mutex_unlock(&mutex_quantity_occupied);
		return ret;
	}

	/**
	 * @brief Decrease the counter of how much memory is in use.
	 * @param freed_space Size of the memory freed.
	 * @return 1 if the counter was correctly decreased, 0 in other case,
	 * for example, if the counter is below of 0.
	 */
	int DecreaseMemoryOccupied(int64_t freed_space)
	{
		int ret = 1;
		pthread_mutex_lock(&mutex_quantity_occupied);
		if (quantity_occupied - freed_space < 0)
		{
			perror("HERCULES_ERR_DECREASE_MEMORY_OCCUPIED_INCONSISTENCY");
			slog_error("HERCULES_ERR_DECREASE_MEMORY_OCCUPIED_INCONSISTENCY: memory usage cannot be less than 0: %ld, memory occupied=%lu, freed_space=%lu", quantity_occupied - freed_space, quantity_occupied, freed_space);
			ret = 0;
		}
		else
		{
			quantity_occupied = quantity_occupied - freed_space;
			slog_debug("memory occupied=%lu GB, freed_space=%lu", quantity_occupied / GB, freed_space);
		}
		pthread_mutex_unlock(&mutex_quantity_occupied);
		return ret;
	}

	double get_storage_usage_percentage()
	{
		if (max_storage_size == 0)
		{ // to avoid division by zero.
			return 0.0;
		}

		return static_cast<double>(quantity_occupied) * 100.0 / max_storage_size;
	}

} // extern "C"
