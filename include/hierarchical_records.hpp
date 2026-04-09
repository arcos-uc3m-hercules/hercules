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
#ifndef H_H_MAP_RECORDS
#define H_H_MAP_RECORDS

#include <map>
#include "records.hpp"

// Map used to store another map which conatins a list of all the children
// specified in the directory reference by the key "string".
typedef std::map<std::string, std::shared_ptr<map_records>> HierarchicalMap;

class HierarchicalRecords
{
public:
	HierarchicalRecords(const std::string &root);
	~HierarchicalRecords();
	void HierarchicalMapDestroy(void *map);
	std::vector<std::string> HierarchicalMapGetAllDatasetKeys();
	size_t HierarchicalMapGetSize();
	int HierarchicalMapPut(std::string key, void *address, uint64_t length, int reused_buffer, GNode *gnode, int is_zero_block);
	int32_t HierarchicalMapDeleteEntry(const std::string &key);
	std::shared_ptr<map_records> HierarchicalMapGetDir(const char *k);
	char *HierarchicalMapListDir(const char *desired_dir, int32_t *numdir_elems);
	int HierarchicalMapRenameKey(const char *old_dir, const char *new_dir);
	std::shared_ptr<map_records> HierarchicalMapGetChild(const char *k);
	int32_t HierarchicalMapGetChildrenSerialized(std::string k, char **buffer, const int uri_size);
	int32_t HierarchicalMapGet(std::string k, void **add_, uint64_t *size_);
	ssize_t HierarchicalMapGetPrefetch(const std::string &base_key, uint32_t start_block_id, int num_data_servers, char *prefetch_buffer, size_t prefetch_size);
	// void HierarchicalMapUpdate( const char *k, int v, struct stat stat_info);
	int32_t HierarchicalMapRenameRegularFile(const std::string &oldname, const std::string &newname);
	int32_t BackEndHierarchicalMapRenameDirDir(std::string old_dir, std::string rdir_dest, GNode **gnode);
	// **  Garbage collector methods. **
	int32_t HierarchicalMapPutInGarbageCollector(const std::string &key);
	int32_t HierarchicalMapPopFromGarbageCollector(const std::string &key);
	int32_t HierarchicalMapSearchInGarbageCollector(const std::string &key);
	int32_t HierarchicalMapCleanGarbageCollector();

private:
	HierarchicalMap *hiermap;

	std::shared_ptr<map_records> get_child_unsafe(const char *k);
	std::shared_ptr<map_records> get_dir_unsafe(const char *k);
	int InsertDirectory(const std::string &key);
	int CheckIfDirectory(const std::string &key, void *address);
};

#ifdef __cplusplus
extern "C"
{
#endif
	extern char SERVER_TYPE;
	extern uint64_t max_storage_size;
	extern int64_t quantity_occupied;

	// Memory usage.
	double get_storage_usage_percentage();
	int IncreaseMemoryOccupied(int64_t required_space);
	int DecreaseMemoryOccupied(int64_t freed_space);
	int CheckForMemorySpace(int64_t required_space);
	int64_t get_size();

#ifdef __cplusplus
}
#endif

#endif // H_H_MAP_RECORDS
