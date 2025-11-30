#ifndef H_H_MAP_RECORDS
#define H_H_MAP_RECORDS

#include <map>
#include "records.hpp"

// Map used to store another map which conatins a list of all the children
// specified in the directory reference by the key "string".
typedef std::map<std::string, std::shared_ptr<map_records>> HierarchicalMap;

#ifdef __cplusplus
extern "C"
{
#endif
	extern char SERVER_TYPE;
	extern uint64_t max_storage_size;
	extern int64_t quantity_occupied;
	void *HierarchicalMapCreate(std::string root);
	void HierarchicalMapDestroy(void *map);
	size_t HierarchicalMapGetSize(void *hierarchical_map);
	int HierarchicalMapPut(void *hierarchical_map, std::string key, void *address, uint64_t length, int reused_buffer, GNode *gnode, int is_zero_block);
	int32_t HierarchicalMapDeleteEntry(void *hierarchical_map, const std::string &key);
	std::shared_ptr<map_records> HierarchicalMapGetDir(void *hierarchical_map, const char *k);
	char *HierarchicalMapListDir(void *hierarchical_map, const char *desired_dir, int32_t *numdir_elems);
	int HierarchicalMapRenameKey(void *hierarchical_map, const char *old_dir, const char *new_dir);
	std::shared_ptr<map_records> HierarchicalMapGetChild(void *hierarchical_map, const char *k);
	int32_t HierarchicalMapGetChildrenSerialized(void *hierarchical_map, std::string k, char **buffer, const int uri_size);
	int32_t HierarchicalMapGet(void *hierarchical_map, std::string k, void **add_, uint64_t *size_);
	ssize_t HierarchicalMapGetPrefetch(void *hierarchical_map, const std::string &base_key, uint32_t start_block_id, int num_data_servers, char *prefetch_buffer, size_t prefetch_size);
	// void HierarchicalMapUpdate(void *hierarchical_map, const char *k, int v, struct stat stat_info);
	int32_t HierarchicalMapRenameRegularFile(void *hierarchical_map, const std::string &oldname, const std::string &newname);
	int32_t BackEndHierarchicalMapRenameDirDir(void *hierarchical_map, std::string old_dir, std::string rdir_dest, GNode **gnode);
	// void HierarchicalMapFree(void *hierarchical_map);
	// **  Garbage collector methods. **
	int32_t HierarchicalMapPutInGarbageCollector(void *hierarchical_map, const std::string &key);
	int32_t HierarchicalMapPopFromGarbageCollector(void *hierarchical_map, const std::string &key);
	int32_t HierarchicalMapSearchInGarbageCollector(void *hierarchical_map, const std::string &key);
	int32_t HierarchicalMapCleanGarbageCollector(void *hierarchical_map);

#ifdef __cplusplus
}
#endif

#endif // H_H_MAP_RECORDS
