#ifndef H_H_MAP
#define H_H_MAP

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
	std::shared_ptr<map_records> HierarchicalMapGetDir(void *hierarchical_map, const char *k);
	int HierarchicalMapRenameKey(void *hierarchical_map, const char *old_dir, const char *new_dir);
	std::shared_ptr<map_records> HierarchicalMapGetChild(void *hierarchical_map, const char *k);
	int32_t HierarchicalMapGet(void *hierarchical_map, std::string k, void **add_, uint64_t *size_);
	// void HierarchicalMapUpdate(void *hierarchical_map, const char *k, int v, struct stat stat_info);
	// void HierarchicalMapErase(void *hierarchical_map, const char *k);
	int32_t HierarchicalMapRenameRegularFile(void *hierarchical_map, const std::string &oldname, const std::string &newname);
	int32_t HierarchicalMapRenameDirDir(void *hierarchical_map, std::string old_dir, std::string rdir_dest, GNode **gnode);
	// void HierarchicalMapFree(void *hierarchical_map);
	// **  Garbage collector methods. **
	int32_t HierarchicalMapPutInGarbageCollector(void *hierarchical_map, const std::string &key);
	int32_t HierarchicalMapPopFromGarbageCollector(void *hierarchical_map, const std::string &key);
	int32_t HierarchicalMapSearchInGarbageCollector(void *hierarchical_map, const std::string &key);

#ifdef __cplusplus
}
#endif

#endif // H_MAP
