#ifndef H_H_MAP
#define H_H_MAP

#include <map>
#include "map.hpp"

// Map used to store another map which conatins a list of all the children
// specified in the directory reference by the key "string".
typedef std::map<std::string, Map *> HierarchicalMap;

#ifdef __cplusplus
extern "C"
{
#endif

	void *HierarchicalMapCreate(std::string root);
	void HierarchicalMapDestroy(void *map);
	int HierarchicalMapPut(void *hierarchical_map, const char *k, int v, struct stat stat_info, char *aux);
	Map *HierarchicalMapGetDir(void *hierarchical_map, const char *k);
	Map *HierarchicalMapGetChild(void *hierarchical_map, const char *k);
	int HierarchicalMapSearch(void *hierarchical_map, const char *k, int *v, struct stat *stat_info, char **aux);
	void HierarchicalMapUpdate(void *hierarchical_map, const char *k, int v, struct stat stat_info);
	void HierarchicalMapErase(void *hierarchical_map, const char *k);
	int HierarchicalMapRename(void *hierarchical_map, const char *oldname, const char *newname);
	int HierarchicalMapRenameKey(void *hierarchical_map, const char *old_dir, const char *new_dir);
	int HierarchicalMapRenameDirDir(void *hierarchical_map, const char *old_dir, const char *rdir_dest);
	void HierarchicalMapFree(void *hierarchical_map);

#ifdef __cplusplus
}
#endif

#endif // H_MAP
