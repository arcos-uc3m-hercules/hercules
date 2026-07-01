#ifndef H_MAP
#define H_MAP

#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <mutex>
#include <stdint.h>
#include <sys/stat.h>
#include <vector>

static struct elements
{
	int fd;
	struct stat stats;
	char *aux;
	bool dirty; // true = stat metadata must be updated in the back end.
} elements;

typedef std::map<std::string, struct elements> Map;

#ifdef __cplusplus
extern "C"
{
#endif

	void *map_create();
	void map_destroy(void *map);
	void map_put(void *map, const char *k, int v, struct stat stat, char *aux);
	void map_erase(void *map, const char *k);
	int map_search(void *map, const char *k, struct elements *elem);
	int map_rename(void *map, const char *oldname, const char *newname);
	void map_rename_key(void *map, const char *old_key, const char *new_key);
	void map_update(void *map, const char *k, int v, struct stat *stat, char *aux);
	// int map_rename_dir_dir(void *old_map, void *new_map, const char *old_dir, const char *rdir_dest);
	int map_rename_dir_dir(void *old_map, const char *old_dir, const char *rdir_dest);
	void map_free(void *map);

#ifdef __cplusplus
}
#endif

#endif // H_MAP
