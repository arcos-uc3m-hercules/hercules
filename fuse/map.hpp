#ifndef H_MAP
#define H_MAP

#include <map>
#include <iostream>
#include <vector>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <stdint.h>
#include <mutex>
#include <sys/stat.h>

static struct elements
{
	int fd;
	struct stat stat;
	char *aux;
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
	int map_search(void *map, const char *k, int *v, struct stat *stat, char **aux);
	int map_rename(void *map, const char *oldname, const char *newname);
	void map_update(void *map, const char *k, int v, struct stat stat);
	int map_rename_dir_dir(void *map, const char *old_dir, const char *rdir_dest);
	void map_free(void *map);

#ifdef __cplusplus
}
#endif

#endif // H_MAP
