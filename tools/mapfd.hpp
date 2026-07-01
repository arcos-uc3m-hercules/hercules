#ifndef H_MAP_FD
#define H_MAP_FD

#include <string>

#ifdef __cplusplus
extern "C" {
#endif

void *map_fd_create();
void map_fd_destroy(void *map);
int map_fd_put(void *map, const char *pathname, const int fd, off_t offset);
int map_fd_dup(void *map, const int old_fd, const int new_fd);
void map_fd_update_value(void *map, const char *pathname, const int fd, off_t offset);
void map_fd_update_fd(void *map, const char *pathname, const int fd, const int new_fd, off_t offset);
int map_fd_erase(void *map, const int fd);
int map_fd_search(void *map, const char *pathname, const int fd, off_t *offset);
int map_fd_search_by_pathname(void *map, const char *pathname, int *fd, long *offset);
int map_fd_erase_by_pathname(void *map, const char *pathname);
int map_fd_search_by_val_close(void *map, int fd);
std::string map_fd_search_by_val(void *map, const int fd);
int map_fd_serialize(void *map, void **out_buf, size_t *out_size);
int map_fd_deserialize(void *map, const void *buf, size_t size);

#ifdef __cplusplus
}
#endif

#endif // H_MAP_FD
