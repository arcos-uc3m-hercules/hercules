#ifndef H_MAP_PREFETCH
#define H_MAP_PREFETCH

#ifdef __cplusplus
extern "C" {
#endif

// #include <sys/stat.h>
//structura con int, stat y char * malloc
void* map_create_prefetch();
char * map_get_buffer_prefetch (void* map, char * path, int * first_block, int * last_block);
void map_update_prefetch(void* map, char * path,   int first_block,  int last_block);
void map_release_prefetch(void* map, const char * path);
// void map_init_prefetch(void* map, const char * path, char * buff);
void map_init_prefetch(void *map, const char *path, long int buf_size);
int map_rename_prefetch(void* map, const char * oldname, const char * newname);
int map_rename_dir_dir_prefetch(void* map, const char * old_dir, const char * rdir_dest);
int free_prefetch(void *map);

#ifdef __cplusplus
}
#endif

#endif // H_MAP_PREFETCH

