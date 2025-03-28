#ifndef IMSS_REDIS
#define IMSS_REDIS

#include <hiredis/hiredis.h>
#include <pthread.h>


// Mehtod initializing the Redis connection.
redisContext* redis_init(const char *hostname, int port);

// Method closing the Redis connection.
void redis_close(redisContext *context);

// Method inserting a new path.
int32_t redis_insert_data(redisContext *context, const char *desired_data);

// Method deleting a new path.
int32_t redis_delete_data(redisContext *context, const char *desired_data);

// Method retrieving a buffer with all the files within a directory.
char *redis_getdir(redisContext *context, const char *desired_dir, int32_t *numdir_elems);

// Method renaming a new path.
int32_t redis_rename(redisContext *context, const char *old_key, const char *new_key);

// Method renaming dir to dir.
int32_t redis_rename_dir_dir(redisContext *context, const char *old_dir, const char *rdir_dest);


// Helper functions
char* get_parent_dir(const char* path);
const char* find_last_slash(const char* path);
char* get_path_last_part(const char* path);
int parent_dir_exists(redisContext *context, const char *parent_dir);
void delete_subdirectories(redisContext *context, const char* parent_dir);
int rename_key(redisContext *context, const char *old_key, const char *new_key);
int rename_subdirectories(redisContext *context, const char *old_dir, const char *new_dir);


#endif // IMSS_REDIS