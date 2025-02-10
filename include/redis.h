#ifndef IMSS_REDIS
#define IMSS_REDIS

#include <hiredis/hiredis.h>

// Mehtod initializing the Redis connection.
redisContext* redis_init(const char *hostname, int port);

// Method closing the Redis connection.
void redis_close(redisContext *context);

// Method inserting a new path.
int32_t redis_insert_data(redisContext *context, const char *desired_data);

// Method deleting a new path.
int32_t redis_delete_data(redisContext *context, const char *desired_data);

// Method retrieving a buffer with all the files within a directory.
int32_t redis_get_dir_content(redisContext *context, const char *key, char **buffer, int32_t *num_elems);

// Method renaming a new path.
int32_t redis_rename(redisContext *context, const char *old_key, const char *new_key);

// Method renaming dir to dir.
int32_t redis_rename_dir_dir(redisContext *context, const char *old_dir, const char *rdir_dest);

// Serialize directory children into a buffer
int32_t serialize_dir_childrens(redisContext *context, const char *key, char **buffer);

#endif // IMSS_REDIS