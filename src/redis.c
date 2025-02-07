#include <redis.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>


// Method initializing the Redis connection.
redisContext* redis_init(const char *hostname, int port)
{
    redisContext *context = redisConnect(hostname, port);
    if (context == NULL || context->err)
    {
        if (context)
        {
            slog_error("Error: %s\n", context->errstr);
            return NULL;
        }
        else
        {
            slog_error("Error: Cannot allocate context\n");
            return NULL;
        }
    }
    return context;
}

// Method closing the Redis connection.
void redis_close(redisContext *context)
{
    redisFree(context);
}

// Method inserting a new path.
int32_t redis_insert_data(redisContext *context, const char *desired_data)
{
    char *parent_dir = get_parent_dir(desired_data);
    char *file = get_path_last_part(desired_data); // This can be either a file or a dir

    redisReply *reply = (redisReply *)redisCommand(context, "SADD %s %s", parent_dir, file);
    if (reply == NULL)
    {
        slog_error("Error: %s\n", context->errstr);
        return -1;
    }
    freeReplyObject(reply);
    free(parent_dir);
    free(file);
    return 0;
}

// Helper method to get the parent directory of a path
static char* get_parent_dir(const char* path) {
    // Find the last occurrence of '/'
    const char* last_slash = strrchr(path, '/');
    if (last_slash == NULL || last_slash == path) {
        // If no '/' is found or the path is the root '/', return "/"
        return strdup("/");
    }

    // Calculate the length of the parent directory path
    size_t parent_len = last_slash - path;

    // Allocate memory for the parent directory path
    char* parent_dir = (char*)malloc(parent_len + 1);
    if (parent_dir == NULL) {
        slog_error("Memory allocation error\n");
        return NULL;
    }

    // Copy the parent directory path
    strncpy(parent_dir, path, parent_len);
    parent_dir[parent_len] = '\0';

    return parent_dir;
} 

// Helper method to get the last part of the path (file or directory name)
static char* get_path_last_part(const char* path) {
    // Find the last occurrence of '/'
    const char* last_slash = strrchr(path, '/');
    if (last_slash == NULL) {
        // If no '/' is found, return the entire path
        return strdup(path);
    }

    // Return the substring that follows the last '/'
    return strdup(last_slash + 1);
}

int32_t redis_delete_data(redisContext *context, const char *desired_data) {
    char *parent_dir = get_parent_dir(desired_data);
    char *file = get_path_last_part(desired_data); // This can be either a file or a dir

    redisReply *reply = (redisReply *)redisCommand(context, "SREM %s %s", parent_dir, file);

    if (reply == NULL)
    {
        slog_error("Error: %s\n", context->errstr);
        return -1;
    }
    int exit_code = reply->integer;
    freeReplyObject(reply);
    free(parent_dir);
    free(file);
    return exit_code;
}