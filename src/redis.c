#include <redis.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "imss.h"


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

// Method deleting a new path.
int32_t redis_delete_data(redisContext *context, const char *desired_data) {
    char *parent_dir = get_parent_dir(desired_data);
    char *file = get_path_last_part(desired_data); // This can be either a file or a dir

    // First, remove the file from the parent directory
    redisReply *reply = (redisReply *)redisCommand(context, "SREM %s %s", parent_dir, file);

    if (reply == NULL)
    {
        slog_error("Error: %s\n", context->errstr);
        return -1;
    }
    // This will be the return value of the function
    int exit_code = reply->integer;

    // If the file was a directory, delete all its subdirectories
    delete_subdirectories(context, desired_data);

    freeReplyObject(reply);
    free(parent_dir);
    free(file);
    return exit_code;
}

// Helper method for deleting all subdirectories of a directory
static void delete_subdirectories(redisContext *context, const char* parent_dir) {
    // Get all the subdirectories of the parent directory
    redisReply *reply = (redisReply *)redisCommand(context, "SMEMBERS %s", parent_dir);
    if (reply == NULL) {
        slog_error("Error: %s\n", context->errstr);
        return;
    }

    // Iterate over the subdirectories and delete them recursively
    for (size_t i = 0; i < reply->elements; i++) {
        const char* sub_dir = reply->element[i]->str;
        // Recursively delete the subdirectory. If the subdir is actually a file or is empty, this will do nothing
        delete_subdirectories(context, sub_dir);
        redisReply *del_reply = (redisReply *)redisCommand(context, "DEL %s", sub_dir);
        if (del_reply == NULL) {
            slog_error("Error: %s\n", context->errstr);
        }
        freeReplyObject(del_reply);
    }

    freeReplyObject(reply);
}

// Function to get the directory contents from Redis
char *redis_getdir(redisContext *context, const char *desired_dir, int32_t *numdir_elems) {
    // Retrieve the contents of the directory
    redisReply *reply = (redisReply *)redisCommand(context, "SMEMBERS %s", desired_dir);
    if (reply == NULL || reply->type != REDIS_REPLY_ARRAY) {
        slog_error("Error: %s\n", context->errstr);
        return NULL;
    }

    // Number of elements contained by the directory
    uint32_t num_children = reply->elements;
    *numdir_elems = num_children + 1; // +1 for the actual directory + children
    slog_info("[redis_getdir] num_children=%d", num_children);

    // Buffer containing the whole set of elements within a certain directory
    char *dir_elements = (char *)malloc((num_children + 1) * URI_);
    if (dir_elements == NULL) {
        slog_error("Memory allocation error\n");
        freeReplyObject(reply);
        return NULL;
    }
    // Serialize the directory children into the buffer
    for (size_t i = 0; i < reply->elements; i++) {
        const char *child = reply->element[i]->str;
        size_t len = strlen(child);
        memcpy(dir_elements, child, len);
        dir_elements += len;
        *dir_elements = '\0'; // Null-terminate the string
        dir_elements++;
    }

    freeReplyObject(reply);
    return dir_elements;
}

// Helper function to serialize directory children into a buffer
static void serialize_dir_childrens(redisContext *context, const char *dir, char **buffer) {
    redisReply *reply = (redisReply *)redisCommand(context, "SMEMBERS %s", dir);
    if (reply == NULL || reply->type != REDIS_REPLY_ARRAY) {
        fprintf(stderr, "Error: %s\n", context->errstr);
        return;
    }

    for (size_t i = 0; i < reply->elements; i++) {
        const char *child = reply->element[i]->str;
        size_t len = strlen(child);
        memcpy(*buffer, child, len);
        *buffer += len;
        **buffer = '\0'; // Null-terminate the string
        (*buffer)++;
    }

    freeReplyObject(reply);
}