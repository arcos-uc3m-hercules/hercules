#include "redis.h"
#include <imss.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>



pthread_mutex_t hiredis_mut = PTHREAD_MUTEX_INITIALIZER;
char redis_host[256];
int redis_port;
redisContext *hiredis_context;


// Method initializing the Redis connection.
redisContext* redis_init(const char *hostname, int port)
{
    redisContext *context = redisConnect(hostname, port);
    if (context == NULL || context->err)
    {
        if (context)
        {
            slog_error("Error: %s\n", context->errstr);
            return context;
        }
        else
        {
            slog_error("Error: Cannot allocate context\n");
            return NULL;
        }
    }

    return context;
}

// Method closing the Redis connection and resetting the server.
void redis_close(redisContext *context)
{
    if (context != NULL && !context->err) {
        // Flush all data from all databases
        redisReply *reply = (redisReply *)redisCommand(context, "FLUSHALL");
        if (reply == NULL) {
            slog_error("Error: %s\n", context->errstr);
        } else {
            freeReplyObject(reply);
        }
    }
    redisFree(context);
}

// Method inserting a new path.
int32_t redis_insert_data(redisContext *context, const char *desired_data)
{
    char *parent_dir = get_parent_dir(desired_data);
    char *data_to_insert = get_path_last_part(desired_data); // This can be either a file or a dir

    if (!parent_dir || !data_to_insert){
        slog_error("Error inserting");
        free(parent_dir);
        free(data_to_insert);
        return -1;
    }

    // Check if the parent directory exists
    if (!parent_dir_exists(context, parent_dir))
    {
        // If it does not exist, create it
        int32_t insert_parent_result = redis_insert_data(context, parent_dir);
        if (insert_parent_result == -1)
        {
            slog_error("Error inserting directory\n");
            free(parent_dir);
            free(data_to_insert);
            return -1;
        }
    }

    // Insert the file into the parent directory
    redisReply *reply = (redisReply *)redisCommand(context, "SADD %s %s", parent_dir, data_to_insert);
    free(parent_dir);
    if (reply == NULL)
    {
        slog_error("Error: %s\n", context->errstr);
        free(data_to_insert);
        freeReplyObject(reply);
        return -1;
    }

    free(data_to_insert);
    freeReplyObject(reply);
    return 0;
}

// Helper method to get the parent directory of a path
char* get_parent_dir(const char* path) {
    size_t len = strlen(path);

    // Make a modifiable copy of the path
    char* temp = strdup(path);
    if (temp == NULL) {
        slog_error("Memory allocation error");
        return NULL;
    }

    // Remove trailing '/' unless it's part of "imss://"
    if (len > strlen("imss://") && temp[len - 1] == '/') {
        temp[len - 1] = '\0';
    }

    // Find the last occurrence of '/'
    const char* last_slash = strrchr(temp, '/');
    if (last_slash == NULL || last_slash == temp + strlen("imss:/")) {
        free(temp);
        return strdup("imss://");
    }

    // Calculate the length of the parent directory path
    size_t parent_len = last_slash - temp + 1;

    // Allocate memory for the parent directory path
    char* parent_dir = (char*)malloc(parent_len + 1);
    if (parent_dir == NULL) {
        slog_error("Memory allocation error");
        free(temp);
        return NULL;
    }

    // Copy the parent directory path
    strncpy(parent_dir, temp, parent_len);
    parent_dir[parent_len] = '\0';

    free(temp);
    return parent_dir;
}

// Helper method to get the last part of the path (file or directory name)
char* get_path_last_part(const char* path) {
    size_t len = strlen(path);

    // Make a modifiable copy of the path
    char* temp = strdup(path);
    if (temp == NULL) {
        slog_error("Memory allocation error");
        return NULL;
    }

    // Remove trailing '/' unless it's part of "imss://"
    if (len > strlen("imss://") && temp[len - 1] == '/') {
        temp[len - 1] = '\0';
    }

    // Find the last occurrence of '/'
    const char* last_slash = strrchr(temp, '/');
    char* result;

    if (last_slash == NULL) {
        // No slash found, return whole path
        result = strdup(temp);
    } else {
        // Return the substring after the last '/'
        result = strdup(last_slash + 1);
    }

    // Restore the trailing '/' if it was removed
    if (len > strlen("imss://") && path[len - 1] == '/') {
        size_t result_len = strlen(result);
        char* result_with_slash = (char*)malloc(result_len + 2); // +1 for '/' and +1 for '\0'
        if (result_with_slash == NULL) {
            slog_error("Memory allocation error");
            free(temp);
            free(result);
            return NULL;
        }
        snprintf(result_with_slash, result_len + 2, "%s/", result);
        free(result);
        result = result_with_slash;
    }

    free(temp);
    return result;
}

int parent_dir_exists(redisContext *context, const char *parent_dir) {
    // If the parent directory is the root, it exists
    if (strcmp(parent_dir, "imss://") == 0)
    {
        return 1;
    }
    char *grandparent_dir = get_parent_dir(parent_dir);
    char *parent_dir_name = get_path_last_part(parent_dir);
    redisReply *reply = (redisReply *)redisCommand(context, "SISMEMBER %s %s", grandparent_dir, parent_dir_name);
    free(grandparent_dir);
    if (reply == NULL)
    {
        slog_error("Error: %s\n", context->errstr);
        return -1;
    }
    int exists = reply->integer;
    freeReplyObject(reply);
    return exists;
}

// Method deleting a new path.
int32_t redis_delete_data(redisContext *context, const char *desired_data) {
    char *parent_dir = get_parent_dir(desired_data);
    char *data_to_insert = get_path_last_part(desired_data); // This can be either a file or a dir

    if (!parent_dir || !data_to_insert) {
        slog_error("Error deleting");
        free(parent_dir);
        free(data_to_insert);
        return -1;
    }

    // First, remove the file from the parent directory
    redisReply *reply = (redisReply *)redisCommand(context, "SREM %s %s", parent_dir, data_to_insert);
    free(parent_dir);
    free(data_to_insert);
    if (reply == NULL)
    {
        slog_error("Error: %s\n", context->errstr);
        return -1;
    }

    // If the file was not found in the parent directory, end execution
    if (reply->integer == 0) {
        freeReplyObject(reply);
        return 0;
    }

    // In case the file was a directory, delete it and all its subdirectories
    reply = (redisReply *)redisCommand(context, "DEL %s", desired_data);
    if (reply == NULL)
    {
        slog_error("Error: %s\n", context->errstr);
        return -1;
    }
    // If the deleted element was a dir, the return value will be 1, so delete all subdirs
    if (reply->integer == 1) {
        delete_subdirectories(context, desired_data);
    }

    freeReplyObject(reply);
    return 1;
}

// Helper method for deleting all subdirectories of a directory
void delete_subdirectories(redisContext *context, const char* parent_dir) {
    // Get all the subdirectories of the parent directory
    redisReply *reply = NULL;
    long cursor = 0;
    do {
        reply = (redisReply *)redisCommand(context, "SCAN %ld MATCH %s*", cursor, parent_dir);
        if (reply == NULL) {
            slog_error("Error: %s\n", context->errstr);
            return;
        }
        // Update the cursor
        cursor = strtol(reply->element[0]->str, NULL, 10);
        // Iterate over the subdirectories and delete them
        for (size_t i = 0; i < reply->element[1]->elements; i++) {
            const char* key = reply->element[1]->element[i]->str;
            redisReply *del_reply = (redisReply *)redisCommand(context, "DEL %s", key);
            if (del_reply == NULL) {
                slog_error("Error: %s\n", context->errstr);
               freeReplyObject(reply);
                return;
            }
            freeReplyObject(del_reply);
        }
    } while (cursor != 0);
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

    // Buffer containing the whole set of elements within a certain directory
    char *dir_elements = (char *)malloc((num_children + 1) * URI_);
    char *aux_dir_elements = dir_elements;

    memcpy(aux_dir_elements, desired_dir, URI_);
    aux_dir_elements += URI_;

    char sub_dir[URI_];
    
    for (int32_t i = 0; i < num_children; i++) {
        // Copy the whole path into the buffer
        sprintf(sub_dir, "%s%s", desired_dir, reply->element[i]->str);
        memcpy(aux_dir_elements, sub_dir, URI_);
        aux_dir_elements += URI_;
    } 

    freeReplyObject(reply);
    return dir_elements;
}


int32_t redis_rename(redisContext *context, const char *old_path, const char *new_path) {
    int32_t delete_result = redis_delete_data(context, old_path);
    if (delete_result == -1) {
        slog_error("Error deleting old path\n");
        return -1;
    }
    int32_t insert_result = redis_insert_data(context, new_path);
    if (insert_result == -1) {
        slog_error("Error inserting new path\n");
        return -1;
    }
    return 0;
}

// Function to rename a directory and all its subdirectories in Redis
int32_t redis_rename_dir_dir(redisContext *context, const char *old_dir, const char *new_dir) {
    char *filename = get_path_last_part(old_dir); // This can be either a file or a dir

    int exists = dir_exists(context, old_dir);
    if (exists <= 0) {
        return exists;
    }

    // Rename the parent directory first
    if (rename_key(context, old_dir, new_dir) < 0) {
        return -1;
    }

    // Rename all its subdirectories
    if (rename_subdirectories(context, old_dir, new_dir) != 0) {
        return -1;
    }

    // Delete the old file/dir from the parent
    if (redis_delete_data(context, old_dir) < 0) {
        return -1;
    }

    // Insert the file/dir in the new parent directory key
    if (redis_insert_data(context, new_dir) != 0) {
        return -1;
    }

    return 0;
}

int dir_exists(redisContext *context, const char *path) {
    char *dir_name = get_path_last_part(path);
    char *parent_dir = get_parent_dir(path);

    if (!parent_dir) {
        slog_error("Error checking if dir exists");
        return -1;
    }

    redisReply *reply;
    // If parent is root, check for the key of the whole path (root is never inserted as a key)
    if (strcmp(parent_dir, "imss://") == 0) {
        reply = (redisReply *)redisCommand(context, "EXISTS %s", path);
    } else {
        reply = (redisReply *)redisCommand(context, "SISMEMBER %s %s", parent_dir, dir_name);
    }

    if (!reply) {
        slog_error("Redis error checking if dir exists");
        return -1;
    }

    int result = reply->integer;
    freeReplyObject(reply);
    return result;
}

// Helper function to rename a single key
int rename_key(redisContext *context, const char *old_key, const char *new_key) {
    redisReply *reply = (redisReply *)redisCommand(context, "RENAME %s %s", old_key, new_key);
    if (reply == NULL) {
        slog_error("Error: %s\n", context->errstr);
        return -1;
    }
    freeReplyObject(reply);
    return 0;
}

// Helper function to recursively rename directories
int rename_subdirectories(redisContext *context, const char *old_dir, const char *new_dir) {
    long cursor = 0;
    char *old_dir_last_part = get_path_last_part(old_dir);
    do {
        redisReply *reply = (redisReply *)redisCommand(context, "SCAN %ld MATCH %s/*", cursor, old_dir);
        if (reply == NULL) {
            slog_error("Error: %s\n", context->errstr);
            return -1;
        }

        // Update the cursor
        cursor = strtol(reply->element[0]->str, NULL, 10);

        // Iterate over the keys and rename them
        for (size_t i = 0; i < reply->element[1]->elements; i++) {
            const char *old_key = reply->element[1]->element[i]->str;
            char new_key[256];

            // Construct the new key by replacing the old directory prefix with the new one
            snprintf(new_key, sizeof(new_key), "%s/%s%s", new_dir, old_dir_last_part, old_key + strlen(old_dir));
            // Rename the key
            if (rename_key(context, old_key, new_key) != 0) {
                freeReplyObject(reply);
                return -1;
            }
        }

        freeReplyObject(reply);
    } while (cursor != 0);

    return 0;
}
