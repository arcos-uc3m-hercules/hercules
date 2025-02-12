#include "include/redis.h"
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
            printf("Error: %s\n", context->errstr);
            return NULL;
        }
        else
        {
            printf("Error: Cannot allocate context\n");
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
            fprintf(stderr, "Error: %s\n", context->errstr);
        } else {
            freeReplyObject(reply);
        }
    }
    redisFree(context);
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
        printf("Memory allocation error\n");
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

int parent_dir_exists(redisContext *context, const char *parent_dir) {
    // If the parent directory is the root, it exists
    if (strcmp(parent_dir, "/") == 0)
    {
        printf("Parent directory is root\n");
        return 1;
    }
    char *grandparent_dir = get_parent_dir(parent_dir);
    char *parent_dir_name = get_path_last_part(parent_dir);
    printf("Grandparent dir: %s\n", grandparent_dir);
    redisReply *reply = (redisReply *)redisCommand(context, "SISMEMBER %s %s", grandparent_dir, parent_dir_name);
    free(grandparent_dir);
    if (reply == NULL)
    {
        printf("Error: %s\n", context->errstr);
        return -1;
    }
    int exists = reply->integer;
    freeReplyObject(reply);
    printf("Parent directory exists: %d\n", exists);
    return exists;
}

// Method inserting a new path.
int32_t redis_insert_data(redisContext *context, const char *desired_data)
{
    printf("Inserting data: %s\n", desired_data);
    char *parent_dir = get_parent_dir(desired_data);
    char *file = get_path_last_part(desired_data); // This can be either a file or a dir
    printf("Parent dir: %s\n", parent_dir);

    // Check if the parent directory exists
    if (!parent_dir_exists(context, parent_dir))
    {
        printf("Error: Parent directory does not exist\n");
        printf("\n");

        free(parent_dir);
        free(file);
        return -1;
    }
    printf("\n");

    // Insert the file into the parent directory
    redisReply *reply = (redisReply *)redisCommand(context, "SADD %s %s", parent_dir, file);
    if (reply == NULL)
    {
        printf("Error: %s\n", context->errstr);
        return -1;
    }
    freeReplyObject(reply);
    free(parent_dir);
    free(file);
    return 0;
}

// Helper method for deleting all subdirectories of a directory
static void delete_subdirectories(redisContext *context, const char* parent_dir) {
    // Get all the subdirectories of the parent directory
    redisReply *reply = NULL;
    long cursor = 0;
    do {
        reply = (redisReply *)redisCommand(context, "SCAN %ld MATCH %s/*", cursor, parent_dir);
        if (reply == NULL) {
            printf("Error: %s\n", context->errstr);
            return;
        }
        // Update the cursor
        cursor = strtol(reply->element[0]->str, NULL, 10);
        // Iterate over the subdirectories and delete them
        for (size_t i = 0; i < reply->element[1]->elements; i++) {
            const char* key = reply->element[1]->element[i]->str;
            redisReply *del_reply = (redisReply *)redisCommand(context, "DEL %s", key);
            if (del_reply == NULL) {
                printf("Error: %s\n", context->errstr);
                freeReplyObject(reply);
                return;
            }
            freeReplyObject(del_reply);
        }
    } while (cursor != 0);
    freeReplyObject(reply);
}

// Method deleting a new path.
int32_t redis_delete_data(redisContext *context, const char *desired_data) {
    char *parent_dir = get_parent_dir(desired_data);
    char *file = get_path_last_part(desired_data); // This can be either a file or a dir

    // First, remove the file from the parent directory
    redisReply *reply = (redisReply *)redisCommand(context, "SREM %s %s", parent_dir, file);

    if (reply == NULL)
    {
        printf("Error: %s\n", context->errstr);
        return -1;
    }

    // If the file was not found in the parent directory, end execution
    if (reply->integer == 0) {
        freeReplyObject(reply);
        free(parent_dir);
        free(file);
        return 0;
    }


    
    // In case the file was a directory, delete it and all its subdirectories
    reply = (redisReply *)redisCommand(context, "DEL %s", desired_data);
    if (reply == NULL)
    {
        printf("Error: %s\n", context->errstr);
        return -1;
    }
    // If the deleted element was a dir, the return value will be 1, so delete all subdirs
    if (reply->integer == 1) {
        delete_subdirectories(context, desired_data);
    }

    freeReplyObject(reply);
    free(parent_dir);
    free(file);
    return 1;
}





// Function to get the directory contents from Redis
char *redis_getdir(redisContext *context, const char *desired_dir, int32_t *numdir_elems) {
    // Retrieve the contents of the directory
    redisReply *reply = (redisReply *)redisCommand(context, "SMEMBERS %s", desired_dir);
    if (reply == NULL || reply->type != REDIS_REPLY_ARRAY) {
        printf("Error: %s\n", context->errstr);
        return NULL;
    }

    // Number of elements contained by the directory
    uint32_t num_children = reply->elements;
    *numdir_elems = num_children + 1; // +1 for the actual directory + children
    printf("[redis_getdir] num_children=%d\n", num_children);

    // Buffer containing the whole set of elements within a certain directory
    char *dir_elements = (char *)malloc((num_children + 1) * 256);
    if (dir_elements == NULL) {
        printf("Memory allocation error\n");
        freeReplyObject(reply);
        return NULL;
    }
    // Serialize the directory children into the buffer
    char *aux_dir_elements = dir_elements;
    memcpy(aux_dir_elements, desired_dir, 256);
    *aux_dir_elements += 256;

    for (size_t i = 0; i < reply->elements; i++) {
        const char* sub_dir = reply->element[i]->str;
        printf("Subdir: %s\n", sub_dir);
        memcpy(aux_dir_elements, sub_dir, 256);
        *aux_dir_elements += 256;
    }


    freeReplyObject(reply);
    return dir_elements;
}



// Helper function to rename a single key
static int rename_key(redisContext *context, const char *old_key, const char *new_key) {
    redisReply *reply = (redisReply *)redisCommand(context, "RENAME %s %s", old_key, new_key);
    if (reply == NULL) {
        printf("Error: %s\n", context->errstr);
        return -1;
    }
    int result = reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0;
    freeReplyObject(reply);
    return result;
}

// Helper function to recursively rename directories
static int rename_subdirectories(redisContext *context, const char *old_dir, const char *new_dir) {
    long cursor = 0;
    do {
        redisReply *reply = (redisReply *)redisCommand(context, "SCAN %ld MATCH %s/*", cursor, old_dir);
        if (reply == NULL) {
            printf("Error: %s\n", context->errstr);
            return -1;
        }

        // Update the cursor
        cursor = strtol(reply->element[0]->str, NULL, 10);

        // Iterate over the keys and rename them
        for (size_t i = 0; i < reply->element[1]->elements; i++) {
            const char *old_key = reply->element[1]->element[i]->str;
            char new_key[256];

            // Construct the new key by replacing the old directory prefix with the new one
            snprintf(new_key, sizeof(new_key), "%s%s", new_dir, old_key + strlen(old_dir));

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

// Function to rename a directory and all its subdirectories in Redis
int32_t redis_rename_dir_dir(redisContext *context, const char *old_dir, const char *new_dir) {
    char new_parent_dir[256];
    char *filename = get_path_last_part(old_dir); // This can be either a file or a dir
    // Construct the new parent directory path
    snprintf(new_parent_dir, sizeof(new_parent_dir), "%s/%s", new_dir, filename); 
    printf("New parent dir: %s\n", new_parent_dir);

    // Insert the file/dir in the new parent directory key
    if (redis_insert_data(context, new_parent_dir) != 0) {
        return -1;
    }

    // Rename the parent directory if it exists
    int rename_result = rename_key(context, old_dir, new_parent_dir); 
    if (rename_result != 0) {
        return rename_result;
    }

    // Rename all its subdirectories
    if (rename_subdirectories(context, old_dir, new_parent_dir) != 0) {
        return -1;
    }

    return 0;
}

int main() {
    redisContext *context = redis_init("localhost", 6379);
    if (context == NULL) {
        return 1;
    }

    // redis_insert_data(context, "/home/user1/dir1/file1.txt");
    redis_insert_data(context, "/home");
    redis_insert_data(context, "/tmp");
    redis_insert_data(context, "/home/user1");
    redis_insert_data(context, "/home/user2");
    redis_insert_data(context, "/home/user2/file");
    redis_insert_data(context, "/home/user2/file/file2");
    redis_insert_data(context, "/home/user2/file/file3");
    redis_rename_dir_dir(context, "/home", "/tmp");
    //  redis_delete_data(context, "/tmp");
    // redis_insert_data(context, "/home/user1/dir1");
    // int32_t numdir_elems;
    // char *dir_elements = redis_getdir(context, "/home", &numdir_elems);

    // char *current_element = dir_elements;
    // for (int i = 0; i < numdir_elems; i++) {
    //     printf("Element %d: %s\n", i, current_element);
    //     current_element += 256;
    // }
    

    redis_close(context);
    return 0;
}