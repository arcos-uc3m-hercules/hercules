#ifndef ARG_PARSER_H
#define ARG_PARSER_H

#include <stdint.h>
#include <argp.h>
#include <limits.h>

/********** argp options **********/
/* common options */
#define PORT_OPT 'p'
#define BUFSIZE_OPT 'b'
#define ID_OPT 'r'

/* data server options */
#define IMSS_URI_OPT 'i'
#define STAT_HOST_OPT 'H'
#define STAT_PORT_OPT 'P'
#define NUM_SERVERS_OPT 'n'
#define DEPLOY_HOSTFILE_OPT 'd'
#define BLOCK_SIZE_OPT 'B'
#define STORAGE_SIZE_OPT 's'
#define THREAD_POOL_OPT 't'

/* metadata server options */
#define STAT_LOGFILE_OPT 'l'

/*** TYPE argument legal values ***/
#define TYPE_DATA_SERVER 'd'
#define TYPE_METADATA_SERVER 'm'


struct logging_opts
{
    uint32_t hercules_debug_file;
    uint32_t hercules_debug_screen;
    uint32_t hercules_debug_level;
};

struct arguments
{
    char type;                      /* type arg */
    char data_hostfile[PATH_MAX]; /* deploy hostfile arg to '-d' */
    char stat_logfile[PATH_MAX];    /* metadata logfile arg to '-l' */
    char imss_uri[32];        /* IMSS URI arg to '-i' */
    char hercules_path[PATH_MAX];   /* hercules path */
    char policy[PATH_MAX];
    char meta_hostfile[PATH_MAX];
    char debug_level[PATH_MAX];
    char mount_point[PATH_MAX];
    char hercules_checkpoint_path[PATH_MAX];
    char checkpoint_paths_list[PATH_MAX];
    char ignore_paths_list[PATH_MAX];
    char data_hostname[PATH_MAX];
    char *stat_host;                /* Metadata server hostname arg to '-H' */
    void *pool_memory;
    uint64_t data_port;                  /* port arg to '-p' */
    uint64_t storage_size;          /* total storage size in GB to -s */
    uint64_t thread_pool;           /* thread pool size '-t' */
    int64_t bufsize;                /* buffer size arg to '-b' */
    int64_t stat_port;              /* Metadata server port number arg to '-P' */
    int64_t num_data_servers;       /* number of data servers arg to '-n' */
    int64_t num_metadata_servers;   /* number of metaa servers */
    int32_t malleability;
    int32_t malleability_type;
    int32_t upper_bound_servers;
    int32_t lower_bound_servers;
    int32_t repl_factor;
    int32_t repl_type;
    size_t block_size;            /* block size in KB arg to -B */
    int id;                         /* server ID arg to -r */
    struct logging_opts logging;
};

int parse_args(int argc, char **argv, struct arguments *args);

#endif
