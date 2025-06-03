/*
FUSE: Filesystem in Userspace
Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

This program can be distributed under the terms of the GNU GPL.
See the file COPYING.

gcc -Wall imss.c `pkg-config fuse --cflags --libs` -o imss
 */

#define FUSE_USE_VERSION 26
#include "map.hpp"
#include "mapprefetch.hpp"
#include "hercules.hpp"
#include "imss_posix_api.h"
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <inttypes.h>
#include <stdlib.h>
#include <time.h>
#include <limits.h>
#include <math.h>
#include <sys/time.h>
#include <libgen.h>

#define KB 1024
#define GB 1073741824
#define HEADER sizeof(uint32_t)

/*
   -----------	IMSS Global variables, filled at the beggining or by default -----------
 */
int32_t error_print = 0; // Temporal solution maybe to change in the future.

extern int32_t REPL_FACTOR;
extern int32_t REPL_TYPE;

extern int32_t N_SERVERS;
extern int32_t N_BLKS;
extern char POLICY[MAX_POLICY_LEN];
extern uint64_t IMSS_BLKSIZE;
extern uint64_t IMSS_DATA_BSIZE;
extern void *map;
extern void *map_prefetch;

extern uint16_t PREFETCH;
extern uint16_t threshold_read_servers;
extern uint16_t BEST_PERFORMANCE_READ;
extern uint16_t MULTIPLE_READ;
extern uint16_t MULTIPLE_WRITE;

extern char *BUFFERPREFETCH;
extern char prefetch_path[256];
extern char *MOUNT_POINT;

extern int32_t prefetch_first_block;
extern int32_t prefetch_last_block;
extern int32_t prefetch_pos;

extern int16_t prefetch_ds;
extern int32_t prefetch_offset;

extern pthread_cond_t cond_prefetch;
extern pthread_mutex_t mutex_prefetch;

#define MAX_PATH 256
extern pthread_mutex_t lock;
pthread_mutex_t lock_file = PTHREAD_MUTEX_INITIALIZER;

extern int32_t IMSS_DEBUG;

// to simulate maliability.
int32_t ior_operation_number;
int32_t mall_th_1 = 30;
int32_t mall_th_2 = 60;
int32_t MALLEABILITY;
int32_t MALLEABILITY_TYPE;
int32_t UPPER_BOUND_SERVERS;
int32_t LOWER_BOUND_SERVERS;

#ifdef __cplusplus
extern "C"
{
#endif

	// const char *TESTX = "imss://lorem_text.txt$1";
	// const char *TESTX = "imss://wfc1.dat$1";
	// const char *TESTX = "p4x2.save/wfc1.dat";

	/*
	   (*) Mapping for REPL_FACTOR values:
	   NONE = 1;
	   DRM = 2;
	   TRM = 3;
	 */

	void print_file_type(struct stat s, const char *pathname)
	{
		slog_debug("File type (%s), mode=%x:", pathname, s.st_mode);
		switch (s.st_mode & S_IFMT)
		{
		case S_IFBLK:
			slog_debug("\tblock device");
			break;
		case S_IFCHR:
			slog_debug("\tcharacter device");
			break;
		case S_IFDIR:
			slog_debug("\tdirectory");
			break;
		case S_IFIFO:
			slog_debug("\tFIFO/pipe");
			break;
		case S_IFLNK:
			slog_debug("\tsymlink");
			break;
		case S_IFREG:
			slog_debug("\tregular file");
			break;
		case S_IFSOCK:
			slog_debug("\tsocket");
			break;
		default:
			slog_debug("unknown?");
			break;
		}
	}

	int get_number_of_data_servers(int i_blk, int num_of_blk)
	{
		int32_t num_storages = 0;
		switch (MALLEABILITY_TYPE)
		{
		case 1: // Expanding the Hercules file system.
		{
			if (i_blk < 0.3F * num_of_blk)
			{
				num_storages = LOWER_BOUND_SERVERS;
			}
			else if (i_blk < 0.6F * num_of_blk)
			{
				num_storages = (LOWER_BOUND_SERVERS + UPPER_BOUND_SERVERS) / 2;
			}
			else
			{
				num_storages = UPPER_BOUND_SERVERS;
			}
			break;
		}
		case 2: // Shrinking the Hercules file system.
		{
			if (i_blk < 0.3F * num_of_blk)
			{
				num_storages = UPPER_BOUND_SERVERS;
			}
			else if (i_blk < 0.6F * num_of_blk)
			{
				num_storages = (LOWER_BOUND_SERVERS + UPPER_BOUND_SERVERS) / 2;
			}
			else
			{
				num_storages = LOWER_BOUND_SERVERS;
			}
			break;
		}
		}
		return num_storages;
	}

	/*
	   -----------	Auxiliar Functions -----------
	 */
	/**
	 * @brief Seek for the fd and stats of a dataset stored on a local map.
	 * @param path Name of the file to be seek.
	 * @param fd Pointer to the file descriptor of path.
	 * @param s Pointer to the stat structure where the file attributes will be stored.
	 * @param aux Pointer to the block 0 data.
	 */
	void fd_lookup(char *path, int *fd, struct stat *s, char **aux)
	{
		// pthread_mutex_lock(&lock_file);
		*fd = -1;
		// Seek for the fd on the map.
		int found = map_search(map, path, fd, s, aux);
		// pthread_mutex_unlock(&lock_file);

		if (found == -1)
		{
			// if the file was not find with the current name,
			// try again adding an extra slash if they does not have it.
			slog_warn("file not found, %s", path);
			char aux_path[MAX_PATH] = {0};
			size_t len = strlen(path);
			if (len > 0 && path[len - 1] != '/')
			{
				strcpy(aux_path, path);
				strcat(aux_path, "/");
				found = map_search(map, aux_path, fd, s, aux);
			}
			else
			{
				strncpy(aux_path, path, strlen(path) - 1);
				found = map_search(map, aux_path, fd, s, aux);
			}

			if (found == -1)
			{
				*fd = -1;
				slog_warn("file not found, %s", aux_path);
			}
			else
			{
				// if the fd was find with the aux_path, we replace it.
				// strcat(path, "/");
				// TODO: change for strncpy.
				strcpy(path, aux_path);
			}
		}
		if (*fd != -1)
		{ // print data if the fd exists.
			slog_debug("path=%s, found=%d, fd=%d, stat.st_nlink=%lu", path, found, *fd, s->st_nlink);
		}
	}

	void get_iuri(const char *path, /*output*/ char *uri)
	{
		if (!strncmp(path, "imss:/", strlen("imss:/")))
		{
			strcat(uri, path);
		}
		else
		{
			// Copying protocol
			strcpy(uri, "imss:/");
			strcat(uri, path);
		}
	}

	/**
	 * @brief Checks if an Hercules instance exists.
	 * @param instance_name name of the Hercules instance.
	 * @return 1 if it exists, -1 in other case.
	 */
	int is_alive(char *instance_name)
	{
		return (imss_check(instance_name) == -1) ? 0 : 1;
	}

	/*
	   -----------	FUSE IMSS implementation -----------
	 */

	int imss_truncate(const char *path, off_t offset)
	{
		return 0;
	}

	int imss_access(const char *path, int permission)
	{
		return 0;
	}

	int imss_refresh(const char *path)
	{
		int32_t ret = 0;
		struct stat *stats;
		struct stat old_stats;
		uint32_t ds = 0;
		int fd = -1;
		void *aux = NULL;
		char *imss_path = (char *)path; // this pointer should not be free.
		// Lookup the current file on the local front-end map.
		fd_lookup((char *)imss_path, &fd, &old_stats, (char **)&aux);
		if (fd >= 0)
		{
			ds = fd;
		}
		else
		{
			slog_warn("%s", strerror(ENOENT));
			return -ENOENT;
		}

		// get block 0 from data server.
		ret = TIMING(get_ndata(imss_path, ds, 0, aux, 0, 0);, "get_ndata 0", int32_t, 0);
		if (ret < 0)
		{
			char err_msg[MAX_ERR_MSG_LEN];
			sprintf(err_msg, "HERCULES_ERR_REFRESH: %s", path);
			slog_error("[imss_refresh] %s", err_msg);
			return -1;
		}
		stats = (struct stat *)aux;

		// HERE STATS->ST_NLINK RETURNS 0, IT MUST BE 1.
		slog_debug("ret=%d, ds=%d, st_size=%ld, stats->st_nlink=%lu, old_stats.st_nlink=%lu", ret, ds, stats->st_size, stats->st_nlink, old_stats.st_nlink);
		map_update(map, imss_path, ds, *stats);

		return 0;
	}

	void free_entries(char **refs, int n_ent)
	{
		// slog_debug("Freeing memory of %d entries", n_ent);
		if (refs != NULL)
		{
			for (size_t i = 0; i < n_ent; i++)
			{
				if (refs[i] != NULL)
				{
					// fprintf(stdout, "%s\n", refs[i]);
					free(refs[i]);
				}
			}
			free(refs);
		}
	}

	/**
	 * @brief Method to retrieve the file attributes of a given path.
	 * @param path Path to the file.
	 * @param stbuf Pointer to the stat structure where the file attributes will be stored.
	 * @return 0 on success, negative errno value on error.
	 */
	int imss_getattr(const char *path, struct stat *stbuf)
	{
		// Needed variables for the call
		// char *buffer = NULL;
		char **refs = NULL;
		int n_ent = -1;
		const char *imss_path = path; // this pointer should no be free.
		struct timespec spec;
		memset(stbuf, 0, sizeof(struct stat));
		clock_gettime(CLOCK_REALTIME, &spec);

		stbuf->st_atim = spec;
		stbuf->st_mtim = spec;
		stbuf->st_ctim = spec;
		stbuf->st_uid = getuid();
		stbuf->st_gid = getgid();
		stbuf->st_blksize = IMSS_DATA_BSIZE; // block size in bytes.

		slog_debug("before get_type, imss_path=%s", imss_path);
		char type = TIMING(get_type(imss_path), "get type", char, 0);
		slog_debug("after get_type(%s):%c", imss_path, type);

		int32_t ds = -1;
		int fd = -1;
		struct stat stats;
		char *aux = NULL;
		switch (type)
		{
		case TYPE_HERCULES_INSTANCE:
		{
			slog_debug("n_ent=%d", n_ent);
			stbuf->st_size = 4;
			slog_debug("is a directy, setting st_nlink to 1");
			stbuf->st_nlink = 1;
			stbuf->st_mode = S_IFDIR | 0775;
			// free(refs);
			return 0;
			break;
		}
		case TYPE_DIRECTORY:
		{

			ConcatLastSlashC((char *)path);
			// int len = strlen(path);
			// if (path[len - 1] != '/')
			// {
			// 	strcat((char *)path, "/");
			// }
		}
		break;
		case TYPE_REGULAR_FILE: // Case file
		{
			break;
		}
		default:
			slog_error("Unkown type", type);
			return -ENOENT; // to check!
		}

		// Seek for the dataset on the local map. If it not found,
		// we open it.
		TIMING_NO_RETURN(fd_lookup((char *)imss_path, &fd, &stats, &aux), "fd_lookup", 0);
		if (fd < 0)
		{ // not found.
			slog_debug("Opening file %s", imss_path);
			ds = open_dataset((char *)imss_path, 0);
			slog_debug("[imss_getattr] ds=%d", ds);
			if (ds >= (int32_t)0) // TODO: add ds == -EEXIST in the condition and remove the else.
			{
				int ret = 0;
				// slog_debug("[imss_getattr] IMSS_BLKSIZE=%lu KBytes, IMSS_DATA_BSIZE=%lu Bytes", IMSS_BLKSIZE, IMSS_DATA_BSIZE);
				void *data = NULL;
				data = (void *)malloc(IMSS_DATA_BSIZE * sizeof(char));

				if (data == NULL)
				{
					perror("HERCULES_ERR_GETATTR_MEMORY_ALLOC_1");
					slog_error("HERCULES_ERR_GETATTR_MEMORY_ALLOC_1");
					return -ENOMEM;
				}

				//  data is filled in "get data".
				// get block 0.
				ret = get_ndata((char *)imss_path, ds, 0, data, 0, 0);
				if (ret < 0)
				{
					slog_error("Error getting data: %s", imss_path);
					return -ENOENT;
				}
				memcpy(&stats, data, sizeof(struct stat));
				// pthread_mutex_lock(&lock_file);
				slog_debug("file=%s, st_nlink=%lu", imss_path, stats.st_nlink);
				// Put the file descriptor (ds), stats info and data on the local map.
				map_put(map, imss_path, ds, stats, (char *)data);
			}
			else if (ds == -EEXIST)
			{ // file aready exists on the remote metadata server.
				int ret = 0;
				// slog_debug("[imss_getattr] IMSS_BLKSIZE=%lu KBytes, IMSS_DATA_BSIZE=%lu Bytes", IMSS_BLKSIZE, IMSS_DATA_BSIZE);
				void *data = NULL;
				data = (void *)malloc(IMSS_DATA_BSIZE * sizeof(char));
				if (data == NULL)
				{
					perror("HERCULES_ERR_GETATTR_MEMORY_ALLOC_2");
					slog_error("HERCULES_ERR_GETATTR_MEMORY_ALLOC_2");
					return -ENOMEM;
				}
				//  data is filled in "get data".
				ret = get_ndata((char *)imss_path, ds, 0, data, 0, 0);
				if (ret < 0)
				{
					slog_error("Error getting data: %s", imss_path);
					return -ENOENT;
				}
				memcpy(&stats, data, sizeof(struct stat));
				// pthread_mutex_lock(&lock_file);
				slog_debug("file=%s, st_nlink=%lu", imss_path, stats.st_nlink);
				// Put the file descriptor (ds), stats info and data on the local map.
				map_put(map, imss_path, ds, stats, (char *)data);
				// fprintf(stderr, "[imss_getattr] ds=%d, %s\n", ds, strerror(EEXIST));
				return 0;
			}
			else
			{ // file does not exist on the remote metadata server.
				// fprintf(stderr, "[IMSS-FUSE]	Cannot get dataset metadata.");
				return -ENOENT;
			}
		}
		memcpy(stbuf, &stats, sizeof(struct stat));

		switch (type)
		{
		case TYPE_DIRECTORY:
			if ((n_ent = get_dir(imss_path, &refs)) != -1)
			{
				stbuf->st_size = 4;
				slog_debug("is a directy, setting st_nlink to 1");
				stbuf->st_nlink = 1;
				stbuf->st_mode = S_IFDIR | 0775;
				// free all memory in refs.
				free_entries(refs, n_ent);
			}
		case TYPE_REGULAR_FILE:
			stbuf->st_blocks = ceil((double)stbuf->st_size / 512.0);
			break;
		default:
			slog_error("Unkown type", type);
			return -ENOENT; // to check!
		}

		// pthread_mutex_unlock(&lock_file);
		// free(aux);
		return 0;
	}

	/*
	   Read directory

	   The filesystem may choose between two modes of operation:

	   -->	1) The readdir implementation ignores the offset parameter, and passes zero to the filler function's offset.
	   The filler function will not return '1' (unless an error happens), so the whole directory is read in a single readdir operation.

	   2) The readdir implementation keeps track of the offsets of the directory entries. It uses the offset parameter
	   and always passes non-zero offset to the filler function. When the buffer is full (or an error happens) the filler function will return '1'.
	   */

	uint32_t imss_readdir(std::string imss_path, char ***buf, posix_fill_dir_t filler, off_t offset)
	{
		// Needed variables for the call
		// char *buffer = NULL;
		//  char **refs = NULL;
		uint32_t n_ent = 0;
		// uint32_t imss_path_len = imss_path.length();

		// TODO: "ls" when there is more than one "/".

		slog_debug("[IMSS] imss_path=%s", imss_path.c_str());
		// Call IMSS to get metadata
		// int ret = 0;
		n_ent = get_dir(imss_path, buf);
		slog_debug("[IMSS] imss_path=%s, n_ent=%d", imss_path.c_str(), n_ent);
		// buf = refs;
		return ++n_ent;

		// if (n_ent < 0)
		// {
		// 	// strcat(imss_path, "/");
		// 	// slog_debug("[IMSS] imss_path=%s", imss_path);
		// 	// fprintf(stderr,"try again imss_path=%s\n",imss_path);
		// 	// n_ent = get_dir((char *)imss_path, &buffer, &refs);
		// 	// if (n_ent < 0)
		// 	// {
		// 	// 	fprintf(stderr, "[IMSS-FUSE]	Error retrieving directories for URI=%s\n", path);
		// 	return -ENOENT;
		// 	// }
		// }
		// // slog_debug("[IMSS] Before flush data");
		// // flush_data();

		// // Fill buffer
		// // TODO: Check if subdirectory
		// slog_debug("[IMSS] imss_readdir %s has=%" PRIu32, imss_path.c_str(), n_ent);
		// // reserve memory for the string contaning all the entries.
		// *buf=(char *)calloc(1, n_ent*URI_*sizeof(char *)+1);
		// char *aux_pointer = *buf;
		// uint64_t aux_i = 0;
		// for (uint32_t i = 0; i < n_ent; ++i)
		// {

		// 	clock_t t;
		// 	double time_taken;
		// 	t = clock();
		// 	if(i%1000==0){
		// 		t = clock() - t;
		// 		time_taken = ((double)t) / (CLOCKS_PER_SEC);
		// 		// print a message every 1000 files.
		// 		printf("[%s][%.2f] Reading entry %" PRIu32 " of %" PRIu32 ", please wait, it=%" PRIu64 "\n", imss_path.c_str(), time_taken, i, n_ent, aux_i);
		// 	}
		// 	aux_i++;
		// 	if (i == 0)
		// 	{
		// 		// slog_debug("[IMSS]. y ..");
		// 		filler(aux_pointer, "..", NULL, 0);
		// 		filler(aux_pointer, ".", NULL, 0);
		// 	}
		// 	else
		// 	{
		// 		// slog_debug("[IMSS]%s", refs[i]);
		// 		// the stbuf is not used after here.
		// 		// struct stat stbuf;
		// 		// int error = imss_getattr(refs[i] + 6, &stbuf);
		// 		// if (!error)
		// 		{
		// 			// char *last = refs[i] + strlen(refs[i]) - 1;
		// 			// // slog_info("last=%s of %s", last, refs[i]);
		// 			// if (last[0] == '/')
		// 			// {
		// 			// 	last[0] = '\0';
		// 			// }
		// 			// int offset = 0;
		// 			// for (int j = 0; j < strlen(refs[i]); ++j)
		// 			// {
		// 			// 	if (refs[i][j] == '/')
		// 			// 	{
		// 			// 		offset = j;
		// 			// 	}
		// 			// }
		// 			// fprintf(stdout, "expected element %s\n", refs[i]+imss_path_len);
		// 			// filler(buf, refs[i] + offset + 1, &stbuf, 0); // original
		// 			// slog_full("refs[i] + offset + 1=%s", refs[i] + offset + 1);
		// 			// fprintf(stdout,"refs[%d]=%s, strlen(refs[i])=%lu, last=%s, refs[i] + offset + 1=%s\n", i, refs[i], strlen(refs[i]), last, refs[i] + offset + 1);
		// 			// filler(*buf, refs[i] + offset + 1, NULL, 0);
		// 			// filler(buf, refs[i], NULL, 0);
		// 			filler(aux_pointer, refs[i]+imss_path_len, NULL, 0);
		// 		}
		// 	}
		// 	if (refs[i] != NULL)
		// 	{
		// 		free(refs[i]);
		// 	}
		// }
		// // Free resources
		// if (refs != NULL)
		// {
		// 	free(refs);
		// }
		// printf("Ending imss_readdir\n");
		// return 0;
	}

	int imss_open(const char *path, uint64_t *fh)
	{
		int ret = -1;
		// TODO -> Access control
		// DEBUG
		char *imss_path = (char *)calloc(MAX_PATH, sizeof(char));
		int32_t file_desc = -1;
		get_iuri(path, imss_path);

		ior_operation_number = 0;

		int fd = 0;
		struct stat stats;
		char *aux = NULL;
		// Look for the 'file descriptor' of 'imss_path' in the local map.
		fd_lookup((char *)imss_path, &fd, &stats, &aux);
		slog_info("[FUSE]imss_path=%s, fd looked up=%d", imss_path, fd);
		// fprintf(stderr, "[FUSE]imss_path=%s, fd looked up=%d\n", imss_path, fd);
		if (fd >= 0)
		{
			print_file_type(stats, imss_path);
			ret = open_local_dataset(imss_path, 1);
			file_desc = fd;
			slog_debug("[FUSE]open_local_dataset, ret=%d, file_desc=%d, nlink=%lu", ret, file_desc, stats.st_nlink);
		}
		else if (fd == -2)
		{
			return -1;
		}
		else
		{
			file_desc = open_dataset(imss_path, 1);
			switch (file_desc)
			{
			case -EEXIST: // file already exists case.
			{
				file_desc = 0;
				break;
			}
			case -2: // symbolic link case.
			{
				slog_warn("[FUSE]imss_path=%s, file_desc=%d", imss_path, file_desc);
				*fh = file_desc;
				// errno = 2;
				// path = imss_path;
				// TODO: check how to avoid to cast to char *.
				strcpy((char *)path, imss_path);
				// return -2;
				return file_desc;
				break;
			}
			case -1: // no such file or directory case.
			{
				return -ENOENT;
				break;
			}
			}

			// aux = (char *)malloc(IMSS_DATA_BSIZE);
			void *data = (void *)malloc(IMSS_DATA_BSIZE * sizeof(char));
			ret = get_ndata(imss_path, file_desc, 0, data, 0, 0);
			if (ret < 0)
			{
				free(data);
				perror("ERR_HERCULES_IMSS_OPEN_GET_DATA");
				slog_error("ERR_HERCULES_IMSS_OPEN_GET_DATA");
				return -1;
			}

			slog_debug("ret=%d, file_desc=%d", ret, file_desc);
			memcpy(&stats, data, sizeof(struct stat));
			// pthread_mutex_lock(&lock_file);
			// storing block 0 on the local map.
			map_put(map, imss_path, file_desc, stats, (char *)data);
			print_file_type(stats, imss_path);
			// if (PREFETCH != 0)
			// {
			// 	// char *buff = (char *)malloc(PREFETCH * IMSS_DATA_BSIZE);
			// 	map_init_prefetch(map_prefetch, imss_path, PREFETCH * IMSS_DATA_BSIZE);
			// }
			// // pthread_mutex_unlock(&lock_file);
			// free(aux);
		}

		// File does not exist
		if (file_desc < 0)
		{
			slog_error("[FUSE]file descriptor: %d", file_desc);
			return -1;
		}

		// Assign file descriptor
		*fh = file_desc;
		/*if ((fi->flags & 3) != O_RDONLY)
		  return -EACCES;*/
		free(imss_path);
		return 0;
	}

	int performance = 0;
	ssize_t total_amount_read = 0;

	ssize_t imss_sread(const char *path, void *buf, size_t size, off_t offset)
	{
		int32_t length = 0;
		int eof_found = 0;
		const char *rpath = path; // this pointer should not be free.

		size_t curr_blk, num_of_blk, end_blk, start_offset, end_offset, block_offset, i_blk;
		size_t first = 0;
		int ds = 0;
		curr_blk = offset / IMSS_DATA_BSIZE + 1; // Plus one to skip the header (0) block
		start_offset = offset % IMSS_DATA_BSIZE;
		end_offset = (offset + size) % IMSS_DATA_BSIZE;
		// end_blk = (offset+size) / IMSS_DATA_BSIZE + 1; //Plus one to skip the header (0) block
		end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
		num_of_blk = end_blk - curr_blk;

		// Needed variables
		ssize_t to_read = 0;
		ssize_t byte_count = 0;
		int64_t rbytes;

		int fd = -1;
		struct stat stats;
		char *aux;

		fd_lookup((char *)rpath, &fd, &stats, &aux);
		if (fd >= 0)
			ds = fd;
		else if (fd == -1)
			return -ENOENT;

		if (stats.st_size < size)
		{
			end_blk = ceil((double)(offset + stats.st_size) / IMSS_DATA_BSIZE);
		}

		slog_debug("TotalSizeToRead=%ld (%ld kb), start_offset=%ld, curr_blk=%ld, end_blk=%ld, num_of_blks=%ld, offset=%ld, end_offset=%ld, IMSS_DATA_BSIZE=%ld, stats.st_size=%ld", size, size / 1024, start_offset, curr_blk, end_blk, num_of_blk, offset, end_offset, IMSS_DATA_BSIZE, stats.st_size);

		// Check if offset is bigger than filled, return 0 because is EOF case.
		// If the file offset is at or past the end of file,
		// no bytes are read, and read() returns zero
		// https://man7.org/linux/man-pages/man2/read.2.html
		if (offset >= stats.st_size)
		{
			slog_warn("[imss_read] returning EOF");
			buf = (void *)'\0';
			// memset(buf, '\0', size);
			return 0;
		}

		if (start_offset >= stats.st_size)
		{

			slog_warn("[imss_read] returning EOF");
			return 0;
		}

		i_blk = 0;
		while (curr_blk <= end_blk)
		{
			if (first == 0) // First block case
			{
				block_offset = start_offset;
				if (size < (stats.st_size - start_offset) && size < IMSS_DATA_BSIZE)
				{
					slog_info("[imss_read] case 1");
					to_read = size;
				}
				else
				{
					if (stats.st_size < IMSS_DATA_BSIZE)
					{
						slog_info("[imss_read] case 2");
						to_read = stats.st_size - start_offset;
					}
					else
					{
						slog_info("[imss_read] case 3");
						to_read = IMSS_DATA_BSIZE - start_offset;
					}
				}
				slog_debug("[imss_read] FIRST BLOCK CASE, to_read=%ld, fd=%d, ds=%d", to_read, fd, ds);

				++first;
				// Check if offset is bigger than filled, return 0 because is EOF case
				slog_debug("[imss_read] start_offset=%ld, to_read=%ld, stats.st_size=%ld, start_offset + to_read=%ld", start_offset, to_read, stats.st_size, start_offset + to_read);
				// if (start_offset + to_read > stats.st_size)
				// {
				// 	to_read = stats.st_size - start_offset + to_read;
				// 	slog_warn("data block overflow, reducing the amount of data to read in the block #%lu to %lu", curr_blk, to_read);
				// 	// slog_warn("[imss_read] returning size 0");
				// 	// return 0;
				// }

				// prevents to read out of the block.
				if (block_offset + to_read > IMSS_DATA_BSIZE)
				{
					to_read = IMSS_DATA_BSIZE - block_offset;
					slog_warn("data block overflow, reducing the amount of data to read in the block #%lu to %lu", curr_blk, to_read);
				}
				// prevents to read out of the EOF.
				if (offset + to_read > stats.st_size)
				{
					to_read = stats.st_size - offset;
					eof_found = 1;
					slog_warn("EOF overflow, reducing the amount of data to read in the block #%lu to %lu", curr_blk, to_read);
				}
			}
			else if (curr_blk != end_blk) // Middle block case
			{
				to_read = IMSS_DATA_BSIZE;
			}
			else // End block case
			{
				// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
				to_read = size - byte_count;
				slog_debug("END BLOCK CASE, to_read=%zd", to_read);
			}
			slog_debug("curr_blk=%ld, reading %ld bytes (%ld kilobytes) with an offset of %ld bytes (%ld kilobytes), byte_count=%zd bytes (%zd kilobytes)", curr_blk, to_read, to_read / 1024, block_offset, block_offset / 1024, byte_count, byte_count / 1024);

			if (to_read <= 0)
			{
				return to_read;
			}

			// get data from the data server.
			// if (MALLEABILITY)
			// {
			// 	int32_t num_storages = 0;
			// 	num_storages = get_number_of_data_servers(i_blk, num_of_blk);
			// 	slog_debug("[imss_read] i_blk=%ld, num_storages=%ld, N_SERVERS=%ld", i_blk, num_storages, N_SERVERS);

			// 	to_read = get_data_mall(ds, curr_blk, buf + byte_count, to_read, block_offset, num_storages);
			// 	i_blk++;
			// }
			// else
			{
				to_read = TIMING(get_ndata((char *)path, ds, curr_blk, (char *)buf + byte_count, to_read, block_offset), "get_ndata", ssize_t, -1);
			}
			// Error handling when get_ndata does not found the request data.
			// if (to_read == -1)
			if (to_read < 0)
			{
				return to_read;
			}

			block_offset = 0;

			++curr_blk;
			byte_count += to_read;
			// If eof was found, we end the while bucle to avoid trying to read
			// additional blocks.
			if (eof_found)
			{
				break;
			}
		}

		total_amount_read += byte_count;
		slog_read("TotalSizeToRead=%lu B (%lu kB, %lu mB), offset=%lu, total(to_read+offset)=%lu B (%lu mB), file size=%ld B (%ld mB), readed=%lu B, total_amount_read=%d", size, size / 1024, size / 1024 / 1024, offset, size + offset, (size + offset) / 1024 / 10240, stats.st_size, stats.st_size / 1024 / 1024, byte_count, total_amount_read);

		// performance =

		// free(rpath);
		return byte_count;
	}

	int imss_vread_prefetch(const char *path, char *buf, size_t size, off_t offset)
	{

		// printf("IMSS_READV size=%ld, path=%s\n", size, path);
		// Compute current block and offsets
		int64_t curr_blk, end_blk, start_offset, end_offset;
		int64_t first = 0;
		int ds = 0;
		curr_blk = offset / IMSS_DATA_BSIZE + 1; // Plus one to skip the header (0) block
		start_offset = offset % IMSS_DATA_BSIZE;
		// end_blk = (offset+size) / IMSS_DATA_BSIZE+1; //Plus one to skip the header (0) block
		end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
		end_offset = (offset + size) % IMSS_DATA_BSIZE;
		size_t to_read = 0;

		// Needed variables
		size_t byte_count = 0;
		int64_t rbytes;

		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		int fd;
		struct stat stats;
		char *aux;
		// printf("imss_read aux before %p\n", aux);
		fd_lookup((char *)rpath, &fd, &stats, &aux);
		// printf("imss_read aux after %p\n", aux);
		if (fd >= 0)
			ds = fd;
		else if (fd == -2)
			return -ENOENT;

		memset(buf, 0, size);
		// Read remaining blocks
		if (stats.st_size < size && stats.st_size < IMSS_DATA_BSIZE)
		{
			// total = IMSS_DATA_BSIZE;
			size = stats.st_size;
			end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
			end_offset = (offset + size) % IMSS_DATA_BSIZE;
		}
		// Check if offset is bigger than filled, return 0 because is EOF case
		if (start_offset >= stats.st_size)
		{
			free(rpath);
			return 0;
		}

		int err;
		// printf("READ path:%s, start block=%ld, end_block=%ld, size=%ld\n",rpath, curr_blk, end_blk, size);

		// printf("READ curr_block=%ld end_block=%ld numbers of block=%ld\n",curr_blk, end_blk,(end_blk-curr_blk+1));
		while (curr_blk <= end_blk)
		{

			int exist_first_block, exist_last_block;
			aux = map_get_buffer_prefetch(map_prefetch, rpath, &exist_first_block, &exist_last_block);

			if (aux != NULL)
			{ // Existe fichero es normal esta creado anteriormente

				if (curr_blk >= exist_first_block && curr_blk <= exist_last_block)
				{ // Tiene el bloque
					// Tengo que mover el puntero al bloque correspondiente
					// printf("Existe se lo doy bloque=%ld\n", curr_blk);
					int pos = curr_blk - exist_first_block;
					aux = aux + (IMSS_DATA_BSIZE * pos);
					err = 1;
				}
				else
				{ // Existe pero no tiene ese bloque especifico

					if (first == 0)
					{ // readv si es el primero leo todos
						// printf("READV TODOS\n");
						pthread_mutex_lock(&lock);
						err = readv_multiple((char *)path, ds, curr_blk, end_blk, buf, IMSS_BLKSIZE, start_offset, size);
						pthread_mutex_unlock(&lock);
						// PREFETCH
						pthread_mutex_lock(&lock);
						prefetch_ds = ds;
						strcpy(prefetch_path, rpath);
						prefetch_first_block = end_blk + 1;
						// prefetch_last_block = min ((end_blk + PREFETCH), stats.st_blocks);
						if ((end_blk + PREFETCH) <= stats.st_blocks)
						{
							prefetch_last_block = (end_blk + PREFETCH);
						}
						else
						{
							prefetch_last_block = stats.st_blocks;
						}
						prefetch_offset = start_offset;
						pthread_cond_signal(&cond_prefetch);
						pthread_mutex_unlock(&lock);
						if (err == -1)
						{
							free(rpath);
							return -1;
						}
						free(rpath);
						return size;
					}
					else
					{ // si ya tengo alguno guardado
						// printf("READV LOS QUE FALTABAN\n");
						pthread_mutex_lock(&lock);
						err = readv_multiple((char *)path, ds, curr_blk, end_blk, buf + byte_count, IMSS_BLKSIZE,
											 start_offset, IMSS_DATA_BSIZE * (end_blk - curr_blk + 1));
						pthread_mutex_unlock(&lock);
						// PREFETCH
						pthread_mutex_lock(&lock);
						prefetch_ds = ds;
						strcpy(prefetch_path, rpath);
						prefetch_first_block = end_blk + 1;
						// prefetch_last_block = min ((end_blk + PREFETCH), stats.st_blocks);
						if ((end_blk + PREFETCH) <= stats.st_blocks)
						{
							prefetch_last_block = (end_blk + PREFETCH);
						}
						else
						{
							prefetch_last_block = stats.st_blocks;
						}
						prefetch_offset = start_offset;
						pthread_cond_signal(&cond_prefetch);
						pthread_mutex_unlock(&lock);
						if (err == -1)
						{
							free(rpath);
							return -1;
						}
						free(rpath);
						return size;
					}
				}
			}
			else
			{
				if (err != -1)
				{
				}
				else
				{
					free(rpath);
					return -ENOENT;
				}
			}

			if (err != -1)
			{
				// First block case
				if (first == 0)
				{
					if (size < stats.st_size - start_offset)
					{
						// to_read = size;
						to_read = IMSS_DATA_BSIZE - start_offset;
					}
					else
					{
						if (stats.st_size < IMSS_DATA_BSIZE)
						{
							to_read = stats.st_size - start_offset;
						}
						else
						{
							to_read = IMSS_DATA_BSIZE - start_offset;
						}
					}

					// Check if offset is bigger than filled, return 0 because is EOF case
					if (start_offset > stats.st_size)
					{
						free(rpath);
						return 0;
					}
					memcpy(buf, aux + start_offset, to_read);
					byte_count += to_read;
					++first;

					// Middle block case
				}
				else if (curr_blk != end_blk)
				{
					// memcpy(buf + byte_count, aux + HEADER, IMSS_DATA_BSIZE);
					memcpy(buf + byte_count, aux, IMSS_DATA_BSIZE);
					byte_count += IMSS_DATA_BSIZE;
					// End block case
				}
				else
				{

					// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
					int64_t pending = size - byte_count;
					memcpy(buf + byte_count, aux, pending);
					byte_count += pending;
				}
			}
			++curr_blk;
		}

		flush_data();

		// PREFETCH
		pthread_mutex_lock(&lock);
		prefetch_ds = ds;
		strcpy(prefetch_path, rpath);
		prefetch_first_block = end_blk + 1;
		if ((end_blk + PREFETCH) <= stats.st_blocks)
		{
			prefetch_last_block = (end_blk + PREFETCH);
		}
		else
		{
			prefetch_last_block = stats.st_blocks;
		}
		prefetch_offset = start_offset;
		pthread_cond_signal(&cond_prefetch);
		pthread_mutex_unlock(&lock);
		free(rpath);
		return byte_count;
	}

	int imss_vread_no_prefetch(const char *path, char *buf, size_t size, off_t offset)
	{
		// printf("IMSS_READV size=%ld, path=%s, offset=%ld\n", size, path, offset);
		// Compute current block and offsets
		int64_t curr_blk, end_blk, start_offset, end_offset;
		int64_t first = 0;
		int ds = 0;
		curr_blk = offset / IMSS_DATA_BSIZE + 1; // Plus one to skip the header (0) block
		start_offset = offset % IMSS_DATA_BSIZE;
		// end_blk = (offset+size) / IMSS_DATA_BSIZE+1; //Plus one to skip the header (0) block
		end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
		end_offset = (offset + size) % IMSS_DATA_BSIZE;
		size_t to_read = 0;

		// Needed variables
		size_t byte_count = 0;
		int64_t rbytes;

		slog_debug("size %ld, start_blk %ld, start_offset %ld, end_blk %ld, end_offset %ld, curr_blk %ld", size, first, start_offset, end_blk, end_offset, curr_blk);

		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);
		// printf("rpath=%s\n",rpath);
		int fd;
		struct stat stats;
		char *aux;
		// printf("imss_read aux before %p\n", aux);
		fd_lookup((char *)rpath, &fd, &stats, &aux);

		// printf("imss_read aux after %p\n", aux);
		if (fd >= 0)
			ds = fd;
		else if (fd == -2)
			return -ENOENT;

		memset(buf, 0, size);
		int total = size;
		// Read remaining blocks
		if (stats.st_size < size && stats.st_size < IMSS_DATA_BSIZE)
		{
			// total = IMSS_DATA_BSIZE;
			size = stats.st_size;
			end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
			end_offset = (offset + size) % IMSS_DATA_BSIZE;
		}
		// Check if offset is bigger than filled, return 0 because is EOF case
		if (start_offset >= stats.st_size)
		{
			free(rpath);
			return 0;
		}
		int err;
		// printf("READ path:%s, start block=%ld, end_block=%ld, size=%ld\n",rpath, curr_blk, end_blk, size);

		// printf("READ curr_block=%ld end_block=%ld numbers of block=%ld\n",curr_blk, end_blk,(end_blk-curr_blk+1));
		while (curr_blk <= end_blk)
		{

			if (first == 0)
			{ // readv si es el primero leo todos
				// printf("READV TODOS\n");
				pthread_mutex_lock(&lock);
				err = readv_multiple((char *)path, ds, curr_blk, end_blk, buf, IMSS_BLKSIZE, start_offset, total);
				pthread_mutex_unlock(&lock);
				if (err == -1)
				{
					return -1;
				}
				free(rpath);
				return size;
			}
			else
			{ // si ya tengo alguno guardado
				// printf("READV LOS QUE FALTABAN\n");
				pthread_mutex_lock(&lock);
				err = readv_multiple((char *)path, ds, curr_blk, end_blk, buf + byte_count, IMSS_BLKSIZE,
									 start_offset, IMSS_BLKSIZE * KB * (end_blk - curr_blk + 1));
				pthread_mutex_unlock(&lock);
				if (err == -1)
				{
					return -1;
				}
				free(rpath);
				return size;
			}

			if (err != -1)
			{
				// First block case
				if (first == 0)
				{
					if (size < stats.st_size - start_offset)
					{
						// to_read = size;
						to_read = IMSS_DATA_BSIZE - start_offset;
					}
					else
					{
						if (stats.st_size < IMSS_DATA_BSIZE)
						{
							to_read = stats.st_size - start_offset;
						}
						else
						{
							to_read = IMSS_DATA_BSIZE - start_offset;
						}
					}

					// Check if offset is bigger than filled, return 0 because is EOF case
					if (start_offset > stats.st_size)
					{
						free(rpath);
						return 0;
					}
					memcpy(buf, aux + start_offset, to_read);
					byte_count += to_read;
					++first;

					// Middle block case
				}
				else if (curr_blk != end_blk)
				{
					// memcpy(buf + byte_count, aux + HEADER, IMSS_DATA_BSIZE);
					memcpy(buf + byte_count, aux, IMSS_DATA_BSIZE);
					byte_count += IMSS_DATA_BSIZE;
					// End block case
				}
				else
				{

					// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
					int64_t pending = size - byte_count;
					memcpy(buf + byte_count, aux, pending);
					byte_count += pending;
				}
			}
			++curr_blk;
		}

		flush_data();

		free(rpath);
		return byte_count;
	}

	int imss_vread_2x(const char *path, char *buf, size_t size, off_t offset)
	{

		// printf("IMSS_READV size=%ld, path=%s, offset=%ld\n", size, path, offset);
		// Compute current block and offsets
		int64_t curr_blk, end_blk, start_offset, end_offset;
		int64_t first = 0;
		int ds = 0;
		curr_blk = offset / IMSS_DATA_BSIZE + 1; // Plus one to skip the header (0) block
		start_offset = offset % IMSS_DATA_BSIZE;
		// end_blk = (offset+size) / IMSS_DATA_BSIZE+1; //Plus one to skip the header (0) block
		end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
		end_offset = (offset + size) % IMSS_DATA_BSIZE;
		size_t to_read = 0;

		// Needed variables
		size_t byte_count = 0;
		int64_t rbytes;

		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		int fd;
		struct stat stats;
		char *aux;
		// printf("imss_read aux before %p\n", aux);
		fd_lookup((char *)rpath, &fd, &stats, &aux);
		// printf("imss_read aux after %p\n", aux);
		if (fd >= 0)
			ds = fd;
		else if (fd == -2)
			return -ENOENT;

		memset(buf, 0, size);
		// Read remaining blocks
		if (stats.st_size < size && stats.st_size < IMSS_DATA_BSIZE)
		{
			// total = IMSS_DATA_BSIZE;
			size = stats.st_size;
			end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
			end_offset = (offset + size) % IMSS_DATA_BSIZE;
		}
		// Check if offset is bigger than filled, return 0 because is EOF case
		if (start_offset >= stats.st_size)
		{
			free(rpath);
			return 0;
		}
		PREFETCH = (end_blk - curr_blk) * 3;
		int err;
		// printf("READ curr_block=%ld end_block=%ld numbers of block=%ld\n",curr_blk, end_blk,(end_blk-curr_blk+1));
		while (curr_blk <= end_blk)
		{

			int exist_first_block, exist_last_block;
			aux = map_get_buffer_prefetch(map_prefetch, rpath, &exist_first_block, &exist_last_block);
			// curr_blk, exist_first_block, end_blk, exist_last_block);
			if (curr_blk >= exist_first_block && curr_blk <= exist_last_block)
			{ // Tiene el bloque
				// Tengo que mover el puntero al bloque correspondiente
				int pos = curr_blk - exist_first_block;
				aux = aux + (IMSS_BLKSIZE * KB * pos);
				err = 1;
			}
			else
			{
				if (first == 0)
				{ // readv si es el primero leo todos
					// printf("READV TODOS\n");
					pthread_mutex_lock(&lock);
					err = readv_multiple((char *)path, ds, curr_blk, end_blk, buf, IMSS_BLKSIZE, start_offset, size);
					pthread_mutex_unlock(&lock);

					// PERSONAL PREFETCH
					prefetch_ds = ds;
					strcpy(prefetch_path, rpath);
					prefetch_first_block = end_blk;
					// prefetch_last_block = min ((end_blk + PREFETCH), stats.st_blocks);
					if ((end_blk + PREFETCH) <= stats.st_blocks)
					{
						prefetch_last_block = (end_blk + PREFETCH);
					}
					else
					{
						prefetch_last_block = stats.st_blocks;
					}
					if (prefetch_last_block < prefetch_first_block)
					{
						free(rpath);
						return size;
					}

					prefetch_offset = start_offset;
					char *buf = map_get_buffer_prefetch(map_prefetch, prefetch_path, &exist_first_block, &exist_last_block);
					int err = readv_multiple((char *)path, prefetch_ds, prefetch_first_block, prefetch_last_block, buf, IMSS_BLKSIZE, prefetch_offset, IMSS_BLKSIZE * KB * (prefetch_last_block - prefetch_first_block));
					map_update_prefetch(map_prefetch, prefetch_path, prefetch_first_block, prefetch_last_block);
					if (err == -1)
						return -1;
					return size;
				}
				else
				{ // si ya tengo alguno guardado
					// printf("READV LOS QUE FALTABAN\n");
					pthread_mutex_lock(&lock);
					// printf("2ASK real ones\n");
					err = readv_multiple((char *)path, ds, curr_blk, end_blk, buf + byte_count, IMSS_BLKSIZE,
										 start_offset, IMSS_BLKSIZE * KB * (end_blk - curr_blk));
					pthread_mutex_unlock(&lock);

					// PERSONAL PREFETCH
					prefetch_ds = ds;
					strcpy(prefetch_path, rpath);
					prefetch_first_block = end_blk + 1;
					if ((end_blk + PREFETCH) <= stats.st_blocks)
					{
						prefetch_last_block = (end_blk + PREFETCH);
					}
					else
					{
						prefetch_last_block = stats.st_blocks;
					}
					if (prefetch_last_block < prefetch_first_block)
					{
						free(rpath);
						return size;
					}

					prefetch_offset = start_offset;
					char *buf = map_get_buffer_prefetch(map_prefetch, prefetch_path, &exist_first_block, &exist_last_block);
					int err = readv_multiple((char *)path, prefetch_ds, prefetch_first_block, prefetch_last_block, buf, IMSS_BLKSIZE, prefetch_offset, IMSS_BLKSIZE * KB * (prefetch_last_block - prefetch_first_block));
					map_update_prefetch(map_prefetch, prefetch_path, prefetch_first_block, prefetch_last_block);

					if (err == -1)
					{
						free(rpath);
						return -1;
					}
					free(rpath);
					return size;
				}
			}
			// printf("End update_prefetch\n");
			if (err != -1)
			{
				// First block case
				if (first == 0)
				{
					if (size < stats.st_size - start_offset)
					{
						// to_read = size;
						to_read = IMSS_DATA_BSIZE - start_offset;
					}
					else
					{
						if (stats.st_size < IMSS_DATA_BSIZE)
						{
							to_read = stats.st_size - start_offset;
						}
						else
						{
							to_read = IMSS_DATA_BSIZE - start_offset;
						}
					}

					// Check if offset is bigger than filled, return 0 because is EOF case
					if (start_offset > stats.st_size)
						return 0;

					memcpy(buf, aux + start_offset, to_read);
					byte_count += to_read;
					++first;

					// Middle block case
				}
				else if (curr_blk != end_blk)
				{
					// memcpy(buf + byte_count, aux + HEADER, IMSS_DATA_BSIZE);
					memcpy(buf + byte_count, aux, IMSS_DATA_BSIZE);
					byte_count += IMSS_DATA_BSIZE;
					// End block case
				}
				else
				{

					// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
					int64_t pending = size - byte_count;
					memcpy(buf + byte_count, aux, pending);
					byte_count += pending;
				}
			}
			++curr_blk;
		}
		free(rpath);
		return byte_count;
	}

	ssize_t imss_read(const char *path, void *buf, size_t size, off_t offset)
	{
		ssize_t ret;
		// BEST_PERFORMANCE_READ (default 0)
		if (BEST_PERFORMANCE_READ == 0)
		{
			// MULTIPLE_READ (default 0)
			if (MULTIPLE_READ == 1)
			{
				ret = imss_vread_prefetch(path, (char *)buf, size, offset);
			}
			else if (MULTIPLE_READ == 2)
			{
				ret = imss_vread_no_prefetch(path, (char *)buf, size, offset);
			}
			else if (MULTIPLE_READ == 3)
			{
				ret = imss_vread_2x(path, (char *)buf, size, offset);
			}
			else if (MULTIPLE_READ == 4)
			{
				// printf("ENTER IMSS_SPLIT_READV\n");
				ret = imss_split_readv(path, (char *)buf, size, offset);
			}
			else
			{
				ret = imss_sread(path, buf, size, offset);
			}
		}
		else if (BEST_PERFORMANCE_READ == 1)
		{
			if (N_SERVERS < threshold_read_servers)
			{
				// printf("[BEST_PERFORMANCE_READ] SREAD\n");
				ret = imss_sread(path, buf, size, offset);
			}
			else
			{
				// printf("[BEST_PERFORMANCE_READ] SPLIT_READV\n");
				ret = imss_split_readv(path, (char *)buf, size, offset);
			}
		}

		return ret;
	}

	ssize_t imss_write(const char *path, const void *buf, size_t size, off_t off)
	{
		ssize_t ret;
		clock_t t, tm, tmm;
		tmm = 0;

		t = clock();
		// MULTIPLE_WRITE (default 0: NORMAL WRITE)
		if (MULTIPLE_WRITE == 2)
		{
			ret = imss_split_writev(path, (const char *)buf, size, off);
			return ret;
		}

		// char *aux_block = NULL;

		// Compute offsets to write
		int64_t curr_blk, end_blk, start_offset, end_offset, block_offset, i_blk, num_of_blk;
		int64_t start_blk = off / IMSS_DATA_BSIZE + 1; // Add one to skip block 0
		start_offset = off % IMSS_DATA_BSIZE;
		end_blk = ceil((double)(off + size) / IMSS_DATA_BSIZE);

		end_offset = (off + size) % IMSS_DATA_BSIZE; // writev stuff
		curr_blk = start_blk;

		num_of_blk = end_blk - start_blk;

		// Needed variables
		size_t byte_count = 0;
		ssize_t bytes_stored = 0;
		int first_block_stored = 0;
		int ds = 0;
		int64_t to_copy = 0;
		uint64_t bytes_to_copy = 0;
		uint64_t space_in_block;
		uint32_t filled = 0;
		struct stat header;
		char *aux = NULL;
		// char *data_pointer = (char *)buf; // points to the buffer containing all bytes to be stored
		const void *data_pointer = buf; // points to the buffer containing all bytes to be stored
		const char *rpath = path;		// this pointer should not be free.
		int middle = 0;

		int fd = -1;
		struct stat stats;
		fd_lookup((char *)rpath, &fd, &stats, &aux);
		if (fd >= 0)
			ds = fd;
		else if (fd == -1)
			return -ENOENT;

		slog_debug("size=%ld, IMSS_DATA_BSIZE=%ld, stats.st_size=%ld, start_blk=%ld, start_offset=%ld, end_offset=%ld, end_blk=%ld, curr_blk=%ld, ds=%d, off=%ld", size, IMSS_DATA_BSIZE, stats.st_size, start_blk, start_offset, end_offset, end_blk, curr_blk, ds, off);

		// if (MULTIPLE_WRITE == 1)
		// {
		// 	slog_debug("[imss_write] MULTIPLE_WRITE %d", MULTIPLE_WRITE);
		// 	if ((end_blk - curr_blk) > 1)
		// 	{
		// 		writev_multiple((const char *)buf, ds, curr_blk, end_blk, start_offset, end_offset, IMSS_DATA_BSIZE, size);

		// 		// Update header count if the file has become bigger
		// 		if (size + off > stats.st_size)
		// 		{
		// 			stats.st_size = size + off;
		// 			stats.st_blocks = end_blk - 1;
		// 			pthread_mutex_lock(&lock);
		// 			map_update(map, rpath, ds, stats);
		// 			pthread_mutex_unlock(&lock);
		// 		}
		// 		// free(rpath);
		// 		return size;
		// 	}
		// }

		i_blk = 0;
		// WRITE NORMAL CASE
		while (curr_blk <= end_blk)
		{
			if (!first_block_stored)
			{
				// first block, special case
				// may be the only block to store, possible offset
				space_in_block = IMSS_DATA_BSIZE - start_offset;
				slog_debug("first block %ld: start_offset=%ld, stats.st_size=%ld, space_in_block=%ld", curr_blk, start_offset, stats.st_size, space_in_block);
				bytes_to_copy = (size < space_in_block) ? size : space_in_block;
				block_offset = start_offset;
				first_block_stored = 1;
			}
			else if (curr_blk == end_blk)
			{
				// last block, special case: might be skipped
				if (size == bytes_stored)
					break;

				slog_debug("last block %ld/%ld, bytes_stored=%ld", curr_blk, end_blk, bytes_stored);
				bytes_to_copy = size - bytes_stored;
			}
			else
			{
				// middle block: complete block, no offset
				slog_debug("middle block %ld", curr_blk);
				bytes_to_copy = IMSS_DATA_BSIZE;
			}

			// store block
			slog_debug("writting %" PRIu64 " kilobytes (%" PRIu64 " bytes) with an offset of %" PRIu64 " kilobytes (%" PRIu64 " bytes)", bytes_to_copy / 1024, bytes_to_copy, block_offset / 1024, block_offset);

			// Send data to data server.
			// if (MALLEABILITY)
			// {

			// 	int32_t num_storages = 0;
			// 	num_storages = get_number_of_data_servers(i_blk, num_of_blk);
			// 	slog_debug("[imss_write] i_blk=%ld, num_storages=%ld, N_SERVERS=%ld", i_blk, num_storages, N_SERVERS);
			// 	if (set_data_mall(ds, curr_blk, data_pointer, bytes_to_copy, block_offset, num_storages) < 0)
			// 	{
			// 		slog_error("[IMSS-FUSE]	Error writing to imss.\n");
			// 		error_print = -ENOENT;
			// 		return -ENOENT;
			// 	}
			// 	i_blk++;
			// }
			// else
			{
				if (TIMING(set_data((char *)path, ds, curr_blk, data_pointer, bytes_to_copy, block_offset), "set_data", int32_t, -1) < 0)
				{
					slog_error("[imss_write] Error writing to Hercules.\n");
					error_print = -ENOENT;
					return -ENOENT;
				}
			}

			bytes_stored += bytes_to_copy;
			data_pointer = (char *)data_pointer + bytes_to_copy;
			block_offset = 0; // first block has been stored, next blocks don't have an offset
			++curr_blk;
		}

		// Update header count if the file has become bigger.
		if (size + off > stats.st_size)
		{
			// if(size + off != stats.st_size){
			stats.st_size = size + off;
			stats.st_blocks = curr_blk - 1;
			slog_debug("[imss_write] Updating stat, st_size=%ld, st_nlink=%lu", stats.st_size, stats.st_nlink);
			map_update(map, rpath, ds, stats);
		}
		// free(rpath);

		t = clock() - t;
		double time_taken = ((double)t) / CLOCKS_PER_SEC; // in seconds
		double time_mem = ((double)tmm) / CLOCKS_PER_SEC; // in seconds

		slog_debug(">>>>>>> [API] imss_write time  total %f mem %f  s, total bytes = %ld, IMSS_DATA_BSIZE = %ld <<<<<<<<<", time_taken, time_mem, bytes_stored, IMSS_DATA_BSIZE);

		return bytes_stored;
	}

	int imss_split_writev(const char *path, const char *buf, size_t size, off_t off)
	{
		// printf("IMSS SPLIT WRITEV\n");
		// Compute offsets to write
		int64_t curr_blk, end_blk, start_offset, end_offset;
		int64_t start_blk = off / IMSS_DATA_BSIZE + 1; // Add one to skip block 0
		start_offset = off % IMSS_DATA_BSIZE;
		// end_blk = (off+size) / IMSS_DATA_BSIZE + 1; //Add one to skip block 0
		end_blk = ceil((double)(off + size) / IMSS_DATA_BSIZE);
		end_offset = (off + size) % IMSS_DATA_BSIZE;
		curr_blk = start_blk;

		// Needed variables
		size_t byte_count = 0;
		int first = 0;
		int ds = 0;
		int64_t to_copy = 0;
		uint32_t filled = 0;
		struct stat header;
		char *aux;
		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		int fd;
		struct stat stats;
		fd_lookup((char *)rpath, &fd, &stats, &aux);
		if (fd >= 0)
			ds = fd;
		else if (fd == -2)
			return -ENOENT;

		// List of servers for the blocks
		int **list_servers = (int **)calloc(N_SERVERS, sizeof(int *));
		int total = end_blk - curr_blk + 1;

		// Initialization to 0.
		for (int i = 0; i < N_SERVERS; i++)
		{
			list_servers[i] = (int *)calloc(size, sizeof(int));
		}

		// Getting list of servers for each block
		split_location_servers((char *)path, list_servers, ds, curr_blk, end_blk);

		int lenght_message = 102400;
		char **msg; // save block read for each server
		msg = (char **)calloc(N_SERVERS, sizeof(char *));
		for (int z = 0; z < N_SERVERS; z++)
		{
			msg[z] = (char *)calloc(lenght_message, sizeof(char));
		}
		int amount[N_SERVERS]; // save how many are sent to each server.

		// Preparing message for the server
		int count;

		char *all_blocks = (char *)calloc(lenght_message, sizeof(char));
		char *block = (char *)calloc(64, sizeof(char));
		char *number = (char *)calloc(64, sizeof(char));
		for (int server = 0; server < N_SERVERS; server++)
		{

			memset(all_blocks, '\0', lenght_message);
			count = 0;
			for (int i = 0; i < total; i++)
			{
				if (list_servers[server][i] > 0)
				{
					// printf("**list_servers[%d][%d]=%d\n", server,i,list_servers[server][i]);
					sprintf(block, "$%d", list_servers[server][i]);
					// printf("block=%s\n",block);
					// printf("all_block=%s length=%ld\n",all_blocks, strlen(all_blocks));
					strcat(all_blocks, block);

					count++;
				}
			}
			amount[server] = count;
			sprintf(number, "%d", count);
			strcat(msg[server], number);
			strcat(msg[server], all_blocks);
			// printf("server=%d msg_full=%s\n",server, msg[server]);
			// printf("amount=%d\n",amount[server]);
		}

		free(block);
		free(number);
		free(all_blocks);

		char **buffer_servers; // save blocks to write for each server
		buffer_servers = (char **)calloc(N_SERVERS, sizeof(char *));
		for (int z = 0; z < N_SERVERS; z++)
		{
			// printf("buffer_servers[%d]=%ld\n",z, amount[z]*IMSS_DATA_BSIZE);
			buffer_servers[z] = (char *)calloc(amount[z] * IMSS_DATA_BSIZE, sizeof(char));
		}

		for (int server = 0; server < N_SERVERS; server++)
		{
			count = 0;

			for (int i = 0; i < total; i++)
			{
				if (list_servers[server][i] > 0)
				{
					/*******COPYING DATA TO EACH SERVER BUFFER******/
					// printf("buffer_servers[%d]+%ld, buf+%ld, copy=%ld\n",server,count*IMSS_DATA_BSIZE, i*IMSS_DATA_BSIZE, IMSS_DATA_BSIZE);
					memcpy(buffer_servers[server] + count, buf + (i * IMSS_DATA_BSIZE), IMSS_DATA_BSIZE);
					/*******COPYING DATA TO EACH SERVER BUFFER******/
					count = count + IMSS_DATA_BSIZE;
				}
			}
		}

		//*********************Threads*******************************
		// Initialize pool of threads.
		pthread_t threads[(N_SERVERS)];
		thread_argv arguments[(N_SERVERS)];

		for (int server = 0; server < N_SERVERS; server++)
		{
			arguments[server].n_server = server;
			arguments[server].path = path;
			arguments[server].msg = msg[server];
			arguments[server].buffer = buffer_servers[server];
			arguments[server].size = amount[server];
			arguments[server].BLKSIZE = IMSS_BLKSIZE;
			arguments[server].start_offset = start_offset;
			arguments[server].stats_size = stats.st_size;
			arguments[server].lenght_key = lenght_message;

			if (arguments[server].size > 0)
			{
				if (pthread_create(&threads[server], NULL, split_writev, (void *)&arguments[server]) == -1)
				{
					perror("ERRIMSS_METAWORKER_DEPLOY");
					pthread_exit(NULL);
				}
			}
		}
		// Wait for the threads to conclude.
		for (int32_t server = 0; server < (N_SERVERS); server++)
		{

			if (arguments[server].size > 0)
			{
				// printf("Esperando hilo=%d\n",server);
				if (pthread_join(threads[server], NULL) != 0)
				{
					perror("ERRIMSS_METATH_JOIN");
					pthread_exit(NULL);
				}
			}
		}

		//*********************Threads*******************************

		// UPDATE FILE
		stats.st_size = size + off;
		stats.st_blocks = end_blk - 1;
		pthread_mutex_lock(&lock);
		map_update(map, rpath, ds, stats);
		pthread_mutex_unlock(&lock);
		return size; /////////////
	}

	int imss_split_readv(const char *path, char *buf, size_t size, off_t offset)
	{
		int64_t curr_blk, end_blk, start_offset, end_offset;
		int64_t first = 0;
		int ds = 0;
		curr_blk = offset / IMSS_DATA_BSIZE + 1; // Plus one to skip the header (0) block
		start_offset = offset % IMSS_DATA_BSIZE;
		// end_blk = (offset+size) / IMSS_DATA_BSIZE + 1; //Plus one to skip the header (0) block
		end_blk = ceil((double)(offset + size) / IMSS_DATA_BSIZE);
		end_offset = (offset + size) % IMSS_DATA_BSIZE;
		size_t to_read = 0;

		// printf("\n[CLIENT] [SPLIT_READ] size=%ld  offset=%ld start block=%ld, end_block=%ld\n",size, offset, curr_blk, end_blk);
		// Needed variables
		size_t byte_count = 0;
		int64_t rbytes;

		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));

		get_iuri(path, rpath);

		int fd;
		struct stat stats;
		char *aux;

		fd_lookup((char *)rpath, &fd, &stats, &aux);
		// printf("stats_size=%ld\n",stats.st_size);
		if (stats.st_size < size)
		{
			end_blk = ceil((double)(offset + stats.st_size) / IMSS_DATA_BSIZE);
		}

		// Check if offset is bigger than filled, return 0 because is EOF case
		if (start_offset >= stats.st_size)
		{
			return 0;
		}

		if (fd >= 0)
			ds = fd;
		else if (fd == -2)
			return -ENOENT;

		// List of servers for the blocks
		int **list_servers = (int **)calloc(N_SERVERS, sizeof(int *));
		int total = end_blk - curr_blk + 1;

		// Initialization to 0.
		for (int i = 0; i < N_SERVERS; i++)
		{
			list_servers[i] = (int *)calloc(size, sizeof(int));
			// bzero(list_servers[i],size*sizeof(int));
		}

		// Getting list of servers for each block
		split_location_servers((char *)path, list_servers, ds, curr_blk, end_blk);

		int lenght_message = 102400;
		char **msg; // save block read for each server
		msg = (char **)calloc(N_SERVERS, sizeof(char *));
		for (int z = 0; z < N_SERVERS; z++)
		{
			msg[z] = (char *)calloc(lenght_message, sizeof(char));
		}
		int amount[N_SERVERS]; // save how many are sent to each server.

		// Preparing message for the server
		int count;

		char *all_blocks = (char *)calloc(lenght_message, sizeof(char));
		char *block = (char *)calloc(64, sizeof(char));
		char *number = (char *)calloc(64, sizeof(char));
		for (int server = 0; server < N_SERVERS; server++)
		{
			memset(all_blocks, '\0', lenght_message);
			count = 0;
			for (int i = 0; i < total; i++)
			{
				if (list_servers[server][i] > 0)
				{
					// printf("**list_servers[%d][%d]=%d\n", server,i,list_servers[server][i]);
					sprintf(block, "$%d", list_servers[server][i]);
					//	printf("block=%s\n",block);
					//	printf("all_block=%s length=%ld\n",all_blocks, strlen(all_blocks));
					strcat(all_blocks, block);
					count++;
				}
			}
			amount[server] = count;
			sprintf(number, "%d", count);
			strcat(msg[server], number);
			strcat(msg[server], all_blocks);
			// printf("server=%d msg_full=%s\n", server, msg[server]);
			//	printf("amount=%d\n",amount[server]);
		}

		free(block);
		free(number);
		free(all_blocks);

		char **buffer_servers; // save block read for each server
		buffer_servers = (char **)calloc(N_SERVERS, sizeof(char *));
		for (int z = 0; z < N_SERVERS; z++)
		{
			buffer_servers[z] = (char *)calloc(amount[z] * IMSS_DATA_BSIZE, sizeof(char));
		}
		//*********************Lineal*******************************
		/*for(int server = 0; server < N_SERVERS; server++){
		  printf("server=%d, N_SERVER=%d\n",server, N_SERVERS);
		  int err = split_readv(server, path, msg[server],buffer_servers[server], amount[server], IMSS_BLKSIZE, start_offset, stats.st_size);
		  if(err == -1)
		  return -1;
		  }*/
		//*********************Lineal*******************************

		//*********************Threads*******************************
		// Initialize pool of threads.
		pthread_t threads[(N_SERVERS)];
		thread_argv arguments[(N_SERVERS)];

		for (int server = 0; server < N_SERVERS; server++)
		{
			arguments[server].n_server = server;
			arguments[server].path = path;
			arguments[server].msg = msg[server];
			arguments[server].buffer = buffer_servers[server];
			arguments[server].size = amount[server];
			arguments[server].BLKSIZE = IMSS_BLKSIZE;
			arguments[server].start_offset = start_offset;
			arguments[server].stats_size = stats.st_size;
			arguments[server].lenght_key = lenght_message;

			/*printf("\nCustom   ->buffer %p\n", buffer_servers[server]);
			  printf("arguments->buffer %p\n", arguments[server].buffer);
			  printf("arguments.n_server=%d\n",arguments[server].n_server);
			  printf("arguments.path=%s\n",arguments[server].path);
			  printf("arguments.msg=%s\n",arguments[server].msg);
			  printf("arguments.size=%d\n",arguments[server].size);
			  printf("arguments.BLKSIZE=%ld\n",arguments[server].BLKSIZE);
			  printf("arguments.start_offset=%ld\n",arguments[server].start_offset);
			  printf("arguments.stats-size=%d\n",arguments[server].stats_size);*/

			if (arguments[server].size > 0)
			{
				if (pthread_create(&threads[server], NULL, split_readv, (void *)&arguments[server]) == -1)
				{
					perror("ERRIMSS_METAWORKER_DEPLOY");
					pthread_exit(NULL);
				}
			}
		}

		// Wait for the threads to conclude.
		for (int32_t server = 0; server < (N_SERVERS); server++)
		{

			if (arguments[server].size > 0)
			{
				//	printf("Esperando hilo=%d\n",server);
				if (pthread_join(threads[server], NULL) != 0)
				{
					perror("ERRIMSS_METATH_JOIN");
					pthread_exit(NULL);
				}
			}
		}

		//*********************Threads*******************************

		size_t byte_count_servers[N_SERVERS]; // offset telling me how many i have write
		for (int server = 0; server < N_SERVERS; server++)
		{
			byte_count_servers[server] = 0;
		}

		for (int i = 0; i < total; i++)
		{
			for (int server = 0; server < N_SERVERS; server++)
			{

				if (list_servers[server][i] == curr_blk)
				{
					// printf("block find list_servers[%d][%d]=%d=%ld\n",server,i,list_servers[server][i],curr_blk);

					// First block case
					if (first == 0)
					{
						//	printf("FIRST BLOCK\n");
						if (size < (stats.st_size - start_offset) && size < IMSS_DATA_BSIZE && total == 1)
						{
							//		printf("*First block 1 case to_read=size=%ld\n",size);
							to_read = size;
						}
						else
						{
							if (stats.st_size < IMSS_DATA_BSIZE)
							{
								//			printf("*First block 2 case to_read=stats.st_size - start_offset=%ld\n",stats.st_size-start_offset);
								to_read = stats.st_size - start_offset;
							}
							else
							{
								//			printf("*First block 3 case to_read=IMSS_DATA_BSIZE- start_offset=%ld\n",IMSS_DATA_BSIZE-start_offset);
								to_read = IMSS_DATA_BSIZE - start_offset;
							}
						}
						// Check if offset is bigger than filled, return 0 because is EOF case
						if (start_offset > stats.st_size)
							return 0;

						memcpy(buf, buffer_servers[server] + start_offset, to_read);
						byte_count_servers[server] += to_read;
						byte_count += to_read;
						++first;
						// Middle block case
					}
					else if (curr_blk != end_blk)
					{
						//	printf("MIDDLE BLOCK\n");
						//	printf("curr_block=%ld, end_block=%ld\n",curr_blk, end_blk);
						//	printf("byte_count=%ld, byte_count_servers[%d]=%ld\n",byte_count,server,byte_count_servers[server]);
						memcpy(buf + byte_count, buffer_servers[server] + byte_count_servers[server], IMSS_DATA_BSIZE);
						byte_count_servers[server] += IMSS_DATA_BSIZE;
						byte_count += IMSS_DATA_BSIZE;
						// End block case
					}
					else
					{
						//	printf("LAST BLOCK\n");
						// Read the minimum between end_offset and filled (read_ = min(end_offset, filled))
						int64_t pending = size - byte_count;
						memcpy(buf + byte_count, buffer_servers[server] + byte_count_servers[server], pending);
						byte_count_servers[server] += pending;
						byte_count += pending;
					}
				}
			}
			curr_blk++;
		}

		// Releasing
		for (int i = 0; i < N_SERVERS; i++)
		{
			free(list_servers[i]);
		}
		free(list_servers);

		for (int z = 0; z < N_SERVERS; z++)
		{
			free(buffer_servers[z]);
		}
		free(buffer_servers);

		for (int z = 0; z < N_SERVERS; z++)
		{
			free(msg[z]);
		}
		free(msg);
		free(rpath);
		return byte_count;
	}

	/**
	 * @brief Updates the stats (block 0) on the remote data server of a dataset pointed by "path".
	 * @return 0 on success, on error a negative value (< 0) is returned.
	 */
	int imss_release(const char *path)
	{
		// Update dates
		int ds = 0;
		int fd = 0;
		const char *rpath = path; // this pointer should not be free.

		struct stat stats;
		char *aux = NULL;
		fd_lookup((char *)rpath, &fd, &stats, &aux);
		if (fd >= 0)
			ds = fd;
		else
			return -ENOENT;

		// char *head = (char *)malloc(IMSS_DATA_BSIZE);

		// Get time
		struct timespec spec;
		clock_gettime(CLOCK_REALTIME, &spec);

		// Update time
		stats.st_mtim = spec;
		stats.st_ctim = spec;

		// write metadata
		slog_debug("stats.st_nlink=%lu", stats.st_nlink);
		// memcpy(head, &stats, sizeof(struct stat));

		// Updates the size of the file in the block 0.
		// if (set_data(ds, 0, head, 0, 0) < 0)
		if (set_data((char *)path, ds, 0, (char *)&stats, 0, 0) < 0)
		{
			perror("HERCULES_ERR_WRITTING_BLOCK");
			slog_error("HERCULES_ERR_WRITTING_BLOCK");
			// free(head);
			return -ENOENT;
		}

		// free(head);
		return ds;
	}

	/**
	 * @brief Closes the dataset on the backend and delete it when the dataset status is "dest" and no more process has the file open.
	 * @return 1 if the file was correctly closed,
	 * 0 if the file was deleted (e.g., file has been removed using unlink),
	 * on error -1 is returned.
	 */
	int imss_close(const char *path, int fd)
	{
		int ret = 0;
		int ds = 0;
		// slog_debug("Calling imss_flush_data");
		// imss_flush_data();
		// slog_debug("Ending imss_flush_data");
		ds = imss_release(path);
		slog_debug("Ending imss_release, ret=%d", ds);
		ret = close_dataset(path, fd);
		slog_debug("Ending close_dataset, ret=%d", ret);
		if (ret)
		{ // if the file was not deleted by the close we update the stat.
			// imss_refresh is too slow.
			// When we remove it pass from 3.45 sec to 0.008505 sec.
			imss_refresh(path);
			slog_debug("Ending imss_refresh");
		}
		else
		{ // if the file was deleted by the close, we delete registers from the
			// local tree and local map.
			clear_dataset(path);
			map_erase(map, path);
		}

		// TODO: Tell data server the file is ready to be copied to disk.

		return ret;
	}

	int imss_create(const char *path, mode_t mode, uint64_t *fh, int opened, char file_type)
	{
		int ret = 0;
		struct timespec spec;
		// TODO check mode
		struct stat ds_stat;
		// Check if already created!
		const char *rpath = path;

		// Assing file handler and create dataset.
		int res = 0;
		res = create_dataset((char *)rpath, POLICY, N_BLKS, IMSS_BLKSIZE, REPL_FACTOR, REPL_TYPE, N_SERVERS, NO_LINK, opened, file_type);
		slog_debug("create_dataset((char*)rpath:%s, POLICY:%s,  N_BLKS:%ld, IMSS_BLKSIZE:%d, REPL_FACTOR:%ld, REPL_TYPE:%ld, N_SERVERS:%d, FILE_TYPE=%c), res:%d", (char *)rpath, POLICY, N_BLKS, IMSS_BLKSIZE, REPL_FACTOR, REPL_TYPE, N_SERVERS, file_type, res);
		if (res < 0)
		{
			slog_error("Cannot create new dataset.\n");
			// fprintf(stderr, "Cannot create new dataset %s, may already exist.\n", path);
			// free(rpath);
			return res;
		}
		else
		{
			*fh = res;
		}
		clock_gettime(CLOCK_REALTIME, &spec);

		memset(&ds_stat, 0, sizeof(struct stat));

		// Create initial block
		ds_stat.st_size = 0;
		ds_stat.st_nlink = 1;
		ds_stat.st_atime = spec.tv_sec;
		ds_stat.st_mtime = spec.tv_sec;
		ds_stat.st_ctime = spec.tv_sec;
		ds_stat.st_uid = getuid();
		ds_stat.st_gid = getgid();
		ds_stat.st_blocks = 0;
		ds_stat.st_blksize = IMSS_DATA_BSIZE;
		ds_stat.st_ino = res;
		ds_stat.st_dev = 0;

		if (!S_ISDIR(mode))
			mode |= S_IFREG;
		ds_stat.st_mode = mode;
		print_file_type(ds_stat, path);

		// Write initial block
		void *buff = (void *)malloc(IMSS_DATA_BSIZE);
		memcpy(buff, &ds_stat, sizeof(struct stat));
		pthread_mutex_lock(&lock); // lock.
		// stores block 0.
		ret = set_data((char *)rpath, *fh, 0, buff, 0, 0);
		if (ret < 0)
		{
			slog_error("HERCULES_ERR_SETDATA_0");
		}
		pthread_mutex_unlock(&lock); // unlock.

		map_erase(map, rpath);
		slog_debug("map_erase(map, rpath:%s)", rpath);
		// if(ret < 1){
		// 	slog_debug("No elements erased by map_erase, ret:%d", ret);
		// }

		pthread_mutex_lock(&lock_file); // lock.
		map_put(map, rpath, *fh, ds_stat, (char *)buff);
		slog_debug("map_put(map, rpath:%s, fh:%ld, ds_stat.st_blksize=%ld)", rpath, *fh, ds_stat.st_blksize);
		// if (PREFETCH != 0)
		// {
		// 	// char *buff = (char *)malloc(PREFETCH * IMSS_BLKSIZE * KB);
		// 	map_init_prefetch(map_prefetch, rpath, PREFETCH * IMSS_BLKSIZE * KB);
		// 	slog_debug("PREFETCH:%ld, map_init_prefetch(map_prefetch, rpath:%s)", PREFETCH, rpath);
		// }
		pthread_mutex_unlock(&lock_file); // unlock.
		// free(rpath);
		return 0;
	}

	// int set_num_servers()
	// {
	// 	int n_servers = N_SERVERS;
	// }

	// Does nothing
	int imss_opendir(const char *path)
	{
		return 0;
	}

	// Does nothing
	int imss_releasedir(const char *path)
	{
		return 0;
	}

	/**
	 * @brief Deletes a directory, which must be empty.
	 * https://man7.org/linux/man-pages/man2/rmdir.2.html
	 * @return 0 on success. On error, the value of errno is returned.
	 */
	int imss_rmdir(const char *imss_path)
	{

		// Needed variables for the call
		char **refs = NULL;
		int n_ent = 0;

		if ((n_ent = get_dir(imss_path, &refs)) > 0)
		{
			// fprintf(stdout, "Dir %s still has %d entries\n", imss_path, n_ent);
			free_entries(refs, n_ent);
			if (n_ent > 1)
			{
				slog_debug("n_ent > 1");
				return -EPERM;
			}
		}
		else
		{
			// fprintf(stderr, "*** [imss_rmdir] Error getting dir %s, n_ent=%d\n", imss_path, n_ent);
			slog_error("[imss_rmdir] Error getting dir %s, n_ent=%d", imss_path, n_ent);
			// free(imss_path);
			return -ENOENT;
		}
		imss_unlink(imss_path);
		// free(imss_path);
		return 0;
	}

	int imss_unlink(const char *path)
	{
		const char *imss_path = path; // this pointer should not be free.
		slog_info("path=%s, imss_path=%s", path, imss_path);

		uint32_t ds = -1;
		int fd = 0;
		struct stat stats;
		char *buff = NULL;
		int ret = 0;
		pthread_mutex_lock(&lock);
		fd_lookup((char *)imss_path, &fd, &stats, &buff);
		// if (fd >= 0)
		// 	ds = fd;
		// else
		// {
		// 	slog_debug("%s not found in lookup", imss_path);
		// 	pthread_mutex_unlock(&lock);
		// 	return -ENOENT;
		// }
		int file_desc = 0;
		if (fd >= 0)
			file_desc = fd;
		else if (fd == -2)
			return -ENOENT;
		else
		{
			// If not in the local map, open de dataset.
			file_desc = open_dataset((char *)imss_path, 1);
			if (file_desc < 0)
			{ // files does not exist.
				return -ENOENT;
			}
			
			// Get initial block (0).
			char *data = NULL;
			data = (char *)malloc(IMSS_DATA_BSIZE * sizeof(char));
			if (data == NULL)
			{
				perror("HERCULES_ERR_IMSS_UNLINK_MEMORY_ALLOC");
				slog_error("HERCULES_ERR_IMSS_UNLINK_MEMORY_ALLOC");
				return -ENOMEM;
			}
			memcpy(&stats, data, sizeof(struct stat));
			map_put(map, imss_path, file_desc, stats, (char *)data);
			buff = data;
		}

		//  data is filled in "get data".
		ret = get_ndata((char *)path, file_desc, 0, buff, 0, 0);
		if (ret < 0)
		{
			perror("HERCULES_ERR_IMSS_UNLINK_GET_DATA");
			slog_error("HERCULES_ERR_IMSS_UNLINK_GET_DATA");
			pthread_mutex_unlock(&lock);
			return -1;
		}
		// pthread_mutex_lock(&lock);
		// Read header.
		struct stat header;
		memcpy(&header, buff, sizeof(struct stat));

		if (header.st_nlink > 0)
		{
			header.st_nlink = header.st_nlink - 1;
		}

		// Write initial block (0).
		memcpy(buff, &header, sizeof(struct stat));
		slog_debug("header.st_nlink=%lu", header.st_nlink);
		set_data((char *)path, file_desc, 0, (char *)buff, 0, 0);
		pthread_mutex_unlock(&lock);

		// if it is the last link, the file is deleted (we also should to check if other process is open).
		if (header.st_nlink == 0)
		{
			// Those operations must be performed by the server itself when it knows no more process are using the file.
			// Erase metadata in the backend.
			ret = delete_dataset(imss_path, file_desc);
			slog_debug("delete_dataset %s, ret=%d", imss_path, ret);

			switch (ret)
			{
			case 1: // datasate was not delete.
			{
				break;
			}
			case 0: // dataset was delete.
			{
				// ******************************* TO CHECK!
				pthread_mutex_lock(&lock_file);
				map_erase(map, imss_path);
				pthread_mutex_unlock(&lock_file);

				slog_debug("Calling map_release_prefetch %s", path);
				// map_release_prefetch(map_prefetch, imss_path);
				slog_debug("Finish map_release_prefetch %s", path);
				// *******************************
				// ret = release_dataset(file_desc);
				ret = release_dataset(imss_path);
				slog_debug("relese_dataset ret=%d", ret);
				if (ret < 0)
				{
					slog_error("HERCULES_ERR_RELEASE_DATASET");
				}

				ret = 3;
				break;
			}
			default: // error deleting the dataset.
				slog_error("ERR_HERCULES_DELETE_DATASET");
				ret = -1;
				break;
			}
			// **************************
		}
		else
		{
			ret = 1; // file was not deleted.
		}

		return ret;
	}

	int imss_utimens(const char *path, const struct timespec tv[2])
	{
		struct stat ds_stat;
		struct timespec spec;
		clock_gettime(CLOCK_REALTIME, &spec);
		uint32_t file_desc;

		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		// Assing file handler and create dataset
		int fd;
		struct stat stats;
		char *buff;
		fd_lookup((char *)rpath, &fd, &stats, &buff);

		if (fd >= 0)
			file_desc = fd;
		else if (fd == -2)
			return -ENOENT;
		else
			file_desc = open_dataset(rpath, 0);

		if (file_desc < 0)
		{
			slog_error("[IMSS-FUSE]    Cannot open dataset.");
		}

		pthread_mutex_lock(&lock);
		get_ndata((char *)path, file_desc, 0, buff, 0, 0);
		pthread_mutex_unlock(&lock);

		memcpy(&ds_stat, buff, sizeof(struct stat));

		ds_stat.st_mtime = spec.tv_sec;

		// Write initial block
		memcpy(buff, &ds_stat, sizeof(struct stat));

		pthread_mutex_lock(&lock);
		set_data((char *)path, file_desc, 0, (char *)buff, 0, 0);
		pthread_mutex_unlock(&lock);

		// free(rpath);

		return 0;
	}

	/**
	 * @brief creates a directory.
	 */
	int imss_mkdir(const char *path, mode_t mode)
	{
		uint64_t fi;
		int ret = -1;
		// opened is equals to 2 to indicate this was not created with a 'open' syscall.
		ret = imss_create(path, mode | S_IFDIR, &fi, 2, TYPE_DIRECTORY);
		return ret;
	}

	int imss_symlinkat(char *new_path_1, char *new_path_2, int _case)
	{
		int ret = 0;
		struct timespec spec;
		// TODO check mode
		struct stat ds_stat;
		char *rpath1 = (char *)calloc(MAX_PATH, sizeof(char));
		char *rpath2 = (char *)calloc(MAX_PATH, sizeof(char));

		int fd, file_desc;
		struct stat stats;
		char *aux;
		int res = 0;
		// int32_t n_servers = 0;

		switch (_case)
		{
		case 0:
			slog_debug("[FUSE]Entering case 0 ");
			get_iuri(new_path_1, rpath1);
			get_iuri(new_path_2, rpath2);
			fd_lookup(new_path_1, &fd, &stats, &aux);
			if (fd >= 0)
			{
				file_desc = fd;
			}
			else if (fd == -2)
				return -1;
			else
			{
				file_desc = open_dataset(new_path_1, 0);
				if (file_desc < 0)
				{ // dataset was not found.
					return -1;
				}
				aux = (char *)malloc(IMSS_DATA_BSIZE);
				ret = get_ndata(new_path_1, file_desc, 0, aux, 0, 0);
				memcpy(&stats, aux, sizeof(struct stat));
				pthread_mutex_lock(&lock_file);
				map_put(map, new_path_1, file_desc, stats, aux);
				// if (PREFETCH != 0)
				// {
				// 	char *buff = (char *)malloc(PREFETCH * IMSS_DATA_BSIZE);
				// 	map_init_prefetch(map_prefetch, new_path_1, PREFETCH * IMSS_DATA_BSIZE);
				// }
				pthread_mutex_unlock(&lock_file);
				// free(aux);
			}
			res = create_dataset((char *)rpath2, POLICY, N_BLKS, IMSS_BLKSIZE, REPL_FACTOR, REPL_TYPE, N_SERVERS, new_path_1, 3, TYPE_REGULAR_FILE); // TODO: CHECK file type.

			break;
		case 1:
			slog_debug("[FUSE]Entering case 1 ");
			// rpath1 = new_path_1;
			get_iuri(new_path_2, rpath2);
			res = create_dataset((char *)rpath2, POLICY, N_BLKS, IMSS_BLKSIZE, REPL_FACTOR, REPL_TYPE, N_SERVERS, new_path_1, 3, TYPE_REGULAR_FILE); // TODO: CHECK file type
			break;
		default:
			break;
		}

		slog_debug("[FUSE]rpath1=%s, rpath2=%s", rpath1, rpath2);

		// Assing file handler and create dataset

		if (res < 0)
		{
			// fprintf(stderr, "[imss_create]	Cannot create new dataset.\n");
			slog_error("Cannot create new dataset.\n");
			free(rpath1);
			free(rpath2);
			return res;
		}

		map_erase(map, rpath2);
		slog_debug("map_erase(map, rpath:%s), ret:%d", rpath2, ret);
		// if(ret < 1){
		// 	slog_debug("No elements erased by map_erase, ret:%d", ret);
		// }

		pthread_mutex_lock(&lock_file); // lock.
		// map_put(map, rpath2, file_desc, ds_stat, aux);
		// slog_debug("map_put(map, rpath:%s, fh:%ld, ds_stat, buff:%s)", rpath, *fh, buff);
		slog_debug("map_put(map, rpath:%s, fh:%ld, ds_stat.st_blksize=%ld)", rpath2, file_desc, ds_stat.st_blksize);

		pthread_mutex_unlock(&lock_file); // unlock.
		free(rpath1);
		free(rpath2);
		return 0;
	}

	int imss_flush(const char *path)
	{

		if (error_print != 0)
		{
			int32_t err = error_print;
			error_print = 0;
			return err;
		}
		// struct stat ds_stat;
		struct timespec spec;
		clock_gettime(CLOCK_REALTIME, &spec);
		uint32_t file_desc;

		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		// Assing file handler and create dataset
		int fd;
		struct stat stats;
		char *buff;
		fd_lookup(rpath, &fd, &stats, &buff);

		if (fd >= 0)
			file_desc = fd;
		else if (fd == -2)
			return -ENOENT;
		else
			file_desc = open_dataset(rpath, 0);
		if (file_desc < 0)
		{
			slog_error("[IMSS-FUSE]    Cannot open dataset.");
			// fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
			return -EACCES;
		}

		stats.st_mtime = spec.tv_sec;

		// Write initial block
		memcpy(buff, &stats, sizeof(struct stat));

		pthread_mutex_lock(&lock);
		if (set_data((char *)path, file_desc, 0, (char *)buff, 0, 0) < 0)
		{
			// fprintf(stderr, "[IMSS-FUSE]	Error writing to imss.\n");
			slog_error("[IMSS-FUSE]	Error writing to imss.");
			error_print = -ENOENT;
			pthread_mutex_unlock(&lock);
			return -ENOENT;
		}

		pthread_mutex_unlock(&lock);
		free(rpath);
		return 0;
	}

	int imss_getxattr(const char *path, const char *attr, char *value, size_t s)
	{
		return 0;
	}

	int imss_chmod(const char *path, mode_t mode)
	{

		struct stat ds_stat;
		uint32_t file_desc;
		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		// Assing file handler and create dataset
		int fd;
		struct stat stats;
		void *buff = NULL;
		fd_lookup(rpath, &fd, &stats, (char **)&buff);

		if (fd >= 0)
			file_desc = fd;
		else if (fd == -2)
			return -ENOENT;
		else
			file_desc = open_dataset(rpath, 0);
		if (file_desc < 0)
		{
			slog_error("[IMSS-FUSE]    Cannot open dataset.");
			// fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
		}

		pthread_mutex_lock(&lock);
		get_ndata((char *)path, file_desc, 0, buff, 0, 0);
		pthread_mutex_unlock(&lock);

		memcpy(&ds_stat, buff, sizeof(struct stat));

		slog_debug("[imss_chmod] st_mode=%lu, new mode=%lu", ds_stat.st_mode, mode);
		mode_t type = ds_stat.st_mode & 0xFFFF000;
		// fprintf(stderr, "Hola hola hola %d\n", type >> 3);
		ds_stat.st_mode = mode | type;
		slog_debug("[imss_chmod] After st_mode=%lu", ds_stat.st_mode);

		// Write initial block
		memcpy(buff, &ds_stat, sizeof(struct stat));

		pthread_mutex_lock(&lock);
		set_data((char *)path, file_desc, 0, buff, 0, 0);
		pthread_mutex_unlock(&lock);

		free(rpath);
		return 0;
	}

	int imss_chown(const char *path, uid_t uid, gid_t gid)
	{
		struct stat ds_stat;
		uint32_t file_desc;
		char *rpath = (char *)calloc(MAX_PATH, sizeof(char));
		get_iuri(path, rpath);

		// Assing file handler and create dataset
		int fd;
		struct stat stats;
		char *buff;
		fd_lookup((char *)rpath, &fd, &stats, &buff);

		if (fd >= 0)
			file_desc = fd;
		else if (fd == -2)
			return -ENOENT;
		else
			file_desc = open_dataset(rpath, 0);
		if (file_desc < 0)
		{
			slog_error("[IMSS-FUSE]    Cannot open dataset.");
			// fprintf(stderr, "[IMSS-FUSE]    Cannot open dataset.\n");
		}

		pthread_mutex_lock(&lock);
		get_ndata((char *)path, file_desc, 0, buff, 0, 0);
		pthread_mutex_unlock(&lock);

		memcpy(&ds_stat, buff, sizeof(struct stat));

		ds_stat.st_uid = uid;
		if (gid != -1)
		{
			ds_stat.st_gid = gid;
		}

		// Write initial block
		memcpy(buff, &ds_stat, sizeof(struct stat));

		pthread_mutex_lock(&lock);
		set_data((char *)path, file_desc, 0, (char *)buff, 0, 0);
		pthread_mutex_unlock(&lock);
		free(rpath);
		return 0;
	}

	// Function to recursively read and print files and directories.
	int read_directory(const char *path, const char *parent)
	{
		DIR *dir;
		struct dirent *entry;
		struct stat stat_buf;
		char full_path[MAX_PATH] = {0};
		char hercules_dir_path[MAX_PATH] = {0};
		// char *basec = NULL;
		// char *parent_bname = NULL;

		// Open the directory.
		dir = opendir(path);
		if (dir == NULL)
		{
			perror("HERCULES_ERR_READ_DIRECTORY_OPEN_DIR"); // Use perror for more informative error message.
			slog_error("HERCULES_ERR_READ_DIRECTORY_OPEN_DIR: %s\n", path);
			return -1;
		}
		slog_debug("Reading dir=%s, parent=%s", path, parent);
		// slog_debug("dirname=%s, basename=%s, parent=%s", path, dirname(strdup((char *)path)), basename(strdup((char *)path)), parent);
		// basec = strdup(path);
		// parent_bname = basename(basec);

		// Read the directory entries.
		while ((entry = readdir(dir)) != NULL)
		{
			// Skip "." and ".." entries to avoid infinite recursion.
			if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			{
				continue;
			}

			// Construct the full path of the entry.
			// snprintf(full_path, sizeof(full_path), "%s/%s", path, entry->d_name);
			strncpy(full_path, path, sizeof(full_path) - strlen(full_path) - 1);
			strncat(full_path, "/", sizeof(full_path) - strlen(full_path) - 1);
			strncat(full_path, entry->d_name, sizeof(full_path) - strlen(full_path) - 1);

			slog_debug("Entry=%s", full_path);

			int fd = open(full_path, O_RDONLY);
			if (fd < 0)
			{ // file does not exist.
				slog_error("HERCULES_ERR_MOVE_OPEN_SYSTEM_FILE");
				continue;
			}

			// Get the file/directory status.
			// if (stat(full_path, &stat_buf) == -1)
			if (__fxstat(1, fd, &stat_buf) < 0)
			{
				perror("HERCULES_ERR_READ_DIRECTORY_STAT");
				slog_error("HERCULES_ERR_READ_DIRECTORY_STAT: %s\n", full_path);
				continue; // Skip to the next entry on error
			}

			if (close(fd) == -1)
			{
				// continue;
			}

			slog_debug("d_name=%s", entry->d_name);

			// Makes the Hercules path.
			if (parent == NULL)
			{
				perror("HERCULES_ERR_READ_DIRECTORY_NO_PARENT");
				slog_error("HERCULES_ERR_READ_DIRECTORY_NO_PARENT");
				return -1;
			}
			// Format the Hercules path for this entry.
			// parent directory (which includes the MOUNT POINT) + entry
			// snprintf(hercules_dir_path, sizeof(hercules_dir_path), "%s/%s", parent, entry->d_name);
			strncpy(hercules_dir_path, parent, sizeof(hercules_dir_path) - strlen(hercules_dir_path) - 1);
			strncat(hercules_dir_path, "/", sizeof(hercules_dir_path) - strlen(hercules_dir_path) - 1);
			strncat(hercules_dir_path, entry->d_name, sizeof(hercules_dir_path) - strlen(hercules_dir_path) - 1);

			// else
			// 	snprintf(hercules_dir_path, sizeof(hercules_dir_path), "%s/%s/%s", MOUNT_POINT, parent_bname, entry->d_name);

			// Check if it's a directory.
			if (S_ISDIR(stat_buf.st_mode))
			{
				slog_info("** Dir=%s, Hercules path=%s", full_path, hercules_dir_path);
				// Creates the directory in Hercules.
				mkdir(hercules_dir_path, 0700);

				// Recursively call read_directory for subdirectories
				read_directory(full_path, hercules_dir_path);
			}
			else
			{
				slog_info("** File=%s, Hercules path=%s", full_path, hercules_dir_path);
				if (rename(full_path, hercules_dir_path) == -1)
				{
					slog_error("HERCULES_ERR_READ_DIRECTORY_RENAME: %s to %s", full_path, hercules_dir_path);
					perror("HERCULES_ERR_READ_DIRECTORY_RENAME");
					continue;
				}
			}
		}

		// Close the directory
		if (closedir(dir) == -1)
		{
			perror("HERCULES_ERR_READ_DIRECTORY_CLOSEDIR");
			slog_error("HERCULES_ERR_READ_DIRECTORY_CLOSEDIR: %s\n", path);
		}
		return 0;
	}

	int hercules_move_file_from_disk(int fd_old, const char *old_path, const char *new_path)
	{
		struct stat old_file_stat;

		int to_close = 0;

		// checks if an invalid file descriptor was passed.
		// this means this method should to open the file.
		if (fd_old < 0)
		{
			to_close = 1;
			// open the file from the file system.
			fd_old = open(old_path, O_RDONLY);
			if (fd_old < 0)
			{ // file does not exist.
				slog_error("HERCULES_ERR_MOVE_OPEN_SYSTEM_FILE");
				return -ENOENT;
			}
		}

		// open the files from hercules because it begins with the mount point.
		int fd_new = open(new_path, O_WRONLY | O_APPEND | O_CREAT, 0644);
		if (fd_new < 0)
		{
			slog_error("HERCULES_ERR_RENAME_OPEN_HERCULES_FILE: %s", new_path);
			close(fd_old);
			return -ENOENT;
		}

		// get old file stat.
		if (__fxstat(1, fd_old, &old_file_stat) < 0)
		{
			perror("HERCULES_ERR_MOVE_FILE_FROM_DISK_STAT_SYSTEM_FILE");
			slog_error("HERCULES_ERR_MOVE_FILE_FROM_DISK_STAT_SYSTEM_FILE");
			return -EAGAIN;
		}

		// old file size.
		off_t old_file_size = old_file_stat.st_size;

		// read old file.
		char *old_file_buffer = NULL;
		old_file_buffer = (char *)malloc(old_file_size * sizeof(char));
		if (old_file_buffer == NULL)
		{
			perror("HERCULES_ERR_MOVE_FILE_MEMORY_ALLOC");
			slog_error("HERCULES_ERR_MOVE_FILE_MEMORY_ALLOC");
			return -ENOSPC;
		}

		ssize_t bytes_read = -1, bytes_write, total_bytes = -1;
		while ((bytes_read = read(fd_old, old_file_buffer, old_file_size)) > 0)
		{
			slog_info("[POSIX]. bytes read from %s = %ld/%ld", old_path, bytes_read, old_file_size);
			// writes to Hercules.
			bytes_write = write(fd_new, old_file_buffer, bytes_read);
			if (bytes_write < 0)
			{
				perror("HERCULES_ERR_RENAME_WRITE_FILE");
				slog_error("HERCULES_ERR_RENAME_WRITE_FILE: %s, fd_new=%d", old_path, fd_new);
				break;
			}
			total_bytes += bytes_write;
		}

		if (bytes_read == -1)
		{
			perror("HERCULES_ERR_READ_FILE");
			slog_error("HERCULES_ERR_READ_FILE: %s", old_path);
		}

		slog_info("[POSIX]. bytes write to %s = %ld/%ld", new_path, total_bytes, old_file_size);
		if (total_bytes != old_file_size)
		{
			slog_warn("Original file has %ld bytes but Hercules only wrote %ld", old_file_size, total_bytes);
		}

		// close open files.
		if (to_close)
		{
			if (close(fd_old) == -1)
			{
				slog_error("HERCULES_ERR_RENAME_CLOSE_FILE_OLD: %s", old_path);
				perror("HERCULES_ERR_RENAME_CLOSE_FILE_OLD");
			}
		}
		if (close(fd_new) == -1)
		{
			slog_error("HERCULES_ERR_RENAME_CLOSE_FILE_NEW: %s", new_path);
			perror("HERCULES_ERR_RENAME_CLOSE_FILE_NEW");
		}

		// free memory.
		free(old_file_buffer);
		return 0;
	}

	int HerculesMove(const char *given_old_path, const char *given_new_pathname, const char *hercules_path)
	{
		int ret = 0;
		char full_path[PATH_MAX] = {0}, name[PATH_MAX] = {0};
		int old_is_dir = 0;
		int new_is_dir = 0;
		int to_sub = 0;
		struct stat new_parentdir_stat_n;
		struct stat new_file_stat;
		struct stat old_file_stat;

		// open file system file.
		int fd_old = open(given_old_path, O_RDONLY);
		if (fd_old < 0)
		{ // file does not exist.
			slog_error("HERCULES_ERR_MOVE_OPEN_SYSTEM_FILE");
			return -1;
		}

		// get old file stat.
		if (__fxstat(1, fd_old, &old_file_stat) < 0)
		{
			perror("HERCULES_ERR_MOVE_STAT_SYSTEM_FILE");
			slog_error("HERCULES_ERR_MOVE_STAT_SYSTEM_FILE");
			return -1;
		}

		old_is_dir = S_ISDIR(old_file_stat.st_mode);
		slog_debug("old path %s is_dir? =%d", given_old_path, old_is_dir);

		// check if the parent directory of the new path exists.
		char last_parent_dir[URI_] = {0};
		int last_parent_offset = find_last_parent_dir((char *)hercules_path, last_parent_dir);
		slog_debug("last_parent_dir=%s, last_parent_offset=%d", last_parent_dir, last_parent_offset);
		if (strncmp(last_parent_dir, given_old_path, strlen(last_parent_dir)) && last_parent_offset > 0)
		{ // parent and old_rpath are not the same.
			// last_parent_offset == 0 means the hercules_path is on the root directory.
			// Due origin file (given_old_path) will be moved to a new directory
			// we check if the new parent directoy exists.
			ret = imss_getattr(last_parent_dir, &new_parentdir_stat_n);
			if (ret != 0)
			{
				slog_error("HERCULES_ERR_MOVE_DEST_PARENT_DIR_DOES_NOT_EXIST");
				return -ENOENT;
			}
			// here we know that the file will be move inside an existing subdirectory (not in the root).
			to_sub = 1;
		}
		else
		{
			// TODO: error handling.
		}

		// checks if the new path exists.
		int new_exists = 0;
		int fd_new = open(given_new_pathname, O_RDONLY);
		if (fd_new < 0)
		{ // file does not exist.
			// if the new path does not exists, new_is_dir takes the value of old_is_dir.
			new_is_dir = old_is_dir;
		}
		else
		{
			slog_debug("%s exists", given_new_pathname);
			if (__fxstat(1, fd_new, &new_file_stat) < 0)
			{
				perror("HERCULES_ERR_MOVE_STAT_NEW_FILE");
				slog_error("HERCULES_ERR_MOVE_STAT_NEW_FILE: %s", hercules_path);
				return -EAGAIN;
			}
			new_exists = 1;
			// if new path exists, checks if it directory.
			new_is_dir = S_ISDIR(new_file_stat.st_mode);
		}

		slog_debug("new path %s is_dir? =%d", given_new_pathname, new_is_dir);

		// get the name of the directory.
		// 	when the new path exists, we will use the basename of the old path.
		int pos = 0;
		const char *aux_path = NULL;
		if (new_exists)
		{
			aux_path = given_old_path;
		}
		else
		{
			aux_path = given_new_pathname;
		}
		slog_debug("new_exists=%d, aux_path=%s", new_exists, aux_path);
		// get the basename of the path.
		for (int c = 0; c < strlen(aux_path); ++c)
		{
			if (aux_path[c] == '/')
			{
				if (c + 1 < strlen(aux_path))
					pos = c;
			}
		}
		strncpy(name, aux_path + pos, strlen(aux_path) - pos);
		// concat the MOUNT POINT.
		strcpy(full_path, MOUNT_POINT);
		// if the new path exists, we concat the dir name (hercules_path + strlen("imss://")) to the full path.
		// this makes the old directory to be stored inside the existing directory.
		if (new_exists)
			strcat(full_path, hercules_path + strlen("imss://"));
		strcat(full_path, name);

		slog_debug("pos=%d, full_path=%s, given_old_path=%s, hercules_path=%s, last_parent_dir=%s\n", pos, full_path, given_old_path, hercules_path, last_parent_dir);
		// return -1;
		// }
		int op_not_allowed = 0;
		if (old_is_dir && new_is_dir)
		{ // old and new path is directory.
			// move the old directory into the new directory because it already exists.
			// create the new path to the given_old_path file because it will be moved to a directory.
			slog_debug("Moving directory %s to directory %s", given_old_path, full_path);
			// if (!strcmp(given_old_path, full_path))
			// {
			// 	ret = -EPERM;
			// 	return ret;
			// }

			// Creates the directory in Hercules.
			if (mkdir(full_path, 0700) == 0)
			{
				ret = read_directory(given_old_path, full_path);
			}
			else
			{
				perror("HERCULES_ERR_HERCULES_PATH_MKDIR_FULL_PATH");
				slog_error("HERCULES_ERR_HERCULES_PATH_MKDIR_FULL_PATH");
				ret = -EAGAIN;
			}
		}
		else if (old_is_dir && !new_is_dir)
		{ // old is a directory and new is a regular file: NOT POSSIBLE.
			// cannot overwrite non-directory 'FILENAME' with directory 'DIRNAME'.
			slog_debug("Cannot overwrite non-directory %s with directory %s", given_old_path, full_path);
			op_not_allowed = 1;
		}
		else if (!old_is_dir && new_is_dir)
		{ // old is a regular file and new is a directory.
		  // copy the old file into the directory.
			// TODO: check flags.
			aux_path = given_old_path;
			pos = 0;
			for (int c = 0; c < strlen(aux_path); ++c)
			{
				if (aux_path[c] == '/')
				{
					if (c + 1 < strlen(aux_path))
						pos = c;
				}
			}
			memset(name, 0, sizeof(name));
			strncpy(name, aux_path + pos, strlen(aux_path) - pos);
			char x[MAX_PATH] = {0};
			strncpy(x, given_new_pathname, sizeof(x) - strlen(x) - 1);
			if (aux_path[strlen(given_new_pathname)] != '/')
			{
				strncat(x, "/", sizeof(x) - strlen(x) - 1);
			}
			strncat(x, name, sizeof(x) - strlen(x) - 1);
			slog_debug("Moving regular file %s into the directory %s", given_old_path, x);
			ret = hercules_move_file_from_disk(fd_old, given_old_path, x);
			// unlink the file old file.
			// if it is the last link, it will be removed.
			// if (unlink(given_old_path) == -1)
			// {
			// 	perror("HERCULES_ERR_RENAME_UNLINK_FILE_OLD");
			// 	slog_error("HERCULES_ERR_RENAME_UNLINK_FILE_OLD: %s", hercules_path);
			// }
		}
		else if (!old_is_dir && !new_is_dir)
		{ // old is a regular file and new is a regular file.
			// TODO: check flags.
			slog_debug("Moving regular file %s to regular file %s", given_old_path, given_new_pathname);
			ret = hercules_move_file_from_disk(fd_old, given_old_path, given_new_pathname);
			// unlink the file old file.
			// if it is the last link, it will be removed.
			// if (unlink(given_old_path) == -1)
			// {
			// 	perror("HERCULES_ERR_RENAME_UNLINK_FILE_OLD");
			// 	slog_error("HERCULES_ERR_RENAME_UNLINK_FILE_OLD: %s", hercules_path);
			// }
		}

		if (op_not_allowed)
		{
			errno = EPERM;
			ret = -EPERM;
		}
		// TODO: checks for errors.
		return ret;
	}
	// else
	// { // new path does not exists.

	// }
	// }

	int imss_rename(char *old_path, char *new_path)
	{
		int ret = 0;
		char full_path[PATH_MAX] = {0}, name[PATH_MAX] = {0};
		int old_is_dir = 0;
		int new_is_dir = 0;
		int to_sub = 0;
		struct stat new_parentdir_stat_n;
		struct stat old_file_stat;
		struct stat new_file_stat;

		// check if new_path is the same as old_path
		if (!strcmp(old_path, new_path))
		{
			ret = -EPERM;
			return ret;
		}

		// check old_path if it is a directory.
		int old_exists = imss_getattr(old_path, &old_file_stat);
		if (old_exists < 0)
		{
			slog_error("HERCULES_ERR_IMSS_RENAME: Old path '%s' does not exist.", old_path);
			return -ENOENT;
		}
		old_is_dir = S_ISDIR(old_file_stat.st_mode);
		slog_debug("old path %s is_dir? =%d", old_path, old_is_dir);

		// check if the parent directory of the new path exists.
		char last_parent_dir[URI_] = {0};
		int last_parent_offset = find_last_parent_dir((char *)new_path, last_parent_dir);
		slog_debug("new path=%s, last_parent_dir=%s, last_parent_offset=%d", new_path, last_parent_dir, last_parent_offset);
		if (strncmp(last_parent_dir, old_path, strlen(last_parent_dir)) && last_parent_offset > 0)
		{ // parent and old_path are not the same.
			// last_parent_offset == 0 means the hercules_path is on the root directory.
			// Due origin file (old_path) will be moved to a new directory
			// we check if the new parent directoy exists.
			ret = imss_getattr(last_parent_dir, &new_parentdir_stat_n);
			if (ret != 0)
			{
				slog_error("HERCULES_ERR_IMSS_RENAME_DEST_PARENT_DIR_DOES_NOT_EXIST");
				return -ENOENT;
			}
			// here we know that the file will be move inside an existing subdirectory (not in the root).
			to_sub = 1;
		}
		else
		{
			// TODO: error handling.
		}

		// checks if the new path exists.
		int new_exists = imss_getattr(new_path, &new_file_stat);
		if (new_exists < 0)
		{ // file does not exist.
			// if the new path does not exists, new_is_dir takes the value of old_is_dir.
			new_is_dir = old_is_dir;
		}
		else
		{ // if new path exists, checks if it directory.
			slog_debug("%s exists", new_path);
			new_exists = 1;
			new_is_dir = S_ISDIR(new_file_stat.st_mode);
		}

		slog_debug("new path %s is_dir? =%d", new_path, new_is_dir);
		// get the name of the directory.
		// 	when the new path exists, we will use the basename of the old path.
		int pos = 0;
		const char *aux_path = NULL;
		if (new_exists)
		{
			aux_path = old_path;
		}
		else
		{
			aux_path = new_path;
		}
		slog_debug("new_exists=%d, aux_path=%s", new_exists, aux_path);
		// get the basename of the path.
		for (int c = 0; c < strlen(aux_path); ++c)
		{
			if (aux_path[c] == '/')
			{
				if (c + 1 < strlen(aux_path))
					pos = c;
			}
		}
		strncpy(name, aux_path + pos, strlen(aux_path) - pos);
		// concat the MOUNT POINT.
		strcpy(full_path, MOUNT_POINT);
		// if the new path exists, we concat the dir name (hercules_path + strlen("imss://")) to the full path.
		// this makes the old directory to be stored inside the existing directory.
		if (new_exists)
			strcat(full_path, new_path + strlen("imss://"));
		strcat(full_path, name);

		slog_debug("pos=%d, full_path=%s, given_old_path=%s, hercules_path=%s, last_parent_dir=%s\n", pos, full_path, old_path, new_path, last_parent_dir);
		
		// Check all cases.
		int op_not_allowed = 0;
		if (old_is_dir && new_is_dir)
		{ // old and new paths are directories.
			// move the old directory into the new directory because it already exists.
			// create the new path to the given_old_path file because it will be moved to a directory.
			slog_debug("Moving Hercules directory %s to directory %s", old_path, new_path);

			ConcatLastSlashC(new_path);
			// if (!strcmp(given_old_path, full_path))
			// {
			// 	ret = -EPERM;
			// 	return ret;
			// }

			map_rename_dir_dir(map, old_path, new_path);

			// RENAME LOCAL_IMSS(GARRAY), SRV_STAT(MAP & TREE)
			rename_dataset_metadata_dir_dir(old_path, new_path);

			// RENAME SRV_WORKER(MAP)
			rename_dataset_srv_worker_dir_dir(old_path, new_path, -1, 0);

		}
		else if (old_is_dir && !new_is_dir)
		{ // old is a directory and new is a regular file: NOT POSSIBLE.
			// cannot overwrite non-directory 'FILENAME' with directory 'DIRNAME'.
			slog_debug("Cannot overwrite non-directory %s with directory %s", old_path, full_path);
			op_not_allowed = 1;
		}
		else if (!old_is_dir && new_is_dir)
		{ // old is a regular file and new is a directory.
		  // copy the old file into the directory.
			// TODO: check flags.
			aux_path = old_path;
			pos = 0;
			// get the basename of the old regular file.
			for (int c = 0; c < strlen(aux_path); ++c)
			{
				if (aux_path[c] == '/')
				{
					if (c + 1 < strlen(aux_path))
						pos = c;
				}
			}
			pos++; // +1 to avoid the found slash.
			memset(name, 0, sizeof(name));
			strncpy(name, aux_path + pos, strlen(aux_path) - pos);
			slog_debug("name=%s, aux_path=%s, strlen(aux_path)=%d, pos=%d", name, aux_path, strlen(aux_path), pos);
			char destination_path[MAX_PATH] = {0};
			strncpy(destination_path, new_path, sizeof(destination_path) - strlen(destination_path) - 1);
			// if (aux_path[strlen(new_path)] != '/')
			// {
			// 	strncat(destination_path, "/", sizeof(destination_path) - strlen(destination_path) - 1);
			// }
			ConcatLastSlashC(destination_path);
			strncat(destination_path, name, sizeof(destination_path) - strlen(destination_path) - 1);
			slog_debug("Moving Hercules regular file %s into the Hercules directory %s", old_path, destination_path);

			if (!strcmp(old_path, destination_path))
			{
				ret = -EPERM;
				return ret;
			}

			// RENAME LOCAL_IMSS(GARRAY), SRV_STAT(MAP & TREE)
			ret = rename_dataset_metadata(old_path, destination_path);
			if (ret < 0)
			{
				// errno = EEXIST;
				return ret;
			}
			// RENAME SRV_WORKER(MAP)
			rename_dataset_srv_worker(old_path, destination_path, -1, 0);
			map_rename(map, old_path, destination_path);
		}
		else if (!old_is_dir && !new_is_dir)
		{ // old is a regular file and new is a regular file.
			// TODO: check flags.
			slog_debug("Moving Hercules regular file %s to Hercules regular file %s", old_path, new_path);

			imss_unlink(new_path);

			// printf("old_rpath=%s, new_rpath=%s\n",old_rpath, new_rpath);
			// TODO   map_rename_prefetch(map_prefetch, old_rpath, new_rpath);
			// RENAME LOCAL_IMSS(GARRAY), SRV_STAT(MAP & TREE)
			ret = rename_dataset_metadata(old_path, new_path);
			if (ret < 0)
			{
				errno = EEXIST;
				return ret;
			}
			// RENAME SRV_WORKER(MAP)
			rename_dataset_srv_worker(old_path, new_path, -1, 0);

			// replace the old name by the new one.
			// replace_dataset_entry_key(old_path, new_path);

			map_rename(map, old_path, new_path);
		}

		if (op_not_allowed)
		{
			errno = EPERM;
			ret = -EPERM;
		}
		else
		{
			ret = 0;
		}
		// TODO: checks for errors.
		return ret;

		// /********* */
		// slog_debug("old path=%s, new path=%s", old_path, new_path);
		// struct stat ds_stat_n;
		// int file_desc_o = -1, file_desc_n = -1;
		// int fd = 0;
		// char *old_rpath = old_path;
		// char *new_rpath = new_path;

		// // CHECKING IF IS MV DIR TO DIR
		// // check old_path if it is a directory if it is add / at the end
		// int ret = imss_getattr(old_path, &ds_stat_n);
		// slog_debug("after imss_getattr=%d, st_nlink=%lu", ret, ds_stat_n.st_nlink);
		// if (ret == 0)
		// {
		// 	// check if new_path is the same as old_path
		// 	if (!strcmp(old_path, new_path))
		// 	{
		// 		ret = -EPERM;
		// 		return ret;
		// 	}

		// 	slog_debug("%s is_dir? =%d", old_path, S_ISDIR(ds_stat_n.st_mode));
		// 	if (S_ISDIR(ds_stat_n.st_mode))
		// 	{

		// 		size_t len = strlen(old_rpath);
		// 		if (len > 0 && old_rpath[len - 1] != '/')
		// 		{
		// 			strcat(old_rpath, "/");
		// 		}

		// 		int fd = -1;
		// 		struct stat stats;
		// 		char *aux = NULL;
		// 		fd_lookup(old_rpath, &fd, &stats, &aux);
		// 		if (fd >= 0)
		// 			file_desc_o = fd;
		// 		else if (fd == -2)
		// 			return -ENOENT;
		// 		else
		// 			file_desc_o = open_dataset(old_rpath, 0);

		// 		if (file_desc_o < 0)
		// 		{

		// 			slog_error("HERCULES_ERR_IMSS_RENAME_CANNOT_OPEN_DATASET");
		// 			// free(new_rpath);
		// 			return -ENOENT;
		// 		}

		// 		// If origin path is a directory then we are in the case of mv dir to dir
		// 		// Extract destination directory from path
		// 		// int pos = 0;
		// 		// for (int c = strlen("imss://"); c < strlen(new_path); ++c)
		// 		// {
		// 		// 	if (new_path[c] == '/')
		// 		// 	{
		// 		// 		if (c + 1 < strlen(new_path))
		// 		// 			pos = c;
		// 		// 	}
		// 		// }
		// 		// char *dir_dest = (char *)calloc(MAX_PATH, sizeof(char));
		// 		// memcpy(dir_dest, &new_path[0], pos + 1);
		// 		// memcpy(dir_dest, &new_path[0], pos);

		// 		// char *rdir_dest = (char *)calloc(MAX_PATH, sizeof(char));
		// 		// get_iuri(dir_dest, rdir_dest);

		// 		// slog_debug("dir_dest=%s", dir_dest);
		// 		ret = imss_getattr(new_path, &ds_stat_n);
		// 		slog_debug("ret = %d", ret);
		// 		if (ret == 0)
		// 		{
		// 			if (S_ISDIR(ds_stat_n.st_mode))
		// 			{
		// 				// WE ARE IN MV DIR TO DIR
		// 				size_t len = strlen(new_path);
		// 				if (len > 0 && new_path[len - 1] != '/')
		// 				{
		// 					strcat(new_path, "/");
		// 				}

		// 				map_rename_dir_dir(map, old_rpath, new_path);
		// 				if (MULTIPLE_READ == 1)
		// 				{
		// 					map_rename_dir_dir_prefetch(map_prefetch, old_rpath, new_path);
		// 				}
		// 				// RENAME LOCAL_IMSS(GARRAY), SRV_STAT(MAP & TREE)
		// 				rename_dataset_metadata_dir_dir(old_rpath, new_path);

		// 				// RENAME SRV_WORKER(MAP)

		// 				rename_dataset_srv_worker_dir_dir(old_rpath, new_path, file_desc_o, 0);
		// 				// free(dir_dest);
		// 				// free(rdir_dest);
		// 				// free(old_rpath);
		// 				// free(new_rpath);
		// 				return 0;
		// 			}
		// 		}
		// 	}
		// }

		// // MV FILE TO FILE OR MV FILE TO DIR
		// // Assing file handler
		// // fd;
		// struct stat stats;
		// char *aux;
		// fd_lookup(old_rpath, &fd, &stats, &aux);

		// if (fd >= 0)
		// {
		// 	file_desc_o = fd;
		// 	slog_debug("stats from lookup, st_nlink=%lu", stats.st_nlink);
		// }
		// else if (fd == -2)
		// 	return -ENOENT;
		// else
		// 	file_desc_o = open_dataset(old_rpath, 0);

		// if (file_desc_o < 0)
		// {
		// 	slog_error("Cannot open dataset, old_rpath=%s", old_rpath);
		// 	return -ENOENT;
		// }

		// ret = imss_getattr(new_path, &ds_stat_n);
		// slog_debug("After imss_getattr, new_path=%s, ret=%d", new_path, ret);
		// if (ret == 0)
		// {
		// 	// fprintf(stderr, "**************EXISTE EL DESTINO=%s\n", new_path);
		// 	// printf("new_path[last]=%c\n",new_path[strlen(new_path) -1]);
		// 	slog_debug("%s is_dir? =%d", new_path, S_ISDIR(ds_stat_n.st_mode));
		// 	if (S_ISDIR(ds_stat_n.st_mode))
		// 	{
		// 		// create the new path for the origin file because it will be moved to a directory.
		// 		int pos = 0;
		// 		for (int c = 0; c < strlen(old_rpath); ++c)
		// 		{
		// 			if (old_rpath[c] == '/')
		// 			{
		// 				if (c + 1 < strlen(old_rpath))
		// 					pos = c;
		// 			}
		// 		}

		// 		char full_path[PATH_MAX], name[PATH_MAX];
		// 		strncpy(name, old_rpath + pos + 1, strlen(old_rpath) - pos);
		// 		strcpy(full_path, new_path);
		// 		strcat(full_path, name);
		// 		// printf("%d, %s\n", pos, full_path);
		// 		slog_debug("%d, full_path=%s, old_rpath=%s\n", pos, full_path, old_rpath);

		// 		if (!strcmp(old_rpath, full_path))
		// 		{
		// 			ret = -EPERM;
		// 			return ret;
		// 		}

		// 		// printf("old_rpath=%s, new_rpath=%s\n",old_rpath, new_rpath);
		// 		// TODO   map_rename_prefetch(map_prefetch, old_rpath, new_rpath);
		// 		// RENAME LOCAL_IMSS(GARRAY), SRV_STAT(MAP & TREE)
		// 		ret = rename_dataset_metadata(old_rpath, full_path);
		// 		if (ret < 0)
		// 		{
		// 			// errno = EEXIST;
		// 			return ret;
		// 		}
		// 		// RENAME SRV_WORKER(MAP)
		// 		rename_dataset_srv_worker(old_rpath, full_path, fd, 0);
		// 		map_rename(map, old_rpath, full_path);
		// 	}
		// 	else
		// 	{
		// 		// printf("**************TENGO QUE BORRARLO ES UN FICHERO=%s\n",new_path);
		// 		imss_unlink(new_path);

		// 		// printf("old_rpath=%s, new_rpath=%s\n",old_rpath, new_rpath);
		// 		// TODO   map_rename_prefetch(map_prefetch, old_rpath, new_rpath);
		// 		// RENAME LOCAL_IMSS(GARRAY), SRV_STAT(MAP & TREE)
		// 		ret = rename_dataset_metadata(old_rpath, new_rpath);
		// 		if (ret < 0)
		// 		{
		// 			errno = EEXIST;
		// 			return ret;
		// 		}
		// 		// RENAME SRV_WORKER(MAP)
		// 		rename_dataset_srv_worker(old_rpath, new_rpath, fd, 0);

		// 		// replace the old name by the new one.
		// 		replace_dataset_entry_key(old_rpath, new_rpath);

		// 		map_rename(map, old_rpath, new_rpath);
		// 	}
		// }
		// else
		// {
		// 	/// fprintf(stderr, "**************NO EXISTE EL DESTINO=%s\n", new_path);
		// 	slog_error("HERCULES_ERR_IMSS_RENAME_DEST_DOES_NOT_EXIST");
		// 	// printf("old_rpath=%s, new_rpath=%s\n",old_rpath, new_rpath);
		// 	// TODO   map_rename_prefetch(map_prefetch, old_rpath, new_rpath);
		// 	// RENAME LOCAL_IMSS(GARRAY), SRV_STAT(MAP & TREE)
		// 	char last_parent_dir[URI_] = {0};
		// 	int last_parent_offset = find_last_parent_dir((char *)new_path, last_parent_dir);
		// 	slog_debug("last_parent_dir=%s, last_parent_offset=%d", last_parent_dir, last_parent_offset);
		// 	if (strncmp(last_parent_dir, old_rpath, strlen(last_parent_dir)) && last_parent_offset > 0)
		// 	{ // parent and old_rpath are not the same.
		// 		// last_parent_offset == 0 means the new_path is on the root directory.
		// 		// Due origin file (old_rpath) will be moved to a new directory
		// 		// we check if the new parent directoy exists.
		// 		ret = imss_getattr(last_parent_dir, &ds_stat_n);
		// 		if (ret != 0)
		// 		{
		// 			slog_error("HERCULES_ERR_RENAME_DEST_PARENT_DIR_DOES_NOT_EXIST");
		// 			return ret;
		// 		}
		// 	}

		// 	if (!strcmp(old_rpath, new_path))
		// 	{
		// 		ret = -EPERM;
		// 		return ret;
		// 	}

		// 	ret = rename_dataset_metadata(old_rpath, new_rpath);
		// 	if (ret < 0)
		// 	{
		// 		// errno = EEXIST;
		// 		return ret;
		// 	}
		// 	// RENAME SRV_WORKER(MAP)
		// 	rename_dataset_srv_worker(old_rpath, new_rpath, fd, 0);
		// 	map_rename(map, old_rpath, new_rpath);
		// 	// return res;
		// }

		// // free(old_rpath);
		// // free(new_rpath);
		// return 0;
	}

#ifdef __cplusplus
}
#endif