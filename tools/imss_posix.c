#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#include "slog.h"
#include <cstdlib>
#include <pthread.h>
#include <unistd.h>
#endif

#include "imss_posix.h"

#define __USE_FILE_OFFSET64

#undef __USE_GNU

// #define KB 1024
// #define GB 1073741824
// uint32_t DEPLOYMENT = 2; // Default 1=ATACHED, 0=DETACHED ONLY METADATA SERVER 2=DETACHED METADATA AND DATA SERVERS
char POLICY[MAX_POLICY_LEN];
uint64_t IMSS_SRV_PORT = 1; // Not default, 1 will fail
uint64_t METADATA_PORT = 1; // Not default, 1 will fail
extern int32_t N_SERVERS;
int32_t N_BLKS = 1; // Default 1
int32_t N_META_SERVERS = 1;
char *METADATA_FILE = NULL; // Not default
char *IMSS_HOSTFILE = NULL; // Not default
extern char *IMSS_ROOT;
extern size_t IMSS_ROOT_LEN;
char *META_HOSTFILE = NULL;
uint64_t STORAGE_SIZE = 16;   // In GB
uint64_t META_BUFFSIZE = 16;  // In GB
uint64_t IMSS_BLKSIZE = 1024; // In KB
// uint64_t IMSS_BUFFSIZE = 2;			 // In GB
uint64_t IMSS_DATA_BSIZE = 512 * KB; // In Bytes.
int32_t REPL_FACTOR = NONE;	     // Default none
int32_t REPL_TYPE = ASYNC;	     // Default async

extern int32_t MALLEABILITY;
// extern int32_t MALLEABILITY_TYPE;
extern int32_t UPPER_BOUND_SERVERS;
extern int32_t LOWER_BOUND_SERVERS;

uint16_t PREFETCH = 6;

uint16_t threshold_read_servers = 5;
uint16_t BEST_PERFORMANCE_READ = 0; // if 1    then n_servers < threshold => SREAD, else if n_servers > threshold => SPLIT_READV
// if 0 only one method of read applied specified in MULTIPLE_READ

uint16_t MULTIPLE_READ = 0;  // 1=vread with prefetch, 2=vread without prefetch, 3=vread_2x 4=imss_split_readv(distributed) else sread
uint16_t MULTIPLE_WRITE = 0; // 1=writev(only 1 server), 2=imss_split_writev(distributed) else swrite
char prefetch_path[256];
int32_t prefetch_first_block = -1;
int32_t prefetch_last_block = -1;
int32_t prefetch_pos = 0;
pthread_t prefetch_t;
int16_t prefetch_ds = 0;
int32_t prefetch_offset = 0;
char *prefetch_uri = NULL;

pthread_cond_t cond_prefetch;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
// pthread_mutex_t lock2 = PTHREAD_MUTEX_INITIALIZER;
// pthread_mutex_t system_lock = PTHREAD_MUTEX_INITIALIZER;

int LD_PRELOAD = 0;
// void *map = NULL;
void *hierarchical_map = NULL;
void *map_prefetch;
char *MOUNT_POINT;
size_t MOUNT_POINT_LEN = 0;

char *HERCULES_PATH;
void *map_fd = NULL;
extern struct arguments args;

uint32_t rank = -1;
static int init = 0;

// log path.
char log_path[PATH_MAX] = {0};
pid_t g_pid = -1;

// prefech.
char *buf_pref = NULL;

// #ifdef __cplusplus
// extern "C"
// {
// #endif

void SetErrno(int value)
{
	if (value >= 0)
	{
		// expected negative errno value.
		fprintf(stderr, "HERCULES_WARN_SET_ERRNO: Expected negative value for errno.\n");
		return;
	}
	errno = -value;
}

void copy_stat_to_statx(const struct stat *src, struct statx *dest)
{
	if (!src || !dest)
	{
		return; // Handle null pointers
	}

	memset(dest, 0, sizeof(struct statx)); // Initialize to zero

	dest->stx_mask = STATX_BASIC_STATS; // Indicates which fields are valid
	dest->stx_blksize = src->st_blksize;
	dest->stx_attributes = 0; // stat does not provide equivalent attributes

	dest->stx_dev_major = major(src->st_dev);
	dest->stx_dev_minor = minor(src->st_dev);
	dest->stx_rdev_major = major(src->st_rdev);
	dest->stx_rdev_minor = minor(src->st_rdev);

	dest->stx_mode = src->st_mode;
	dest->stx_uid = src->st_uid;
	dest->stx_gid = src->st_gid;
	dest->stx_ino = src->st_ino;
	dest->stx_size = src->st_size;
	dest->stx_blocks = src->st_blocks;
	dest->stx_nlink = src->st_nlink;

	// Convert timestamps
	dest->stx_atime.tv_sec = src->st_atime;
	dest->stx_mtime.tv_sec = src->st_mtime;
	dest->stx_ctime.tv_sec = src->st_ctime;
#ifdef _STATX_BTIME
	dest->stx_btime.tv_sec = 0;
#endif
}

int __fxstat(int ver, int fd, struct stat *buf);

void WarnOperationNotSupported(const char *call_name, const char *pathname)
{
	slog_warn("[POSIX] '%s' not currently supported, using real '%s' for pathname=%s\n", call_name, call_name, pathname);
	fprintf(stdout, "[POSIX][WARNING] '%s' not currently supported, using real '%s' for pathname=%s\n", call_name, call_name, pathname);
	fflush(stdout);
}

void checkOpenFlags(const char *pathname, int flags)
{
	slog_live("Checking flags");
	if (flags & O_CREAT)
	{
		slog_live("[POSIX]. O_CREAT flag, pathname=%s, flags=%x, O_CREAT=%x", pathname, flags, O_CREAT);
		// fprintf(stderr, "[POSIX]. O_CREAT flag, pathname=%s, flags=%x, O_CREAT=%x\n", pathname, flags, O_CREAT);
	}
	if (flags & O_TRUNC)
	{
		slog_live("[POSIX]. O_TRUNC flag, pathname=%s, flags=%x, O_TRUNC=%x", pathname, flags, O_TRUNC);
		// fprintf(stderr, "[POSIX]. O_TRUNC flag, pathname=%s, flags=%x, O_TRUNC=%x\n", pathname, flags, O_TRUNC);
	}
	if (flags & O_EXCL)
	{

		slog_live("[POSIX]. O_EXCL flag, pathname=%s, flags=%x, O_EXCL=%x", pathname, flags, O_EXCL);
		// fprintf(stderr, "[POSIX]. O_EXCL flag, pathname=%s, flags=%x, O_EXCL=%x\n", pathname, flags, O_EXCL);
	}
	if (flags & O_RDONLY)
	{

		slog_live("[POSIX]. O_RDONLY flag, pathname=%s\n", pathname);
		// fprintf(stderr, "[POSIX]. O_RDONLY flag, pathname=%s\n", pathname);
	}
	if (flags & O_WRONLY)
	{

		slog_live("[POSIX]. O_WRONLY flag, pathname=%s, flags=%x, O_WRONLY=%x", pathname, flags, O_WRONLY);
		// fprintf(stderr, "[POSIX]. O_WRONLY flag, pathname=%s, flags=%x, O_WRONLY=%x\n", pathname, flags, O_WRONLY);
	}
	if (flags & O_RDWR)
	{

		slog_live("[POSIX]. O_RDWR flag, pathname=%s, flags=%x, O_RDWR=%x", pathname, flags, O_RDWR);
		// fprintf(stderr, "[POSIX]. O_RDWR flag, pathname=%s, flags=%x, O_RDWR=%x\n", pathname, flags, O_RDWR);
	}
	if (flags & O_APPEND)
	{

		slog_live("[POSIX]. O_APPEND flag, pathname=%s, flags=%x, O_APPEND=%x", pathname, flags, O_APPEND);
		// fprintf(stderr, "[POSIX]. O_RDWR flag, pathname=%s, flags=%x, O_RDWR=%x\n", pathname, flags, O_RDWR);
	}
	if (flags & O_NONBLOCK)
	{

		slog_live("[POSIX]. O_NONBLOCK flag, pathname=%s, flags=%x, O_NONBLOCK=%x", pathname, flags, O_NONBLOCK);
		// fprintf(stderr, "[POSIX]. O_RDWR flag, pathname=%s, flags=%x, O_RDWR=%x\n", pathname, flags, O_RDWR);
	}
	if (flags & O_SYNC)
	{

		slog_live("[POSIX]. O_SYNC flag, pathname=%s, flags=%x, O_SYNC=%x", pathname, flags, O_SYNC);
		// fprintf(stderr, "[POSIX]. O_RDWR flag, pathname=%s, flags=%x, O_RDWR=%x\n", pathname, flags, O_RDWR);
	}
	if (flags & O_PATH)
	{

		slog_live("[POSIX]. O_PATH flag, pathname=%s, flags=%x, O_PATH=%x", pathname, flags, O_PATH);
		// fprintf(stderr, "[POSIX]. O_RDWR flag, pathname=%s, flags=%x, O_RDWR=%x\n", pathname, flags, O_RDWR);
	}
}

void ResolvePathsAndFD(const int fd_dir, const char *path_to_check, std::string &directory_path, char **file_path)
{
	// int ret = 0;
	// AT_FDCWD (fd = -100) means current working directory.
	if (fd_dir == AT_FDCWD)
	{ // checks if the path_to_check is from Hercules.
		*file_path = checkHerculesPath(path_to_check);
		directory_path = "";
	}
	else
	{ // checks if the file descriptor is from a Hercules directory.
		// std::string directory_path_ob
		directory_path = map_fd_search_by_val(map_fd, fd_dir);
		// slog_debug("path found=%s", directory_path.c_str());
		// *directory_path = directory_path_ob.c_str();
		if (!directory_path.empty())
		{
			slog_debug("directory path found in the map: %s, fd=%d", directory_path.c_str(), fd_dir);
		}
		// else
		// {
		// 	slog_debug("file descriptor %d is not a Hercules directory", fd_dir);
		// }

		*file_path = NULL;
	}
	// return ret;
}

char *checkHerculesPath(const char *pathname)
{
	char *new_path = NULL;
	char *workdir = getenv("PWD");
	char absolute_pathname[PATH_MAX] = {0};
	int ret = 0;

	// To check if pathname or MOUNT_POINT are not NULL.
	if (!pathname || !MOUNT_POINT)
	{
		perror("HERCULES_ERR_NULL_PATH");
		slog_fatal("HERCULES_ERR_NULL_PATH, pathname=%s, MOUNT_POINT=%s", pathname, MOUNT_POINT);
		return NULL;
	}

	// if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT) - 1)) // error when  pathname=/mnt/hercules/data/unet3d and MOUNT_POINT=/mnt/hercules,
	// if (!strncmp(pathname, MOUNT_POINT, strlen(pathname) - 1))
	size_t pathname_len = strlen(pathname);
	// size_t max_lenght = MAX(pathname_len, strlen(MOUNT_POINT));
	size_t compare_len = 0;

	// To calculate the string lenght to be compared to know if the user want to intercept
	// the HERCULES root (MOUNT_POINT).
	// For example: if MOUNT_POINT=/mnt/hercules/ and pathname=/mnt/hercules/x
	// then, the correct lenght must be the one of pathname (15).
	// But, if pathname=/mnt/hercules, the max lenght to be compared is
	// the MOUNT_POINT lenght minus 1 because we do not want to consider the last "/".
	// In this last case we will make the comparition of MOUNT_POINT=/mnt/hercules and
	//  pathname=/mnt/hercules.
	if (pathname_len >= MOUNT_POINT_LEN)
	{
		if (pathname[pathname_len - 1] == '/')
		{
			compare_len = pathname_len - 1;
		}
		else
		{
			compare_len = pathname_len;
		}
	}
	else
	{
		if (MOUNT_POINT[MOUNT_POINT_LEN - 1] == '/')
		{
			compare_len = MOUNT_POINT_LEN - 1;
		}
		else
		{
			compare_len = MOUNT_POINT_LEN;
		}
	}

	// fprintf(stderr, "[HERCULES] compare_len=%zu, pathname=%s, MOUNT_POINT=%s\n", compare_len, pathname, MOUNT_POINT);
	// To check if "pathname" is the root of HERCULES.
	if (!strncmp(pathname, MOUNT_POINT, compare_len))
	{
		// fprintf(stderr, "root path\n");
		slog_live("[HERCULES] pathname=%s, MOUNT_POINT=%s, pathname=%s, Success", pathname, MOUNT_POINT, pathname);
		new_path = (char *)calloc(strlen("imss://") + 1, sizeof(char));
		if (!new_path)
		{
			perror("HERCULES_ERR_ALLOC_MEMORY_NEW_PATH");
			slog_fatal("HERCULES_ERR_ALLOC_MEMORY_NEW_PATH");
			return NULL;
		}

		strcpy(new_path, "imss://");
	}
	else
	{ // To check if "pathname" is part of HERCULES.
		if (!strncmp(pathname, MOUNT_POINT, strlen(MOUNT_POINT) - 1) || (pathname[0] != '/' && !strncmp(workdir, MOUNT_POINT, strlen(MOUNT_POINT) - 1)))
		{
			if (!strncmp(pathname, ".", strlen(pathname)))
			{
				slog_live("[HERCULES] pathname=%s, workdir=%s", pathname, workdir);
				new_path = convert_path(workdir);
			}
			else if (!strncmp(pathname, "./", strlen("./")))
			{
				slog_live("[HERCULES] ./ case=%s", pathname);
				new_path = convert_path(pathname + strlen("./"));
			}
			else
			{
				// slog_live("[HERCULES] after resolve path, pathname=%s, real_pathname=%s", pathname, real_pathname);
				ret = ResolvePath(pathname, absolute_pathname);
				// fprintf(stderr, "[IMSS] last option, pathname=%s, absolute_pathname=%s, absolute_pathname_len=%d, workdir=%s\n", pathname, absolute_pathname, ret,  workdir);
				if (ret > 0)
				{ // absolute path.
					// slog_live("[IMSS] absolute_pathname=%s", absolute_pathname);
					new_path = convert_path(absolute_pathname);
				}
				else
				{
					new_path = convert_path(pathname);
				}

				// slog_live("[HERCULES] pathname=%s, absolute_pathname=%s, new_path=%s", pathname, absolute_pathname,new_path);
				//  free(real_pathname);
			}
		}
	}
	// slog_live("[HERCULES] pathname=%s, new_path=%s", pathname, new_path);
	return new_path;
}

uint32_t MurmurOAAT32(const char *key)
{
	uint32_t h = 335ul;
	for (; *key; ++key)
	{
		h ^= *key;
		h *= 0x5bd1e995;
		h ^= h >> 15;
	}
	return (uint32_t)abs((int)h);
}

char *convert_path(const char *name)
{
	char *path = (char *)calloc(PATH_MAX, sizeof(char));
	char *original_path = path; // To free later
	char *new_path = (char *)calloc(PATH_MAX, sizeof(char));

	if (path == NULL || new_path == NULL)
	{
		perror("HERCULES_ERR_IMSS_POSIX_CONVERT_PATH_MEMORY_ERR");
		slog_error("HERCULES_ERR_IMSS_POSIX_CONVERT_PATH_MEMORY_ERR");
		exit(-1);
	}

	strncpy(path, name, PATH_MAX);
	// fprintf(stderr, "Received name=%s\n", name);
	size_t len = strlen(MOUNT_POINT);
	// remove MOUNT_POINT prefix from the path.
	if (len > 0)
	{
		char *p = path;
		while ((p = strstr(p, MOUNT_POINT)) != NULL)
		{
			memmove(p, p + len, strlen(p + len) + 1);
		}
	}

	// seeks initial slashes "/" in the path.
	len = strlen(path);
	size_t desplacements = 0;
	for (size_t i = 0; i < len; i++)
	{
		if (!strncmp(path + i, "/", strlen("/")))
		{
			// fprintf(stderr,"Increasing desplacements\n");
			desplacements++;
		}
		else
		{
			// path += desplacements;
			// strcat(new_path, "imss://");
			break;
		}
	}
	// deletes initial slashes "/" from the path.
	if (desplacements > 0)
	{
		path += desplacements;
	}
	// add the URL to the new path.
	// strcat(new_path, "imss://");
	strncpy(new_path, "imss://", PATH_MAX);

	size_t len_path = strlen(path);
	if (path[len_path - 1] == '/')
	{
		// deletes the last slash.
		path[len_path - 1] = '\0';
	}

	// add the path to the new_path, which has the URL prefix.
	if (desplacements < len)
	{
		strcat(new_path, path);
	}

	free(original_path);

	new_path[PATH_MAX - 1] = '\0';

	return new_path;
}

uint32_t GetRank()
{
	// Getting a mostly unique id for the distributed deployment.
	char hostname_[512] = {0}, hostname[1024] = {0};
	int ret = gethostname(&hostname_[0], 512);
	if (ret == -1)
	{
		perror("HERCULES_ERR_FRONTEND_GETHOSTNAME");
		exit(EXIT_FAILURE);
	}
	sprintf(hostname, "%s:%d", hostname_, getpid());
	g_pid = getpid();

	return MurmurOAAT32(hostname);
}

__attribute__((constructor)) void imss_posix_init(void)
{

	struct timeval start, end;
	int ret = 0;
	gettimeofday(&start, NULL);

	slog_time("Info,,Rank,Function,Time(msec),Comment");

	map_fd = map_fd_create();

	rank = GetRank();

	// fill global variables with the enviroment variables value.
	ret = getConfiguration(&args);
	if (ret == -1)
	{
		exit(EXIT_FAILURE);
	}

	IMSS_BLKSIZE = args.block_size;
	IMSS_DATA_BSIZE = IMSS_BLKSIZE * KB; // block size in bytes.
	MOUNT_POINT = args.mount_point;
	MOUNT_POINT_LEN = strlen(MOUNT_POINT);
	IMSS_ROOT = args.imss_uri;
	IMSS_ROOT_LEN = strlen(IMSS_ROOT);
	IMSS_HOSTFILE = args.data_hostfile;
	N_SERVERS = args.num_data_servers;
	IMSS_SRV_PORT = args.data_port;
	// IMSS_BUFFSIZE = args.bufsize;
	META_HOSTFILE = args.meta_hostfile;
	METADATA_PORT = args.stat_port;
	N_META_SERVERS = args.num_metadata_servers;
	STORAGE_SIZE = args.storage_size;
	MALLEABILITY = args.malleability;
	// MALLEABILITY_TYPE = args.malleability_type;
	UPPER_BOUND_SERVERS = args.upper_bound_servers;
	LOWER_BOUND_SERVERS = args.lower_bound_servers;
	REPL_FACTOR = args.repl_factor;
	REPL_TYPE = args.repl_type;
	strncpy(POLICY, args.policy, sizeof(POLICY));

	// log init.
	time_t t = time(NULL);
	struct tm tm = *localtime(&t);
	sprintf(log_path, "client-thread-%ld.%02d-%02d.%d", pthread_self(), tm.tm_hour, tm.tm_min, getpid());
	// {
	// 	fprintf(stderr, "LOG PATH= %s\n", log_path); // this line raise an exception running a python app with threads.
	// }
	slog_init(log_path, args.logging.hercules_debug_level, args.logging.hercules_debug_file, args.logging.hercules_debug_screen, 1, 1, 1, rank);

	// if (args.logging.hercules_debug_file > 0)
	// {
	// 	printf("Log path = %s\n", log_path);
	// 	fflush(stdout);
	// }
	// slog_info(",Time(msec), Comment, RetCode");
	slog_time("%d,TIMING,Time(s),Msg", rank);

	slog_live(" -- HERCULES_MOUNT_POINT: %s", MOUNT_POINT);
	slog_live(" -- HERCULES_ROOT: %s", IMSS_ROOT);
	slog_live(" -- HERCULES_HOSTFILE: %s", IMSS_HOSTFILE);
	slog_live(" -- HERCULES_N_SERVERS: %d", N_SERVERS);
	slog_live(" -- HERCULES_SRV_PORT: %d", IMSS_SRV_PORT);
	// slog_live(" -- HERCULES_BUFFSIZE: %ld", IMSS_BUFFSIZE);
	slog_live(" -- META_HOSTFILE: %s", META_HOSTFILE);
	slog_live(" -- HERCULES_META_PORT: %d", METADATA_PORT);
	slog_live(" -- HERCULES_META_SERVERS: %d", N_META_SERVERS);
	slog_live(" -- HERCULES_BLKSIZE: %ld kB", IMSS_BLKSIZE);
	slog_live(" -- HERCULES_STORAGE_SIZE: %ld GB", STORAGE_SIZE);
	slog_live(" -- HERCULES_METADATA_FILE: %s", METADATA_FILE);
	// slog_live(" -- HERCULES_DEPLOYMENT: %d", DEPLOYMENT);
	slog_live(" -- HERCULES_MALLEABILITY: %d", MALLEABILITY);
	// slog_live(" -- HERCULES_MALLEABILITY_TYPE: %d", MALLEABILITY_TYPE);
	slog_live(" -- UPPER_BOUND_SERVERS: %d", UPPER_BOUND_SERVERS);
	slog_live(" -- LOWER_BOUND_SERVERS: %d", LOWER_BOUND_SERVERS);
	slog_live(" -- REPL_FACTOR: %d", REPL_FACTOR);
	slog_live(" -- REPL_TYPE: %d", REPL_TYPE);
	slog_live(" -- POLICY: %s", POLICY);
	slog_live(" -- SYNC: %d", ASYNC_IO);
	// fprintf(stderr, "ASYNC IO %d\n", ASYNC_IO);

	// Metadata server
	if (TIMING(stat_init(META_HOSTFILE, METADATA_PORT, N_META_SERVERS, rank), "stat init", int32_t, rank) == -1)
	{
		// In case of error notify and exit
		slog_error("Stat init failed, cannot connect to Metadata server.");
		// imss_comm_cleanup();
		exit(1);
	}

	uint32_t num_active_storages = 0;
	// Hercules init -- Attached deploy
	// default is 2.
	// if (DEPLOYMENT == 1)
	// {
	// 	// Hercules init -- Attached deploy
	// 	if (hercules_init(0, STORAGE_SIZE, IMSS_SRV_PORT, 1, METADATA_PORT, META_BUFFSIZE, METADATA_FILE) == -1)
	// 	{
	// 		// In case of error notify and exit
	// 		slog_fatal("[IMSS-FUSE]	Hercules init failed, cannot deploy IMSS.\n");
	// 	}
	// }
	// if (DEPLOYMENT == 2)
	// {
	// Make the connection to all data servers.
	int ret_open_imss = TIMING(open_imss(IMSS_ROOT, &num_active_storages), "open imss", int32_t, rank);
	if (ret_open_imss < 0)
	{
		slog_fatal("Error creating HERCULES's resources, the process cannot be started.");
		printf("Error creating HERCULES's resources, the process cannot be started. Please, make sure servers are running and clients can establish connections.\n");
		// return;
		exit(1);
	}

	if (num_active_storages != N_SERVERS)
	{
		fprintf(stderr, "Number of storage servers does not match from the values retrieved from the metadata server. Setting from %d to %d\n", N_SERVERS, num_active_storages);
		slog_warn("Number of storage servers does not match from the values retrieved from the metadata server. Setting from %d to %d\n", N_SERVERS, num_active_storages);
		N_SERVERS = num_active_storages;
	}
	// }

	// if (DEPLOYMENT != 2)
	// {
	// 	// Initialize the IMSS servers
	// 	if (init_imss(IMSS_ROOT, IMSS_HOSTFILE, META_HOSTFILE, N_SERVERS, IMSS_SRV_PORT, IMSS_BUFFSIZE, DEPLOYMENT, "hercules_server", METADATA_PORT) < 0)
	// 	{
	// 		slog_fatal("[IMSS-FUSE]	IMSS init failed, cannot create servers.\n");
	// 	}
	// }

	// map_prefetch = map_create_prefetch();

	// Map used to store "stats" of the datasets.
	// stats_map = map_create();
	// void *root_stat_map = map_create();
	// Hierarchical map used to store the "stats" maps.
	// std::string root_;
	// root_.assign(IMSS_ROOT);
	hierarchical_map = HierarchicalMapCreate(std::string(IMSS_ROOT));
	// Store the Hercules root.
	// hierarchical_map[IMSS_ROOT] = (Map *)root_stat_map;
	// struct stat fakestat;
	// hierarchical_map_put(hierarchical_map, IMSS_ROOT, 0, fakestat, NULL);

	if (MULTIPLE_READ == 1)
	{
		int ret = 0;
		pthread_attr_t tattr;
		ret = pthread_attr_init(&tattr);
		ret = pthread_attr_setdetachstate(&tattr, PTHREAD_CREATE_DETACHED);

		if (pthread_create(&prefetch_t, &tattr, prefetch_function, NULL) == -1)
		{
			perror("ERRIMSS_PREFETCH_DEPLOY");
			pthread_exit(NULL);
		}
	}

	slog_live("IMSS EXIST=%d\n", is_alive(IMSS_ROOT));
	// slog_live("[CLIENT %d] ready!\n", rank);

	gettimeofday(&end, NULL);
	long seconds, useconds;
	double elapsed;
	seconds = end.tv_sec - start.tv_sec;
	useconds = end.tv_usec - start.tv_usec;
	elapsed = seconds + useconds / 1e6;

	slog_live("Client %d ready in %f sec.\n", rank, elapsed);
	// fprintf(stdout, "[%s] Client %d/%d ready in %f sec.\n", args.data_hostname, rank, getpid(), elapsed);
	// slog_debug("init before assigment=%d\n", init);
	init = 1;
	// fprintf(stderr, "\033[0;31m The number of active servers is %d \033[0m \n", num_active_storages);
}

void __attribute__((destructor)) run_me_last()
{
	errno = 0;
	slog_live("Calling 'run_me_last', pid=%d, rank=%d", g_pid, rank);
	// clock_t t_s;
	// double time_taken;
	// t_s = clock();
	slog_live("[POSIX] release_imss()");
	// releases endpoints created with the DATA servers.
	release_imss("imss://", CLOSE_DETACHED);
	slog_live("[POSIX] stat_release()");
	// releases endpoints created with the METADATA servers.
	stat_release();
	// free_prefetch(map_prefetch);
	imss_comm_cleanup();
	//   t_s = clock() - t_s;
	// map_free(map);
	HierarchicalMapFree(hierarchical_map);
	// map_destroy(map);
	HierarchicalMapDestroy(hierarchical_map);
	//   time_taken = ((double)t_s) / (CLOCKS_PER_SEC);
	// fprintf(stderr, "End 'run_me_last', pid=%d, release=%d\n", g_pid, release);
	map_fd_destroy(map_fd);

	slog_live("End 'run_me_last', pid=%d", g_pid);
	init = 0;
	// slog_close();
}

// void check_ld_preload(void)
// {

// 	if (LD_PRELOAD == 0)
// 	{
// 		// DPRINT("\nActivating... ld_preload=%d\n\n", LD_PRELOAD);
// 		fprintf(stderr, "\nActivating... ld_preload\n");
// 		LD_PRELOAD = 1;
// 		imss_posix_init();
// 	}
// }

int close(int fd)
{
	if (!real_close)
		real_close = (int (*)(int))dlsym(RTLD_NEXT, "close");

	if (!init)
	{
		return real_close(fd);
	}

	errno = 0;
	int ret = 0;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'close', pathname=%s, fd=%d", pathname, fd);
		if (!strcmp(pathname, MOUNT_POINT))
		{
			slog_debug("Trying to close the mount point %s", pathname);
			// stores the file descriptor "ret" into the map "map_fd".
			slog_live("[POSIX] Erasing fd %d from map", ret);
			map_fd_erase(map_fd, fd);
			return ret;
		}
		// fprintf(stderr, "[POSIX]. Calling Hercules 'close', pathname=%s, fd=%d\n", pathname, fd);
		ret = imss_close(pathname, fd);
		if (ret)
		{
			// close() returns zero on success.  On error, -1 is returned, and errno is set to indicate the error.
			ret = 0;
		}
		// Set offset to 0.
		// map_fd_update_value(map_fd, pathname, fd, 0);
		map_fd_erase(map_fd, fd);
		// TODO: check this.
		// if (real_close(fd) == -1)
		// {
		// 	slog_error("Cannot close aux fd used by HERCULES %d", fd);
		// 	errno = 0;
		// }
		slog_live("[POSIX]. Ending Hercules 'close', pathname=%s, ret=%d\n", pathname, ret);
		// free(pathname);
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'close', fd=%d", fd);
		ret = real_close(fd);
		slog_full("[POSIX]. Ending Real 'close', fd=%d, ret=%d", fd, ret);
	}
	return ret;
}

int __lxstat(int ver, const char *pathname, struct stat *buf)
{
	if (!real__lxstat)
		real__lxstat = (int (*)(int, const char *, struct stat *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real__lxstat(ver, pathname, buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules '__lxstat', pathname=%s, new_path=%s, ver=%d", pathname, new_path, ver);
		// imss_refresh(new_path);
		// if (!strncmp(new_path, "Success", strlen("Success")))
		// {
		// 	ret = 0;
		// }
		// else
		{
			ret = imss_refresh(new_path);
			// if (ret < 0)
			// {
			// 	errno = -ret;
			// 	ret = -1;
			// 	// perror("ERRIMSS_ACCESS_IMSSREFRESH");
			// }
			// else
			// {
			ret = imss_getattr(new_path, buf);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
			}
			// }
		}
		slog_live("[POSIX]. End Hercules '__lxstat', pathname=%s, new_path=%s, ver=%d, ret=%d, file_size=%lu\n", pathname, new_path, ver, ret, buf->st_size);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling real '__lxstat', pathname=%s", pathname);
		ret = real__lxstat(ver, pathname, buf);
		slog_full("[POSIX]. End real '__lxstat', pathname=%s", pathname);
	}

	return ret;
}

int __lxstat64(int fd, const char *pathname, struct stat64 *buf)
{
	if (!real__lxstat64)
		real__lxstat64 = (int (*)(int, const char *, struct stat64 *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real__lxstat64(fd, pathname, buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules '__lxstat64', pathname=%s", pathname);

		imss_refresh(new_path);
		ret = imss_getattr(new_path, (struct stat *)buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		slog_live("[POSIX]. End Hercules '__lxstat64', ret=%d, errno=%d:%s", ret, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		// fprintf(stderr, "[POSIX]. Calling Real '__lxstat64', pathname=%s\n", pathname);
		slog_full("[POSIX]. Calling Real '__lxstat64', pathname=%s", pathname);
		ret = real__lxstat64(fd, pathname, buf);
		slog_full("[POSIX]. End Real '__lxstat64', pathname=%s", pathname);
		// fprintf(stderr, "[POSIX]. Ending Real '__lxstat64', pathname=%s, ret=%d\n", pathname, ret);
	}

	return ret;
}

int __xstat(int ver, const char *pathname, struct stat *stat_buf)
{
	if (!real___xstat)
		real___xstat = (int (*)(int, const char *, struct stat *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real___xstat(ver, pathname, stat_buf);
	}

	errno = 0;
	int ret = -1;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX] Calling Hercules '__xstat', pathname=%s, ver=%d, new_path=%s", pathname, ver, new_path);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, stat_buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules '__xstat', %s, %d:%s", pathname, errno, strerror(errno));
		}

		slog_live("[POSIX] End Hercules '__xstat', pathname=%s, ver=%d, new_path=%s, ret=%d, filesize=%ld\n", pathname, ver, new_path, ret, stat_buf->st_size);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling Real '__xstat', pathname=%s, ver=%d", pathname, ver);
		ret = real___xstat(ver, pathname, stat_buf);
		slog_full("[POSIX]. End Real '__xstat', pathname=%s, ver=%d, errno=%d:%s, ret=%d", pathname, ver, errno, strerror(errno), ret);
	}
	return ret;
}

pid_t wait(int *wstatus)
{
	if (!real_wait)
		real_wait = (pid_t (*)(int *))dlsym(RTLD_NEXT, "wait");

	if (!init)
	{
		return real_wait(wstatus);
	}

	// errno = 0;
	// pid_t wpid;
	// while ((wpid = wait(wstatus)) > 0)
	// 	;

	// return real_wait(wstatus);

	pid_t ret_pid = real_wait(wstatus);
	if (ret_pid > 0)
	{
		slog_debug("[POSIX] wait() successful: PID %d waited for child PID %d.\n", getpid(), ret_pid);
		if (wstatus && WIFEXITED(*wstatus))
		{
			slog_debug("[POSIX] Child %d exited with status %d.\n", ret_pid, WEXITSTATUS(*wstatus));
		}
		else if (wstatus)
		{
			slog_debug("[POSIX] Child %d terminated abnormally.\n", ret_pid);
		}
	}
	else if (ret_pid == -1)
	{
		slog_debug("[POSIX] wait() failed or no children to wait for in PID: %d.\n", getpid());
	}
	else
	{
		// This case should not typically happen for wait()
		slog_debug("[POSIX] wait() returned unexpected PID: %d in PID: %d.\n", ret_pid, getpid());
	}
	return ret_pid;
}

// pid_t waitpid(pid_t pid, int *wstatus, int options)
// {
// 	if (!real_waitpid)
// 		real_waitpid = (pid_t (*)(pid_t, int *, int))dlsym(RTLD_NEXT, __func__);

// 	if (!init)
// 	{
// 		return real_waitpid(pid, wstatus, options);
// 	}

// 	// fprintf(stderr, "[POSIX] Calling waitpid %d\n", pid);
// 	slog_live("[POSIX] Calling waitpid %d", pid);

// 	return real_waitpid(pid, wstatus, options);
// }

pid_t fork(void)
{
	if (!real_fork)
		real_fork = (pid_t (*)())dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fork();
	}

	errno = 0;
	// fprintf(stdout, "[POSIX] Calling fork\n");
	slog_live("[POSIX] Calling 'fork'");

	// pthread_mutex_destroy(&lock_network);

	release_network_resources(IMSS_ROOT, 1, rank);
	imss_comm_cleanup();

	pthread_mutex_lock(&lock_network);
	slog_debug("[POSIX] Init real_fork");
	pid_t pid = real_fork();

	if (pid == -1)
	{
		pthread_mutex_unlock(&lock_network);
		perror("Fork error");
		slog_error("[POSIX] Error 'real fork', errno=%d:%s", errno, strerror(errno));
		return pid;
	}

	slog_debug("Locking network, add=%p", &lock_network);

	switch (pid)
	{
	case -1:
	{
		slog_live("[POSIX] Fork failed");
	}
	break;
	case 0: // child process.
	{
		// pthread_mutex_init(&lock_network, NULL);
		slog_debug("The child is unlocking the mutex");
		pthread_mutex_unlock(&lock_network);
		slog_live("[POSIX] Child process");
		ucp_context_client = NULL;
		ucp_worker_meta = NULL;
		ucp_worker_data = NULL;
		local_addr_meta = NULL;
		local_addr_data = NULL;
		stat_eps = NULL;
		stat_addr = NULL;

		// slog_debug("ucp_worker_meta=%p", &ucp_worker_meta);
		// slog_debug("ucp_worker_meta=%p", &ucp_worker_data);s
		print_worker_pointer(ucp_worker_meta);
		// print_worker_pointer(ucp_worker_data);

		rank = GetRank();
		// log init.
		// time_t t = time(NULL);
		// struct tm tm = *localtime(&t);
		// sprintf(log_path, "client-thread-%ld.%02d-%02d.%d", pthread_self(), tm.tm_hour, tm.tm_min, getpid());
		// slog_init(log_path, args.logging.hercules_debug_level, args.logging.hercules_debug_file, args.logging.hercules_debug_screen, 1, 1, 1, rank);
		init_network_resources(META_HOSTFILE, METADATA_PORT, N_META_SERVERS, rank, IMSS_ROOT);
		slog_live("[POSIX] Child process: network resources has been initialized.");
	}
	break;
	default: // parent process.
	{
		slog_debug("The parent is unlocking the mutex");
		pthread_mutex_unlock(&lock_network);
		// pthread_mutex_init(&lock_network, NULL);
		slog_live("[POSIX] Parent process, child pid=%d", pid);

		ucp_context_client = NULL;
		ucp_worker_meta = NULL;
		ucp_worker_data = NULL;
		local_addr_meta = NULL;
		local_addr_data = NULL;
		stat_eps = NULL;
		stat_addr = NULL;

		print_worker_pointer(ucp_worker_meta);
		init_network_resources(META_HOSTFILE, METADATA_PORT, N_META_SERVERS, rank, IMSS_ROOT);
		slog_live("[POSIX] Parent process: network resources has been initialized.");
	}
	break;
	}

	slog_live("[POSIX] Exit 'fork'");

	return pid;
}

pid_t vfork(void)
{
	static pid_t (*real_vfork)(void) = nullptr;

	if (!real_vfork)
	{
		real_vfork = reinterpret_cast<pid_t (*)(void)>(dlsym(RTLD_NEXT, __func__));
	}

	if (!init)
	{
		return real_vfork();
	}

	errno = 0;
	slog_live("[POSIX] Calling vfork");

	// Do NOT lock mutexes here. vfork suspends the parent process.
	slog_debug("[POSIX] Init real_vfork");

	pid_t pid = real_vfork();

	if (pid == -1)
	{
		perror("vfork error");
		slog_error("[POSIX] Error 'real vfork', errno=%d:%s", errno, strerror(errno));
		return pid;
	}

	switch (pid)
	{
	case -1:
	{
		slog_live("[POSIX] vfork failed");
	}
	break;

	case 0: // child process.
	{
		// Memory is shared with the parent.
		// Do NOT reinitialize mutexes, nullify pointers, or initialize network resources.
		slog_live("[POSIX] Child process (vfork)");
	}
	break;

	default: // parent process.
	{
		// Parent resumes execution only after the child calls exec() or _exit()
		slog_live("[POSIX] Parent process, vfork child pid=%d", pid);
	}
	break;
	}

	return pid;
}

int lstat64(const char *__restrict__ pathname, struct stat64 *__restrict__ buf)
{
	if (!real_lstat64)
		real_lstat64 = (int (*)(const char *, struct stat64 *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_lstat64(pathname, buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		WarnOperationNotSupported(__func__, pathname);
		ret = real_lstat64(pathname, buf);
		free(new_path);
	}
	else
	{
		slog_full("Calling 'lstat64', pathname=%s", pathname);
		ret = real_lstat64(pathname, buf);
	}
	return ret;
}

int lstat(const char *pathname, struct stat *buf)
{
	if (!real_lstat)
		real_lstat = (int (*)(const char *, struct stat *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_lstat(pathname, buf);
	}

	// fprintf(stderr, "Calling lstat, pathname=%s\n", pathname);
	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX] Calling Hercules 'lstat', new_path=%s", new_path);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		slog_live("[POSIX] Ending Hercules 'lstat', new_path=%s, errno=%d:%s", new_path, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real 'lstat', pathname=%s", pathname);
		ret = real_lstat(pathname, buf);
		slog_full("[POSIX] End real 'lstat', pathname=%s, ret=%d", pathname, ret);
	}

	return ret;
}

int stat(const char *pathname, struct stat *buf)
{
	if (!real_stat)
		real_stat = (int (*)(const char *, struct stat *))dlsym(RTLD_NEXT, "stat");

	if (!init)
	{
		return real_stat(pathname, buf);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'stat', new_path=%s.", new_path);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		slog_live("[POSIX]. Ending Hercules 'stat', new_path=%s, ret=%d\n", new_path, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'stat', pathname=%s", pathname);
		ret = real_stat(pathname, buf);
		// slog_live("[POSIX]. Ending Real 'stat', pathname=%s", pathname);
	}

	return ret;
}

extern int statvfs(const char *__restrict __file, struct statvfs *__restrict __buf)
{
	if (!real_statvfs)
		real_statvfs = (int (*)(const char *, struct statvfs *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_statvfs(__file, __buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(__file);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'statvfs', path=%s", new_path);
		memset(__buf, 0, sizeof(struct statvfs));

		// TODO: check the following. It is used to help benchmarks to show the correct FS information.
		unsigned long block_size = IMSS_BLKSIZE * KB;
		unsigned long total_size_bytes = 1024UL * 1024UL * 1024UL * 100UL; // Example: 100 GB fake capacity
		unsigned long used_bytes = 0;					   // Replace with actual usage tracking if you have it

		__buf->f_bsize = block_size;  // Filesystem block size
		__buf->f_frsize = block_size; // Fragment size (CRITICAL: df/mdtest use this multiplier)

		// Calculate blocks
		__buf->f_blocks = total_size_bytes / __buf->f_frsize;		    // Total data blocks
		__buf->f_bfree = (total_size_bytes - used_bytes) / __buf->f_frsize; // Free blocks
		__buf->f_bavail = __buf->f_bfree;				    // Free blocks for unprivileged users

		// Populate Inode Information (For Inode Usage)
		// fake high numbers to avoid running out
		__buf->f_files = 1000000;  // Total file nodes
		__buf->f_ffree = 1000000;  // Free file nodes (sets usage to 0%)
		__buf->f_favail = 1000000; // Free nodes for unprivileged users

		// Standard constants
		__buf->f_namemax = URI_;

		// __buf->f_bsize = IMSS_BLKSIZE * KB;
		// __buf->f_namemax = URI_;
		slog_live("[POSIX]. End Hercules 'statvfs', path=%s, ret=%d\n", new_path, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'statvfs', path=%s", __file);
		ret = real_statvfs(__file, __buf);
		slog_full("[POSIX]. Ending Real 'statvfs', path=%s", __file);
	}

	return ret;
}

int fstatvfs(int fd, struct statvfs *buf)
{
	if (!real_fstatvfs)
		real_fstatvfs = (int (*)(int, struct statvfs *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fstatvfs(fd, buf);
	}

	errno = 0;
	int ret = 0;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		WarnOperationNotSupported(__func__, pathname);
		ret = real_fstatvfs(fd, buf);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'fstatvfs', fd=%d", fd);
		ret = real_fstatvfs(fd, buf);
	}

	return ret;
}

int statvfs64(const char *path, struct statvfs64 *buf)
{
	if (!real_statvfs64)
		real_statvfs64 = (int (*)(const char *, struct statvfs64 *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_statvfs64(path, buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'statvfs64', path=%s", new_path);
		buf->f_bsize = IMSS_BLKSIZE * KB;
		buf->f_namemax = URI_;
		slog_live("[POSIX]. End Hercules 'statvfs64', path=%s", new_path);
		free(new_path);
	}
	else
	{
		slog_live("[POSIX]. Calling Real 'statvfs64', path=%s", path);
		ret = real_statvfs64(path, buf);
		slog_live("[POSIX]. Ending Real 'statvfs64', path=%s", path);
	}

	return ret;
}

int statfs(const char *path, struct statfs *buf)
{
	if (!real_statfs)
		real_statfs = (int (*)(const char *, struct statfs *))dlsym(RTLD_NEXT, "statfs");

	if (!init)
	{
		return real_statfs(path, buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'statfs', path=%s, new_path=%s", path, new_path);
		buf->f_bsize = IMSS_BLKSIZE * KB;
		buf->f_namelen = URI_;
		slog_live("[POSIX]. Ending Hercules 'statfs', path=%s, ret=%d", path, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'statfs', path=%s.", path);
		ret = real_statfs(path, buf);
		slog_full("[POSIX]. Ending Real 'statfs', path=%s, ret=%d.", path, ret);
	}
	return ret;
}

/**
 * Used by MPI_File_open.
 */
int statfs64(const char *path, struct statfs64 *buf)
{
	if (!real_statfs64)
		real_statfs64 = (int (*)(const char *, struct statfs64 *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_statfs64(path, buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		// fprintf(stderr, "[POSIX]. Calling Hercules 'statfs64', path=%s.\n", path);
		slog_live("[POSIX]. Calling Hercules 'statfs64', path=%s, new_path=%s", path, new_path);
		buf->f_bsize = IMSS_BLKSIZE * KB;
		buf->f_namelen = URI_;
		slog_live("[POSIX]. Ending Hercules 'statfs64', path=%s, ret=%d", path, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'statfs64', path=%s.", path);
		// fprintf(stderr, "[POSIX]. Calling real 'statfs64', path=%s.\n", path);
		ret = real_statfs64(path, buf);
		slog_full("[POSIX]. Ending real 'statfs64', path=%s, ret=%d.", path, ret);
	}
	return ret;
}

int fstatfs(int fd, struct statfs *buf)
{
	if (!real_fstatfs)
		real_fstatfs = (int (*)(int, struct statfs *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fstatfs(fd, buf);
	}
	// fprintf(stderr, "fstatfs\n");

	errno = 0;
	int ret = 0;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		WarnOperationNotSupported(__func__, pathname);
		ret = real_fstatfs(fd, buf);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'fstatfs', fd=%d", fd);
		ret = real_fstatfs(fd, buf);
	}
	return ret;
}

int fstatfs64(int fd, struct statfs64 *buf)
{
	if (!real_fstatfs)
		real_fstatfs64 = (int (*)(int, struct statfs64 *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fstatfs64(fd, buf);
	}

	errno = 0;
	int ret = 0;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		WarnOperationNotSupported(__func__, pathname);
		ret = real_fstatfs64(fd, buf);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'fstatfs', fd=%d", fd);
		ret = real_fstatfs64(fd, buf);
	}
	return ret;
}

int __xstat64(int ver, const char *pathname, struct stat64 *stat_buf)
{
	if (!real__xstat64)
		real__xstat64 = (int (*)(int, const char *, struct stat64 *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real__xstat64(ver, pathname, stat_buf);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules '__xstat64', pathname=%s, new_path=%s", pathname, new_path);

		imss_refresh(new_path);
		ret = imss_getattr(new_path, (struct stat *)stat_buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		free(new_path);

		slog_live("[POSIX]. Ending Hercules '__xstat64', pathname=%s, ret=%d\n", pathname, ret);
	}
	else
	{
		slog_full("[POSIX]. Calling real '__xstat64', pathname=%s.", pathname);
		// fprintf(stderr, "[POSIX]. Calling real '__xstat64', pathname=%s.\n", pathname);
		ret = real__xstat64(ver, pathname, stat_buf);
		slog_full("[POSIX]. Ending real '__xstat64', pathname=%s, ret=%d", pathname, ret);
	}

	return ret;
}

char *__realpath_chk(const char *pathname, char *resolved_path, size_t resolved_len)
{
	if (!real__realpath_chk)
		real__realpath_chk = (char *(*)(const char *, char *, size_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real__realpath_chk(pathname, resolved_path, resolved_len);
		// slog_live("[POSIX]. Calling Real 'realpath', pathname=%s.", pathname);
	}

	errno = 0;
	int ret = 0;
	char *p = NULL;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules '__realpath_chk', pathname=%s, new_path=%s", pathname, new_path);
		// p = real_realpath(pathname, resolved_path);
		strcpy(resolved_path, pathname);
		p = resolved_path;
		slog_live("[POSIX]. Ending Hercules '__realpath_chk', pathname=%s, resolved_path=%s, ret=%d, errno=%d:%s\n", pathname, resolved_path, ret, errno, strerror(errno));
		// TO CHECK!
		//  If there is no error, realpath() returns a pointer to the resolved_path.
		//    Otherwise, it returns NULL, the contents of the array
		//    resolved_path are undefined, and errno is set to indicate the
		//    error.
		// p = resolved_path;
	}
	else
	{
		slog_live("[POSIX]. Calling real '__realpath_chk', pathname=%s", pathname);
		p = real__realpath_chk(pathname, resolved_path, resolved_len);
		slog_live("[POSIX]. Ending real '__realpath_chk', pathname=%s, resolved_path=%s, ret=%d, p=%s\n", pathname, resolved_path, ret, p);
	}
	return p;
}

char *realpath(const char *pathname, char *resolved_path)
{
	if (!real_realpath)
		real_realpath = (char *(*)(const char *, char *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_realpath(pathname, resolved_path);
		// slog_live("[POSIX]. Calling Real 'realpath', pathname=%s.", pathname);
	}

	errno = 0;
	int ret = 0;
	char *p = NULL;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'realpath', pathname=%s, new_path=%s", pathname, new_path);
		strcpy(resolved_path, pathname);
		p = resolved_path;
		slog_live("[POSIX]. Ending Hercules 'realpath', pathname=%s, resolved_path=%s, ret=%d, errno=%d:%s\n", pathname, resolved_path, ret, errno, strerror(errno));
	}
	else
	{
		slog_full("[POSIX]. Calling real 'realpath', pathname=%s", pathname);
		p = real_realpath(pathname, resolved_path);
		slog_full("[POSIX]. Ending real 'realpath', pathname=%s, resolved_path=%s, ret=%d\n", pathname, resolved_path, ret);
	}
	return p;
}

// used by IOR.
// int __open_2(const char *pathname, int flags, ...)
int __open_2(const char *pathname, int flags)
{
	if (!real__open_2)
	{
		real__open_2 = (int (*)(const char *, int, ...))dlsym(RTLD_NEXT, __func__);
	}

	// // Access additional arguments when O_CREAT flag is set.
	mode_t mode = 0;
	// if (flags & O_CREAT)
	// {
	// 	va_list ap;
	// 	va_start(ap, flags);
	// 	mode = va_arg(ap, mode_t);
	// 	va_end(ap);
	// }

	if (!init)
	{
		if (!mode)
			return real__open_2(pathname, flags);
		else
			return real__open_2(pathname, flags, mode);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules '__open_2', new_path=%s.", new_path);

		// checkOpenFlags(pathname, flags);

		ret = generalOpen(new_path, flags, mode, -1);

		slog_live("[POSIX]. Ending Hercules '__open_2', new_path=%s, ret=%d, errno=%d:%s\n", new_path, ret, errno, strerror(errno));
		// if (ret < 0)
		free(new_path);
	}
	else
	{
		slog_full("Calling real '__open_2', pathname=%s\n", pathname);
		if (!mode)
			ret = real__open_2(pathname, flags);
		else
			ret = real__open_2(pathname, flags, mode);
	}
	return ret;
}

int open64(const char *pathname, int flags, ...)
{
	if (!real_open64)
		real_open64 = (int (*)(const char *, int, ...))dlsym(RTLD_NEXT, __func__);

	// Access additional arguments when O_CREAT flag is set.
	mode_t mode = 0;
	if (flags & O_CREAT)
	{
		va_list ap;
		va_start(ap, flags);
		mode = va_arg(ap, mode_t);
		va_end(ap);
	}

	if (!init)
	{
		if (!mode)
			return real_open64(pathname, flags);
		else
			return real_open64(pathname, flags, mode);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'open64', pathname=%s, new_path=%s", pathname, new_path);
		// checkOpenFlags(pathname, flags);

		ret = generalOpen(new_path, flags, mode, -1);

		slog_live("[POSIX]. Ending Hercules 'open64', pathname=%s, fd=%d\n", pathname, ret);
		// if (ret < 0)
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'open64', pathname=%s", pathname);
		if (!mode)
			ret = real_open64(pathname, flags);
		else
			ret = real_open64(pathname, flags, mode);
		slog_full("[POSIX]. Ending real 'open64', pathname=%s, ret=%d, errno=%d:%s", pathname, ret, errno, strerror(errno));
	}
	return ret;
}

int mkstemp(char *template_name)
{
	if (!real_mkstemp)
		real_mkstemp = (int (*)(char *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_mkstemp(template_name);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(template_name);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'mkstemp', new_path=%s, template_name=%s", new_path, new_path, template_name);
		char *new_path_by_template;
		mode_t mode = 0;
		int flags = 0;

		// Get a unique pathname.
		try_tempname_len(template_name);
		// Get HERCULES uri by using the unique pathname.
		new_path_by_template = checkHerculesPath(template_name);

		// Set mode and flags.
		mode |= S_IRUSR | S_IWUSR;
		// flags = O_EXCL | O_CREAT;
		flags = O_RDWR | O_CREAT | O_EXCL;

		// Creates the file and returns the file descriptor.
		ret = generalOpen(new_path_by_template, flags, mode, -1);

		slog_live("[POSIX]. Ending Hercules 'mkstemp', new_path=%s, template_name=%s, fd=%d, new_path_by_template=%s\n", new_path, template_name, ret, new_path_by_template);
		// if (ret < 0)
		free(new_path_by_template);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'mkstemp', pathname=%s", template_name);
		ret = real_mkstemp(template_name);
		slog_full("[POSIX]. Ending Real 'mkstemp', pathname=%s", template_name);
	}
	return ret;
}

int flock(int fd, int operation)
{
	if (!real_flock)
		real_flock = (int (*)(int, int))dlsym(RTLD_NEXT, __func__);

	if (!init)
		return real_flock(fd, operation);

	errno = 0;
	int ret = 0;

	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();

		const int lock_type = operation & ~LOCK_NB; // turn off the no-lock indicator
		const bool non_blocking = (operation & LOCK_NB) != 0;

		slog_live("[POSIX]. Calling Hercules 'flock', pathname=%s, fd=%d, "
			  "operation=0x%x (%s%s)",
			  pathname, fd, operation,
			  lock_type == LOCK_SH ? "LOCK_SH" : lock_type == LOCK_EX ? "LOCK_EX"
							 : lock_type == LOCK_UN	  ? "LOCK_UN"
										  : "UNKNOWN",
			  non_blocking ? "|LOCK_NB" : "");

		switch (lock_type)
		{
		// Shared lock
		case LOCK_SH:
		{
			slog_live("[POSIX][flock] pathname=%s, fd=%d, LOCK_SH%s",
				  pathname, fd, non_blocking ? "|LOCK_NB" : "");

			if (non_blocking)
			{
				// Grant immediately.
				// TODO: implement a backend blocking implementation.
				ret = 0;
			}
			else
			{
				// Blocking shared lock.
				ret = real_flock(fd, operation);
			}
			break;
		}

		// Exclusive lock
		case LOCK_EX:
		{
			slog_live("[POSIX][flock] pathname=%s, fd=%d, LOCK_EX%s",
				  pathname, fd, non_blocking ? "|LOCK_NB" : "");

			if (non_blocking)
			{
				// Grant immediately.
				// TODO: implement a backend blocking implementation.
				ret = 0;
			}
			else
			{
				// Blocking exclusive lock.
				ret = real_flock(fd, operation);
			}
			break;
		}

		// Unlock
		case LOCK_UN:
		{
			slog_live("[POSIX][flock] pathname=%s, fd=%d, LOCK_UN", pathname, fd);
			ret = real_flock(fd, LOCK_UN);
			break;
		}

		// Invalid operation.
		default:
		{
			slog_warn("[POSIX][flock] pathname=%s, fd=%d, invalid operation=0x%x",
				  pathname, fd, operation);
			ret = -1;
			errno = EINVAL;
			break;
		}
		}

		slog_live("[POSIX]. Ending Hercules 'flock', pathname=%s, fd=%d, "
			  "operation=0x%x, ret=%d, errno=%d",
			  pathname, fd, operation, ret, ret == -1 ? errno : 0);
	}
	else
	{
		slog_live("[POSIX] Calling real 'flock', fd=%d, operation=0x%x", fd, operation);
		ret = real_flock(fd, operation);
		slog_live("[POSIX] Ending real 'flock', fd=%d, ret=%d", fd, ret);
	}

	return ret;
}

int fclose(FILE *fp)
{
	if (!real_fclose)
		real_fclose = (int (*)(FILE *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fclose(fp);
	}

	errno = 0;
	int ret = 0;
	int fd = fp->_fileno;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();

		slog_live("[POSIX]. Calling Hercules 'fclose', pathname=%s, fd=%d", pathname, fd);
		ret = imss_close(pathname, fd);
		// Upon successful completion, fclose() shall return 0;
		// otherwise, it shall return EOF and set errno to indicate the error.
		// To control this situations, we check the value of "ret" before we return it.
		// greater than 0.
		if (ret > 0)
		{
			ret = 0;
		}
		// less than 0 (error).
		if (ret < 0)
		{
			ret = EOF;
		}

		slog_live("[POSIX]. Ending Hercules 'fclose' pathname=%s, fd=%d\n", pathname, fd);
		// Set offset to 0.
		map_fd_update_value(map_fd, pathname, fd, 0);
		map_fd_erase(map_fd, fd);
		// TODO: TO CHECK!
		// real_fclose(fp);
		// free(pathname);
	}
	else
	{ // don't call slog here!
		// slog_live("[POSIX]. Calling real 'fclose', fd=%d", fd); // don't call slog here!
		// fprintf(stderr, "Calling Real 'fclose', fd=%d, ret=%d\n", fd, ret);
		ret = real_fclose(fp);
	}
	return ret;
}

size_t fwrite(const void *buf, size_t size, size_t count, FILE *fp)
{
	if (!real_fwrite)
		real_fwrite = (size_t (*)(const void *, size_t, size_t, FILE *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fwrite(buf, size, count, fp);
	}

	errno = 0;
	size_t ret = -1;
	int fd = fp->_fileno;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		size_t to_write = size * count;

		unsigned long offset = -1;
		slog_live("[POSIX]. Calling Hercules 'fwrite', pathname=%s, to_write=%ld, size=%ld, count=%ld, fd=%d", pathname, to_write, size, count, fd);

		ret = generalWrite(pathname, fd, buf, to_write, offset);

		slog_live("[POSIX]. Ending Hercules 'fwrite', pathname=%s, ret=%ld, fd=%d\n", pathname, ret, fd);
	}
	else
	{
		// slog_full("[POSIX]. Calling real 'fwrite', fd=%d", fd);
		// fprintf(stdout, "Calling real fwrite, fd=%d\n", fd);
		ret = real_fwrite(buf, size, count, fp);
		// slog_full("[POSIX]. Ending real 'fwrite', fd=%d, ret=%d\n", fd, ret);
	}

	return ret;
}

ssize_t readv(int fd, const struct iovec *iov, int iovcnt)
{
	if (!real_readv)
		real_readv = (ssize_t (*)(int, const iovec *, int))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_readv(fd, iov, iovcnt);
	}

	errno = 0;
	ssize_t ret = -1;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'readv', pathname=%s, ret=%ld\n", pathname, ret);
		unsigned long offset = 0;
		/* Find the total number of bytes to be written.  */
		size_t bytes = 0;
		for (int i = 0; i < iovcnt; ++i)
		{
			/* Check for ssize_t overflow.  */
			if (SSIZE_MAX - bytes < iov[i].iov_len)
			{
				errno = EINVAL;
				return -1;
			}
			bytes += iov[i].iov_len;
		}

		/* Allocate a temporary buffer to hold the data.  We should normally
		 use alloca since it's faster and does not require synchronization
		 with other threads.  But we cannot if the amount of memory
		 required is too large.  */
		char *buffer = NULL;
		// char *malloced_buffer = NULL;

		// malloced_buffer =
		buffer = (char *)malloc(bytes);
		if (buffer == NULL)
			/* XXX I don't know whether it is acceptable to try writing
			   the data in chunks.  Probably not so we just fail here.  */
			return -1;

		/* Read the data */
		ssize_t bytes_read = read(fd, buffer, bytes);
		if (bytes_read < 0)
			return -1;

		/* Copy the data from BUFFER into the memory specified by VECTOR.  */
		bytes = bytes_read;
		for (int i = 0; i < iovcnt; ++i)
		{
			size_t copy = MIN(iov[i].iov_len, bytes);
			(void)memcpy((void *)iov[i].iov_base, (void *)buffer, copy);
			buffer += copy;
			bytes -= copy;
			if (bytes == 0)
				break;
		}

		ret = bytes_read;

		slog_live("[POSIX]. Ending Hercules 'readv', pathname=%s, ret=%ld\n", pathname, ret);
	}
	else
	{
		ret = real_readv(fd, iov, iovcnt);
		slog_full("[POSIX]. Ending real 'readv', fd=%d", fd);
	}

	return ret;
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt)
{
	if (!real_writev)
		real_writev = (ssize_t (*)(int, const struct iovec *, int))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_writev(fd, iov, iovcnt);
	}

	errno = 0;
	size_t ret = -1;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();

		unsigned long offset = -1;
		/* Find the total number of bytes to be written.  */
		size_t bytes = 0;
		for (int i = 0; i < iovcnt; ++i)
		{
			/* Check for ssize_t overflow.  */
			if (SSIZE_MAX - bytes < iov[i].iov_len)
			{
				errno = EINVAL;
				return -1;
			}
			bytes += iov[i].iov_len;
		}

		/* Allocate a temporary buffer to hold the data.  We should normally
		 use alloca since it's faster and does not require synchronization
		 with other threads.  But we cannot if the amount of memory
		 required is too large.  */
		char *buffer;
		// char *malloced_buffer = NULL;

		// malloced_buffer =
		buffer = (char *)malloc(bytes);
		if (buffer == NULL)
			/* XXX I don't know whether it is acceptable to try writing
			   the data in chunks.  Probably not so we just fail here.  */
			return -1;

		/* Copy the data into BUFFER.  */
		size_t to_copy = bytes;
		char *bp = buffer;
		for (int i = 0; i < iovcnt; ++i)
		{
			size_t copy = MIN(iov[i].iov_len, to_copy);
			bp = (char *)__mempcpy((void *)bp, (void *)iov[i].iov_base, copy);
			to_copy -= copy;
			if (to_copy == 0)
				break;
		}

		slog_live("[POSIX]. Calling Hercules 'writev', pathname=%s", pathname);
		ret = generalWrite(pathname, fd, buffer, bytes, offset);
		slog_live("[POSIX]. Ending Hercules 'writev', pathname=%s, ret=%ld, errno=%d:%s\n", pathname, ret, errno, strerror(errno));
	}
	else
	{
		// fprintf(stderr, "[POSIX]. Ending real 'writev', fd=%d\n", fd);
		ret = real_writev(fd, iov, iovcnt);
		slog_full("[POSIX]. Ending real 'writev', fd=%d", fd);
	}

	return ret;
}

ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset)
{
	if (!real_pwritev)
		real_pwritev = (ssize_t (*)(int, const struct iovec *, int, off_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_pwritev(fd, iov, iovcnt, offset);
	}

	errno = 0;
	size_t ret = -1;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();

		slog_live("[POSIX]. Calling Hercules 'pwritev', pathname=%s, fd=%d, offset=%d", pathname, fd, offset);
		// unsigned long p = 0;
		/* Find the total number of bytes to be written.  */
		size_t bytes = 0;
		for (int i = 0; i < iovcnt; ++i)
		{
			/* Check for ssize_t overflow.  */
			if (SSIZE_MAX - bytes < iov[i].iov_len)
			{
				errno = EINVAL;
				return -1;
			}
			bytes += iov[i].iov_len;
		}

		/* Allocate a temporary buffer to hold the data.  We should normally
		 use alloca since it's faster and does not require synchronization
		 with other threads.  But we cannot if the amount of memory
		 required is too large.  */
		char *buffer;
		// char *malloced_buffer = NULL;

		// malloced_buffer =
		buffer = (char *)malloc(bytes);
		if (buffer == NULL)
			/* XXX I don't know whether it is acceptable to try writing
			   the data in chunks.  Probably not so we just fail here.  */
			return -1;

		/* Copy the data into BUFFER.  */
		size_t to_copy = bytes;
		char *bp = buffer;
		for (int i = 0; i < iovcnt; ++i)
		{
			size_t copy = MIN(iov[i].iov_len, to_copy);
			bp = (char *)__mempcpy((void *)bp, (void *)iov[i].iov_base, copy);
			to_copy -= copy;
			if (to_copy == 0)
				break;
		}

		ret = generalWrite(pathname, fd, buffer, bytes, offset);
		slog_live("[POSIX]. Ending Hercules 'pwritev', pathname=%s, fd=%d, ret=%ld,  errno=%d:%s\n", pathname, fd, ret, errno, strerror(errno));
	}
	else
	{
		// fprintf(stderr, "[POSIX]. Calling real 'pwritev', fd=%d, errno=%d:%s\n", fd, errno, strerror(errno));
		ret = real_pwritev(fd, iov, iovcnt, offset);
		slog_live("[POSIX]. Ending real 'pwritev', fd=%d, errno=%d:%s", fd, errno, strerror(errno));
	}
	return ret;
}

ssize_t pwrite64(int fd, const void *buf, size_t count, off64_t offset)
{

	if (!real_pwrite64)
		real_pwrite64 = (ssize_t (*)(int, const void *, size_t, off64_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_pwrite64(fd, buf, count, offset);
	}

	errno = 0;
	size_t ret = -1;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();

		slog_live("[POSIX]. Calling Hercules 'pwrite64', pathname=%s, fd=%d, offset=%d", pathname, fd, offset);
		// ret = -1;
		// errno = -2;
		ret = generalWrite(pathname, fd, buf, count, offset);

		slog_live("[POSIX]. Ending Hercules 'pwrite64', pathname=%s, fd=%d, ret=%ld,  errno=%d:%s\n", pathname, fd, ret, errno, strerror(errno));
	}
	else
	{
		// fprintf(stderr, "[POSIX]. Ending real ' pwrite64', fd=%d, errno=%d:%s\n", fd, errno, strerror(errno));
		ret = real_pwrite64(fd, buf, count, offset);
		slog_live("[POSIX]. Ending real ' pwrite64', fd=%d, errno=%d:%s", fd, errno, strerror(errno));
	}
	return ret;
}

// int poll(struct pollfd *fds, nfds_t nfds, int timeout)
// {
// 	if (!real_poll)
// 		real_poll = dlsym(RTLD_NEXT, "poll");

// 	if (!init)
// 	{
// 		return real_poll(fds, nfds, timeout);
// 	}

// 	errno = 0;
// 	size_t ret = -1;
// 	int fd = fds->fd;
// 	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
// 	if (!pathname_ob.empty())
// 	{
// 		slog_live("[POSIX]. Calling Hercules 'poll', pathname=%s, fd=%d", pathname, fds->fd);
// 		ret = 0;
// 		// errno = ETIME;
// 		slog_live("[POSIX]. Ending Hercules 'poll', pathname=%s, fd=%d, ret=%ld\n", pathname, fds->fd, ret);
// 	}
// 	else
// 	{
// 		// slog_live("[POSIX]. Calling real 'poll', fd=%d, errno=%d:%s", fds->fd, errno, strerror(errno))
// 		ret = real_poll(fds, nfds, timeout);
// 		slog_live("[POSIX]. Ending real 'poll', fd=%d", fds->fd)
// 	}
// 	return ret;
// }

// int ppoll(struct pollfd *fds, nfds_t nfds, const struct timespec tmo_p, const sigset_t sigmask)
// {
// 	if (!real_ppoll)
// 		real_ppoll = dlsym(RTLD_NEXT, "ppoll");

// 	if (!init)
// 	{
// 		return real_ppoll(fds, nfds, tmo_p, sigmask);
// 	}

// 	errno = 0;
// 	size_t ret = -1;
// 	char *pathname = map_fd_search_by_val(map_fd, fds->fd);
// 	if (!pathname_ob.empty())
// 	{
// 		slog_live("[POSIX]. Calling Hercules 'ppoll', pathname=%s, fd=%d", pathname, fds->fd);
// 		ret = 0;
// 		// errno = ETIME;
// 		slog_live("[POSIX]. Ending Hercules 'ppoll', pathname=%s, fd=%d, ret=%ld,  errno=%d:%s\n", pathname, fds->fd, ret, errno, strerror(errno));
// 	}
// 	else
// 	{
// 		// slog_live("[POSIX]. Calling real 'ppoll', fd=%d, errno=%d:%s", fds->fd, errno, strerror(errno));
// 		ret = real_ppoll(fds, nfds, tmo_p, sigmask);
// 		slog_live("[POSIX]. Ending real 'ppoll', fd=%d", fds->fd);
// 	}
// 	return ret;
// }

void clearerr(FILE *fp)
{
	if (!real_clearerr)
		real_clearerr = (void (*)(FILE *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_clearerr(fp);
	}

	errno = 0;
	int fd = fp->_fileno;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'clearerr', pathname=%s", pathname);
		fp->_flags &= ~(_IO_ERR_SEEN | _IO_EOF_SEEN);
		slog_live("[POSIX]. End Hercules 'clearerr', pathname=%s\n", pathname);
	}
	else
	{
		real_clearerr(fp);
	}
}

int ferror(FILE *fp)
{
	if (!real_ferror)
		real_ferror = (int (*)(FILE *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_ferror(fp);
	}

	errno = 0;
	int ret = 0;
	int fd = fp->_fileno;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'ferror', pathname=%s", pathname);
		// fprintf(stderr, "[POSIX][TODO]. Calling Hercules 'ferror', pathname=%s\n", pathname);
		ret = ((fp->_flags & _IO_ERR_SEEN) != 0);
		// fprintf(stderr, "[POSIX][TODO]. Ending Hercules 'ferror', pathname=%s, ret=%d\n", pathname, ret);
		slog_live("[POSIX]. Ending Hercules 'ferror', pathname=%s, ret=%d", pathname, ret);
	}
	else
	{
		ret = real_ferror(fp);
	}

	return ret;
}

int feof(FILE *fp)
{
	if (!real_feof)
		real_feof = (int (*)(FILE *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_feof(fp);
	}

	errno = 0;
	int ret = 0;
	int fd = fp->_fileno;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'feof', pathname=%s", pathname);
		// if ((fp->_flags & _IO_EOF_SEEN) != 0)
		// ret = _IO_feof_unlocked(fp);
		if ((fp->_flags & _IO_EOF_SEEN) != 0)
		{
			ret = 1;
		}
		else
		{
			ret = 0;
		}

		slog_live("[POSIX]. End Hercules 'feof', pathname=%s, ret=%d\n", pathname, ret);
	}
	else
	{
		ret = real_feof(fp);
	}

	return ret;
}

long int ftell(FILE *fp)
{
	if (!real_ftell)
		real_ftell = (long int (*)(FILE *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_ftell(fp);
	}

	errno = 0;
	long int ret = -1;
	int fd = fp->_fileno;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		unsigned long offset = 0;
		slog_live("[POSIX]. Calling Hercules 'ftell', pathname=%s, fd=%d, errno=%d:%s", pathname, fd, errno, strerror(errno));

		ret = map_fd_search(map_fd, pathname, fd, &offset);
		slog_live("[POSIX]. ret=%ld, offset=%ld", ret, offset);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_live("[POSIX]. Error in 'ftell', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
			return ret;
		}
		else
		{
			ret = offset;
		}
		// map_fd_update_value(map_fd, pathname, fd, ret);
		slog_live("[POSIX]. Ending Hercules 'ftell', ret=%ld\n", ret);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'ftell', fd=%d, errno=%d:%s", fd, errno, strerror(errno));
		ret = real_ftell(fp);
	}

	return ret;
}

void rewind(FILE *stream)
{
	if (!real_rewind)
		real_rewind = (void (*)(FILE *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_rewind(stream);
	}

	errno = 0;
	int fd = stream->_fileno;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'rewind', pathname=%s, fd=%d", pathname, fd);

		fseek(stream, 0L, SEEK_SET);

		slog_live("[POSIX]. Ending Hercules 'rewind'");
	}
	else
	{
		return real_rewind(stream);
	}
}

FILE *fopen(const char *pathname, const char *mode)
{
	if (!real_fopen)
		real_fopen = (FILE * (*)(const char *, const char *)) dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fopen(pathname, mode);
	}

	errno = 0;
	FILE *file = NULL;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'fopen', pathname=%s", pathname);
		uint64_t ret_ds;
		unsigned long offset = 0;
		int flags = 0, oflags = 0;

		if ((flags = __sflags_(mode, &oflags)) == 0)
			return (NULL);

		file = real_fopen("/dev/null", mode);
		if (file == NULL)
		{
			return NULL;
		}
		ret = file->_fileno; // get file descriptor.

		slog_live("[POSIX] File descriptor=%d", ret);

		ret = generalOpen(new_path, oflags, ALLPERMS, ret);

		slog_live("[POSIX]. Calling Hercules 'fopen', pathname=%s, ret=%d", pathname, ret);
		if (ret < 0)
		{
			free(new_path);
			return NULL;
		}
	}
	else /* Do not try to use slog_ here! This function uses 'fopen' internally. */
	{
		// slog_live("[POSIX]. Calling Real 'fopen', pathname=%s", pathname);
		// fprintf(stdout, "[POSIX]. Calling Real 'fopen', pathname=%s\n", pathname);
		// fflush(stdout);
		file = real_fopen(pathname, mode);
		// slog_live("[POSIX]. Closing Real 'fopen', pathname=%s", pathname);
	}

	return file;
}

FILE *fdopen(int fildes, const char *mode)
{
	if (!real_fdopen)
		real_fdopen = (FILE * (*)(int, const char *)) dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fdopen(fildes, mode);
	}

	errno = 0;
	FILE *file = NULL;
	int ret = 0;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fildes);
	if (!pathname_ob.empty()) // recibe un fd que debe estar el mapa si el archivo pertenece a Hercules, se hace el generalOpen y se rellena la estructura.
	{
		const char *pathname = pathname_ob.c_str();
		uint64_t ret_ds;
		unsigned long offset = 0;
		// mode_t new_mode = 0;

		int flags = 0, oflags = 0;

		if ((flags = __sflags_(mode, &oflags)) == 0)
			return (NULL);

		// To interpret the mode recived:
		// Opening a file in append mode (a as the first character of mode)
		// causes all subsequent write operations to this stream to occur at
		// end-of-file, as if preceded by the call:
		//   fseek(stream, 0, SEEK_END);
		// if (strstr(mode, "w"))
		// {
		// 	new_mode |= S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;
		// 	flags = O_WRONLY | O_CREAT | O_TRUNC;
		// }

		// slog_live("[POSIX] Calling Hercules 'fdopen', pathname=%s, mode=%s, new_mode=%o, fildes=%d", pathname, mode, new_mode, fildes);
		slog_live("[POSIX] Calling Hercules 'fdopen', pathname=%s, mode=%s, fildes=%d", pathname, mode, fildes);

		file = real_fopen("/dev/null", mode); // real_fdopen(fildes, mode); // real_fopen("/dev/null", mode);
		if (file == NULL)
		{
			slog_debug("Failed after calling fdopen.");
			return NULL;
		}
		// ret = file->_fileno; // get file descriptor.
		file->_fileno = fildes;

		slog_live("[POSIX][fdopen] File descriptor=%d", file->_fileno);

		ret = generalOpen(pathname, oflags, ALLPERMS, fildes);

		slog_live("[POSIX] Ending Hercules 'fdopen', pathname=%s, fildes=%d, ret=%d\n", pathname, fildes, ret);
		if (ret < 0)
		{
			return NULL;
		}

		// file = (FILE *)malloc(sizeof(FILE));

		// file->_fileno = ret;
		// file->_flags2 = IMSS_BLKSIZE * KB;
		// file->_offset = offset;
		// // file->_mode = mode;

		// if (file == NULL)
		// {
		// 	slog_live("File %s was not found\n", pathname);
		// }
	}
	else
	{
		slog_full("[POSIX] Calling real 'fdopen', fd=%d", fildes);
		file = real_fdopen(fildes, mode);
	}

	return file;
}

ssize_t generalWrite(const char *pathname, int fd, const void *buf, size_t size, size_t offset)
{
	// pthread_mutex_lock(&system_lock);
	ssize_t ret = 0;
	struct stat ds_stat_n;
	char *aux = NULL;
	int update_offset = 0;
	if (offset == -1)
	{
		update_offset = 1;
		map_fd_search(map_fd, pathname, fd, &offset);
		int fd_lkup = -1;
		fd_lookup((char *)pathname, &fd_lkup, &ds_stat_n, &aux);
		slog_live("current size=%ld", ds_stat_n.st_size);

		// imss_getattr(pathname, &ds_stat_n);
		// if (ret < 0)
		if (fd_lkup == -1)
		{
			// errno = -ret;
			errno = ENOENT;
			ret = -1;
			slog_error("[POSIX] Error Hercules 'write'	: %d:%s", errno, strerror(errno));
			return ret;
		}
	}

	slog_live("[POSIX]. pathname=%s, size to write=%lu, offset=%lu", pathname, size, offset);

	ret = TIMING(imss_write(pathname, buf, size, offset), "imss_write", ssize_t, rank);
	if (ret < 0)
	{
		SetErrno(ret);
		ret = -1;
	}

	if (update_offset && ret >= 0)
	{
		if (ds_stat_n.st_size + size > ds_stat_n.st_size)
		{
			slog_live("pathname=%s, updating , file size=%ld, current size=%ld, new local size=%d", pathname, ds_stat_n.st_size, size, ds_stat_n.st_size + size);
			map_fd_update_value(map_fd, pathname, fd, ds_stat_n.st_size + size);
		}
	}

	return ret;
}

int generalOpen(const char *new_path, int flags, mode_t mode, int createFd)
{
	int ret = 0;
	uint64_t ret_ds = 0;
	long p = 0;
	int is_mount_point = 0;

	if (!strcmp(new_path, "imss://"))
	{
		slog_debug("Trying to open the mount point %s", new_path);
		ret = real_open("/dev/null", 0); // Get a file descriptor
		// stores the file descriptor "ret" into the map "map_fd".
		slog_live("[POSIX] Puting fd %d into map", ret);
		map_fd_put(map_fd, new_path, ret, p);
		return ret;
	}

	// Search for the path "new_path" on the map "map_fd".
	slog_live("[POSIX] Searching for the %s on the map", new_path);
	int exist = map_fd_search_by_pathname(map_fd, new_path, &ret, &p);
	if (exist == -1) // if the "new_path" was not find on the local map:
	{
		int create_flag = (flags & O_CREAT);
		slog_live("[POSIX] new_path:%s, exist: %d, create_flag: %d", new_path, exist, create_flag);
		if (create_flag == O_CREAT) // if the file does not exist, then we create it.
		{
			slog_live("[POSIX] New file %s, ret=%d", new_path, ret);
			int err_create = TIMING(imss_create(new_path, mode, &ret_ds, 1, TYPE_REGULAR_FILE), "generalOpen,imss_create", int, rank);
			slog_live("[POSIX] imss_create(%s, %d, %ld), err_create: %d", new_path, mode, ret_ds, err_create);
			if (err_create == -EEXIST)
			{
				slog_live("[POSIX] 1 - Dataset already exists, imss_open(%s, %ld)", new_path, ret_ds);
				ret = TIMING(imss_open(new_path, &ret_ds), "generalOpen,imss_open", int, rank);
				// errno = EEXIST;
				// errno = -ret;
				slog_live("[POSIX] 1 - imss_open(%s, %ld), ret=%d", new_path, ret_ds, ret);
				ret = 0;
			}
			else if (err_create < 0)
			{
				errno = -err_create;
				ret = -1;
			}
		}
		else // if O_CREAT flag was not set, the file must exists.
		{
			slog_live("[POSIX] File must exists - imss_open(%s, %ld), create_flag: %d", new_path, ret_ds, create_flag);
			ret = TIMING(imss_open(new_path, &ret_ds), "generalOpen,imss_open", int, rank);
			slog_live("[POSIX] 2 - ret_ds=%d, ret=%d, new_path=%s", ret_ds, ret, new_path);

			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
			}

			// If we get a "ret_ds" equal to "-2", we are in the case of symbolic link pointing to a file stored in the system.
			if (ret_ds == -2)
			{
				slog_live("[POSIX] Calling real_open(%s)", new_path);
				// Calling the real open.
				if (!mode)
					ret = real_open(new_path, flags);
				else
					ret = real_open(new_path, flags, mode);
				// stores the file descriptor "ret" into the map "map_fd".
				TIMING_NO_RETURN(map_fd_put(map_fd, new_path, ret, p), "generalOpen,map_fd_put", rank); // TO CHECK!
			}
		}
		// if (ret == 0)
		// {
		// 	slog_live("[POSIX] Puting fd %d into map", ret);
		// 	map_fd_put(map_fd, new_path, ret, p);
		// }
		// else
		if (ret > -1 && createFd == -1)
		{
			// errno = 0;
			ret = real_open("/dev/null", 0); // Get a file descriptor
			// stores the file descriptor "ret" into the map "map_fd".
			slog_live("[POSIX] Puting fd %d into map", ret);
			TIMING_NO_RETURN(map_fd_put(map_fd, new_path, ret, p), "generalOpen,map_fd_put", rank);
		}
		else if (ret > -1 && createFd >= 0)
		{
			slog_live("[POSIX] Puting fd %d into map, passed from arguments.", createFd);
			TIMING_NO_RETURN(map_fd_put(map_fd, new_path, createFd, p), "generalOpen,map_fd_put", rank);
			ret = createFd;
		}
	}
	else
	{
		slog_live("[POSIX]. exist=%d, O_TRUNC=%d, fd=%d", exist, flags & O_TRUNC, ret);
		if (flags & O_TRUNC)
		{
			TIMING_NO_RETURN(map_fd_update_value(map_fd, new_path, ret, 0), "generalOpen,map_fd_update_value", rank);
			int ret_aux = 0;
			struct stat stats;
			ret_aux = TIMING(imss_getattr(new_path, &stats), "generalOpen,imss_getattr", int, rank);
			if (ret_aux < 0)
			{
				errno = -ret_aux;
				ret = -1;
				slog_error("[POSIX] Error Hercules 'open', errno=%d:%s", errno, strerror(errno));
				// pthread_mutex_unlock(&system_lock);
				return ret;
			}
			stats.st_size = 0;
			TIMING_NO_RETURN(HierarchicalMapUpdate(hierarchical_map, new_path, ret, stats), "generalOpen,map_fd_update_value", rank);
		}
	}
	// pthread_mutex_unlock(&system_lock);
	return ret;
}

int open(const char *pathname, int flags, ...)
{
	if (!real_open)
		real_open = (int (*)(const char *, int, ...))dlsym(RTLD_NEXT, __func__);

	// Access additional arguments when O_CREAT flag is set.
	mode_t mode = 0;
	if (flags & O_CREAT)
	{
		va_list ap;
		va_start(ap, flags);
		mode = va_arg(ap, mode_t);
		va_end(ap);
	}

	if (!init)
	{
		if (!mode)
			return real_open(pathname, flags);
		else
			return real_open(pathname, flags, mode);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX] Calling Hercules 'open' flags=%d, mode=%o, pathname=%s, new_path=%s", flags, mode, pathname, new_path);
		checkOpenFlags(pathname, flags);

		ret = generalOpen(new_path, flags, mode, -1);

		slog_live("[POSIX] Ending Hercules 'open', mode=%o, ret=%d\n", mode, ret);
		// if (ret < 0)
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'open', flags=%d, mode=%o, pathname=%s", flags, mode, pathname);
		// checkOpenFlags(pathname, flags);

		if (!mode)
			ret = real_open(pathname, flags);
		else
			ret = real_open(pathname, flags, mode);
		slog_full("[POSIX]. Ending real 'open', mode=%o, pathname=%s, ret=%d, errno=%d:%s", mode, pathname, ret, errno, strerror(errno));
	}

	return ret;
}

/**
 * Verify if @pathname is absolute or relative.
 * @return
 * 1 = path is absolute.
 * 0 = path is relative.
 * -1 = error.
 */
int IsAbsolutePath(const char *pathname)
{
	const char *file_name_without_prefix;
	int ret = -1;

	// file_name_without_prefix = pathname + MOUNT_POINT_LEN;
	// if (file_name_without_prefix[0] == '/') // path is absolute.
	if (pathname[0] == '/') // path is absolute.
	{
		ret = 1;
	}
	else
	{ // path is relative.
		ret = 0;
	}
	return ret;
}

int openat(int dir_fd, const char *pathname, int flags, ...)
{

	if (!real_openat)
		real_openat = (int (*)(int, const char *, int, ...))dlsym(RTLD_NEXT, __func__);

	int mode = 0;
	if (flags & O_CREAT)
	{
		va_list ap;
		va_start(ap, flags);
		mode = va_arg(ap, unsigned);
		va_end(ap);
	}

	if (!init)
	{
		if (!mode)
			return real_openat(dir_fd, pathname, flags);
		else
			return real_openat(dir_fd, pathname, flags, mode);
	}

	errno = 0;
	int ret = 0;
	std::string pathname_dir_op = "";
	char *new_path = NULL;
	ResolvePathsAndFD(dir_fd, pathname, pathname_dir_op, &new_path);
	if (pathname_dir_op != "" || new_path != NULL)
	{
		const char *pathname_dir = pathname_dir_op.c_str();
		// const char *pathname_dir = pathname_dir_ob.c_str();
		slog_live("[POSIX] Calling Hercules 'openat' flags=%d, mode=%o, dir_fd=%d, pathname_dir=%s, pathname=%s", flags, mode, dir_fd, pathname_dir, pathname);
		// checkOpenFlags(pathname, flags);

		int is_absolute_path = IsAbsolutePath(pathname);

		// If pathname is absolute, then dir_fd is ignored.
		if (is_absolute_path == 1)
		{
			char *new_path = checkHerculesPath(pathname);
			slog_live("[POSIX] is absolute, 'openat', new_path=%s", new_path);
			if (flags & O_DIRECTORY)
			{
				DIR *dirp = opendir(pathname);
				if (dirp != NULL)
				{
					ret = dirfd(dirp);
				}
				else
				{
					ret = -1;
				}
			}
			else
			{
				ret = generalOpen(new_path, flags, mode, -1);
				// if (ret < 0)
				// free(new_path);
			}
		}
		else if (is_absolute_path == 0) // pathname is relative.
		{
			if (dir_fd == AT_FDCWD) // dir_fd is the special value AT_FDCWD.
			{			// TO CHECK!
				char *new_path = checkHerculesPath(pathname);
				slog_live("[POSIX] is relative, current directory, 'openat', new_path=%s", new_path);
				// pathname is interpreted relative to the current working directory of the calling process (like real_open).
				ret = generalOpen(new_path, flags, mode, -1);
				// if (ret < 0)
				// 	free(new_path);
			}
			else
			{
				// // get the pathname of the directory pointed by dir_fd if it is storage in the local map "map_fd".
				// char *pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
				// if (pathname_dir == NULL)
				// { // if dir_fd is not storage locally
				// 	// search dir_fd on the metadata server.
				// 	// fprintf(stderr,"");
				// 	slog_error("[POSIX] dir_fd=%d could not be resolved.");
				// 	return -1;
				// }

				char absolute_pathname[PATH_MAX];
				const char *dirr = pathname_dir + strlen("imss://");
				sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, pathname);

				char *new_path = checkHerculesPath(absolute_pathname);
				slog_live("[POSIX] is relative, 'openat', new_path=%s", new_path);
				ret = generalOpen(new_path, flags, mode, -1);
				// if (ret < 0)
				// 	free(new_path);
			}
		}

		slog_live("[POSIX] Ending Hercules 'openat', mode=%o, ret=%d, errno=%d:%s\n", mode, ret, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real 'openat', flags=%d, mode=%o, dir_fd=%d, pathname=%s", flags, mode, dir_fd, pathname);
		if (!mode)
		{
			ret = real_openat(dir_fd, pathname, flags);
		}
		else
		{
			ret = real_openat(dir_fd, pathname, flags, mode);
		}
	}

	return ret;
}

int mkdir(const char *path, mode_t mode)
{
	if (!real_mkdir)
		real_mkdir = (int (*)(const char *, mode_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_mkdir(path, mode);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'mkdir', path=%s, new_path=%s", path, new_path);

		ret = imss_mkdir(new_path, mode);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_live("[POSIX]. Error in 'mkdir', ret=%d, errno=%d:%s", ret, errno, strerror(errno));
		}
		slog_live("[POSIX]. Ending hercules 'mkdir', path=%s, new_path=%s, ret=%d\n", path, new_path, ret);
		// fprintf(stderr, "[POSIX]. Ending hercules 'mkdir', path=%s, new_path=%s, ret=%d\n", path, new_path, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'mkdir', path=%s.", path);
		ret = real_mkdir(path, mode);
		slog_full("[POSIX]. Ending real 'mkdir', path=%s.", path);
	}
	return ret;
}

int symlink(const char *name1, const char *name2)
{
	// TODO.
	if (!real_symlink)
		real_symlink = (int (*)(const char *, const char *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_symlink(name1, name2);
	}

	errno = 0;
	int ret;
	char *new_path_1 = checkHerculesPath(name1);
	char *new_path_2 = checkHerculesPath(name2);
	if (new_path_1 != NULL || new_path_2 != NULL)
	{
		WarnOperationNotSupported(__func__, name1);
	}
	else
	{
		// move real symlink here after add the implementation for Hercules.
	}

	return real_symlink(name1, name2);
}

int symlinkat(const char *name1, int fd, const char *name2)
{

	if (!real_symlinkat)
		real_symlinkat = (int (*)(const char *, int, const char *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_symlinkat(name1, fd, name2);
	}

	// Fix: to check fd.

	errno = 0;
	int ret;
	char *new_path_1 = checkHerculesPath(name1);
	char *new_path_2 = checkHerculesPath(name2);
	if (new_path_1 != NULL || new_path_2 != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'symlinkat', name1=%s, name2=%s.", name1, name2);

		if (new_path_1 != NULL && new_path_2 != NULL)
		{
			slog_live("[POSIX]. Both new_path_1=%s, new_path_2=%s", new_path_1, new_path_2);
			ret = imss_symlinkat(new_path_1, new_path_2, 0);
			free(new_path_1);
			free(new_path_2);
		}

		// if (!strncmp(name2, MOUNT_POINT, strlen(MOUNT_POINT)))
		if (new_path_1 == NULL && new_path_2 != NULL)
		{
			slog_live("[POSIX]. Only second new_path_2=%s", new_path_2);
			// new_path_1 = name1;
			strcpy(new_path_1, name1);
			ret = imss_symlinkat(new_path_1, new_path_2, 1);
			// free(new_path_1) ?
			free(new_path_2);
		}
		slog_live("[POSIX]. Ending Hercules 'symlinkat', name1=%s, name2=%s.", name1, name2);
	}
	else
	{
		slog_live("[POSIX]. Calling Real 'symlinkat', name1=%s, name2=%s.", name1, name2);
		ret = real_symlinkat(name1, fd, name2);
		slog_live("[POSIX]. Ending Real 'symlinkat', name1=%s, name2=%s.", name1, name2);
	}
	return ret;
}

off_t lseek(int fd, off_t offset, int whence)
{
	if (!real_lseek)
		real_lseek = (off_t (*)(int, off_t, int))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_lseek(fd, offset, whence);
	}

	errno = 0;
	off_t ret = -1;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		unsigned long p = 0;
		slog_live("[POSIX]. Calling Hercules 'lseek', pathname=%s, fd=%d, whence=%d, offset=%ld, errno=%d:%s", pathname, fd, whence, offset, errno, strerror(errno));
		if (whence == SEEK_SET)
		{
			slog_live("[POSIX]. SEEK_SET, offset=%ld", offset);
			ret = offset;
			map_fd_update_value(map_fd, pathname, fd, ret);
		}
		else if (whence == SEEK_CUR)
		{
			ret = map_fd_search(map_fd, pathname, fd, &p);
			slog_live("[POSIX]. SEEK_CUR=%ld, ret=%ld, p=%ld", offset, ret, p);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				slog_live("[POSIX]. Error in 'lseek', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
				return ret;
			}
			ret = p + offset;
			slog_live("[POSIX]. SEEK_CUR=%ld, p+offset=%ld", offset, ret);
			map_fd_update_value(map_fd, pathname, fd, ret);
		}
		else if (whence == SEEK_END)
		{
			slog_live("SEEK_END, offset=%ld", offset);
			struct stat ds_stat_n;
			ret = imss_getattr(pathname, &ds_stat_n);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				slog_live("[POSIX]. Error in 'lseek', %s, ret=%ld, errno=%d:%s", pathname, ret, errno, strerror(errno));
				return ret;
			}
			ret = offset + ds_stat_n.st_size;
			map_fd_update_value(map_fd, pathname, fd, ret);
		}

		slog_live("[POSIX]. Ending Hercules 'lseek', ret=%ld, errno=%d:%s\n", ret, errno, strerror(errno));
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'lseek', fd=%d, whence=%d, offset=%ld", fd, whence, offset);
		// fprintf(stderr,"[POSIX]. Calling real 'lseek', fd=%d, whence=%d, offset=%ld, errno=%d:%s\n", fd, whence, offset, errno, strerror(errno));
		ret = real_lseek(fd, offset, whence);
		slog_full("[POSIX]. Ending Real 'lseek', fd=%d, whence=%d, offset=%ld, ret=%d", fd, whence, offset, ret);
		// fprintf(stderr,"[POSIX]. Ending real 'lseek', fd=%d, whence=%d, offset=%ld, errno=%d:%s\n", fd, whence, offset, errno, strerror(errno));
	}
	return ret;
}

off64_t lseek64(int fd, off64_t offset, int whence)
{
	if (!real_lseek64)
		real_lseek64 = (off64_t (*)(int, off64_t, int))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_lseek64(fd, offset, whence);
	}

	errno = 0;
	off64_t ret = -1;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		unsigned long p = 0;
		slog_live("[POSIX]. Calling Hercules 'lseek64', pathname=%s, fd=%d", pathname, fd);
		slog_info("[POSIX]. whence=%d, offset=%ld", whence, offset);
		if (whence == SEEK_SET)
		{
			slog_live("[POSIX]. SEEK_SET=%ld", offset);
			ret = offset;
			map_fd_update_value(map_fd, pathname, fd, ret);
		}
		else if (whence == SEEK_CUR)
		{
			ret = map_fd_search(map_fd, pathname, fd, &p);
			slog_live("[POSIX]. SEEK_CUR=%ld, ret=%ld, p=%ld", offset, ret, p);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				slog_live("[POSIX]. Error in 'lseek64', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
				return ret;
			}
			ret = p + offset;
			slog_live("[POSIX]. SEEK_CUR=%ld, p+offset=%ld", offset, ret);
			map_fd_update_value(map_fd, pathname, fd, ret);
		}
		else if (whence == SEEK_END)
		{
			slog_live("SEEK_END=%ld", offset);
			struct stat ds_stat_n;
			ret = imss_getattr(pathname, &ds_stat_n);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				slog_live("[POSIX]. Error in 'lseek64', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
				return ret;
			}
			ret = offset + ds_stat_n.st_size;
			map_fd_update_value(map_fd, pathname, fd, ret);
		}

		slog_live("[POSIX]. Ending Hercules 'lseek64', ret=%ld\n", ret);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'lseek64', fd=%d", fd);
		ret = real_lseek64(fd, offset, whence);
	}
	return ret;
}

int fseek(FILE *stream, long int offset, int whence)
{
	if (!real_fseek)
		real_fseek = (int (*)(FILE *, long int, int))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fseek(stream, offset, whence);
	}

	errno = 0;
	off_t ret = -1;
	int fd = stream->_fileno;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		unsigned long p = 0;
		slog_live("[POSIX]. Calling Hercules 'fseek', pathname=%s, fd=%d", pathname, fd);
		if (whence == SEEK_SET)
		{
			slog_live("[POSIX]. SEEK_SET=%ld", offset);
			ret = offset;
			map_fd_update_value(map_fd, pathname, fd, ret);
		}
		else if (whence == SEEK_CUR)
		{
			ret = map_fd_search(map_fd, pathname, fd, &p);
			slog_live("[POSIX]. SEEK_CUR=%ld, ret=%ld, p=%ld", offset, ret, p);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				slog_live("[POSIX]. Error in 'fseek', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
				return ret;
			}
			ret = p + offset;
			slog_live("[POSIX]. SEEK_CUR=%ld, p+offset=%ld", offset, ret);
			map_fd_update_value(map_fd, pathname, fd, ret);
		}
		else if (whence == SEEK_END)
		{
			slog_live("SEEK_END=%ld", offset);
			struct stat ds_stat_n;
			ret = imss_getattr(pathname, &ds_stat_n);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				slog_error("[POSIX]. Error in 'fseek', ret=%ld, errno=%d:%s", ret, errno, strerror(errno));
			}
			else
			{
				// ret = offset + ds_stat_n.st_size;
				ret = ds_stat_n.st_size;
				slog_live("Updating offset to %ld, nlinks=%lu", ret, ds_stat_n.st_nlink);
				map_fd_update_value(map_fd, pathname, fd, ret);
			}
		}
		slog_live("[POSIX]. Ending Hercules 'fseek', ret=%ld\n");
		if (ret >= 0)
		{
			ret = 0;
		}
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'fseek', fd=%d, errno=%d:%s", fd, errno, strerror(errno));
		ret = real_fseek(stream, offset, whence);
	}
	// The fseek()and fseeko()functions shall return 0 if they succeed.
	// Otherwise, they shall return -1 and set errno to indicate the error.
	return ret;
}

void seekdir(DIR *dirp, long loc)
{
	if (!real_seekdir)
		real_seekdir = (void (*)(DIR *, long))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_seekdir(dirp, loc);
	}

	errno = 0;
	off_t ret = -1;
	int fd = dirfd(dirp);
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX] Calling Hercules 'seekdir', fd=%d, loc=%ld", fd, loc);
		// lseek(fd, loc, SEEK_SET);
		// dirp->size = 0;
		// dirp->offset = 0;
		// dirp->filepos = loc;
		real_seekdir(dirp, loc);
		slog_live("[POSIX] Ending Hercules 'seekdir', fd=%d, loc=%ld", fd, loc);
	}
	else
	{
		slog_full("[POSIX] Calling real 'seekdir', fd=%d, loc=%ld", fd, loc);
		real_seekdir(dirp, loc);
		slog_full("[POSIX] Ending real 'seekdir', fd=%d, loc=%ld", fd, loc);
	}
}

int truncate(const char *path, off_t length)
{
	if (!real_truncate)
		real_truncate = (int (*)(const char *, off_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_truncate(path, length);
	}

	errno = 0;
	int ret = -1;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		const char *pathname = new_path;
		slog_live("[POSIX]. Calling Hercules 'truncate', pathname=%s, length=%ld", pathname, length);

		ret = TIMING(generalFtruncate(pathname, length), "generalFtruncate", int, rank);

		slog_live("[POSIX]. Ending Hercules 'ftruncate', pathname=%s, length=%ld, ret=%d\n", pathname, length, ret);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'ftruncate', length=%ld", length);
		// fprintf(stderr, "[POSIX]. Calling real 'ftruncate', fd=%d, length=%ld\n", fd, length);
		ret = real_truncate(path, length);
		slog_full("[POSIX]. Ending real 'ftruncate', ret=%d", ret);
	}

	return ret;
}

int ftruncate(int fd, off_t length)
{
	static int (*real_ftruncate)(int, off_t) = nullptr;

	if (!real_ftruncate)
	{
		real_ftruncate = reinterpret_cast<int (*)(int, off_t)>(dlsym(RTLD_NEXT, __func__));
	}

	if (!init)
	{
		return real_ftruncate(fd, length);
	}

	errno = 0;
	int ret = -1;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'ftruncate', pathname=%s, length=%ld, fd=%d", pathname, length, fd);

		ret = TIMING(generalFtruncate(pathname, length), "generalFtruncate", int, rank);

		slog_live("[POSIX]. Ending Hercules 'ftruncate', pathname=%s, length=%ld, ret=%d, fd=%d\n", pathname, length, ret, fd);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'ftruncate', fd=%d, length=%ld", fd, length);
		ret = real_ftruncate(fd, length);
		slog_full("[POSIX]. Ending real 'ftruncate', fd=%d, ret=%d", fd, ret);
	}

	return ret;
}

int generalFtruncate(const char *pathname, off_t length)
{
	int ret = 0;
	struct stat ds_stat_n;
	char *aux = nullptr;
	int fd_lkup = -1;

	fd_lookup(const_cast<char *>(pathname), &fd_lkup, &ds_stat_n, &aux);

	if (fd_lkup == -1)
	{
		errno = ENOENT;
		ret = -1;
		slog_error("[POSIX] Error Hercules 'ftruncate'  : %d:%s", errno, strerror(errno));
		return ret;
	}

	slog_live("[POSIX]. pathname=%s, length to truncate=%ld", pathname, length);

	ret = TIMING(imss_truncate(pathname, length), "imss_truncate", int, rank);
	if (ret < 0)
	{
		SetErrno(ret);
		ret = -1;
	}

	// ftruncate alters the file size but does not modify the file offset for any open file descriptors.

	return ret;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset)
{
	if (!real_pwrite)
		real_pwrite = (ssize_t (*)(int, const void *, size_t, off_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_pwrite(fd, buf, count, offset);
	}

	errno = 0;
	ssize_t ret;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX] Calling Hercules 'pwrite', pathname=%s, fd=%d, count=%ld, offset=%ld, errno=%d:%s", pathname, fd, count, offset, errno, strerror(errno));
		// ret = imss_write(pathname, buf, count, offset);
		ret = generalWrite(pathname, fd, buf, count, offset);
		slog_live("[POSIX] Ending Hercules 'pwrite', pathname=%s, fd=%d, ret=%ld, count=%ld, offset=%ld, errno=%d:%s", pathname, fd, ret, count, offset, errno, strerror(errno));
	}
	else
	{
		slog_live("[POSIX] Calling Real 'pwrite', fd=%d, count=%ld, offset=%ld, errno=%d:%s", fd, count, offset, errno, strerror(errno));
		// fprintf(stderr, "[POSIX] Calling Real 'pwrite', fd=%d, count=%ld, offset=%ld, errno=%d:%s", fd, count, offset, errno, strerror(errno));
		ret = real_pwrite(fd, buf, count, offset);
	}
	return ret;
}

ssize_t write(int fd, const void *buf, size_t size)
{
	if (!real_write)
		real_write = (ssize_t (*)(int, const void *, size_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_write(fd, buf, size);
	}

	errno = 0;
	ssize_t ret = -1;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		unsigned long offset = -1;
		slog_live("[POSIX]. Calling Hercules 'write', pathname=%s, size=%lu, fd=%d", pathname, size, fd);

		ret = TIMING(generalWrite(pathname, fd, buf, size, offset), "generalWrite", ssize_t, rank);

		slog_live("[POSIX]. Ending Hercules 'write', pathname=%s, size=%lu, ret=%ld, fd=%d\n", pathname, size, ret, fd);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'write', fd=%d, size=%lu", fd, size);
		// fprintf(stderr, "[POSIX]. Calling real 'write', fd=%d, size=%lu\n", fd, size);
		ret = real_write(fd, buf, size);
		slog_full("[POSIX]. Ending real 'write', fd=%d, ret=%ld", fd, ret);
	}

	return ret;
}

// void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset)
// {
// 	fprintf(stderr, "[POSIX] Calling 'mmap', addr=%p\n", &addr);
// 	if (!real_mmap)
// 		real_mmap = dlsym(RTLD_NEXT, "mmap");

// 	if (init)
// 	{
// 		slog_live("[POSIX %d] Calling Real 'mmap'", rank);
// 	}

// 	return mmap(addr, length, prot, flags, fd, offset);
// }

ssize_t read(int fd, void *buf, size_t size)
{
	if (!real_read)
		real_read = (ssize_t (*)(int, const void *, size_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_read(fd, buf, size);
	}

	errno = 0;
	ssize_t ret;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		if (size <= 0)
		{
			return 0;
		}

		unsigned long offset = 0;
		slog_live("[POSIX]. Calling Hercules 'read', pathname=%s, size=%ld, fd=%d.", pathname, size, fd);
		// fprintf(stderr, "[POSIX]. Calling Hercules 'read', pathname=%s, size=%ld, fd=%d\n", pathname, size, fd);

		if (fd < 0)
		{
			errno = EBADF;
			slog_error("[POSIX] Error in Hercules while reading '%s', %d:%s", pathname, errno, strerror(errno));
			return -1;
		}

		map_fd_search(map_fd, pathname, fd, &offset);

		// fprintf(stderr, "[POSIX] Read Hercules size=%ld, offset=%lu\n", size, offset);
		if (args.prefetch_size > 0)
		{
			// prefetch is enable.
			// TODO: change this on all read calls.
			// fprintf(stderr, "Prefetching is enable wih a size of %" PRIu64 " GB\n", args.prefetch_size/GB);
			slog_debug("Prefetching is enable wih a size of %" PRIu64 " MB", args.prefetch_size / MB);
			ret = imss_sread_prefetch_v2(pathname, buf, size, offset);
		}
		else
		{
			// original
			// fprintf(stderr, "Prefetching is NOT enable with a size of %" PRIu64 " GB\n", args.prefetch_size/GB);
			ret = TIMING(imss_sread(pathname, buf, size, offset), "imss_sread", ssize_t, rank);
		}

		if (ret > 0)
		{
			offset += ret;
			// fprintf(stderr, "[POSIX] Updating map_fd, offset=%lu, data_size=%ld\n", offset, ds_stat_n.st_size);

			// slog_live("[POSIX] Updating map_fd, offset=%lu, data_size=%ld", offset, ds_stat_n.st_size);
			slog_live("[POSIX] Updating map_fd, offset=%lu", offset);
			map_fd_update_value(map_fd, pathname, fd, offset);
		}
		if (ret == -1)
		{
			errno = ENOENT;
		}
		if (ret == -2)
		{
			errno = EIO;
			// change return value to -1.
			ret = -1;
		}
		// fprintf(stderr, "[POSIX] Hercules read, pathname=%s, ret=%ld\n", pathname, ret);
		// fprintf(stderr, "[POSIX ] READ HERCULES ret=%ld\n", ret);

		slog_live("[POSIX]. End Hercules 'read', pathname=%s, ret=%zd, size=%ld, fd=%d\n", pathname, ret, size, fd);
		// fprintf(stderr, "[POSIX]. Hercules 'read', size=%ld, fd=%d, ret=%lu, offset=%lu, errno=%d:%s\n", size, fd, ret, offset, errno, strerror(errno));
	}
	else
	{
		slog_full("[POSIX]. Calling real 'read', size=%ld, fd=%ld.", size, fd);
		// unsigned long old_offset, new_offset;
		// old_offset = lseek(fd, 0L, SEEK_CUR);
		ret = real_read(fd, buf, size);
		// new_offset = lseek(fd, 0L, SEEK_CUR);
		// unsigned long offset = lseek(fd, 0L, SEEK_CUR);
		// slog_full("[POSIX]. Ending real 'read', size=%ld, fd=%ld, ret=%d, old_offset=%d, new_offset=%d, errno=%d:%s.", size, fd, ret, old_offset, new_offset, errno, strerror(errno));
		// fprintf(stderr, "[POSIX]. Real 'read', size=%ld, fd=%d, ret=%lu, offset=%lu, errno=%d:%s\n", size, fd, ret, offset, errno, strerror(errno));

		slog_full("[POSIX]. Ending real 'read', size=%ld, fd=%ld, ret=%d", size, fd, ret);
	}
	return ret;
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset)
{
	if (!real_pread)
		real_pread = (ssize_t (*)(int, void *, size_t, off_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_pread(fd, buf, count, offset);
	}

	errno = 0;
	ssize_t ret = -1;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'pread', pathname=%s, size=%ld, offset=%ld, fd=%ld.", pathname, count, offset, fd);

		// struct stat ds_stat_n;
		// ret = imss_getattr(pathname, &ds_stat_n);
		// slog_live("[POSIX]. pathname=%s, offset=%ld, stat.size=%ld, remaining=%ld", pathname, offset, ds_stat_n.st_size, ds_stat_n.st_size - offset);
		// if (ret < 0)
		// {
		// 	errno = -ret;
		// 	ret = -1;
		// 	slog_error("[POSIX] Error Hercules 'pread'");
		// }
		// else if (offset >= ds_stat_n.st_size)
		// {
		// 	ret = 0;
		// }
		// else
		{
			// ret = imss_read(pathname, buf, count, offset);
			ret = imss_sread(pathname, buf, count, offset);
			// The file offset is not changed.
		}

		slog_live("[POSIX]. End Hercules 'pread', pathname=%s, ret=%ld, size=%ld, offset=%d, fd=%d", pathname, ret, count, offset, fd);
	}
	else
	{
		slog_live("[POSIX]. Calling real 'pread', size=%ld, fd=%d.", count, fd);
		ret = real_pread(fd, buf, count, offset);
		slog_live("[POSIX]. Ending real 'pread', size=%ld, fd=%d, ret=%d", count, fd, ret);
	}
	return ret;
}

ssize_t pread64(int fd, void *buf, size_t count, off_t offset)
{
	if (!real_pread64)
		real_pread64 = (ssize_t (*)(int, void *, size_t, off_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_pread64(fd, buf, count, offset);
	}

	errno = 0;
	ssize_t ret = -1;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'pread64', pathname=%s, size=%ld, offset=%ld, fd=%ld.", pathname, count, offset, fd);

		ret = imss_sread(pathname, buf, count, offset);
		// The file offset is not changed.

		slog_live("[POSIX]. End Hercules 'pread64', pathname=%s, ret=%ld, size=%ld, offset=%d, fd=%d", pathname, ret, count, offset, fd);
	}
	else
	{
		slog_live("[POSIX]. Calling real 'pread64', size=%ld, fd=%d.", count, fd);
		ret = real_pread64(fd, buf, count, offset);
		slog_live("[POSIX]. Ending real 'pread64', size=%ld, fd=%d, ret=%d", count, fd, ret);
	}
	return ret;
}

size_t fread(void *buf, size_t size, size_t count, FILE *fp)
{
	if (!real_fread)
		real_fread = (size_t (*)(void *, size_t, size_t, FILE *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fread(buf, size, count, fp);
	}

	errno = 0;
	size_t ret;
	int fd = fp->_fileno;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		if (size <= 0)
		{
			buf = (void *)'\0';
			return 0;
		}

		if (fd < 0)
		{
			errno = EBADF;
			fp->_flags |= _IO_ERR_SEEN;
			slog_error("[POSIX] Error Hercules 'fread' %s : %s", pathname, strerror(errno));
			return -1;
		}

		unsigned long offset = 0;
		slog_live("[POSIX]. Calling Hercules 'fread', pathname=%s, size=%lu", pathname, count);
		map_fd_search(map_fd, pathname, fd, &offset);

		// struct stat ds_stat_n;
		// char *aux = NULL;
		// // ret = imss_getattr(pathname, &ds_stat_n);
		// int fd_lkup = -1;
		// fd_lookup(pathname, &fd_lkup, &ds_stat_n, &aux);
		// slog_live("current size=%ld", ds_stat_n.st_size);
		// // imss_getattr(pathname, &ds_stat_n);
		// // if (ret < 0)
		// if (fd_lkup == -1)
		// {
		// 	// errno = -ret;
		// 	errno = ENOENT;
		// 	ret = -1;
		// 	slog_error("[POSIX] Error Hercules 'fread'	: %d:%s", errno, strerror(errno));
		// 	fp->_flags |= _IO_ERR_SEEN;
		// 	return ret;
		// }
		// slog_live("[POSIX]. pathname=%s, ret=%d.", pathname, ret);
		// if (ret < 0)
		// {
		// 	errno = -ret;
		// 	ret = -1;
		// 	fp->_flags |= _IO_ERR_SEEN;
		// 	slog_error("[POSIX] Error Hercules 'fread'	: %s", strerror(errno));
		// }
		// else if
		// if(offset >= ds_stat_n.st_size)
		// {
		// 	fp->_flags |= _IO_EOF_SEEN;
		// 	ret = 0;
		// }
		// else
		// {
		// ret = imss_read(pathname, buf, count, offset);
		ret = imss_sread(pathname, buf, count, offset);

		if (ret > 0) // Success case.
		{
			offset += ret;
			slog_live("[POSIX] Updating map_fd, offset=%d", offset);
			map_fd_update_value(map_fd, pathname, fd, offset);
		}
		if (ret < 0) // Error case.
		{
			errno = -ret;
			ret = -1;
			fp->_flags |= _IO_ERR_SEEN;
			slog_error("[POSIX] Error Hercules 'fread' %s : %s", pathname, strerror(errno));
		}
		if (ret == 0)
		{ // End of file.
			fp->_flags |= _IO_EOF_SEEN;
		}
		slog_live("[POSIX]. End Hercules 'fread', ret=%ld\n", ret);
	}
	else
	{
		// slog_full("[POSIX] Calling real 'fread', fd=%d", fd);
		//		fprintf(stdout, "[POSIX] Calling real 'fread', fd=%d\n", fd);
		ret = real_fread(buf, size, count, fp);
	}

	return ret;
}

int unlink(const char *name)
{
	if (!real_unlink)
		real_unlink = (int (*)(const char *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_unlink(name);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(name);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'unlink', name=%s, new_path=%s", name, new_path);
		struct stat ds_stat_n;
		ret = imss_getattr(new_path, &ds_stat_n);
		if (ret != 0)
		{
			slog_error("HERCULES_ERR_UNLINK_FILE_NOT_FOUND");
		}
		else
		{
			if (S_ISDIR(ds_stat_n.st_mode))
			{ // directory case.
				ret = imss_rmdir(new_path, &ds_stat_n);
			}
			else
			{ // regular file case.
				ret = imss_unlink(new_path, &ds_stat_n);
			}
		}

		// unlink error.
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX]. Error Hercules 'unlink', errno=%d:%s", errno, strerror(errno));
		}
		// remove the file descriptor from the local map.
		if (ret == 1 || ret == 3)
		{
			if (map_fd_erase_by_pathname(map_fd, new_path) == -1)
			{
				// slog_warn("[POSIX]. Hercules Warning, no file descriptor found for the pathname=%s", new_path);
				// ret = -1;
				// errno =
				// file could be deleted but without be opened before.
				ret = 0;
			}
			else
			{
				ret = 0;
			}
		}

		slog_live("[POSIX]. Ending Hercules 'unlink', new_path=%s, ret=%d\n", new_path, ret);
		// fprintf(stderr, "[POSIX]. Ending Hercules 'unlink', type=%d, new_path=%s, ret=%d\n", type, new_path, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'unlink', name=%s", name);
		ret = real_unlink(name);
		slog_full("[POSIX]. Ending Real 'unlink', name=%s, ret=%d", name, ret);
	}

	return ret;
}

int rmdir(const char *path)
{

	if (!real_rmdir)
		real_rmdir = (int (*)(const char *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_rmdir(path);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{

		slog_live("[POSIX]. Calling Hercules 'rmdir', path=%s, new_path=%s", path, new_path);
		ret = imss_rmdir(new_path, NULL);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}

		// if (ret == -1)
		// { // special case io500
		// 	ret = real_unlinkat(0, path, 0);
		// }
		slog_live("[POSIX]. Ending Hercules 'rmdir', new_path=%s, ret=%d\n", new_path, ret);
		free(new_path);
	}
	else if (!strncmp(path, "imss://", strlen("imss://"))) // TO REVIEW!
	{
		ret = imss_rmdir(path, NULL);
	}
	else
	{
		ret = real_rmdir(path);
	}
	return ret;
}

int remove(const char *name)
{
	if (!real_remove)
		real_remove = (int (*)(const char *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_remove(name);
	}

	errno = 0;
	int ret, ret_map = 0;
	char *new_path = checkHerculesPath(name);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'remove', new_path=%s", new_path);
		char type = get_type(new_path);
		slog_live("[POSIX] type=%d, new_path=%s", type, new_path);
		switch (type)
		{
		case TYPE_DIRECTORY:
		case TYPE_HERCULES_INSTANCE: // Directory case?
		{
			// size_t len = strlen(new_path);
			// if (len > 0 && new_path[len - 1] != '/')
			// {
			// 	strcat(new_path, "/");
			// }
			ret = imss_rmdir(new_path, NULL);
			break;
		}
		case TYPE_REGULAR_FILE: // is regular file.
		{
			ret = imss_unlink(new_path, NULL);
			break;
		}
		default:
		{
			slog_error("HERCULES_ERR_REMOVE_NOT_SUPPORTED_TYPE");
			perror("HERCULES_ERR_REMOVE_NOT_SUPPORTED_TYPE");
			ret = -1;
			break;
		}
		}

		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX]. Error Hercules 'remove', errno=%d:%s", errno, strerror(errno));
		}
		else if (ret == 0)
		{
			slog_live("[POSIX]. Removing %s from the map", new_path);
			// remove the file descriptor from the local map.
			ret_map = map_fd_erase_by_pathname(map_fd, new_path);
			if (ret_map == -1)
			{
				slog_error("[POSIX]. Error Hercules no file descriptor found for the pathname=%s", new_path);
			}
		}
		else
		{
			ret = 0;
		}

		slog_live("[POSIX]. Ending Hercules 'remove', type %d, new_path=%s, ret=%d, ret_map=%d\n", type, new_path, ret, ret_map);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real 'remove', pathname=%s", name);
		ret = real_remove(name);
		slog_full("[POSIX] Ending real 'remove', pathname=%s", name);
	}

	return ret;
}

int rename(const char *old_given_path, const char *new_given_pathname)
{

	if (!real_rename)
		real_rename = (int (*)(const char *, const char *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_rename(old_given_path, new_given_pathname);
	}

	errno = 0;
	int ret;
	char *old_path = checkHerculesPath(old_given_path);
	char *new_path = checkHerculesPath(new_given_pathname);
	if (old_path != NULL && new_path != NULL)
	{ // move from Hercules to Hercules.
		slog_live("[POSIX]. Calling Hercules 'rename', old_given_path=%s, new_given_pathname=%s, old path=%s, new_path=%s", old_given_path, new_given_pathname, old_path, new_path);
		ret = TIMING(imss_rename(old_path, new_path), "imss_rename", int, rank);
		if (ret < 0)
		{
			SetErrno(ret);
			ret = -1;
		}
		slog_live("[POSIX]. End Hercules 'rename', old path=%s, new_path=%s, ret=%d\n", old_path, new_path, ret);
		free(old_path);
		free(new_path);
	}
	else if (old_path == NULL && new_path != NULL)
	{ // move from file system to Hercules.
		slog_live("[POSIX]. Calling Hercules 'rename', old_given_path=%s, new_given_pathname=%s, new_path=%s", old_given_path, new_given_pathname, new_path);
		ret = HerculesMove(old_given_path, new_given_pathname, new_path);
		if (ret < 0)
		{
			SetErrno(ret);
			ret = -1;
		}
		slog_live("[POSIX]. End Hercules 'rename', old_given_path=%s, new_given_pathname=%s, new_path=%s, ret=%d", old_given_path, new_given_pathname, new_path, ret);
		// free memory.
		free(new_path);
	}
	else if (old_path != NULL && new_path == NULL)
	{ // move from Hercules to file system.
		slog_live("[POSIX] Calling Hercules 'rename', Hercules %s to file system %s", old_path, new_path);
		ret = HerculesMove(old_given_path, new_given_pathname, old_path);
		if (ret < 0)
		{
			SetErrno(ret);
			ret = -1;
		}
		slog_live("[POSIX] Ending Hercules 'rename', Hercules %s to file system %s, ret=%d", old_path, new_path, ret);
	}
	else
	{
		slog_live("[POSIX]. Calling Real 'rename', old_given_path=%s, new_given_pathname=%s", old_given_path, new_given_pathname);
		ret = real_rename(old_given_path, new_given_pathname);
		slog_live("[POSIX]. End Real 'rename', old_given_path=%s, new_given_pathname=%s", old_given_path, new_given_pathname);
	}
	return ret;
}

int fchmodat(int dir_fd, const char *pathname, mode_t mode, int flags)
{
	if (!real_fchmodat)
		real_fchmodat = (int (*)(int, const char *, mode_t, int))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fchmodat(dir_fd, pathname, mode, flags);
	}

	// Fix: to check dir_fd.

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'fchmodat', pathname=%s", pathname);
		ret = imss_chmod(new_path, mode);
		slog_live("[POSIX]. End Hercules 'fchmodat', pathname=%s, ret=%d", pathname, ret);
		free(new_path);
	}
	else
	{
		ret = real_fchmodat(dir_fd, pathname, mode, flags);
	}

	return ret;
}

int chmod(const char *pathname, mode_t mode)
{
	if (!real_chmod)
		real_chmod = (int (*)(const char *, mode_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_chmod(pathname, mode);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'chmod', pathname=%s", pathname);
		ret = imss_chmod(new_path, mode);
		slog_live("[POSIX]. End Hercules 'chmod', pathname=%s, ret=%d\n", pathname, ret);
		free(new_path);
	}
	else
	{
		slog_live("[POSIX]. Calling real 'chmod', pathname=%s", pathname);
		ret = real_chmod(pathname, mode);
		slog_live("[POSIX]. Calling real 'chmod', pathname=%s, ret=%d", pathname, ret);
	}
	return ret;
}

// int execl(const char *path, const char *arg0, ... /*, (char *)0 */)
// {
// 	fprintf(stderr, "*********** Running execl\n");
// 	return dlsym(RTLD_NEXT, "execl");
// }

// int execlp(const char *file, const char *arg, ... /*, (char *) NULL */)
// {
// 	fprintf(stderr, "*********** Running execlp\n");
// 	return dlsym(RTLD_NEXT, "execlp");
// }

// int execle(const char *pathname, const char *arg, ... /*, (char *) NULL, char *const envp[] */)
// {
// 	fprintf(stderr, "*********** Running execle\n");
// 	return dlsym(RTLD_NEXT, "execle");
// }
int execv(const char *pathname, char *const argv[])
{
	real_execv = (int (*)(const char *, char *const *))dlsym(RTLD_NEXT, "execv");
	if (!init)
	{
		return real_execv(pathname, argv);
	}

	slog_debug("[POSIX] Running execv, pathname=%s\n", pathname);
	// generate unique SHM filename
	char shm_name[256];
	snprintf(shm_name, sizeof(shm_name), "/hercules_state_%d", getpid());

	// serialize the datasets to shared memory
	int shm_fd = imss_serializate_structs(shm_name);

	int ret = real_execv(pathname, argv);

	// only reached if real_execve fails
	if (shm_fd >= 0)
	{
		shm_unlink(shm_name);
	}

	return ret;
}

// int execvp(const char *file, char *const argv[])
// {
// 	fprintf(stderr, "*********** Running execvp\n");
// 	return dlsym(RTLD_NEXT, "execvp");
// }

// int execvpe(const char *file, char *const argv[], char *const envp[])
// {
// 	fprintf(stderr, "*********** Running execvpe\n");
// 	return dlsym(RTLD_NEXT, "execvpe");
// }

int execve(const char *pathname, char *const argv[], char *const envp[])
{
	real_execve = (int (*)(const char *, char *const *, char *const *))dlsym(RTLD_NEXT, "execve");
	if (!init)
	{
		return real_execve(pathname, argv, envp);
	}

	slog_debug("[POSIX] Running execve, pathname=%s\n", pathname);

	return real_execve(pathname, argv, envp);
}

int dup(int oldfd)
{
	if (!real_dup)
		real_dup = (int (*)(int))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_dup(oldfd);
	}

	errno = 0;
	int ret;
	std::string pathname_ob = map_fd_search_by_val(map_fd, oldfd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'dup', pathname=%s, oldfd=%d.", pathname, oldfd);

		int lowest_fd;

		// Attempt to duplicate the lowest available file descriptor (>= 0).
		lowest_fd = real_open("/dev/null", 0); // fcntl(0, F_DUPFD, 0);

		if (lowest_fd != -1)
		{
			slog_live("[POSIX]. Lowest available file descriptor: %d", lowest_fd);
			// ret = close(lowest_fd); // Close the duplicated file descriptor
			// slog_live("[POSIX]. File descriptor %d closed, ret=%d", lowest_fd, ret);
			ret = map_fd_put(map_fd, strdup(pathname), lowest_fd, 0);
			slog_live("[POSIX]. Putting %d in the map, ret=%d, pathname=%s", lowest_fd, ret, pathname);
			if (ret == -1)
			{
				errno = 9;
				slog_error("[POSIX] Error Hercules in 'dup', lowest_fd=%d already exist, errno=%d:%s.", lowest_fd, errno, strerror(errno)); // -1 when error, and errno is set.
			}
			else
			{
				ret = lowest_fd;
			}
		}
		else
		{
			perror("Failed to get the lowest available file descriptor");
			ret = -1;
		}

		slog_live("[POSIX]. End Hercules 'dup', pathname=%s, ret=%d.", pathname, ret);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'dup', oldfd=%d.", oldfd);
		ret = real_dup(oldfd);
		slog_full("[POSIX]. End real 'dup', oldfd=%d, newfd=%d.", oldfd, ret);
	}

	return ret;
}

int dup2(int oldfd, int newfd)
{
	if (!real_dup2)
		real_dup2 = (int (*)(int, int))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_dup2(oldfd, newfd);
	}

	errno = 0;
	int ret;
	std::string pathname_ob = map_fd_search_by_val(map_fd, oldfd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'dup2', pathname=%s, oldfd=%d, newfd=%d.", pathname, oldfd, newfd);
		if (oldfd == newfd)
		{
			ret = newfd;
		}
		else
		{
			// Close the target fd if it's already open
			close(newfd);

			ret = map_fd_dup(map_fd, oldfd, newfd);

			if (ret == -1)
			{
				slog_error("[POSIX] Error Hercules in 'dup2', oldfd=%d tracking does not exist.", oldfd);
			}
			else
			{
				ret = newfd;
			}
		}
		slog_live("[POSIX]. End Hercules 'dup2', pathname=%s, ret=%d.", pathname, ret);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'dup2', oldfd=%d, newfd=%d.", oldfd, newfd);
		ret = real_dup2(oldfd, newfd);
	}

	return ret;
}

int fchmod(int fd, mode_t mode)
{
	if (!real_fchmod)
		real_fchmod = (int (*)(int, mode_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fchmod(fd, mode);
	}

	errno = 0;
	int ret;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'fchmod', pathname=%s.", pathname);
		ret = imss_chmod(pathname, mode);
		slog_live("[POSIX]. End Hercules 'fchmod', pathname=%s, ret=%d.", pathname, ret);
	}
	else
	{
		ret = real_fchmod(fd, mode);
	}

	return ret;
}

int fchownat(int dir_fd, const char *pathname, uid_t owner, gid_t group, int flags)
{
	if (!real_fchownat)
		real_fchownat = (int (*)(int, const char *, uid_t, gid_t, int))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fchownat(dir_fd, pathname, owner, group, flags);
	}

	// Fix: to check dir_fd.

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX %d]. Calling Hercules 'fchownat'.", rank);
		ret = imss_chown(new_path, owner, group);
		free(new_path);
	}
	else
	{
		ret = real_fchownat(dir_fd, pathname, owner, group, flags);
	}

	return ret;
}

DIR *opendir(const char *name)
{
	if (!real_opendir)
		real_opendir = (DIR * (*)(const char *)) dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_opendir(name);
	}

	errno = 0;
	DIR *dirp = NULL;
	char *new_path = checkHerculesPath(name);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'opendir', pathname=%s, new_path=%s", name, new_path);
		int a = 1;
		int ret = 0;
		// int fd = 0;
		unsigned long p = 0;

		// fd = open(new_path, O_CREAT);

		dirp = real_opendir("/tmp");
		// dirp = (DIR *)malloc(sizeof(DIR));
		// if (dirp == NULL)
		// {
		// 	perror("HERCULES_ERR_OPENDIR_ALLOC_MEMORY");
		// 	slog_fatal("HERCULES_ERR_OPENDIR_ALLOC_MEMORY");
		// 	exit(-1);
		// }

		// dirp->fd = fd;
		// dirp->path = new_path;
		seekdir(dirp, 0);
		// Search for the path "new_path" on the map "map_fd",
		// if it exists then a file descriptor "fd" is going to point it.
		// ret = map_fd_search_by_pathname(map_fd, new_path, &fd, (long int *)&p);
		// if (ret != -1)
		// {
		// 	slog_live("[POSIX] map_fd_update_value, new_path=%s, fd=%d, ret=%d", new_path, fd, ret);
		// 	// map_fd_update_value(map_fd, new_path, fd, dirfd(dirp), p);
		// 	map_fd_update_fd(map_fd, new_path, fd, dirfd(dirp), p);
		// }
		// else
		// {
		slog_live("[POSIX] map_fd_put, new_path=%s, fd=%d", new_path, dirfd(dirp));
		ret = map_fd_put(map_fd, new_path, dirfd(dirp), p);
		// }
		slog_live("[POSIX]. End Hercules 'opendir', pathname=%s, new_path=%s, ret=%d\n", name, new_path, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling real 'opendir', pathname=%s", name);
		dirp = real_opendir(name);
		if (dirp == NULL)
		{ // on error, dirp is null.
			slog_full("[POSIX]. Ending real 'opendir', pathname=%s, fd=NULL", name);
		}
		else
		{
			slog_full("[POSIX]. Ending real 'opendir', pathname=%s, fd=%d", name, dirfd(dirp));
		}
	}
	return dirp;
}

char *mystrcat(char *dest, const char *src)
{
	while (*dest)
		dest++;
	while (*dest++ = *src++)
		;
	return --dest;
}

int myfiller(void *buf, const char *name, const struct stat *stbuf, off_t off)
{
	strcat((char *)buf, name);
	strcat((char *)buf, "$");
	return 1;
}

int OptFiller(void *buf, const char *name, const struct stat *stbuf, off_t off)
{
	buf = mystrcat((char *)buf, name);
	buf = mystrcat((char *)buf, "$");
	return 1;
}

ssize_t getdents64(int fd, void *dirp, size_t count)
{
	if (!real_getdents64)
		real_getdents64 = (ssize_t (*)(int, void *, size_t))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_getdents64(fd, dirp, count);
	}

	// slog_warn("Function still not supported");
	WarnOperationNotSupported(__func__, "GENERIC");

	return real_getdents64(fd, dirp, count);
}

int getdents(unsigned int fd, struct linux_dirent *dirp, unsigned int count)
{
	if (!real_getdents)
		real_getdents = (int (*)(unsigned int, struct linux_dirent *, unsigned int))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_getdents(fd, dirp, count);
	}

	// slog_warn("Function still not supported");
	WarnOperationNotSupported(__func__, "GENERIC");

	return real_getdents(fd, dirp, count);
}

struct dirent entry;
int to_read = 1;
char **ori_buf = NULL;
uint32_t n_ent = 0;
uint32_t imss_path_len = 0;
clock_t t;
int init_loop_timer = 1;
int init_number = 0;
struct dirent *readdir(DIR *dirp)
{
	if (!real_readdir)
		real_readdir = (dirent * (*)(DIR *)) dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_readdir(dirp);
	}

	errno = 0;
	size_t ret;
	// char *pathname = map_fd_search_by_val(map_fd, dirfd(dirp));
	std::string pathname_obj = map_fd_search_by_val(map_fd, dirfd(dirp));
	if (!pathname_obj.empty())
	{
		// ConcatLastSlash(pathname_obj);
		const char *pathname = pathname_obj.c_str();

		slog_live("[POSIX]. Calling Hercules 'readdir', pathname=%s, to_read=%d", pathname_obj.c_str(), to_read);

		if (init_loop_timer)
		{
			t = clock();
			init_loop_timer = 0;
		}

		const char *token = NULL;
		// fprintf(stdout,"to read=%d, n_ent=%d\n", to_read, n_ent);
		if (to_read)
		{
			n_ent = imss_readdir(pathname_obj, &ori_buf, OptFiller, 0);
			// fprintf(stderr, "%lu files will be listed\n", n_ent); // comment this line, used for debug.
			to_read = 0;
			imss_path_len = pathname_obj.length();
			if (pathname_obj[imss_path_len - 1] != '/')
			{ // is directory but does not contains the last slash.
				// this help us to avoid the last slash comming from the child entries.
				// for example, from imss://dir/filey.out we extract filey.out moving the pointer
				// "imss_path_len" (length of the parent directory) bytes.
				imss_path_len++;
			}
		}

		long int pos = TIMING(telldir(dirp), "telldir", long int, rank);

		// int i = 0;
		// slog_live("Init while, first token=%s, pos=%lu", token, pos);
		if (pos < n_ent + 1) // +1 to add ".."
		{
			memset(&entry, 0, sizeof(struct dirent));
			int idx = pos - 1;

			if (pos == 0)
			{
				// strncpy(entry.d_name, "..", sizeof(".."));
				token = ".";
			}
			else if (pos == 1)
			{
				token = "..";
			}
			else
			{
				// uint32_t offset = URI_*(pos-1);
				// printf("idx=%d\n", idx);
				token = (char *)ori_buf[idx] + imss_path_len;
				slog_live("[POSIX] ori_buf[%d]=%s, current token=%s, pos=%d", pos, ori_buf[idx], token, pos);
			}

			size_t len = strlen(token);
			if (!strncmp(token, ".", strlen(token)))
			{ // current directory.
				entry.d_type = DT_DIR;
			}
			else if (!strncmp(token, "..", strlen(token)))
			{ // parent directory.
				entry.d_type = DT_DIR;
			}
			else
			{
				// to get the type of this entry.
				if (len > 0 && token[len - 1] != '/')
				{
					entry.d_type = DT_REG;
				}
				else
				{
					entry.d_type = DT_DIR;
					len -= 1; // to skip the last lash.
				}
			}

			// slog_live("[POSIX] current token=%s, i=%d, pos=%d", token, i, pos);
			slog_live("[POSIX] current token=%s, pos=%d", token, pos);
			entry.d_ino = 0;
			entry.d_off = pos;

			// name of file
			strncpy(entry.d_name, token, len);

			// if (pos % 1000 == 0 || pos == n_ent) // TODO: comments this lines, only for debug.
			// {									 // print a message every 1000 files.
			// 	t = clock() - t;
			// 	double time_taken = 0.0;
			// 	time_taken = ((double)t) / (CLOCKS_PER_SEC);
			// 	int diff = (pos == 0) ? 1 : pos - init_number;
			// 	fprintf(stdout, "Loading %d/%lu files, please wait. Time taken to get %d/1000 files: %f\n", pos, n_ent, diff, time_taken);
			// 	init_loop_timer = 1;
			// 	init_number = pos;
			// }
			// printf("[%s] Reading entry %s, %" PRIu32 " of %u, please wait\n", pathname, entry.d_name,  pos, n_ent);

			char path_search[PATH_MAX] = {0};
			// Add the slash between pathname and token if missing.
			len = strlen(pathname);
			if (len > 0 && pathname[len - 1] != '/')
			{
				sprintf(path_search, "%s/%s", pathname, token);
			}
			else
			{
				sprintf(path_search, "%s%s", pathname, token);
			}

			// length of this record
			if (strlen(token) < 5)
			{
				entry.d_reclen = 24;
			}
			else
			{
				entry.d_reclen = ceil((double)(strlen(token) - 4) / 8) * 8 + 24;
			}
			slog_live("[imss_posix] path_searched = %s", path_search);

			// if (pos > 1)
			// {
			// 	if (ori_buf[idx] != NULL)
			// 	{
			// 		free(ori_buf[idx]);
			// 	}
			// }

			TIMING_NO_RETURN(seekdir(dirp, pos + 1), "seekdir", rank);
		}
		if (token == NULL)
		{
			slog_live("[POSIX]. Last token for the directory %s reached.", pathname);
			to_read = 1;
			// Free memory.
			// fprintf(stdout,"Last token\n");
			free_entries(&ori_buf, n_ent);
			// fprintf(stdout,"after free ori_buf=%p\n", ori_buf);

			return NULL;
		}
		slog_live("[POSIX]. Ending Hercules 'readdir',  pathname=%s\n", pathname);
		return &entry;
	} // end of Hercules case.

	slog_full("[POSIX]. Calling real 'readdir', fd=%d.", dirfd(dirp));
	return real_readdir(dirp);
}

struct dirent64 *readdir64(DIR *dirp)
{
	if (!real_readdir64)
		real_readdir64 = (dirent64 * (*)(DIR *)) dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_readdir64(dirp);
	}

	errno = 0;
	std::string pathname_ob = map_fd_search_by_val(map_fd, dirfd(dirp));
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX]. Calling Hercules 'readdir64', pathname=%s", pathname);
		struct dirent64 *entry;
		entry = (struct dirent64 *)readdir(dirp);
		slog_live("[POSIX]. Ending Hercules 'readdir64', pathname=%s\n", pathname);
		return entry;
	}
	else
	{
		slog_full("[POSIX]. Calling real 'readdir64', fd=%d", dirfd(dirp));
		return real_readdir64(dirp);
	}
}

/**
 * @return The closedir() function returns 0 on success.
 * On error, -1 is returned, and errno is set to indicate the error.
 */
int closedir(DIR *dirp)
{
	if (!real_closedir)
		real_closedir = (int (*)(DIR *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_closedir(dirp);
	}

	errno = 0;
	int ret = -1;
	int fd = dirfd(dirp);
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		// size_t len = strlen(pathname);
		// if (len > 0 && pathname[len - 1] != '/')
		// {
		// 	strcat(pathname, "/");
		// }
		// fprintf(stderr, "Hercules closedir, %s\n", pathname);
		slog_live("[POSIX] Calling Hercules 'closedir', pathname=%s, fd=%d.", pathname, fd);

		// Closes the dataset on the backend and delete it when the dataset status is "dest" and no more process has the file open.
		ret = map_fd_search_by_val_close(map_fd, fd);

		if (ret != 0)
		{
			errno = EEXIST;
			slog_error("[POSIX] Error while Hercules closed the directory %s, errno=%d:%s", pathname, errno, strerror(errno));
		}

		// ret = real_closedir(dirp);
		real_closedir(dirp);
		// free(pathname);
		// if(ret == -1) {
		// 	slog_error("[POSIX] Error closing the real directory descriptor for %s", pathname);
		// 	perror("HERCULES_ERR_CLOSING_REAL_DIRECTORY");
		// }

		// Free resources
		// free_entries(&ori_buf, n_ent);
		to_read = 1;
		n_ent = 0;

		slog_live("[POSIX] End Hercules 'closedir', fd=%d, ret=%d\n", fd, ret);
	}
	else
	{
		slog_full("[POSIX] Calling Real 'closedir', fd=%d", fd);
		ret = real_closedir(dirp);
		slog_full("[POSIX] End Real 'closedir', fd=%d, ret=%d", fd, ret);
	}

	return ret;
}

// int _openat(int dirfd, const char *pathname, int flags)
// {
// 	fprintf(stderr, "Calling _openat %s\n", pathname);
// 	return 1;
// }

// int __openat64(int fd, const char *file, int oflag)
// {
// 	fprintf(stderr, "Calling __openat64 %s\n", file);
// 	return 1;
// }

// int __openat64_2(int fd, const char *file, int oflag)
// {
// 	fprintf(stderr, "Calling __openat64_2 %s\n", file);
// 	return 1;
// }

// int __libc_openat(int fd, const char *file, int oflag, ...)
// {
// 	fprintf(stderr, "Calling __libc_openat %s\n", file);
// 	return 1;
// }

// int __libc_open64(const char *file, int oflag, ...)
// {
// 	fprintf(stderr, "Calling __libc_open64 %s\n", file);
// 	return 1;
// }

// int __open64(const char *file, int oflag, ...)
// {
// 	fprintf(stderr, "Calling __open64 %s\n", file);
// 	return 1;
// }

// int __open(const char *file, int oflag, ...)
// {
// 	fprintf(stderr, "Calling __open %s\n", file);
// 	return 1;
// }

// int _open(const char *pathname, int flags, ...)
// {
// 	fprintf(stderr, "Calling _open %s\n", pathname);
// 	return 1;
// }

/*****************************/

int stat64(const char *pathname, struct stat64 *buf)
{
	if (!real_stat64)
		real_stat64 = (int (*)(const char *, struct stat64 *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_stat64(pathname, buf);
	}

	errno = 0;
	int ret;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'stat', new_path=%s.", new_path);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, (struct stat *)buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		slog_live("[POSIX]. Ending Hercules 'stat', new_path=%s, ret=%d, errno=%d:%s", new_path, ret, errno, strerror(errno));
		free(new_path);
	}
	else
	{
		slog_live("[POSIX]. Calling Real 'stat64', pathname=%s.", pathname);
		ret = real_stat64(pathname, buf);
		slog_live("[POSIX]. Ending Real 'stat64', pathname=%s.", pathname);
	}

	return ret;
}

// TODO: add support for stat64 on "imss_getattr".
int fstat64(int fd, struct stat64 *buf)
{
	if (!real_fstat64)
		real_fstat64 = (int (*)(int, struct stat64 *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fstat64(fd, buf);
	}

	errno = 0;
	int ret;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX] Calling Hercules 'fstat64', pathname=%s, fd=%d.", pathname, fd);
		imss_refresh(pathname);
		ret = imss_getattr(pathname, (struct stat *)buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules 'fstat64', errno=%d:%s", errno, strerror(errno));
		}

		slog_live("[POSIX] End Hercules 'fstat64', pathname=%s, fd=%d, errno=%d:%s, ret=%d, st_size=%ld, st_blocks=%ld, st_blksize=%ld\n", pathname, fd, errno, strerror(errno), ret, buf->st_size, buf->st_blocks, buf->st_blksize);
	}
	else
	{
		slog_live("[POSIX] Calling Real 'fstat64', fd=%d", fd);
		// fprintf(stderr, "[POSIX] Calling Real 'fstat', fd=%d", fd);
		ret = real_fstat64(fd, buf);
		slog_live("[POSIX] End Real 'fstat64', fd=%d, errno=%d:%s, ret=%d, st_size=%ld, st_blocks=%ld, st_blksize=%ld", fd, errno, strerror(errno), ret, buf->st_size, buf->st_blocks, buf->st_blksize);
	}
	return ret;
}

int fstat(int fd, struct stat *buf)
{
	if (!real_fstat)
		real_fstat = (int (*)(int, struct stat *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real_fstat(fd, buf);
	}

	errno = 0;
	int ret;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX] Calling Hercules 'fstat', pathname=%s, fd=%d.", pathname, fd);
		imss_refresh(pathname);
		ret = imss_getattr(pathname, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules 'fstat', errno=%d:%s", errno, strerror(errno));
		}

		slog_live("[POSIX] End Hercules 'fstat', pathname=%s, fd=%d, errno=%d:%s, ret=%d, st_size=%ld, st_blocks=%ld, st_blksize=%ld\n", pathname, fd, errno, strerror(errno), ret, buf->st_size, buf->st_blocks, buf->st_blksize);
	}
	else
	{
		slog_live("[POSIX] Calling Real 'fstat', fd=%d", fd);
		// fprintf(stderr, "[POSIX] Calling Real 'fstat', fd=%d", fd);
		ret = real_fstat(fd, buf);
		slog_live("[POSIX] End Real 'fstat', fd=%d, errno=%d:%s, ret=%d, st_size=%ld, st_blocks=%ld, st_blksize=%ld", fd, errno, strerror(errno), ret, buf->st_size, buf->st_blocks, buf->st_blksize);
	}
	return ret;
}

int __fxstatat(int ver, int dir_fd, const char *pathname, struct stat *stat_buf, int flags)
{
	if (!real___fxstatat)
	{
		real___fxstatat = (int (*)(int, int, const char *, struct stat *, int))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real___fxstatat(ver, dir_fd, pathname, stat_buf, flags);
	}

	errno = 0;
	int ret = 0;
	// char *pathname_dir = NULL, *new_path = NULL;
	// if (dir_fd == AT_FDCWD)
	// {
	// 	// pathname_dir = getenv("PWD");
	// 	// new_path = checkHerculesPath(pathname_dir);
	// 	// pathname_dir = NULL;
	// 	new_path = checkHerculesPath(pathname);
	// 	pathname_dir = NULL;
	// }
	// else
	// {
	// 	pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
	// }
	std::string pathname_dir_op = "";
	char *new_path = NULL;
	ResolvePathsAndFD(dir_fd, pathname, pathname_dir_op, &new_path);

	if (pathname_dir_op != "" || new_path != NULL)
	{
		const char *pathname_dir = pathname_dir_op.c_str();
		// fprintf(stderr, "[POSIX] Calling Hercules '__fxstatat' flags=%d, dir_fd=%d, pathname_dir=%s, new_path_dir=%s, pathname=%s\n", flags, dir_fd, pathname_dir, new_path, pathname);
		slog_live("[POSIX] Calling Hercules '__fxstatat', flags=%d, dir_fd=%d, pathname_dir=%s, pathname=%s", flags, dir_fd, pathname_dir, pathname);
		int is_absolute_path = IsAbsolutePath(pathname);

		// If pathname is absolute, then dir_fd is ignored.
		if (is_absolute_path == 1)
		{
			// char *new_path = checkHerculesPath(pathname);
			slog_live("[POSIX] is absolute, '__fxstatat', pathname=%s", pathname);
			ret = stat(pathname, stat_buf);

			// slog_live("[POSIX] stat_buf->st_size=%ld", stat_buf->st_size);

			// ret = generalOpen(new_path, flags, mode);
			// free(new_path);
		}
		else if (is_absolute_path == 0) // pathname is relative.
		{
			if (dir_fd == AT_FDCWD) // dir_fd is the special value AT_FDCWD.
			{			// TO CHECK!
				// char *new_path = checkHerculesPath(pathname);
				// slog_live("[POSIX] is relative, current directory, '__fxstatat', new_path=%s", new_path);
				slog_live("[POSIX] is relative, current directory, '__fxstatat', pathname=%s", pathname);
				// pathname is interpreted relative to the current working directory of the calling process (like real_open).
				// ret = generalOpen(new_path, flags, mode);
				ret = stat(pathname, stat_buf);
				// free(new_path);
			}
			else
			{
				// // get the pathname of the directory pointed by dir_fd if it is storage in the local map "map_fd".
				// char *pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
				// if (pathname_dir == NULL)
				// { // if dir_fd is not storage locally
				// 	// search dir_fd on the metadata server.
				// 	// fprintf(stderr,"");
				// 	slog_error("[POSIX] dir_fd=%d could not be resolved.");
				// 	return -1;
				// }

				char absolute_pathname[PATH_MAX] = {0};
				char *dirr = (char *)pathname_dir + strlen("imss://");
				sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, pathname);

				// char *new_path = checkHerculesPath(absolute_pathname);
				//  slog_live("[POSIX] is relative, '__fxstatat', new_path=%s", new_path);
				slog_live("[POSIX] is relative, '__fxstatat', pathname_dir=%s, absolute_pathname=%s", pathname_dir, absolute_pathname);
				ret = stat(absolute_pathname, stat_buf);

				// ret = generalOpen(new_path, flags, mode);
				// free(new_path);
			}
		}

		slog_live("[POSIX] Ending Hercules '__fxstatat', ret=%d\n", ret);
		if (new_path != NULL)
			free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real '__fxstatat', flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
		// fprintf(stderr, "[POSIX] Calling real '__fxstatat' flags=%d, dir_fd=%d, pathname=%s\n", flags, dir_fd, pathname);
		ret = real___fxstatat(ver, dir_fd, pathname, stat_buf, flags);
		slog_full("[POSIX] Ending real '__fxstatat', flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
	}

	return ret;
}

int __fxstatat64(int ver, int dir_fd, const char *pathname, struct stat64 *stat_buf, int flags)
{
	if (!real___fxstatat64)
	{
		real___fxstatat64 = (int (*)(int, int, const char *, struct stat64 *, int))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real___fxstatat64(ver, dir_fd, pathname, stat_buf, flags);
	}

	errno = 0;
	int ret = 0;
	// char *pathname_dir, *new_path = NULL;
	// if (dir_fd == AT_FDCWD)
	// {
	// 	// pathname_dir = getenv("PWD");
	// 	// new_path = checkHerculesPath(pathname_dir);
	// 	// pathname_dir = NULL;
	// 	new_path = checkHerculesPath(pathname);
	// 	pathname_dir = NULL;
	// }
	// else
	// {
	// 	pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
	// }

	std::string pathname_dir_op = "";
	char *new_path = NULL;
	ResolvePathsAndFD(dir_fd, pathname, pathname_dir_op, &new_path);

	if (pathname_dir_op != "" || new_path != NULL)
	{
		const char *pathname_dir = pathname_dir_op.c_str();
		// fprintf(stderr, "[POSIX] Calling Hercules '__fxstatat' flags=%d, dir_fd=%d, pathname_dir=%s, new_path_dir=%s, pathname=%s\n", flags, dir_fd, pathname_dir, new_path, pathname);
		slog_live("[POSIX] Calling Hercules '__fxstatat64' flags=%d, dir_fd=%d, pathname_dir=%s, pathname=%s", flags, dir_fd, pathname_dir, pathname);
		int is_absolute_path = IsAbsolutePath(pathname);

		// If pathname is absolute, then dir_fd is ignored.
		if (is_absolute_path == 1)
		{
			// char *new_path = checkHerculesPath(pathname);
			slog_live("[POSIX] is absolute, '__fxstatat', pathname=%s", pathname);
			ret = stat64(pathname, stat_buf);

			// ret = generalOpen(new_path, flags, mode);
			// free(new_path);
		}
		else if (is_absolute_path == 0) // pathname is relative.
		{
			if (dir_fd == AT_FDCWD) // dir_fd is the special value AT_FDCWD.
			{			// TO CHECK!
				// char *new_path = checkHerculesPath(pathname);
				// slog_live("[POSIX] is relative, current directory, '__fxstatat', new_path=%s", new_path);
				slog_live("[POSIX] is relative, current directory, '__fxstatat', pathname=%s", pathname);
				// pathname is interpreted relative to the current working directory of the calling process (like real_open).
				// ret = generalOpen(new_path, flags, mode);
				ret = stat64(pathname, stat_buf);
				// free(new_path);
			}
			else
			{
				// // get the pathname of the directory pointed by dir_fd if it is storage in the local map "map_fd".
				// char *pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
				// if (pathname_dir == NULL)
				// { // if dir_fd is not storage locally
				// 	// search dir_fd on the metadata server.
				// 	// fprintf(stderr,"");
				// 	slog_error("[POSIX] dir_fd=%d could not be resolved.");
				// 	return -1;
				// }

				char absolute_pathname[PATH_MAX] = {0};
				char *dirr = (char *)pathname_dir + strlen("imss://");
				sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, pathname);

				// char *new_path = checkHerculesPath(absolute_pathname);
				//  slog_live("[POSIX] is relative, '__fxstatat', new_path=%s", new_path);
				slog_live("[POSIX] is relative, '__fxstatat', absolute_pathname=%s", absolute_pathname);
				ret = stat64(absolute_pathname, stat_buf);

				// ret = generalOpen(new_path, flags, mode);
				// free(new_path);
			}
		}

		slog_live("[POSIX] Ending Hercules '__fxstatat64', ret=%d, errno=%d:%s\n", ret, errno, strerror(errno));
		if (new_path != NULL)
			free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real '__fxstatat64' flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
		// fprintf(stderr, "[POSIX] Calling real '__fxstatat' flags=%d, dir_fd=%d, pathname=%s\n", flags, dir_fd, pathname);
		ret = real___fxstatat64(ver, dir_fd, pathname, stat_buf, flags);
		// slog_live("[POSIX] Ending real '__fxstatat64' flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
	}

	return ret;
}

int GeneralFAccessAt(int dir_fd, const char *pathname, int mode, int flags, char *pathname_dir)
{
	int ret = -1;
	int is_absolute_path = IsAbsolutePath(pathname);

	// If pathname is absolute, then dir_fd is ignored.
	if (is_absolute_path == 1)
	{
		// char *new_path = checkHerculesPath(pathname);
		slog_live("[POSIX] is absolute, 'faccessat2', pathname=%s", pathname);
		ret = access(pathname, mode);
	}
	else if (is_absolute_path == 0) // pathname is relative.
	{
		if (dir_fd == AT_FDCWD) // dir_fd is the special value AT_FDCWD.
		{			// TO CHECK!
			slog_live("[POSIX] is relative, current directory, 'faccessat2', pathname=%s", pathname);
			ret = access(pathname, mode);
		}
		else
		{
			char absolute_pathname[PATH_MAX];
			char *dirr = pathname_dir + strlen("imss://");
			sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, pathname);

			slog_live("[POSIX] is relative, 'faccessat2', absolute_pathname=%s", absolute_pathname);
			ret = access(absolute_pathname, mode);
		}
	}
	return ret;
}

int faccessat2(int dir_fd, const char *pathname, int mode, int flags)
{
	if (!real_faccessat2)
	{
		real_faccessat2 = (int (*)(int, const char *, int, int))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real_faccessat2(dir_fd, pathname, mode, flags);
	}

	errno = 0;
	int ret = 0;
	// char *pathname_dir = NULL, *new_path = NULL;
	// if (dir_fd == AT_FDCWD)
	// {
	// 	new_path = checkHerculesPath(pathname);
	// 	pathname_dir = NULL;
	// }
	// else
	// {
	// 	pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
	// }

	std::string pathname_dir_op = "";
	char *new_path = NULL;
	ResolvePathsAndFD(dir_fd, pathname, pathname_dir_op, &new_path);

	if (pathname_dir_op != "" || new_path != NULL)
	{
		const char *pathname_dir = pathname_dir_op.c_str();
		slog_live("[POSIX] Calling Hercules 'faccessat2' flags=%d, dir_fd=%d, pathname_dir=%s, pathname=%s", flags, dir_fd, pathname_dir, pathname);

		ret = GeneralFAccessAt(dir_fd, pathname, mode, flags, (char *)pathname_dir);

		slog_live("[POSIX] Ending Hercules 'faccessat2', ret=%d, errno=%d:%s\n", ret, errno, strerror(errno));
		if (new_path != NULL)
			free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real 'faccessat2' flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
		ret = real_faccessat2(dir_fd, pathname, mode, flags);
		slog_full("[POSIX] Ending real 'faccessat2' flags=%d, dir_fd=%d, pathname=%s, errno=%d:%s", flags, dir_fd, pathname, errno, strerror(errno));
	}

	return ret;
}

int faccessat(int dir_fd, const char *pathname, int mode, int flags)
{
	if (!real_faccessat)
	{
		real_faccessat = (int (*)(int, const char *, int, int))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real_faccessat(dir_fd, pathname, mode, flags);
	}

	errno = 0;
	// int saved_errno;
	int ret = 0;
	// char *pathname_dir = NULL, *new_path = NULL;
	// if (dir_fd == AT_FDCWD)
	// {
	// 	// pathname_dir = getenv("PWD");
	// 	// new_path = checkHerculesPath(pathname_dir);
	// 	// pathname_dir = NULL;
	// 	new_path = checkHerculesPath(pathname);
	// 	pathname_dir = NULL;
	// }
	// else
	// {
	// 	pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
	// }

	std::string pathname_dir_op = "";
	char *new_path = NULL;
	ResolvePathsAndFD(dir_fd, pathname, pathname_dir_op, &new_path);

	if (pathname_dir_op != "" || new_path != NULL)
	{
		const char *pathname_dir = pathname_dir_op.c_str();
		slog_live("[POSIX] Calling Hercules 'faccessat' flags=%d, dir_fd=%d, pathname_dir=%s, pathname=%s", flags, dir_fd, pathname_dir, pathname);

		ret = GeneralFAccessAt(dir_fd, pathname, mode, flags, (char *)pathname_dir);

		slog_live("[POSIX] Ending Hercules 'faccessat', ret=%d, errno=%d:%s\n", ret, errno, strerror(errno));
		if (new_path != NULL)
			free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real 'faccessat' flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
		ret = real_faccessat(dir_fd, pathname, mode, flags);
		slog_full("[POSIX] Ending real 'faccessat' flags=%d, dir_fd=%d, pathname=%s, errno=%d:%s", flags, dir_fd, pathname, errno, strerror(errno));
	}

	return ret;
}

int unlinkat(int dir_fd, const char *pathname, int flags)
{
	if (!real_unlinkat)
	{
		real_unlinkat = (int (*)(int, const char *, int))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real_unlinkat(dir_fd, pathname, flags);
	}

	errno = 0;
	int ret = 0;
	// char *pathname_dir, *new_path = NULL;
	// if (dir_fd == AT_FDCWD)
	// {
	// 	// pathname_dir = getenv("PWD");
	// 	// new_path = checkHerculesPath(pathname_dir);
	// 	// pathname_dir = NULL;
	// 	new_path = checkHerculesPath(pathname);
	// 	pathname_dir = NULL;
	// }
	// else
	// {
	// 	pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
	// }

	std::string pathname_dir_op = "";
	char *new_path = NULL;
	ResolvePathsAndFD(dir_fd, pathname, pathname_dir_op, &new_path);

	if (pathname_dir_op != "" || new_path != NULL)
	{
		const char *pathname_dir = pathname_dir_op.c_str();
		slog_live("[POSIX] Calling Hercules 'unlinkat' flags=%d, dir_fd=%d, pathname_dir=%s, pathname=%s", flags, dir_fd, pathname_dir, pathname);
		int is_absolute_path = IsAbsolutePath(pathname);

		// If pathname is absolute, then dir_fd is ignored.
		if (is_absolute_path == 1)
		{
			// char *new_path = checkHerculesPath(pathname);
			slog_live("[POSIX] is absolute, 'unlinkat', pathname=%s", pathname);
			ret = unlink(pathname);
			// ret = generalOpen(new_path, flags, mode);
			// free(new_path);
		}
		else if (is_absolute_path == 0) // pathname is relative.
		{
			if (dir_fd == AT_FDCWD) // dir_fd is the special value AT_FDCWD.
			{			// TO CHECK!
				// char *new_path = checkHerculesPath(pathname);
				// slog_live("[POSIX] is relative, current directory, 'unlinkat', new_path=%s", new_path);
				slog_live("[POSIX] is relative, current directory, 'unlinkat', pathname=%s", pathname);
				// pathname is interpreted relative to the current working directory of the calling process (like real_open).
				// ret = generalOpen(new_path, flags, mode);
				ret = unlink(pathname);
				// free(new_path);
			}
			else
			{
				// // get the pathname of the directory pointed by dir_fd if it is storage in the local map "map_fd".
				// char *pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
				// if (pathname_dir == NULL)
				// { // if dir_fd is not storage locally
				// 	// search dir_fd on the metadata server.
				// 	slog_error("[POSIX] dir_fd=%d could not be resolved.");
				// 	return -1;
				// }

				char absolute_pathname[PATH_MAX];
				char *dirr = (char *)pathname_dir + strlen("imss://");
				sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, pathname);

				// char *new_path = checkHerculesPath(absolute_pathname);
				//  slog_live("[POSIX] is relative, 'unlinkat', new_path=%s", new_path);
				slog_live("[POSIX] is relative, 'unlinkat', absolute_pathname=%s", absolute_pathname);
				ret = unlink(absolute_pathname);

				// ret = generalOpen(new_path, flags, mode);
				// free(new_path);
			}
		}

		slog_live("[POSIX] Ending Hercules 'unlinkat', ret=%d\n", ret);
		if (new_path != NULL)
			free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real 'unlinkat' flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
		ret = real_unlinkat(dir_fd, pathname, flags);
		slog_full("[POSIX] Ending real 'unlinkat' flags=%d, dir_fd=%d, pathname=%s", flags, dir_fd, pathname);
	}

	return ret;
}

extern int renameat(int olddirfd, const char *oldpath, int newdirfd, const char *newpath)
{
	if (!init)
	{
		return renameat2(olddirfd, oldpath, newdirfd, newpath, 0);
	}
	WarnOperationNotSupported(__func__, oldpath);
	return renameat2(olddirfd, oldpath, newdirfd, newpath, 0);
}

extern int renameat2(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags)
{
	if (!real_renameat2)
	{
		real_renameat2 = (int (*)(int, const char *, int, const char *, unsigned int))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real_renameat2(olddirfd, oldpath, newdirfd, newpath, flags);
	}

	errno = 0;
	int ret = 0;
	std::string old_pathname_dir_op = "";
	char *old_new_path = NULL;
	std::string new_pathname_dir_op = "";
	char *new_new_path = NULL;
	// int to_free_old_path = 0, to_free_new_path = 0;

	ResolvePathsAndFD(olddirfd, oldpath, old_pathname_dir_op, &old_new_path);
	ResolvePathsAndFD(newdirfd, newpath, new_pathname_dir_op, &new_new_path);

	// if (pathname_dir != NULL || new_path != NULL)
	if (old_new_path || old_pathname_dir_op != "" || new_new_path || new_pathname_dir_op != "")
	{
		const char *old_pathname_dir = old_pathname_dir_op.c_str();
		const char *new_pathname_dir = new_pathname_dir_op.c_str();

		// size_t len = strlen(new_new_path);
		// if (len > 0 && new_new_path[len - 1] == '/')
		// {
		// 	strcat(new_new_path, old_new_path);
		// }

		// TODO: check if old path is directory.

		// TODO: check if new path is directory.

		slog_live("[POSIX] Calling Hercules 'renameat2', flags=%d, newdirfd=%d, oldpath=%s, newdirfd=%d, newpath=%s", flags, newdirfd, oldpath, newdirfd, newpath);
		int is_absolute_path = IsAbsolutePath(oldpath);

		// If newpath is absolute, then newdirfd is ignored.
		if (is_absolute_path == 1)
		{
			slog_live("[POSIX] is absolute, 'renameat2', oldpath=%s", oldpath);
			ret = rename(oldpath, newpath);
		}
		else if (is_absolute_path == 0) // newpath is relative.
		{
			if (newdirfd == AT_FDCWD) // newdirfd is the special value AT_FDCWD.
			{			  // TO CHECK!
				slog_live("[POSIX] is relative, current directory, 'renameat2', newpath=%s", newpath);
				// pathname is interpreted relative to the current working directory of the calling process (like real_open).
				ret = rename(oldpath, newpath);
			}
			else
			{
				// // get the pathname of the directory pointed by dir_fd if it is storage in the local map "map_fd".
				char absolute_pathname[PATH_MAX] = {0};
				char *dirr = (char *)new_pathname_dir + strlen("imss://");
				sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, newpath);

				// char *new_path = checkHerculesPath(absolute_pathname);
				//  slog_live("[POSIX] is relative, 'renameat2', new_path=%s", new_path);
				slog_live("[POSIX] is relative, 'renameat2', absolute_pathname=%s", absolute_pathname);
				ret = rename(oldpath, newpath);
			}
		}

		slog_live("[POSIX] Ending Hercules 'renameat2', ret=%d\n", ret);
		if (old_new_path)
			free(old_new_path);
		if (new_new_path)
			free(new_new_path);
	}
	else
	{
		slog_live("[POSIX] Calling real 'renameat2', flags=%d, olddirfd=%d, oldpath=%s, newdirfd=%d, newpath=%s", flags, olddirfd, oldpath, newdirfd, newpath);
		ret = real_renameat2(olddirfd, oldpath, newdirfd, newpath, flags);
		slog_live("[POSIX] Calling real 'renameat2', flags=%d, olddirfd=%d, oldpath=%s, newdirfd=%d, newpath=%s, ret=%d", flags, olddirfd, oldpath, newdirfd, newpath, ret);
	}

	return ret;
}

int __fxstat64(int ver, int fd, struct stat64 *buf)
{
	if (!real__fxstat64)
	{
		real__fxstat64 = (int (*)(int, int, struct stat64 *))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real__fxstat64(ver, fd, buf);
	}

	errno = 0;
	int ret;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX] Calling Hercules '__fxstat64', pathname=%s, fd=%d.", pathname, fd);
		imss_refresh(pathname);
		ret = imss_getattr(pathname, (struct stat *)buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules '__fxstat64'	: %s", strerror(errno));
		}

		slog_live("[POSIX] End Hercules '__fxstat64', pathname=%s, fd=%d, errno=%d:%s, ret=%d, st_size=%ld, st_blocks=%ld, st_blksize=%ld\n", pathname, fd, errno, strerror(errno), ret, buf->st_size, buf->st_blocks, buf->st_blksize);
	}
	else
	{
		slog_full("[POSIX] Calling real '__fxstat64', ver=%d, fd=%d", ver, fd);
		ret = real__fxstat64(ver, fd, buf);
		slog_full("[POSIX] End real '__fxstat64', fd=%d, ret=%d, st_size=%ld, st_blocks=%ld, st_blksize=%ld", fd, ret, buf->st_size, buf->st_blocks, buf->st_blksize);
	}
	// report(pathname, buf);
	// StatReport(fd, *buf);
	return ret;
}

int fstatat(int __fd, const char *__restrict __file, struct stat *__restrict __buf, int __flag)
{
	if (!real_fstatat)
	{
		real_fstatat = (int (*)(int, const char *, struct stat *, int))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real_fstatat(__fd, __file, __buf, __flag);
	}

	// fprintf(stdout, "Calling fstatat, pathname=%s\n", __file);

	int ret = 0;
	std::string pathname_ob = map_fd_search_by_val(map_fd, __fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		WarnOperationNotSupported(__func__, pathname);
		ret = real_fstatat(__fd, __file, __buf, __flag);
	}
	else
	{
		slog_full("[POSIX] Calling real 'fstatat', pathname=%s", __file);
		ret = real_fstatat(__fd, __file, __buf, __flag);
	}

	return ret;
}

int fstatat64(int __fd, const char *__restrict __file, struct stat64 *__restrict __buf, int __flag)
{
	if (!real_fstatat64)
	{
		real_fstatat64 = (int (*)(int, const char *, struct stat64 *, int))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real_fstatat64(__fd, __file, __buf, __flag);
	}

	// fprintf(stderr, "fstatat64\n");

	int ret = 0;
	std::string pathname_ob = map_fd_search_by_val(map_fd, __fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		WarnOperationNotSupported(__func__, pathname);
		// Write here the HERCULES implementation for this system call.
	}
	// Uncomment the following line.
	// else
	{
		slog_full("[POSIX] Calling real 'fstatat64', pathname=%s", __file);
		ret = real_fstatat64(__fd, __file, __buf, __flag);
	}

	return ret;
}

int statx(int dirfd, const char *pathname, int flags, unsigned int mask, struct statx *statxbuf)
{
	if (!real_statx)
	{
		real_statx = (int (*)(int, const char *, int, unsigned int, struct statx *))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real_statx(dirfd, pathname, flags, mask, statxbuf);
	}

	errno = 0;
	int ret;
	struct stat buf;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("[POSIX]. Calling Hercules 'statx', new_path=%s", new_path);
		imss_refresh(new_path);
		ret = imss_getattr(new_path, &buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
		}
		else
		{
			copy_stat_to_statx(&buf, statxbuf);
		}
		slog_live("[POSIX]. Ending Hercules 'statx', new_path=%s, ret=%d\n", new_path, ret);
		free(new_path);
	}
	else
	{
		slog_live("[POSIX]. Calling Real 'statx', pathname=%s.", pathname);
		ret = real_statx(dirfd, pathname, flags, mask, statxbuf);
		slog_live("[POSIX]. Ending Real 'statx', pathname=%s.", pathname);
	}

	return ret;
}

ssize_t readlink(const char *pathname, char *buf, size_t bufsiz)
{
	if (!real_readlink)
	{
		real_readlink = (long int (*)(const char *, char *, size_t))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real_readlink(pathname, buf, bufsiz);
	}

	ssize_t ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		WarnOperationNotSupported(__func__, pathname);
		ret = real_readlink(pathname, buf, bufsiz);
		free(new_path);
	}
	else
	{
		ret = real_readlink(pathname, buf, bufsiz);
	}

	return ret;
}

ssize_t readlinkat(int dirfd, const char *pathname, char *buf, size_t bufsiz)
{
	if (!real_readlinkat)
	{
		real_readlinkat = (ssize_t (*)(int, const char *, char *, size_t))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real_readlinkat(dirfd, pathname, buf, bufsiz);
	}

	ssize_t ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		WarnOperationNotSupported(__func__, pathname);
		ret = real_readlinkat(dirfd, pathname, buf, bufsiz);
		free(new_path);
	}
	else
	{
		ret = real_readlinkat(dirfd, pathname, buf, bufsiz);
	}

	return ret;
}

extern int newfstatat(int __fd, const char *__restrict __file, struct stat *__restrict __buf, int __flag)
{
	if (!real_newfstatat)
		real_newfstatat = (int (*)(int, const char *, struct stat *, int))dlsym(RTLD_NEXT, "newfstatat");

	if (!init)
	{
		return real_newfstatat(__fd, __file, __buf, __flag);
	}

	// slog_warn("[POSIX][TODO]. Calling Real 'newfstatat', pathname=%s\n", __file);
	WarnOperationNotSupported(__func__, __file);
	// fprintf(stderr, "newfstatat\n");
	// TODO.

	return real_newfstatat(__fd, __file, __buf, __flag);
}

void StatReport(int fd, struct stat sb)
{
	slog_info("File descriptor: %d", fd);
	slog_info("ID of containing device:  [%x,%x]",
		  major(sb.st_dev),
		  minor(sb.st_dev));

	slog_info("File type:                ");

	switch (sb.st_mode & S_IFMT)
	{
	case S_IFBLK:
		slog_info("\tblock device");
		break;
	case S_IFCHR:
		slog_info("\tcharacter device");
		break;
	case S_IFDIR:
		slog_info("\tdirectory");
		break;
	case S_IFIFO:
		slog_info("\tFIFO/pipe");
		break;
	case S_IFLNK:
		slog_info("\tsymlink");
		break;
	case S_IFREG:
		slog_info("\tregular file");
		break;
	case S_IFSOCK:
		slog_info("\tsocket");
		break;
	default:
		slog_info("\tunknown?");
		break;
	}

	slog_info("I-node number:            %ju", (uintmax_t)sb.st_ino);

	slog_info("Mode:                     %jo (octal)",
		  (uintmax_t)sb.st_mode);

	slog_info("Link count:               %ju", (uintmax_t)sb.st_nlink);
	slog_info("Ownership:                UID=%ju   GID=%ju",
		  (uintmax_t)sb.st_uid, (uintmax_t)sb.st_gid);

	slog_info("Preferred I/O block size: %jd bytes",
		  (intmax_t)sb.st_blksize);
	slog_info("File size:                %jd bytes",
		  (intmax_t)sb.st_size);
	slog_info("Blocks allocated:         %jd",
		  (intmax_t)sb.st_blocks);

	slog_info("Last status change:       %s", ctime(&sb.st_ctime));
	slog_info("Last file access:         %s", ctime(&sb.st_atime));
	slog_info("Last file modification:   %s", ctime(&sb.st_mtime));
}

int __fxstat(int ver, int fd, struct stat *buf)
{
	if (!real__fxstat)
		real__fxstat = (int (*)(int, int, struct stat *))dlsym(RTLD_NEXT, __func__);

	if (!init)
	{
		return real__fxstat(ver, fd, buf);
	}

	errno = 0;
	int ret;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX] Calling Hercules '__fxstat', pathname=%s, fd=%d.", pathname, fd);
		imss_refresh(pathname);
		ret = imss_getattr(pathname, buf);
		if (ret < 0)
		{
			errno = -ret;
			ret = -1;
			slog_error("[POSIX] Error Hercules '__fxstat'	: %s", strerror(errno));
		}

		slog_live("[POSIX] End Hercules '__fxstat', pathname=%s, fd=%d, ret=%d\n", pathname, fd, ret);
	}
	else
	{
		slog_full("[POSIX] Calling real '__fxstat', ver=%d, fd=%d", ver, fd);
		ret = real__fxstat(ver, fd, buf);
		slog_full("[POSIX] End real '__fxstat', ver=%d, fd=%d, ret=%d", ver, fd);
		// slog_full("[POSIX] End real '__fxstat', fd=%d, ret=%d, st_size=%ld, st_blocks=%ld, st_blksize=%ld", fd, ret, buf->st_size, buf->st_blocks, buf->st_blksize);
	}
	// report(pathname, buf);
	// StatReport(fd, *buf);
	return ret;
}

// int __fwprintf_chk(FILE *stream, int flag, const wchar_t *format)
// {
// 	if (!real___fwprintf_chk)
// 		real___fwprintf_chk = dlsym(RTLD_NEXT, "__fwprintf_chk");
// 	//fprintf(stderr, "Calling __fwprintf_chk\n");
// 	// TODO.
// 	slog_warn("[POSIX][TODO] Calling __fwprintf_chk\n");
// 	return real___fwprintf_chk(stream, flag, format);
// }

int access(const char *path, int mode)
{
	if (!real_access)
		real_access = (int (*)(const char *, int))dlsym(RTLD_NEXT, "access");

	if (!init)
	{
		return real_access(path, mode);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(path);
	if (new_path != NULL)
	{
		// pthread_mutex_lock(&system_lock);
		struct stat stat_buf;
		int permissions = 0;
		slog_live("Calling Hercules 'access', new_path=%s", new_path);

		// Skip special case where the mount point is checked.
		// TODO: change imss:// for a variable.
		if (!strncmp(new_path, "imss://", strlen(new_path)))
		{
			ret = 0;
		}
		else
		{
			ret = imss_refresh(new_path);
			// if (ret < 0)
			// {
			// 	// errno = -ret;
			// 	ret = -1;
			// 	// perror("ERRIMSS_ACCESS_IMSSREFRESH");
			// }
			ret = imss_getattr(new_path, &stat_buf);
			if (ret < 0)
			{
				errno = -ret;
				ret = -1;
				// perror("ERRIMSS_ACCESS_IMSSGETATTR");
			}
			else
			{
				errno = 0;
				// ret = imss_getattr(new_path, &stat_buf);
				// if (ret < 0)
				// {
				// 	errno = -ret;
				// 	ret = -1;
				// 	// perror("ERRIMSS_ACCESS_IMSSGETATTR");
				// }
				// else
				{
					/* check permissions */
					if ((mode & F_OK) == F_OK)
						permissions |= F_OK; /* file exists */
					if ((mode & R_OK) == R_OK && (stat_buf.st_mode & S_IRUSR))
						permissions |= R_OK; /* read permissions granted */
					if ((mode & W_OK) == W_OK && (stat_buf.st_mode & S_IWUSR))
						permissions |= W_OK; /* write permissions granted */
					if ((mode & X_OK) == X_OK && (stat_buf.st_mode & S_IXUSR))
						permissions |= X_OK; /* execute permissions granted */

					/* check if all the tested permissions are granted */
					if (mode == permissions)
						ret = 0;
					else
						ret = -1;
				}
			}
		}

		// pthread_mutex_unlock(&system_lock);
		slog_live("[POSIX]. End Hercules 'access', new_path=%s ret=%d\n", new_path, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX]. Calling Real 'access', path=%s", path);
		ret = real_access(path, mode);
		slog_full("[POSIX]. End Real 'access', path=%s, ret=%d", path, ret);
	}

	return ret;
}

int fsync(int fd)
{
	if (!real_fsync)
	{
		real_fsync = (int (*)(int))dlsym(RTLD_NEXT, __func__);
	}

	if (!init)
	{
		return real_fsync(fd);
	}
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_info("[POSIX] Calling Hercules fsync, fd=%d", fd);
		// TODO: TO CHECK! This function is used by IOR to I/O verification.
		// Upon successful completion, fsync() shall return 0. Otherwise,
		// -1 shall be returned and errno set to indicate the error.
		// If the fsync() function fails, outstanding I/O operations
		// are not guaranteed to have been completed.

		return 0;
	}
	else
	{
		slog_full("Calling Real 'fsync'")
		return real_fsync(fd);
	}
}

// int epoll_ctl(int epfd, int op, int fd, struct epoll_event *event)
// {
// 	if (!real_epoll_ctl)
// 		real_epoll_ctl = dlsym(RTLD_NEXT, "epoll_ctl");
// 	// fprintf(stderr, "Calling 'epoll_ctl'\n");
// 	return real_epoll_ctl(epfd, op, fd, event);
// }

// int change_to_directory(char *newdir, int nolinks, int xattr)
// {
// 	if (!real_change_to_directory)
// 		real_change_to_directory = dlsym(RTLD_NEXT, "change_to_directory");
// 	fprintf(stderr, "Calling change_to_directory\n");
// 	return real_change_to_directory(newdir, nolinks, xattr);
// }

// int bindpwd(int no_symlinks)
// {
// 	if (!real_bindpwd)
// 		real_bindpwd = dlsym(RTLD_NEXT, "bindpwd");
// 	fprintf(stderr, "Calling bindpwd\n");
// 	return real_bindpwd(no_symlinks);
// }

// int sys_chdir(const char *filename)
// {
// 	if (!real_sys_chdir)
// 		real_sys_chdir = dlsym(RTLD_NEXT, "sys_chdir");
// 	fprintf(stderr, "Calling sys_chdir\n");
// 	return real_sys_chdir(filename);
// }

// int _wchdir(const wchar_t *dirname)
// {
// 	if (!real_wchdir)
// 		real_wchdir = dlsym(RTLD_NEXT, "wchdir");
// 	fprintf(stderr, "Calling _wchdir\n");
// 	return real_wchdir(dirname);
// }

// static int (*real_fflush)(FILE * stream);
// int fflush(FILE * stream) {
// 	if(!real_fflush) {
//         real_fflush = dlsym(RTLD_NEXT, "fflush");
// 	}
// 	fprintf(stderr, "Calling real fflush, fd=%d\n", stream->_fileno);
// 	return real_fflush(stream);
// }

// int fprintf(FILE * stream, const char * format, ...)
// {
// 	if (!real_fprintf)
// 	{
// 		// vfprintf always expect a 'va_list' type as last argument.
// 		// The behaviour of both calls are similar, so we can oversuscribe
// 		// 'real_fprintf' by 'vfprintf'.
// 		real_fprintf = dlsym(RTLD_NEXT, "vfprintf");
// 	}

// 	errno = 0;
// 	int ret = -1;
// 	char *pathname;
// 	int fd = stream->_fileno;
// 	if (pathname = map_fd_search_by_val(map_fd, fd))
// 	{
// 		// printf("Calling Hercules fprintf, fd=%d\n", stream->_fileno);
// 		slog_live("Calling Hercules fprintf, fd=%d\n", stream->_fileno);

// 		va_list args;
// 		va_start(args, format);
// 		// Get the size to be copy into the buffer. +1 to '\n'.
// 		size_t size = vsnprintf(NULL, 0, format, args) + 1;
// 		va_end(args);

// 		char *buffer = (char *)malloc(size);
// 		if (!buffer)
// 		{
// 			return -1; // Return an error if malloc fails
// 		}

// 		// Save the formated string into the buffer.
// 		va_start(args, format);
// 		vsnprintf(buffer, size, format, args);
// 		va_end(args);
// 		// printf("Writing '%s'\n", buffer);
// 		// Use fwrite to write the formatted string to the file.
// 		ret = fwrite(buffer, 1, size - 1, stream);
// 		free(buffer);
// 	}
// 	else
// 	{
// 		// printf("Calling real fprintf, fd=%d\n", stream->_fileno);
// 		slog_full("Calling real fprintf, fd=%d\n", stream->_fileno);
// 		va_list args;
// 		va_start(args, format);
// 		// if (args)
// 		ret = real_fprintf(stream, format, args);
// 		// else
// 		// 	ret = real_fprintf(stream, format);
// 		va_end(args);
// 	}

// 	return ret;
// }

char *getcwd(char *buf, size_t size)
{
	static char *(*real_getcwd)(char *, size_t) = nullptr;

	if (!real_getcwd)
	{
		real_getcwd = reinterpret_cast<char *(*)(char *, size_t)>(dlsym(RTLD_NEXT, "getcwd"));
	}

	if (!init)
	{
		return real_getcwd(buf, size);
	}

	char *curr_dir = getenv("PWD");
	if (curr_dir != nullptr && !strncmp(curr_dir, MOUNT_POINT, strlen(MOUNT_POINT)))
	{
		slog_live("[POSIX] Calling Hercules 'getcwd'");

		size_t required_size = strlen(curr_dir) + 1;
		char *result_buf = buf;

		if (result_buf == nullptr)
		{
			slog_debug("result_buf is null");
			// If size is 0, allocate exactly what is needed, otherwise use the requested size
			size_t alloc_size = (size == 0) ? required_size : size;

			if (alloc_size < required_size)
			{
				errno = ERANGE;
				slog_debug("Ending Hercules 'getcwd', alloc size < required size");
				return nullptr;
			}

			result_buf = reinterpret_cast<char *>(malloc(alloc_size));
			if (result_buf == nullptr)
			{
				errno = ENOMEM;
				slog_debug("Ending Hercules 'getcwd', error during malloc");
				return nullptr;
			}
		}
		else
		{
			slog_debug("result_buf is NOT null");
			if (size == 0 || size < required_size)
			{
				errno = ERANGE;
				slog_debug("Ending Hercules 'getcwd', size is 0 or size < required_size");
				return nullptr;
			}
		}

		memcpy(result_buf, curr_dir, required_size);

		slog_live("[POSIX] Ending Hercules 'getcwd', buf=%s", result_buf);
		return result_buf;
	}
	else
	{
		slog_full("[POSIX] Calling real 'getcwd'");
		char *real_buf = real_getcwd(buf, size);

		// avoids passing NULL to %s
		if (real_buf == nullptr)
		{
			slog_full("[POSIX] Ending real 'getcwd', failed (errno=%d)", errno);
		}
		else
		{
			slog_full("[POSIX] Ending real 'getcwd', buf=%s", real_buf);
		}

		return real_buf;
	}
}

int chdir(const char *pathname)
{
	if (!real_chdir)
		real_chdir = (int (*)(const char *))dlsym(RTLD_NEXT, "chdir");

	if (!init)
	{
		return real_chdir(pathname);
	}

	errno = 0;
	int ret = 0;
	char *new_path = checkHerculesPath(pathname);
	if (new_path != NULL)
	{
		slog_live("Calling Hercules 'chdir', pathname=%s", pathname);
		// setenv("PWD", pathname, 1);
		if (setenv("PWD", pathname, 1) == -1)
		{
			slog_debug("[chdir] setenv(PWD, %s) failed -> errno=%d (%s)",
				   pathname, errno, strerror(errno));
			ret = -1;
		}
		slog_live("End Hercules 'chdir', pathname=%s, ret=%d", pathname, ret);
		free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real 'chdir', pathname=%s", pathname);
		ret = real_chdir(pathname);
		slog_full("[POSIX] Ending real 'chdir', pathname=%s, ret=%d\n", pathname, ret);
	}

	return ret;
}

int utimensat(int dirfd, const char *pathname, const struct timespec times[_Nullable 2], int flags)
{
	if (!real_utimensat)
		real_utimensat = (int (*)(int, const char *, const struct timespec *, int))dlsym(RTLD_NEXT, "utimensat");

	if (!init)
	{
		return real_utimensat(dirfd, pathname, times, flags);
	}

	errno = 0;
	int ret = 0;
	mode_t new_mode;
	new_mode |= S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;

	// if (dirfd == AT_FDCWD)
	// {
	// 	// pathname_dir = getenv("PWD");
	// 	// new_path = checkHerculesPath(pathname_dir);
	// 	// pathname_dir = NULL;
	// 	new_path = checkHerculesPath(pathname);
	// 	pathname_dir = NULL;
	// }
	// else
	// {
	// 	pathname_dir = map_fd_search_by_val(map_fd, dirfd);
	// }

	std::string pathname_dir_op = "";
	char *new_path = NULL;
	ResolvePathsAndFD(dirfd, pathname, pathname_dir_op, &new_path);

	if (pathname_dir_op != "" || new_path != NULL)
	{
		const char *pathname_dir = pathname_dir_op.c_str();
		slog_live("[POSIX] Calling Hercules 'utimensat' flags=%d, dirfd=%d, pathname_dir=%s, pathname=%s", flags, dirfd, pathname_dir, pathname);
		int is_absolute_path = IsAbsolutePath(pathname);

		// If pathname is absolute, then dir_fd is ignored.
		if (is_absolute_path == 1)
		{
			// char *new_path = checkHerculesPath(pathname);
			slog_live("[POSIX] is absolute, 'utimensat', pathname=%s", pathname);

			ret = generalOpen((char *)pathname, flags, new_mode, -1);
		}
		else if (is_absolute_path == 0) // pathname is relative.
		{
			if (dirfd == AT_FDCWD) // dir_fd is the special value AT_FDCWD.
			{		       // TO CHECK!
				// char *new_path = checkHerculesPath(pathname);
				// slog_live("[POSIX] is relative, current directory, 'unlinkat', new_path=%s", new_path);
				slog_live("[POSIX] is relative, current directory, 'utimensat', pathname=%s", pathname);
				// pathname is interpreted relative to the current working directory of the calling process (like real_open).
				// ret = generalOpen(new_path, flags, mode);
				ret = generalOpen((char *)pathname, flags, new_mode, -1);
				// free(new_path);
			}
			else
			{
				// // get the pathname of the directory pointed by dir_fd if it is storage in the local map "map_fd".
				// char *pathname_dir = map_fd_search_by_val(map_fd, dir_fd);
				// if (pathname_dir == NULL)
				// { // if dir_fd is not storage locally
				// 	// search dir_fd on the metadata server.
				// 	slog_error("[POSIX] dir_fd=%d could not be resolved.");
				// 	return -1;
				// }

				char absolute_pathname[PATH_MAX] = {0};
				char *dirr = (char *)pathname_dir + strlen("imss://");
				sprintf(absolute_pathname, "%s/%s/%s", MOUNT_POINT, dirr, pathname);

				// char *new_path = checkHerculesPath(absolute_pathname);
				//  slog_live("[POSIX] is relative, 'unlinkat', new_path=%s", new_path);
				slog_live("[POSIX] is relative, 'utimensat', absolute_pathname=%s", absolute_pathname);
				ret = generalOpen(absolute_pathname, flags, new_mode, -1);

				// ret = generalOpen(new_path, flags, mode);
				// free(new_path);
			}
		}

		slog_live("[POSIX] Ending Hercules 'utimensat', ret=%d\n", ret);
		if (new_path != NULL)
			free(new_path);
	}
	else
	{
		slog_full("[POSIX] Calling real 'utimensat' flags=%d, dir_fd=%d, pathname=%s", flags, dirfd, pathname);
		ret = real_utimensat(dirfd, pathname, times, flags);
		slog_full("[POSIX] Ending real 'utimensat' flags=%d, dir_fd=%d, pathname=%s", flags, dirfd, pathname);
	}

	return ret;
}

int fchdir(int fd)
{
	if (!real_fchdir)
		real_fchdir = (int (*)(int))dlsym(RTLD_NEXT, "fchdir");

	if (!init)
	{
		// slog_live("[POSIX][TODO] Calling real 'fchdir', fd=%d", fd);
		return real_fchdir(fd);
	}

	int ret = -1;
	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
	if (!pathname_ob.empty())
	{
		const char *pathname = pathname_ob.c_str();
		slog_live("[POSIX] Calling Hercules 'fchdir', fd=%d, pathname=%s", fd, pathname);
		setenv("PWD", pathname, 1);
		ret = 1;
		slog_live("[POSIX] Ending Hercules 'fchdir', fd=%d, pathname=%s, ret=1", fd, pathname, ret);
	}
	else
	{
		slog_live("[POSIX] Calling Real 'fchdir', fd=%d", fd);
		ret = real_fchdir(fd);
		slog_live("[POSIX] Ending Real 'fchdir', fd=%d", fd);
	}

	return ret;
}

// int __chdir(const char *path)
// {
// 	if (!real___chdir)
// 		real___chdir = dlsym(RTLD_NEXT, "__chdir");

// 	if (init)
// 	{
// 		slog_live("Calling __chdir, pathname=%s", path);
// 	}

// 	return real___chdir(path);
// }

// int _chdir(const char *path)
// {
// 	if (!real__chdir)
// 		real__chdir = dlsym(RTLD_NEXT, "_chdir");
// 	fprintf(stderr, "Calling _chdir, pathname=%s", path);

// 	if (init)
// 	{
// 		slog_live("Calling _chdir, pathname=%s", path);
// 	}

// 	return real__chdir(path);
// }

// static int (*real_do_system)(const char *line);
// static int do_system(const char *line)
// {

// 	fprintf(stderr, "Calling do_system\n");
// 	real_do_system = dlsym(RTLD_NEXT, __func__);

// 	return real_do_system(line);
// }

// static int (*real___libc_system)(const char *line) = NULL;
// int __libc_system(const char *line)
// {
// 	fprintf(stderr, "[PRELOAD] Calling '__libc_system'\n");
// 	real___libc_system = dlsym(RTLD_NEXT, __func__);
// 	return real___libc_system(line);
// }

// static long int (*real_ptrace)(enum __ptrace_request __request, ...) = NULL;
// // extern long ptrace(enum __ptrace_request request, pid_t pid, void *addr, void *data) {
// long int ptrace (enum __ptrace_request __request, ...)
// {
// 	fprintf(stderr, "[PRELOAD] Calling 'ptrace'\n");
// 	real_ptrace = dlsym(RTLD_NEXT, __func__);
// 	return real_ptrace(__request);
// }

// int pthread_create(pthread_t * thread,
// 				   const pthread_attr_t * attr,
// 				   void *(*start_routine)(void *),
// 				   void * arg)
// {
// 	if (!real_pthread_create)
// 		real_pthread_create = dlsym(RTLD_NEXT, __func__);

// 	// fprintf(stderr, "Calling 'pthread_create', fd=%d, errno=%d:%s\n", fd, errno, strerror(errno));
// 	if (!init)
// 	{
// 		// slog_live("Calling pthread_create, fd=%d", fd);
// 		return real_pthread_create(thread, attr, start_routine, arg);
// 	}

// 	fprintf(stderr, "Calling 'pthread_create'\n");

// 	return real_pthread_create(thread, attr, start_routine, arg);
// }

// int posix_fadvise(int fd, off_t offset, off_t len, int advice)
// {
// 	if (!real_posix_fadvise)
// 		real_posix_fadvise = dlsym(RTLD_NEXT, "posix_fadvise");

// 	// fprintf(stderr, "Calling 'posix_fadvise', fd=%d, errno=%d:%s\n", fd, errno, strerror(errno));
// 	if (!init)
// 	{
// 		// slog_live("Calling posix_fadvise, fd=%d", fd);
// 		return real_posix_fadvise(fd, offset, len, advice);
// 	}

// 	errno = 0;
// 	int ret = -1;
// 	std::string pathname_ob = map_fd_search_by_val(map_fd, fd);
// 	if (!pathname_ob.empty())
// 	{
// 		slog_warn("[POSIX][TODO] Calling Hercules 'posix_fadvise', pathname=%s, fd=%d", pathname, fd);
// 		// fprintf(stderr, "[POSIX] Calling Hercules 'posix_fadvise', pathname=%s, fd=%d\n", pathname, fd);
// 		//  imss_refresh(pathname);
// 		//  ret = imss_getattr(pathname, buf);
// 		//  if (ret < 0)
// 		//  {
// 		//  	errno = -ret;
// 		//  	ret = -1;
// 		//  	slog_error("[POSIX] Error Hercules '__fxstat'	: %s", strerror(errno));
// 		//  }
// 		// ret = 9;
// 		ret = 0;
// 		// fprintf(stderr, "[POSIX] Hercules 'posix_fadvise', fd=%d, errno=%d:%s, ret=%d\n", fd, errno, strerror(errno), ret);
// 		//  slog_live("[POSIX] End Hercules '__fxstat', pathname=%s, fd=%d, errno=%d:%s, ret=%d\n", pathname, fd, errno, strerror(errno), ret);
// 	}
// 	else
// 	{
// 		// slog_live("[POSIX] Calling Real 'posix_fadvise', fd=%d", fd);
// 		// fprintf(stderr, "[POSIX] 'posix_fadvise', fd=%d\n", fd);
// 		ret = real_posix_fadvise(fd, offset, len, advice);
// 		// fprintf(stderr, "[POSIX] Real 'posix_fadvise', fd=%d, errno=%d:%s, ret=%d\n", fd, errno, strerror(errno), ret);
// 		//  slog_live("[POSIX] End Real 'posix_fadvise', fd=%d, errno=%d:%s, ret=%d", fd, errno, strerror(errno), ret);
// 	}

// 	return ret;
// }

// void exit(int status)
// {
// 	if (!real_exit)
// 		real_exit = dlsym(RTLD_NEXT, __func__);

// 	if (!init)
// 	{
// 		real_exit(status);
// 	}

// 	if (init)
// 	{
// 		slog_warn("Hercules must be destroy here");
// 	}
// 	real_exit(status);
// }

// Used by hdf5 iotest but does not require a Hercules implementation.
// int fcntl(int fd, int cmd, ...) ...
// int fcntl64(int fd, int cmd, ...)
// {
// 	if (!real_fcntl64)
// 		real_fcntl64 = (int (*)(int, int, ...))dlsym(RTLD_NEXT, __func__);

// 	va_list ap;
// 	void *arg;
// 	va_start(ap, cmd);
// 	arg = va_arg(ap, void *);
// 	va_end(ap);

// 	if (!init)
// 	{
// 		if (!arg)
// 			return real_fcntl64(fd, cmd);
// 		else
// 			return real_fcntl64(fd, cmd, arg);
// 	}

// 	int ret = 0;
// 	if (!arg)
// 		ret = real_fcntl64(fd, cmd);
// 	else
// 		ret = real_fcntl64(fd, cmd, arg);

// 	return ret;
// }

void *prefetch_function(void *th_argv)
{
	for (;;)
	{

		pthread_mutex_lock(&lock);
		while (prefetch_ds < 0)
		{
			pthread_cond_wait(&cond_prefetch, &lock);
		}

		if (prefetch_first_block < prefetch_last_block && prefetch_first_block != -1)
		{
			// printf("Se activo Prefetch path:%s$%d-$%d\n",prefetch_path, prefetch_first_block, prefetch_last_block);
			int exist_first_block, exist_last_block, position;
			char *buf = map_get_buffer_prefetch(map_prefetch, prefetch_path, &exist_first_block, &exist_last_block);
			int err = readv_multiple(prefetch_uri, prefetch_ds, prefetch_first_block, prefetch_last_block, buf, IMSS_BLKSIZE, prefetch_offset, IMSS_DATA_BSIZE * (prefetch_last_block - prefetch_first_block));
			if (err == -1)
			{
				pthread_mutex_unlock(&lock);
				continue;
			}
			map_update_prefetch(map_prefetch, prefetch_path, prefetch_first_block, prefetch_last_block);
		}

		prefetch_ds = -1;
		pthread_mutex_unlock(&lock);
	}

	pthread_exit(NULL);
}

// #ifdef __cplusplus
// }
// #endif
