#ifndef IMSS_POSIX_H
#define IMSS_POSIX_H

// #include "map.hpp"
#include "hierarchical_map.hpp"
#include "mapfd.hpp"
#include "mapprefetch.hpp"
#include "cfg_parse.h"
#include "flags.h"
#include "resolvepath.h"
#include "tempname.h"
#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <dlfcn.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/xattr.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/statvfs.h>
#include <sys/vfs.h> // statfs
extern "C" {
    #include <imss_posix_api.h>
}
#include <stdarg.h>
#include <math.h>
#include <sys/utsname.h>
#include <sys/epoll.h>
#include <sys/time.h>
// Those are used by reports functions in stat.
#include <pwd.h>
#include <sys/sysmacros.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <dirent.h>

#ifdef __cplusplus
extern "C"
{
#endif

#define _Nullable
#define _Nonnull

    // Auxiliar functions.
    char *checkHerculesPath(const char *pathname);
    char *convert_path(const char *name);
    int generalOpen(const char *new_path, int flags, mode_t mode, int createFd);
    ssize_t generalWrite(const char *pathname, int fd, const void *buf, size_t size, size_t offset);
    int GeneralFAccessAt(int dirfd, const char *pathname, int mode, int flags, char *pathname_dir);
    void SetErrno(int value);
    int IsAbsolutePath(const char *pathname);
    int ResolvePath(const char *path_, char *resolved);
    void WarnOperationNotSupported(const char *call_name, const char *pathname);
    void checkOpenFlags(const char *pathname, int flags);
    uint32_t MurmurOAAT32(const char *key);
    void *prefetch_function(void *th_argv);
	void copy_stat_to_statx(const struct stat *src, struct statx *dest);
	void ResolvePathsAndFD(const int fd_dir, const char *path_to_check, std::string &directory_path, char **file_path);
	uint32_t GetRank();


    int __fxstat(int ver, int fd, struct stat *buf);
    
    static off_t (*real_lseek)(int fd, off_t offset, int whence) = NULL;
    static off64_t (*real_lseek64)(int fd, off64_t offset, int whence) = NULL;
    static int (*real_fseek)(FILE *stream, long int offset, int whence) = NULL;
    static void (*real_seekdir)(DIR *dirp, long loc) = NULL;

    static int (*real_stat)(const char *pathname, struct stat *buf) = NULL;
    static int (*real_stat64)(const char *__restrict__ pathname, struct stat64 *__restrict__ buf) = NULL;
    static int (*real__lxstat)(int fd, const char *pathname, struct stat *buf) = NULL;
    static int (*real__lxstat64)(int ver, const char *pathname, struct stat64 *buf) = NULL;
    static int (*real_lstat)(const char *pathname, struct stat *buf) = NULL;
    static int (*real_lstat64)(const char *__restrict__ pathname, struct stat64 *__restrict__ buf);
    static int (*real_xstat)(int fd, const char *pathname, struct stat *buf) = NULL;
    static int (*real_statx)(int dirfd, const char *pathname, int flags, unsigned int mask, struct statx *statxbuf) = NULL;
    static int (*real___xstat)(int ver, const char *pathname, struct stat *stat_buf) = NULL;
    static int (*real__xstat64)(int ver, const char *path, struct stat64 *stat_buf) = NULL;
    static int (*real_fstat)(int fd, struct stat *buf) = NULL;
    static int (*real_fstat64)(int fd, struct stat64 *buf) = NULL;
    static int (*real_fstatat)(int __fd, const char *__restrict __file, struct stat *__restrict __buf, int __flag) = NULL;
    static int (*real_fstatat64)(int __fd, const char *__restrict __file, struct stat64 *__restrict __buf, int __flag) = NULL;
    static int (*real_fstatfs)(int fd, struct statfs *buf) = NULL;
    static int (*real_fstatfs64)(int fd, struct statfs64 *buf) = NULL;
    static int (*real_statfs64)(const char *path, struct statfs64 *buf) = NULL;
    static int (*real_newfstatat)(int __fd, const char *__restrict __file, struct stat *__restrict __buf, int __flag) = NULL;
    static int (*real__fxstat64)(int ver, int fd, struct stat64 *buf) = NULL;
    static int (*real__fxstat)(int ver, int fd, struct stat *buf) = NULL;
    static int (*real___fxstatat)(int __ver, int __fildes, const char *__filename, struct stat *__stat_buf, int __flag) = NULL;
    static int (*real___fxstatat64)(int __ver, int __fildes, const char *__filename, struct stat64 *__stat_buf, int __flag) = NULL;

    static int (*real_close)(int fd) = NULL;
    static int (*real_puts)(const char *str) = NULL;
    static int (*real__open_2)(const char *pathname, int flags, ...) = NULL;
    static int (*real_open64)(const char *pathname, int flags, ...) = NULL;
    static int (*real_open)(const char *pathname, int flags, ...) = NULL;
    static int (*real_creat)(const char *pathname, mode_t mode) = NULL;
    static FILE *(*real_fopen)(const char *pathname, const char *mode) = NULL;
    static FILE *(*real_fdopen)(int fildes, const char *mode) = NULL;
    // static FILE *(*real_fopen64)(const char * pathname, const char * mode) = NULL;
    static int (*real_access)(const char *pathname, int mode) = NULL;
    static int (*real_mkdir)(const char *path, mode_t mode) = NULL;
    static ssize_t (*real_write)(int fd, const void *buf, size_t size) = NULL;
    static ssize_t (*real_read)(int fd, const void *buf, size_t size) = NULL;
    static int (*real_remove)(const char *name) = NULL;
    static int (*real_unlink)(const char *name) = NULL;
    static int (*real_rmdir)(const char *path) = NULL;
    static int (*real_rename)(const char *old, const char *new_pathname) = NULL;
    static int (*real_fchmodat)(int dir_fd, const char *pathname, mode_t mode, int flags) = NULL;
    static int (*real_fchownat)(int dir_fd, const char *pathname, uid_t owner, gid_t group, int flags) = NULL;
    static DIR *(*real_opendir)(const char *name) = NULL;
    static struct dirent *(*real_readdir)(DIR *dirp) = NULL;
    static struct dirent64 *(*real_readdir64)(DIR *dirp) = NULL;
    static int (*real_getdents)(unsigned int fd, struct linux_dirent *dirp, unsigned int count) = NULL;
    static ssize_t (*real_getdents64)(int fd, void *dirp, size_t count) = NULL;
    // static int (*real_readdir)(unsigned int fd, struct old_linux_dirent *dirp, unsigned int count);
    static int (*real_closedir)(DIR *dirp) = NULL;
    static int (*real_statvfs)(const char *path, struct statvfs *buf) = NULL;
    static int (*real_fstatvfs)(int fd, struct statvfs *buf) = NULL;
    static int (*real_statvfs64)(const char *path, struct statvfs64 *buf) = NULL;
    static int (*real_statfs)(const char *path, struct statfs *buf) = NULL;
    static char *(*real_realpath)(const char *path, char *resolved_path) = NULL;
	static char *(*real__realpath_chk)(const char *pathname, char *resolved_path, size_t resolved_len) = NULL;
    // static int (*real__openat)(int dir_fd, const char *pathname, int flags, ...) = NULL;
    static int (*real_openat)(int dir_fd, const char *pathname, int flags, ...) = NULL;
    // static int (*real__openat64)(int fd, const char *file, int oflag, ...) = NULL;
    // static int (*real__openat64_2)(int fd, const char *file, int oflag) = NULL;
    // static int (*real__libc_openat)(int fd, const char *file, int oflag, ...) = NULL;
    static int (*real__libc_open64)(const char *file, int oflag, ...) = NULL;
    // static int (*real_openat)(int dir_fd, const char *pathname, int flags) = NULL;
    static int (*real_fclose)(FILE *fp) = NULL;
    static size_t (*real_fread)(void *buf, size_t size, size_t count, FILE *fp) = NULL;
    static size_t (*real_fwrite)(const void *buf, size_t size, size_t count, FILE *fp) = NULL;
    static void (*real_clearerr)(FILE *fp) = NULL;
    static int (*real_ferror)(FILE *fp) = NULL;
    static int (*real_feof)(FILE *fp) = NULL;
    static long int (*real_ftell)(FILE *fp) = NULL;
    static void (*real_rewind)(FILE *stream) = NULL;
    // static void *(*real_mmap)(void *addr, size_t length, int prot, int flags, int fd, off_t offset) = NULL;
    static int (*real_symlink)(const char *name1, const char *name2) = NULL;
    static int (*real_symlinkat)(const char *name1, int fd, const char *name2) = NULL;
    static int (*real_chdir)(const char *pathname) = NULL;
    static int (*real_fchdir)(int fd) = NULL;
    static int (*real__chdir)(const char *path) = NULL;
    static int (*real___chdir)(const char *path) = NULL;
    static int (*real_sys_chdir)(const char *filename) = NULL;
    static int (*real_wchdir)(const wchar_t *dirname) = NULL;
    static int (*real_chmod)(const char *pathname, mode_t mode) = NULL;
    static int (*real_fchmod)(int fd, mode_t mode) = NULL;
    static int (*real_execve)(const char *pathname, char *const argv[], char *const envp[]) = NULL;
    // static int (*real_execv)(const char *pathname, char *const argv[]) = NULL;
    static char *(*real_getcwd)(char *buf, size_t size) = NULL;
    static int (*real_change_to_directory)(char *, int, int) = NULL;
    static int (*real_bindpwd)(int) = NULL;
    static int (*real_epoll_ctl)(int epfd, int op, int fd, struct epoll_event *event) = NULL;
    static pid_t (*real_fork)(void) = NULL;
    static pid_t (*real_vfork)(void) = NULL;
    static pid_t (*real_wait)(int *wstatus) = NULL;
    static pid_t (*real_waitpid)(pid_t pid, int *wstatus, int options) = NULL;
    static int (*real___fwprintf_chk)(FILE *stream, int flag, const wchar_t *format) = NULL;
    static ssize_t (*real_pread)(int fd, void *buf, size_t count, off_t offset) = NULL;
    static ssize_t (*real_pwrite)(int fd, const void *buf, size_t count, off_t offset) = NULL;
    static int (*real_truncate)(const char *path, off_t length) = NULL;
    static int (*real_ftruncate)(int fd, off_t length) = NULL;
    static int (*real_flock)(int fd, int operation) = NULL;
    static int (*real_dup2)(int oldfd, int newfd) = NULL;
    static int (*real_dup)(int oldfd) = NULL;
    static int (*real_mkstemp)(char *template_name) = NULL;
    static ssize_t (*real_readv)(int fd, const struct iovec *iov, int iovcnt) = NULL;
    static ssize_t (*real_writev)(int fd, const struct iovec *iov, int iovcnt) = NULL;
    // ssize_t preadv(int fd, const struct iovec *iov, int iovcnt, off_t offset);
    static ssize_t (*real_pwritev)(int fd, const struct iovec *iov, int iovcnt, off_t offset) = NULL;
    static ssize_t (*real_pwrite64)(int fd, const void *buf, size_t count, off64_t offset) = NULL;
    // static int (*real_poll)(struct pollfd *fds, nfds_t nfds, int timeout) = NULL;
    // static int (*real_ppoll)(struct pollfd *fds, nfds_t nfds, const struct timespec tmo_p, const sigset_t sigmask) = NULL;
    static int (*real_fcntl)(int fd, int cmd, ... /* arg */) = NULL;
    static int (*real_fcntl64)(int fd, int cmd, ...) = NULL;
    static int (*real_syncfs)(int fd) = NULL;
    static int (*real_posix_fadvise)(int fd, off_t offset, off_t len, int advice) = NULL;
    // static int (*real_posix_fadvise64)(int fd, off64_t offset, off64_t len, int advice) = NULL;
    static int (*real_faccessat)(int dir_fd, const char *pathname, int mode, int flags) = NULL;
    static int (*real_faccessat2)(int dirfd, const char *pathname, int mode, int flags) = NULL;

    static int (*real_unlinkat)(int fd, const char *name, int flag) = NULL;
    
    static int (*real_renameat)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath) = NULL;
    static int (*real_renameat2)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags) = NULL;
    // static int (*real_fstatat)(int dir_fd, const char *pathname, struct stat *buf, int flags) = NULL;
    // static int (*real_getdents)(unsigned int fd, struct linux_dirent *dirp, unsigned int count) = NULL;
    static int (*real_fsync)(int fd) = NULL;
    // static int (*real_pthread_create)(pthread_t * thread,
    // 								  const pthread_attr_t * attr,
    // 								  void *(*start_routine)(void *),
    // 								  void * arg) = NULL;
    static void (*real_exit)(int status) __attribute__((noreturn)) = NULL;
    // static int (*real_fprintf)(FILE * stream, const char * format, ...);
    static int (*real_fprintf)(FILE *stream, const char *format, va_list) = NULL; // not fully supported.

    static ssize_t (*real_readlink)(const char *pathname, char *buf, size_t bufsiz) = NULL;
    static ssize_t (*real_readlinkat)(int dirfd, const char *pathname, char *buf, size_t bufsiz) = NULL;
    static int (*real_utimensat)(int dirfd, const char *pathname, const struct timespec times[_Nullable 2], int flags) = NULL;

    // static int (*real_syscall)(SYS_faccessat2, int dirfd, const char *path, int mode, int flags) = NULL;

#ifdef __cplusplus
}
#endif
#endif // IMSS_POSIX_H
