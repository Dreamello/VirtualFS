#include "virtual_fat12.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <string.h>

/* The FUSE version has to be defined before any call to relevant
   includes related to FUSE. */
#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif
#include <fuse.h>

#ifndef FATDEBUG
#define FATDEBUG 1
#endif

#if FATDEBUG
#define debug_print(...) fprintf(stderr, __VA_ARGS__)
#else
#define debug_print(...) ((void) 0)
#endif

#define VOLUME ((fat12volume *) fuse_get_context()->private_data)

static void *fat12_init(struct fuse_conn_info *conn);
static void fat12_destroy(void *private_data);
static int fat12_getattr(const char *path, struct stat *stbuf);
static int fat12_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi);
static int fat12_open(const char *path, struct fuse_file_info *fi);
static int fat12_release(const char *path, struct fuse_file_info *fi);
static int fat12_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi);

// BONUS FUNCTIONS
static int fat12_mkdir(const char *path, mode_t mode);
static int fat12_rmdir(const char *path);
static int fat12_unlink(const char *path);
static int fat12_rename(const char *oldpath, const char *newpath);

static const struct fuse_operations fat12_operations = {
  .init = fat12_init,
  .destroy = fat12_destroy,
  .open = fat12_open,
  .read = fat12_read,
  .release = fat12_release,
  .getattr = fat12_getattr,
  .readdir = fat12_readdir,
  .mkdir = fat12_mkdir,
  .rmdir = fat12_rmdir,
  .unlink = fat12_unlink,
  .rename = fat12_rename,
};

int main(int argc, char *argv[]) {
  
  char *volumefile = argv[--argc];
  fat12volume *volume = open_volume_file(volumefile);
  argv[argc] = NULL;
  
  if (!volume) {
    fprintf(stderr, "Invalid volume file: '%s'.\n", volumefile);
    exit(1);
  }
  
  fuse_main(argc, argv, &fat12_operations, volume);
  
  return 0;
}

/* fat12_init: Function called when the FUSE file system is mounted.
 */
static void *fat12_init(struct fuse_conn_info *conn) {
  
  debug_print("init()\n");
  
  return VOLUME;
}

/* fat12_destroy: Function called before the FUSE file system is
   unmounted.
 */
static void fat12_destroy(void *private_data) {
  
  debug_print("destroy()\n");
  
  close_volume_file((fat12volume *) private_data);
}

/* fat12_getattr: Function called when a process requests the metadata
   of a file. Metadata includes the file type, size, and
   creation/modification dates, among others (check man 2 fstat).
   
   Parameters:
     path: Path of the file whose metadata is requested.
     stbuf: Pointer to a struct stat where metadata must be stored.
   Returns:
     In case of success, returns 0, and fills the data in stbuf with
     information about the file. In case of error, it will return one
     of these error codes:
       -ENOENT: If the file does not exist;
       -ENOTDIR: If one of the path components is not a directory.
 */
static int fat12_getattr(const char *path, struct stat *stbuf) {
  
  debug_print("getattr(path=%s)\n", path);
  
  /* Some comments about members in the struct stat definition:
     -- st_dev, st_ino, st_rdev: FUSE updates them automatically, no
        need to set them to any value;
     -- st_nlink: set it to 1;
     -- st_ctime, st_mtime, st_atime: all can be set with same value;
     -- st_gid, st_uid: use the result of getgid() and getuid(),
        respectively;
     -- st_mode: for permissions, use 0555 (read/execute permission,
        but no write), but note that it is also used to determine the
        type of the file;
     -- st_blksize: the size of a cluster;
     -- st_blocks: the number of clusters used in this file (it is
        acceptable to leave this field set to 0 for directories);
     -- other members should be updated based on their description in
        the man page for stat.
   */

  dir_entry entry;

  // Locate directory entry
  int ret = find_directory_entry(VOLUME, path, &entry);
  if (ret != 0) return ret;

  // Update struct members
  stbuf->st_mode = entry.is_directory ? S_IFDIR | 0555 : S_IFREG | 0444;
  stbuf->st_nlink = 1;
  stbuf->st_uid = getuid();
  stbuf->st_gid = getgid();
  stbuf->st_size = entry.size;
  stbuf->st_blksize = VOLUME->cluster_size * VOLUME->sector_size;
  stbuf->st_blocks = (entry.size + VOLUME->cluster_size * VOLUME->sector_size - 1) / (VOLUME->cluster_size * VOLUME->sector_size);
  stbuf->st_atime = mktime(&entry.ctime);
  stbuf->st_mtime = mktime(&entry.ctime);
  stbuf->st_ctime = mktime(&entry.ctime);
  return 0;
}

/* fat12_readdir: Function called when a process requests the listing
   of a directory.
   
   Parameters:
     path: Path of the directory whose listing is requested.
     buf: Pointer that must be passed as first parameter to filler
          function.
     filler: Pointer to a function that must be called for every entry
             in the directory.  Will accept four parameters, in this
             order: buf (previous parameter), the filename for the
             entry, a pointer to a struct stat containing the metadata
             of the file (optional, may be passed NULL), and an offset
             (see observation below, you can use 0).
     offset: Not used in this implementation of readdir.
     fi: Not used in this implementation of readdir.

   Returns:
     In case of success, returns 0, and calls the filler function for
     each entry in the provided directory. In case of error, it will
     return one of these error codes:
       -ENOENT: If the directory does not exist;
       -ENOTDIR: If the provided path (or one of the components of
                 the path) is not a directory;
       -EIO: If there was an I/O error trying to obtain the data.
 */
static int fat12_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi) {
  
  debug_print("readdir(path=%s, offset=%ld)\n", path, (long) offset);

  /* There are two ways to implement this function. We highly
     recommend the first one, where you return all entries in the
     directory at once. In this case, you will call filler for each
     entry with the last parameter (offset) equal to 0.
     
     An alternative implementation involves returning the entries in
     the directory in batches (e.g., one call for each cluster). In
     this case you will use the parameter offset and use a non-zero
     value in the fourth parameter of filler. You are not required to
     use this type of implementation, and there will be no bonus or
     extra help for it.
  */

  dir_entry entry;

  // Root directory case
  if (strcmp(path, "/") == 0) {
    for (unsigned int i = 0; i < VOLUME->rootdir_entries; i++) {
      if (VOLUME->rootdir_array[i * DIR_ENTRY_SIZE] == 0) { // end of directory
        break;
      } else if ((VOLUME->rootdir_array[i * DIR_ENTRY_SIZE] & 0xFF) == 0xE5) { // deleted entry
        continue; 
      } else {
        decode_filename(VOLUME->rootdir_array + i * DIR_ENTRY_SIZE, &entry);
        int ret = filler(buf, entry.filename, NULL, 0); // *** ask TA
        if (ret == 1) return -EIO;
      }
    }
    return 0;
  }

  // General case
  int ret = find_directory_entry(VOLUME, path, &entry);
  if (ret != 0) return ret;

  // Check if entry is a directory
  if (!entry.is_directory) return -ENOTDIR;

  // Store directory into buffer
  char* directory = NULL;
  int cluster_count = read_all_clusters(VOLUME, entry.first_cluster, &directory);

  // Check for read error
  if (cluster_count == -1) return -EIO;
  unsigned int dir_size = cluster_count * VOLUME->cluster_size * VOLUME->sector_size / DIR_ENTRY_SIZE;

  // Iterate over all directory entries
  for (unsigned int i = 0; i < dir_size; i++) {
    if (directory[i * DIR_ENTRY_SIZE] == 0) { // end of directory
      break;
    } else if ((directory[i * DIR_ENTRY_SIZE] & 0xFF) == 0xE5) { // deleted entry
      continue; 
    } else {
      decode_filename(directory + i * DIR_ENTRY_SIZE, &entry);
      int ret = filler(buf, entry.filename, NULL, 0);
      if (ret == 1) {
        free(directory);
        return -EIO;
      }
    }
  }
  free(directory);
  return 0;
}

/* fat12_open: Function called when a process opens a file in the file
   system.
   
   Parameters:
     path: Path of the file being opened.
     fi: Data structure containing information about the file being
         opened. Some useful fields include:
	 flags: Flags for opening the file. Check 'man 2 open' for
                information about which flags are available.
	 fh: File handle. The value you set to fi->fh will be
             passed unmodified to all other file operations involving
             the same file.
   Returns:
     In case of success, returns 0. In case of error, it will return
     one of these error codes:
       -ENOENT: If the file does not exist;
       -ENOTDIR: If one of the components of the path is not a
                 directory;
       -EISDIR: If the path corresponds to a directory;
       -EACCES: If the open operation was for writing, and the file is
                read-only.
 */
static int fat12_open(const char *path, struct fuse_file_info *fi) {
  
  debug_print("open(path=%s, flags=0%o)\n", path, fi->flags);

  // If opening for writing, returns error
  if (fi->flags & O_WRONLY || fi->flags & O_RDWR)
    return -EACCES;

  // Locate directory entry
  dir_entry* entry = malloc(sizeof(dir_entry));
  int ret = find_directory_entry(VOLUME, path, entry);
  if (ret != 0) {
    free(entry);
    return ret; 
  }

  // Check if entry is a directory
  if (entry->is_directory) {
    free(entry);
    return -EISDIR;
  }

  // Set file handle
  fi->fh = (uint64_t) entry;
  return 0;
}

/* fat12_release: Function called when a process closes a file in the
   file system. If the open file is shared between processes, this
   function is called when the file has been closed by all processes
   that share it.
   
   Parameters:
     path: Path of the file being closed.
     fi: Data structure containing information about the file being
         opened. This is the same structure used in fat12_open.
   Returns:
     In case of success, returns 0. There is no expected error case.
 */
static int fat12_release(const char *path, struct fuse_file_info *fi) {
  
  debug_print("release(path=%s)\n", path);
  
  free((dir_entry*) fi->fh);
  return 0;
}

/* fat12_read: Function called when a process reads data from a file
   in the file system.
   
   Parameters:
     path: Path of the open file.
     buf: Pointer where data is expected to be stored.
     size: Maximum number of bytes to be read from the file.
     offset: Byte offset of the first byte to be read from the file.
     fi: Data structure containing information about the file being
         opened. This is the same structure used in fat12_open.
   Returns:
     In case of success, returns the number of bytes actually read
     from the file--which may be smaller than size, or even zero, if
     (and only if) offset+size is beyond the end of the file. In case
     of error, may return one of these error codes (not all of them
     are required):
       -ENOENT: If the file does not exist;
       -EISDIR: If the path corresponds to a directory;
       -EIO: If there was an I/O error trying to obtain the data.
 */
static int fat12_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi) {
  
  debug_print("read(path=%s, size=%zu, offset=%zu)\n", path, size, offset);

  dir_entry* entry = (dir_entry*) fi->fh;

  // Check if entry is a directory
  if (entry->is_directory) return -EISDIR;

  // Check if offset is at or beyond EOF
  if (offset >= entry->size) return 0;

  // Shrink size if offset + size is beyond EOF
  if (offset + size > entry->size) size = entry->size - offset;

  // Read file into buffer
  char* file = NULL;
  if (read_all_clusters(VOLUME, entry->first_cluster, &file) == -1) return -EIO;

  // Copy data into buf
  memcpy(buf, file + offset, size);
  free(file);
  return size;
}

/* BONUS: fat12_mkdir
 */
static int fat12_mkdir(const char *path, mode_t mode) {

  debug_print("mkdir(path=%s)\n", path);

  dir_entry entry;

  // Check if path already exists
  if (find_directory_entry(VOLUME, path, &entry) == 0) return -EEXIST;

  // Create new directory
  return create_directory(VOLUME, path);
}

/* BONUS: fat12_rmdir
 */
static int fat12_rmdir(const char *path) {
  
  debug_print("rmdir(path=%s)\n", path);

  dir_entry entry;

  // Locate and validate directory entry
  int ret = find_directory_entry(VOLUME, path, &entry);
  if (ret != 0) return ret;
  if (!entry.is_directory) return -ENOTDIR;

  // Check if directory is empty
  ret = is_directory_empty(VOLUME, &entry);
  if (ret != 0) return ret;

  // Get cluster number and offset of entry
  int entry_cluster = 0;
  int entry_cluster_offset = 0;
  char* ptr = find_directory_entry_ptr_and_cluster(VOLUME, path, &entry_cluster, &entry_cluster_offset);

  // Delete directory entry if it is in root directory, otherwise free the returned buffer
  if (entry_cluster == 0) *ptr = 0xE5;
  else free(ptr);

  // Delete directory entry in volume file
  delete_directory_entry(VOLUME, entry_cluster, entry_cluster_offset);
    
  // Mark directory cluster chain as free
  if (clear_all_clusters(VOLUME, entry.first_cluster) == -1) return -EIO;
  return 0;
}

/* BONUS: fat12_unlink
 */
static int fat12_unlink(const char *path) {
  
  debug_print("unlink(path=%s)\n", path);

  dir_entry entry;

  // Locate and validate directory entry
  int ret = find_directory_entry(VOLUME, path, &entry);
  if (ret != 0) return ret;
  if (entry.is_directory) return -EISDIR;

  // Get cluster number and offset of entry
  int entry_cluster = 0;
  int entry_cluster_offset = 0;
  char* ptr = find_directory_entry_ptr_and_cluster(VOLUME, path, &entry_cluster, &entry_cluster_offset);

  // Delete directory entry if it is in root directory, otherwise free the returned buffer
  if (entry_cluster == 0) *ptr = 0xE5;
  else free(ptr);

  // Delete directory entry in volume file
  delete_directory_entry(VOLUME, entry_cluster, entry_cluster_offset);

  // Mark file cluster chain as free
  if (clear_all_clusters(VOLUME, entry.first_cluster) == -1) return -EIO;
  return 0;
}

/* BONUS: fat12_rename
 */
static int fat12_rename(const char *oldpath, const char *newpath) {

  debug_print("rename(oldpath=%s, newpath=%s)\n", oldpath, newpath);

  dir_entry entry;
  dir_entry new;

  // Locate directory entry of old path
  int ret = find_directory_entry(VOLUME, oldpath, &entry);
  if (ret != 0) return ret;

  // Check if new path already exists
  ret = find_directory_entry(VOLUME, newpath, &new);
  if (ret == 0) return -EEXIST;

  // Get cluster number and offset of old path entry
  int entry_cluster = 0;
  int entry_cluster_offset = 0;
  char* ptr = find_directory_entry_ptr_and_cluster(VOLUME, oldpath, &entry_cluster, &entry_cluster_offset);

  // Copy old path entry into new path
  ret = move_directory_entry(VOLUME, newpath, &entry);
  if (ret != 0) {
    if (entry_cluster != 0) free(ptr);
    return ret;
  }

  // Delete directory entry if it is in root directory, otherwise free the returned buffer
  if (entry_cluster == 0) *ptr = 0xE5;
  else free(ptr);

  // Delete old directory entry in volume file
  delete_directory_entry(VOLUME, entry_cluster, entry_cluster_offset);
  return 0;
}