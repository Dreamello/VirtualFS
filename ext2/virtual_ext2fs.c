#include "virtual_ext2.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <string.h>
#include <inttypes.h>

/* The FUSE version has to be defined before any call to relevant
   includes related to FUSE. */
#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif
#include <fuse.h>

static void *ext2_init(struct fuse_conn_info *conn);
static void ext2_destroy(void *private_data);
static int ext2_getattr(const char *path, struct stat *stbuf);
static int ext2_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			off_t offset, struct fuse_file_info *fi);
static int ext2_open(const char *path, struct fuse_file_info *fi);
static int ext2_release(const char *path, struct fuse_file_info *fi);
static int ext2_read(const char *path, char *buf, size_t size, off_t offset,
		     struct fuse_file_info *fi);
static int ext2_readlink(const char *path, char *buf, size_t size);

// BONUS FUNCTIONS
static int ext2_unlink(const char *path);
static int ext2_rmdir(const char *path);
static int ext2_mkdir(const char *path, mode_t mode);
static int ext2_rename(const char *oldpath, const char *newpath);

static volume_t *volume;

static const struct fuse_operations ext2_operations = {
  .init = ext2_init,
  .destroy = ext2_destroy,
  .open = ext2_open,
  .read = ext2_read,
  .release = ext2_release,
  .getattr = ext2_getattr,
  .readdir = ext2_readdir,
  .readlink = ext2_readlink,
  .unlink = ext2_unlink,
  .rmdir = ext2_rmdir,
  .mkdir = ext2_mkdir,
  .rename = ext2_rename,
};

int main(int argc, char *argv[]) {
  
  char *volumefile = argv[--argc];
  volume = open_volume_file(volumefile);
  argv[argc] = NULL;
  
  if (!volume) {
    fprintf(stderr, "Invalid volume file: '%s'.\n", volumefile);
    exit(1);
  }
  
  fuse_main(argc, argv, &ext2_operations, volume);
  
  return 0;
}

/* ext2_init: Function called when the FUSE file system is mounted.
 */
static void *ext2_init(struct fuse_conn_info *conn) {
  
  printf("init()\n");
  
  return NULL;
}

/* ext2_destroy: Function called before the FUSE file system is
   unmounted.
 */
static void ext2_destroy(void *private_data) {
  
  printf("destroy()\n");
  
  close_volume_file(volume);
}

/* ext2_getattr: Function called when a process requests the metadata
   of a file. Metadata includes the file type, size, and
   creation/modification dates, among others (check man 2
   fstat). Typically FUSE will call this function before most other
   operations, for the file and all the components of the path.
   
   Parameters:
     path: Path of the file whose metadata is requested.
     stbuf: Pointer to a struct stat where metadata must be stored.
   Returns:
     In case of success, returns 0, and fills the data in stbuf with
     information about the file. If the file does not exist, should
     return -ENOENT.
 */
static int ext2_getattr(const char *path, struct stat *stbuf) {

  /* TO BE COMPLETED BY THE STUDENT */

  // Find inode based on given path
  inode_t inode;
  uint32_t inode_no = find_file_from_path(volume, path, &inode);
  if (inode_no == 0) return -ENOENT;

  // Update struct members
  stbuf->st_ino = inode_no;
  stbuf->st_mode = inode.i_mode;
  stbuf->st_nlink = inode.i_links_count;
  stbuf->st_uid = inode.i_uid | (inode.l_i_uid_high << 16);
  stbuf->st_gid = inode.i_gid | (inode.l_i_gid_high << 16);
  stbuf->st_size = inode_file_size(volume, &inode);
  stbuf->st_blksize = volume->block_size;
  stbuf->st_blocks = inode.i_blocks;
  stbuf->st_atime = inode.i_atime;
  stbuf->st_mtime = inode.i_mtime;
  stbuf->st_ctime = inode.i_ctime;
  return 0;
}

/* ext2_readdir: Function called when a process requests the listing
   of a directory.
   
   Parameters:
     path: Path of the directory whose listing is requested.
     buf: Pointer that must be passed as first parameter to filler
          function.
     filler: Pointer to a function that must be called for every entry
             in the directory.  Will accept four parameters, in this
             order: buf (previous parameter), the filename for the
             entry as a string, a pointer to a struct stat containing
             the metadata of the file (optional, may be passed NULL),
             and an offset for the next call to ext2_readdir
             (optional, may be passed 0).
     offset: Will usually be 0. If a previous call to filler for the
             same path passed a non-zero value as the offset, this
             function will be called again with the provided value as
             the offset parameter. Optional.
     fi: Not used in this implementation of readdir.

   Returns:
     In case of success, returns 0, and calls the filler function for
     each entry in the provided directory. If the directory doesn't
     exist, returns -ENOENT.
 */
static int ext2_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi) {

  /* TO BE COMPLETED BY THE STUDENT */
  
  // Find inode based on given path
  inode_t inode;
  uint32_t inode_no = find_file_from_path(volume, path, &inode);
  if (inode_no == 0) return -ENOENT;

  char buffer[volume->block_size];
  uint32_t block, cur;
  uint64_t seen = 0, size = inode_file_size(volume, &inode);
  dir_entry_t entry;

  while (seen < size) {
    cur = 0; // Read at most one block into buffer, iterate over all entires within the block
    block = read_file_content(volume, &inode, seen, volume->block_size, buffer);
    while (cur < block) {
      memcpy(&entry, buffer + cur, 8);
      if (entry.de_inode_no) {
        memcpy(entry.de_name, buffer + cur + 8, entry.de_name_len);
        entry.de_name[entry.de_name_len] = '\0';
        filler(buf, entry.de_name, NULL, 0);
      }
      cur += entry.de_rec_len;
    }
    seen += block;
  }

  return 0;
}

/* ext2_open: Function called when a process opens a file in the file
   system.
   
   Parameters:
     path: Path of the file being opened.
     fi: Data structure containing information about the file being
         opened. Some useful fields include:
	 flags: Flags for opening the file. Check 'man 2 open' for
                information about which flags are available.
	 fh: File handle. The value you set to this handle will be
             passed unmodified to all other file operations involving
             the same file.
   Returns:
     In case of success, returns 0. In case of error, may return one
     of these error codes:
       -ENOENT: If the file does not exist;
       -EISDIR: If the path corresponds to a directory;
       -EACCES: If the open operation was for writing, and the file is
                read-only.
 */
static int ext2_open(const char *path, struct fuse_file_info *fi) {
  
  if (fi->flags & O_WRONLY || fi->flags & O_RDWR)
    return -EACCES;

  /* TO BE COMPLETED BY THE STUDENT */

  // Find inode based on given path
  inode_t *inode = malloc(sizeof(inode_t));
  uint32_t inode_no = find_file_from_path(volume, path, inode);
  if (inode_no == 0) return -ENOENT;
  if ((inode->i_mode & S_IFMT) == S_IFDIR) return -EISDIR;

  // Set file handle
  fi->fh = (uint64_t) inode;

  return 0; // Function not implemented
}

/* ext2_release: Function called when a process closes a file in the
   file system. If the open file is shared between processes, this
   function is called when the file has been closed by all processes
   that share it.
   
   Parameters:
     path: Path of the file being closed.
     fi: Data structure containing information about the file being
         opened. This is the same structure used in ext2_open.
   Returns:
     In case of success, returns 0. There is no expected error case.
 */
static int ext2_release(const char *path, struct fuse_file_info *fi) {

  /* TO BE COMPLETED BY THE STUDENT */
  free((inode_t *) fi->fh);
  return 0; // Function not implemented
}

/* ext2_read: Function called when a process reads data from a file in
   the file system. This function stores, in array 'buf', up to 'size'
   bytes from the file, starting at offset 'offset'. It may store less
   than 'size' bytes only in case of error or if the file is smaller
   than size+offset.
   
   Parameters:
     path: Path of the open file.
     buf: Pointer where data is expected to be stored.
     size: Maximum number of bytes to be read from the file.
     offset: Byte offset of the first byte to be read from the file.
     fi: Data structure containing information about the file being
         opened. This is the same structure used in ext2_open.
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
static int ext2_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi) {

  /* TO BE COMPLETED BY THE STUDENT */

  inode_t *inode = (inode_t *) fi->fh;
  if (offset > inode_file_size(volume, inode)) return 0;
  int ret = read_file_content(volume, inode, offset, size, buf);
  return ret < 0 ? -EIO : ret;
}

/* ext2_read: Function called when FUSE needs to obtain the target of
   a symbolic link. The target is stored in buffer 'buf', which stores
   up to 'size' bytes, as a NULL-terminated string. If the target is
   too long to fit in the buffer, the target should be truncated.
   
   Parameters:
     path: Path of the symbolic link file.
     buf: Pointer where symbolic link target is expected to be stored.
     size: Maximum number of bytes to be stored into the buffer,
           including the NULL byte.
   Returns:
     In case of success, returns 0 (zero). In case of error, may
     return one of these error codes (not all of them are required):
       -ENOENT: If the file does not exist;
       -EINVAL: If the path does not correspond to a symbolic link;
       -EIO: If there was an I/O error trying to obtain the data.
 */
static int ext2_readlink(const char *path, char *buf, size_t size) {

  /* TO BE COMPLETED BY THE STUDENT */

  // Find inode based on given path
  inode_t inode;
  uint32_t inode_no = find_file_from_path(volume, path, &inode);
  if (inode_no == 0) return -ENOENT;
  if ((inode.i_mode & S_IFMT) != S_IFLNK) return -EINVAL;

  // Read symlink content
  int ret = read_file_content(volume, &inode, 0, size - 1, buf);
  if (ret < 0) return -EIO;
  buf[ret] = '\0';
  return 0;
}

// ==============================================================================================================================
// ALL FUNCTIONS AFTER THIS ARE BONUS FUNCTIONS =================================================================================
// ==============================================================================================================================

/* BONUS: ext2_unlink
 */
static int ext2_unlink(const char *path) {

  /* TO BE COMPLETED BY THE STUDENT */

  // Find inode based on given path
  inode_t inode;
  uint32_t inode_no = find_file_from_path(volume, path, &inode);
  if (inode_no == 0) return -ENOENT;
  if ((inode.i_mode & S_IFMT) == S_IFDIR) return -EISDIR;

  // Remove directory entry
  remove_directory_entry(volume, path);

  // Decrement inode link count
  update_links_count(volume, inode_no, -1); 

  return 0;
}

/* BONUS: ext2_rmdir
 */
static int ext2_rmdir(const char *path) {

  /* TO BE COMPLETED BY THE STUDENT */

  // Find inode based on given path, perform initial checks
  inode_t inode;
  uint32_t inode_no = find_file_from_path(volume, path, &inode);
  if (inode_no == 0) return -ENOENT;
  if ((inode.i_mode & S_IFMT) != S_IFDIR) return -ENOTDIR;
  if (!is_directory_empty(volume, &inode)) return -ENOTEMPTY;

  // Remove directory entry
  remove_directory_entry(volume, path);

  // Decrement link counts
  update_links_count(volume, inode_no, -2);
  update_links_count(volume, find_parent_from_path(volume, path, NULL), -1);

  return 0;
}

/* BONUS: ext2_mkdir
 */
static int ext2_mkdir(const char *path, mode_t mode) {

  /* TO BE COMPLETED BY THE STUDENT */

  if (find_file_from_path(volume, path, NULL) != 0) return -EEXIST;

  // Check if parent path is valid and if parent path is a directory
  inode_t parent_inode;
  uint32_t parent_inode_no = find_parent_from_path(volume, path, &parent_inode);
  if (parent_inode_no == 0) return -ENOENT;
  if ((parent_inode.i_mode & S_IFMT) != S_IFDIR) return -ENOTDIR;

  // Check if directory name is at most 255 bytes
  char name[256];
  if (get_last_component(name, path, 255) < 0) return -ENAMETOOLONG;

  // Find free block and zero out the block
  uint32_t block_no = find_free_block(volume, parent_inode_no);
  if (block_no == 0) return -EDQUOT; // Disk block quota exhausted
  update_block_state(volume, block_no, -1, 1);

  // Find free inode
  uint32_t inode_no = find_free_inode(volume, parent_inode_no);
  if (inode_no == 0) return -EDQUOT; // Inode quota exhausted

  // Create new inode
  inode_t inode;
  uint32_t uid = getuid();
  uint32_t gid = getgid();
  time_t seconds = time(NULL);
  memset(&inode, 0, sizeof(inode_t));
  // inode.i_mode = mode | S_IFDIR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH; // ASK TA
  inode.i_mode = mode | S_IFDIR;
  inode.i_uid = uid & 0xFFFF;
  inode.i_size = volume->block_size;
  inode.i_atime = (uint32_t) seconds;
  inode.i_ctime = (uint32_t) seconds;
  inode.i_mtime = (uint32_t) seconds;
  inode.i_gid = gid & 0xFFFF;
  inode.i_blocks = volume->block_size / 512;
  inode.i_block[0] = block_no;
  inode.l_i_uid_high = (uid >> 16) & 0xFFFF;
  inode.l_i_gid_high = (gid >> 16) & 0xFFFF;
  write_inode(volume, inode_no, &inode);
  update_inode_state(volume, inode_no, &inode, -1, 1);
  
  // Create entries for '.' and '..'
  create_directory_entry(volume, ".", inode_no, &inode, inode_no, &inode, 1);
  create_directory_entry(volume, "..", parent_inode_no, &parent_inode, inode_no, &inode, 0);

  // Create new directory entry inside parent. Might have to overflow
  if (create_directory_entry(volume, name, inode_no, &inode, parent_inode_no, &parent_inode, 0) < 0) {
    memset(&inode, 0, sizeof(inode_t));
    write_inode(volume, inode_no, &inode); // Zero the inode we modified earlier
    update_block_state(volume, block_no, 1, 0); // Free the block we found earlier
    update_inode_state(volume, inode_no, &inode, 1, 0); // Free the inode we found earlier
    return -ENOSPC; // No room for new directory entry
  }
  
  // Update ctime & mtime for parent directory
  modify_time(volume, parent_inode_no, &parent_inode, &seconds, &seconds);

  // Increment links count for new inode and parent inode
  update_links_count(volume, inode_no, 2);
  update_links_count(volume, parent_inode_no, 1);

  return 0;
}

/* BONUS: ext2_rename
 */
static int ext2_rename(const char *oldpath, const char *newpath) {

  /* TO BE COMPLETED BY THE STUDENT */

  // printf("Passed in newpath: %s\n", newpath);
  // printf("Passed in oldpath: %s\n", oldpath);

  inode_t inode;
  uint32_t inode_no = find_file_from_path(volume, oldpath, &inode);
  if (inode_no == 0) return -ENOENT;
  if (find_file_from_path(volume, newpath, NULL) != 0) return -EEXIST;

  // Check if newpath is valid
  inode_t newpath_parent_inode;
  uint32_t newpath_parent_inode_no = find_parent_from_path(volume, newpath, &newpath_parent_inode);
  if (newpath_parent_inode_no == 0) return -ENOENT;
  if ((newpath_parent_inode.i_mode & S_IFMT) != S_IFDIR) return -ENOTDIR;

  // Check if directory name is at most 255 bytes
  char newname[256];
  if (get_last_component(newname, newpath, 255) < 0) return -ENAMETOOLONG;

  // Check oldpath isn't a part of newpath
  char oldpath_with_slash[strlen(oldpath) + 2]; 
  strcpy(oldpath_with_slash, oldpath); strcat(oldpath_with_slash, "/");
  if (strlen(newpath) > strlen(oldpath_with_slash) && strncmp(oldpath, newpath, strlen(oldpath_with_slash)) == 0)
    return -EINVAL;

  time_t seconds = time(NULL);

  // check if parent directories are different
  inode_t oldpath_parent_inode;
  uint32_t oldpath_parent_inode_no = find_parent_from_path(volume, oldpath, &oldpath_parent_inode);

  if (oldpath_parent_inode_no != newpath_parent_inode_no) { // different parent directories

    // Create new directory entry. Might have to overflow.
    if (create_directory_entry(volume, newname, inode_no, &inode, newpath_parent_inode_no, &newpath_parent_inode, 0) < 0)
      return -ENOSPC; // No room for new directory entry

    // Update ctime & mtime for new parent directory
    modify_time(volume, newpath_parent_inode_no, &newpath_parent_inode, &seconds, &seconds);

    // Delete old directory entry
    remove_directory_entry(volume, oldpath);

    // Update ctime & mtime for old parent directory
    modify_time(volume, oldpath_parent_inode_no, &oldpath_parent_inode, &seconds, &seconds);

    if ((inode.i_mode & S_IFMT) == S_IFDIR) { // If moving a directory
      modify_directory_entry(volume, "..", newpath_parent_inode_no, "..", inode_no, &inode);
      modify_time(volume, inode_no, &inode, &seconds, &seconds);
      update_links_count(volume, oldpath_parent_inode_no, -1);
      update_links_count(volume, newpath_parent_inode_no, 1);
    } else {
      modify_time(volume, inode_no, &inode, &seconds, NULL);
    }

  } else { // same parent directories 

    // Modify old directory entry. Might have to overflow.
    char oldname[256]; get_last_component(oldname, oldpath, 255);
    if (modify_directory_entry(volume, newname, inode_no, oldname, oldpath_parent_inode_no, &oldpath_parent_inode) < 0)
      return -ENOSPC;

    // Update ctime & mtime for parent directory inode and update ctime for renamed inode
    modify_time(volume, newpath_parent_inode_no, &newpath_parent_inode, &seconds, &seconds);
    modify_time(volume, inode_no, &inode, &seconds, NULL);
    
  }
  
  return 0;
}