#include "virtual_ext2.h"

#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdlib.h>
#include <inttypes.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <sys/fsuid.h>
#include <stdint.h>
#include <sys/stat.h>
#include <fcntl.h>

#define EXT2_OFFSET_SUPERBLOCK 1024
#define EXT2_INVALID_BLOCK_NUMBER ((uint32_t) -1)

/* open_volume_file: Opens the specified file and reads the initial
   EXT2 data contained in the file, including the boot sector, file
   allocation table and root directory.
   
   Parameters:
     filename: Name of the file containing the volume data.
   Returns:
     A pointer to a newly allocated volume_t data structure with all
     fields initialized according to the data in the volume file
     (including superblock and group descriptor table), or NULL if the
     file is invalid or data is missing.
 */
volume_t *open_volume_file(const char *filename) {
  

  
  // Open file
  int fd = open(filename, O_RDWR); // Bonus functions require read + write
  // int fd = open(filename, O_RDONLY);
  if (fd < 0) return NULL;

  // Initialize volume_t data structure
  volume_t *volume = (volume_t *) malloc(sizeof(volume_t));
  volume->fd = fd;

  // Seek to superblock and read data into superblock struct
  uint32_t size = sizeof(superblock_t);
  if (pread(fd, &volume->super, size, EXT2_OFFSET_SUPERBLOCK) != size) {
    free(volume);
    close(fd);
    return NULL;
  }

  // Calculate additional fields
  volume->block_size = 1024 << volume->super.s_log_block_size;
  volume->volume_size = volume->block_size * volume->super.s_blocks_count;
  volume->num_groups = (volume->super.s_blocks_count + volume->super.s_blocks_per_group - 1) / volume->super.s_blocks_per_group;

  // Initialize block group descriptor array
  size = volume->num_groups * sizeof(group_desc_t);
  volume->groups = (group_desc_t *) malloc(size);

  // Read block group descriptor table into group descriptor array
  if (read_block(volume, volume->super.s_first_data_block + 1, 0, size, volume->groups) != size) {
    free(volume->groups);
    free(volume);
    close(fd);
    return NULL;
  }
  
  return volume;
}

/* close_volume_file: Frees and closes all resources used by a EXT2 volume.
   
   Parameters:
     volume: pointer to volume to be freed.
 */
void close_volume_file(volume_t *volume) {



  close(volume->fd);
  free(volume->groups);
  free(volume);
}

/* read_block: Reads data from one or more blocks. Saves the resulting
   data in buffer 'buffer'. This function also supports sparse data,
   where a block number equal to 0 sets the value of the corresponding
   buffer to all zeros without reading a block from the volume.
   
   Parameters:
     volume: pointer to volume.
     block_no: Block number where start of data is located.
     offset: Offset from beginning of the block to start reading
             from. May be larger than a block size.
     size: Number of bytes to read. May be larger than a block size.
     buffer: Pointer to location where data is to be stored.

   Returns:
     In case of success, returns the number of bytes read from the
     disk. In case of error, returns -1.
 */
ssize_t read_block(volume_t *volume, uint32_t block_no, uint32_t offset, uint32_t size, void *buffer) {



  // If sparse data
  if (block_no == 0) {
    memset(buffer, 0, size);
    return size;
  }

  return pread(volume->fd, buffer, size, block_no * volume->block_size + offset);
}

/* read_inode: Fills an inode data structure with the data from one
   inode in disk. Determines the block group number and index within
   the group from the inode number, then reads the inode from the
   inode table in the corresponding group. Saves the inode data in
   buffer 'buffer'.
   
   Parameters:
     volume: pointer to volume.
     inode_no: Number of the inode to read from disk.
     buffer: Pointer to location where data is to be stored.

   Returns:
     In case of success, returns a positive value. In case of error,
     returns -1.
 */
ssize_t read_inode(volume_t *volume, uint32_t inode_no, inode_t *buffer) {
  


  // Calculate block group number and offset for given inode
  uint32_t group_num = (inode_no - 1) / volume->super.s_inodes_per_group;
  uint32_t group_index = (inode_no - 1) % volume->super.s_inodes_per_group;

  // Read inode into buffer
  uint32_t size = sizeof(inode_t);
  if (read_block(volume, volume->groups[group_num].bg_inode_table, group_index * size, size, buffer) == size)
    return size;

  return -1;
}

/* read_ind_block_entry: Reads one entry from an indirect
   block. Returns the block number found in the corresponding entry.
   
   Parameters:
     volume: pointer to volume.
     ind_block_no: Block number for indirect block.
     index: Index of the entry to read from indirect block.

   Returns:
     In case of success, returns the block number found at the
     corresponding entry. In case of error, returns
     EXT2_INVALID_BLOCK_NUMBER.
 */
static uint32_t read_ind_block_entry(volume_t *volume, uint32_t ind_block_no,
				     uint32_t index) {
  


  uint8_t buffer[4];
  if (read_block(volume, ind_block_no, index * 4, 4, buffer) == 4)
    return *(uint32_t *) buffer;
  return EXT2_INVALID_BLOCK_NUMBER;
}

/* read_inode_block_no: Returns the block number containing the data
   associated to a particular index. For indices 0-11, returns the
   direct block number; for larger indices, returns the block number
   at the corresponding indirect block.
   
   Parameters:
     volume: Pointer to volume.
     inode: Pointer to inode structure where data is to be sourced.
     index: Index to the block number to be searched.

   Returns:
     In case of success, returns the block number to be used for the
     corresponding entry. This block number may be 0 (zero) in case of
     sparse files. In case of error, returns
     EXT2_INVALID_BLOCK_NUMBER.
 */
static uint32_t get_inode_block_no(volume_t *volume, inode_t *inode, uint64_t block_idx) {
  


  // Calculate indirect block sizes
  uint64_t indirect1_size = volume->block_size / 4;
  uint64_t indirect2_size = indirect1_size * indirect1_size;
  uint64_t indirect3_size = indirect1_size * indirect2_size;

  // Calculate indirect block boundries
  uint64_t indirect1 = 12 + indirect1_size;
  uint64_t indirect2 = indirect1 + indirect2_size;
  uint64_t indirect3 = indirect2 + indirect3_size;

  // Retrieve block number
  if (block_idx < 12) {
    return inode->i_block[block_idx];
  } else if (block_idx < indirect1) {
    return read_ind_block_entry(volume, inode->i_block_1ind, block_idx - 12);
  } else if (block_idx < indirect2) {
    uint32_t ind2_index = (block_idx - indirect1) / indirect1_size;
    uint32_t ind1_index = (block_idx - indirect1) % indirect1_size;
    return read_ind_block_entry(volume, read_ind_block_entry(volume, inode->i_block_2ind, ind2_index), ind1_index);
  } else if (block_idx < indirect3) {
    uint32_t ind3_index = (block_idx - indirect2) / indirect2_size;
    uint32_t ind2_index = ((block_idx - indirect2) % indirect2_size) / indirect1_size;
    uint32_t ind1_index = ((block_idx - indirect2) % indirect2_size) % indirect1_size;
    uint32_t temp = read_ind_block_entry(volume, inode->i_block_3ind, ind3_index);
    return read_ind_block_entry(volume, read_ind_block_entry(volume, temp, ind2_index), ind1_index);
  } else {
    return EXT2_INVALID_BLOCK_NUMBER;
  }
}

/* read_file_block: Returns the content of a specific file, limited to
   a single block.
   
   Parameters:
     volume: Pointer to volume.
     inode: Pointer to inode structure for the file.
     offset: Offset, in bytes from the start of the file, of the data
             to be read.
     max_size: Maximum number of bytes to read from the block.
     buffer: Pointer to location where data is to be stored.

   Returns:
     In case of success, returns the number of bytes read from the
     disk. In case of error, returns -1.
 */
ssize_t read_file_block(volume_t *volume, inode_t *inode, uint64_t offset, uint64_t max_size, void *buffer) {



  // If file is a symlink and filesize is < 60, directly read from inode
  if ((inode->i_mode & S_IFMT) == S_IFLNK && inode_file_size(volume, inode) < 60) {
    memcpy(buffer, inode->i_symlink_target + offset, max_size);
    return max_size;
  }
  
  // Find block index and block offset
  uint64_t block_idx = offset / volume->block_size;
  uint64_t block_offset = offset % volume->block_size;

  // Shrink max_size if necessary
  if (block_offset + max_size > volume->block_size)
    max_size = volume->block_size - block_offset;

  return read_block(volume, get_inode_block_no(volume, inode, block_idx), block_offset, max_size, buffer);
}

/* read_file_content: Returns the content of a specific file, limited
   to the size of the file only. May need to read more than one block,
   with data not necessarily stored in contiguous blocks.
   
   Parameters:
     volume: Pointer to volume.
     inode: Pointer to inode structure for the file.
     offset: Offset, in bytes from the start of the file, of the data
             to be read.
     max_size: Maximum number of bytes to read from the file.
     buffer: Pointer to location where data is to be stored.

   Returns:
     In case of success, returns the number of bytes read from the
     disk. In case of error, returns -1.
 */
ssize_t read_file_content(volume_t *volume, inode_t *inode, uint64_t offset, uint64_t max_size, void *buffer) {
  uint64_t read_so_far = 0;

  if (offset + max_size > inode_file_size(volume, inode))
    max_size = inode_file_size(volume, inode) - offset;
  
  while (read_so_far < max_size) {
    int rv = read_file_block(volume, inode, offset + read_so_far,
			     max_size - read_so_far, buffer + read_so_far);
    if (rv <= 0) return rv;
    read_so_far += rv;
  }
  return read_so_far;
}

/* follow_directory_entries: Reads all entries in a directory, calling
   function 'f' for each entry in the directory. Stops when the
   function returns a non-zero value, or when all entries have been
   traversed.
   
   Parameters:
     volume: Pointer to volume.
     inode: Pointer to inode structure for the directory.
     context: This pointer is passed as an argument to function 'f'
              unmodified.
     buffer: If function 'f' returns non-zero for any file, and this
             pointer is set to a non-NULL value, this buffer is set to
             the directory entry for which the function returned a
             non-zero value. If the pointer is NULL, nothing is
             saved. If none of the existing entries returns non-zero
             for 'f', the value of this buffer is unspecified.
     f: Function to be called for each directory entry. Receives three
        arguments: the file name as a NULL-terminated string, the
        inode number, and the context argument above.

   Returns:
     If the function 'f' returns non-zero for any directory entry,
     returns the inode number for the corresponding entry. If the
     function returns zero for all entries, or the inode is not a
     directory, or there is an error reading the directory data,
     returns 0 (zero).
 */
uint32_t follow_directory_entries(volume_t *volume, inode_t *inode, void *context,
				  dir_entry_t *buffer,
				  int (*f)(const char *name, uint32_t inode_no, void *context)) {



  // Check if inode is a directory
  if ((inode->i_mode & S_IFMT) != S_IFDIR) return 0;

  char dir_buffer[volume->block_size];
  uint32_t block, cur;
  uint64_t seen = 0, size = inode_file_size(volume, inode);
  dir_entry_t entry;

  while (seen < size) {
    cur = 0; // Read at most one block into buffer, iterate over all entries within this block
    if ((block = read_file_content(volume, inode, seen, volume->block_size, dir_buffer)) < 0) return 0;
    while (cur < block) {
      memcpy(&entry, dir_buffer + cur, 8);
      if (entry.de_inode_no) {
        memcpy(entry.de_name, dir_buffer + cur + 8, entry.de_name_len);
        entry.de_name[entry.de_name_len] = '\0';
        if (f(entry.de_name, entry.de_inode_no, context)) {
          if (buffer) *buffer = entry;
          return entry.de_inode_no;
        }
      }
      cur += entry.de_rec_len;
    }
    seen += block;
  }

  return 0;
}

/* Simple comparing function to be used as argument in find_file_in_directory function */
static int compare_file_name(const char *name, uint32_t inode_no, void *context) {
  return !strcmp(name, (char *) context);
}

/* find_file_in_directory: Searches for a file in a directory.
   
   Parameters:
     volume: Pointer to volume.
     inode: Pointer to inode structure for the directory.
     name: NULL-terminated string for the name of the file. The file
           name must match this name exactly, including case.
     buffer: If the file is found, and this pointer is set to a
             non-NULL value, this buffer is set to the directory entry
             of the file. If the pointer is NULL, nothing is saved. If
             the file is not found, the value of this buffer is
             unspecified.

   Returns:
     If the file exists in the directory, returns the inode number
     associated to the file. If the file does not exist, or the inode
     is not a directory, or there is an error reading the directory
     data, returns 0 (zero).
 */
uint32_t find_file_in_directory(volume_t *volume, inode_t *inode, const char *name, dir_entry_t *buffer) {
  
  return follow_directory_entries(volume, inode, (char *) name, buffer, compare_file_name);
}

/* find_file_from_path: Searches for a file based on its full path.
   
   Parameters:
     volume: Pointer to volume.
     path: NULL-terminated string for the full absolute path of the
           file. Must start with '/' character. Path components
           (subdirectories) must be delimited by '/'. The root
           directory can be obtained with the string "/".
     dest_inode: If the file is found, and this pointer is set to a
                 non-NULL value, this buffer is set to the inode of
                 the file. If the pointer is NULL, nothing is
                 saved. If the file is not found, the value of this
                 buffer is unspecified.

   Returns:
     If the file exists, returns the inode number associated to the
     file. If the file does not exist, or there is an error reading
     any directory or inode in the path, returns 0 (zero).
 */
uint32_t find_file_from_path(volume_t *volume, const char *path, inode_t *dest_inode) {



  // Root directory
  if (strcmp(path, "/") == 0) {
    if (dest_inode)
      read_inode(volume, EXT2_ROOT_INO, dest_inode);
    return EXT2_ROOT_INO;
  }

  // Split path into components
  char **path_components;
  int32_t count = split_path_components(&path_components, path);
  if (count < 0) return 0;

  // Find each path component
  inode_t inode;
  uint32_t inode_no;
  read_inode(volume, EXT2_ROOT_INO, &inode);
  for (int i = 0; i < count; i++) {
    inode_no = find_file_in_directory(volume, &inode, path_components[i], NULL);
    if (!inode_no) { // If any path component cannot be found, return 0
      free_path_components(path_components, count);
      return 0;
    }
    read_inode(volume, inode_no, &inode);
  }

  // Update dest_inode if necessary, free dynamically allocated strings and return
  if (dest_inode) *dest_inode = inode;
  free_path_components(path_components, count);
  return inode_no;
}

int32_t split_path_components(char ***path_components, const char *path) {



  int count = 0; // component counter
  char *copy = strdup(path); // create a copy of pathname since strtok modifies the given string
  char *token = copy;
  char slash[2] = "/";

  // Count the number of components
  while(*token) {
    if (*token == slash[0]) count++;
    token++;
  }

  // Store path components into array
  *path_components = malloc(count * sizeof(char *));
  if (*path_components) {
    int index = 0;
    token = strtok(copy, slash); // NOTE: strtok always returns a non-empty string, first "/" is ignored
    while (token) {
      (*path_components)[index] = strdup(token);
      token = strtok(0, slash); // get next path component
      index++;
    }
  } else { // invalid pathname: component count is 0
    free(copy);
    return -1;
  }

  free(copy);
  return count;
}

void free_path_components(char **path_components, int count) {



  for (int i = 0; i < count; i++)
    free(path_components[i]);
  free(path_components);
}

// ==============================================================================================================================
// ALL FUNCTIONS AFTER THIS ARE BONUS FUNCTIONS =================================================================================
// ==============================================================================================================================

/* write_block: Writes data to one or more blocks. 
   
   Parameters:
     volume: pointer to volume.
     block_no: Block number where data should be written to.
     offset: Offset from beginning of the block to start writing
             from. 
     size: Number of bytes to write. 
     buffer: Pointer to buffer holding the data to be written

   Returns:
     In case of success, returns the number of bytes read from the
     disk. In case of error, returns -1.
 */
ssize_t write_block(volume_t *volume, uint32_t block_no, uint32_t offset, uint32_t size, void *buffer) {



  return pwrite(volume->fd, buffer, size, block_no * volume->block_size + offset);
}

ssize_t zero_block(volume_t *volume, uint32_t block_no) {

  

  uint8_t buf[volume->block_size]; 
  memset(buf, 0, volume->block_size);
  return write_block(volume, block_no, 0, volume->block_size, buf);
}

ssize_t write_inode(volume_t *volume, uint32_t inode_no, inode_t *buffer) {

  

  // Calculate block group number and offset for given inode
  uint32_t group_num = (inode_no - 1) / volume->super.s_inodes_per_group;
  uint32_t group_index = (inode_no - 1) % volume->super.s_inodes_per_group;

  uint32_t size = sizeof(inode_t);
  if (write_block(volume, volume->groups[group_num].bg_inode_table, group_index * size, size, buffer) == size)
    return size;
  return -1;
}

int32_t update_superblock(volume_t *volume, uint32_t offset, uint32_t size, void *buffer) {

  

  // Update in-memeory copy of superblock
  memcpy((u_int8_t *) &volume->super + offset, buffer, size);

  // Update all superblocks in volume file
  write_block(volume, 0, EXT2_OFFSET_SUPERBLOCK + offset, size, buffer);
  for (int i = 1; i < volume->num_groups; i++) {
    write_block(volume, i * volume->super.s_blocks_per_group + volume->super.s_first_data_block, offset, size, buffer);
  }

  return 0;
}

int32_t update_group_desc_table(volume_t *volume, uint32_t group_no, uint32_t offset, uint32_t size, void *buffer) {

  

  // Update in-memeory copy of block group descriptor table
  memcpy((u_int8_t *) &volume->groups[group_no] + offset, buffer, size);

  // Update all group drescriptor tables in volume file
  for (int i = 0; i < volume->num_groups; i++) {
    write_block(volume, i * volume->super.s_blocks_per_group + volume->super.s_first_data_block + 1, group_no * 32 + offset, size, buffer);
  }
  
  return 0;
}

int32_t update_bitmap(volume_t *volume, uint32_t block_no, uint32_t block_index, uint8_t value) {

  

  // Calculate indices
  uint32_t byte_index = block_index / 8;
  uint32_t bit_index = block_index % 8;

  // Read the byte at the correct index into buf
  uint8_t buf;
  read_block(volume, block_no, byte_index, 1, &buf);

  // Set bit to given value
  if (value == 1) 
    buf |= 1 << bit_index;
  else
    buf &= 0b11111111 ^ (1 << bit_index);

  // Write the updated byte back into block bitmap
  write_block(volume, block_no, byte_index, 1, &buf);
  return 0;
}

int32_t update_links_count(volume_t *volume, uint32_t inode_no, int32_t change) {
  
  

  // Read inode into buffer
  inode_t buffer;
  read_inode(volume, inode_no, &buffer);

  // Update link count and update time if necessary
  time_t seconds = time(NULL);
  buffer.i_links_count += change;
  buffer.i_ctime = (uint32_t) seconds;
  if (buffer.i_links_count == 0) buffer.i_dtime = (uint32_t) seconds;

  // Write inode back into volume file
  write_inode(volume, inode_no, &buffer);

  // Return if link count > 0
  if (buffer.i_links_count > 0) return 0;

  // Free blocks associated with file
  if (!((buffer.i_mode & S_IFMT) == S_IFLNK && inode_file_size(volume, &buffer) < 60))
    free_data_blocks(volume, &buffer);

  // Free inode
  update_inode_state(volume, inode_no, &buffer, 1, 0);

  return 0;
}

int32_t update_block_state(volume_t *volume, uint32_t block_no, int32_t change, uint8_t value) {

  

  uint32_t group_no = (block_no - volume->super.s_first_data_block) / volume->super.s_blocks_per_group;
  uint32_t block_index = (block_no - volume->super.s_first_data_block) % volume->super.s_blocks_per_group;
  uint16_t two = volume->groups[group_no].bg_free_blocks_count + change;
  uint32_t four = volume->super.s_free_blocks_count + change;
  update_bitmap(volume, volume->groups[group_no].bg_block_bitmap, block_index, value);
  update_group_desc_table(volume, group_no, 12, 2, &two);
  update_superblock(volume, 12, 4, &four);
  return 0;
}

int32_t update_inode_state(volume_t *volume, uint32_t inode_no, inode_t *inode, int32_t change, uint8_t value) {

  

  // Calculate group number and inode index
  uint32_t group_no = (inode_no - 1) / volume->super.s_inodes_per_group;
  uint32_t inode_index = (inode_no - 1) % volume->super.s_inodes_per_group;

  // Update inode bitmap
  update_bitmap(volume, volume->groups[group_no].bg_inode_bitmap, inode_index, value);

  // Update EVERY block group descriptor table -> bg_free_inodes_count for current group
  uint16_t two = volume->groups[group_no].bg_free_inodes_count + change;
  update_group_desc_table(volume, group_no, 14, 2, &two);

  // Update EVERY block group descriptor table -> bg_used_dirs_count for current group (directory inode only)
  if ((inode->i_mode & S_IFMT) == S_IFDIR) {
    two = volume->groups[group_no].bg_used_dirs_count - change;
    update_group_desc_table(volume, group_no, 16, 2, &two);
  }

  // Update EVERY superblock -> s_free_inodes_count
  uint32_t four = volume->super.s_free_inodes_count + change;
  update_superblock(volume, 16, 4, &four);

  // Return if inode is a symlink with size < 60
  if ((inode->i_mode & S_IFMT) == S_IFLNK && inode_file_size(volume, inode) < 60) return 0;

  // If freeing inode, free indrect blocks
  if (value == 0)
    free_indirect_blocks(volume, inode);

  return 0;
}

int32_t free_data_blocks(volume_t *volume, inode_t *inode) {

  

  uint64_t blocks = (inode_file_size(volume, inode) + volume->block_size - 1) / volume->block_size;
  uint32_t block_no;

  // Update block bitmap and EVERY block group descriptor table -> bg_free_blocks_count for current group
  for (uint64_t i = 0; i < blocks; i++) {
    block_no = get_inode_block_no(volume, inode, i);
    if (block_no) update_block_state(volume, block_no, 1, 0);
  }

  return 0;
}

int32_t free_indirect_blocks(volume_t *volume, inode_t *inode) {

  

  uint32_t blocks = (inode_file_size(volume, inode) + volume->block_size - 1) / volume->block_size;
  uint32_t count = 12;

  if (inode->i_block_1ind)
    count += free_indirect_block(volume, inode->i_block_1ind, blocks - count, 1);
  if (inode->i_block_2ind)
    count += free_indirect_block(volume, inode->i_block_2ind, blocks - count, 2);
  if (inode->i_block_3ind)
    count += free_indirect_block(volume, inode->i_block_3ind, blocks - count, 3);
  return count;
}

int32_t free_indirect_block(volume_t *volume, uint32_t block_no, uint32_t total_blocks, uint8_t level) {

  
  
  // Base case when level of indirection is 1
  if (level == 1) {
    update_block_state(volume, block_no, 1, 0);
    return volume->block_size / 4;
  } 
  
  uint32_t capacity = volume->block_size / 4;
  uint32_t element_size = (level == 3) ? capacity * capacity : capacity;
  char *buffer = malloc(volume->block_size * sizeof(char));
  read_block(volume, block_no, 0, volume->block_size, buffer);
  uint32_t index = 0, count = 0, entry = 0;

  while (count < total_blocks && index < capacity) {
    entry = *(uint32_t *) (buffer + 4 * index++);
    if (entry)
      count += free_indirect_block(volume, entry, total_blocks - count, level - 1);
    else
      count += element_size;
  }

  update_block_state(volume, block_no, 1, 0);
  free(buffer);
  return count;
}

int32_t create_indirect_block_entry(volume_t *volume, uint32_t new_entry, uint32_t indirect_block_no, 
                                    uint32_t inode_no, uint32_t block_idx, uint8_t level) {

  

  if (level == 1) {
    write_block(volume, indirect_block_no, block_idx * 4, 4, &new_entry);
    return 0;
  } 

  uint32_t capacity = volume->block_size / 4;
  if (level == 3) capacity *= capacity;

  uint32_t temp, extra_blocks = 0;
  if (block_idx % capacity == 0) {
    temp = find_free_block(volume, inode_no);
    if (temp == 0) return -1;
    extra_blocks++;
    update_block_state(volume, temp, -1, 1);
    write_block(volume, indirect_block_no, block_idx / capacity * 4, 4, &temp);
  }

  uint32_t next_level = read_ind_block_entry(volume, indirect_block_no, block_idx / capacity);
  temp = create_indirect_block_entry(volume, new_entry, next_level, inode_no, block_idx % capacity, level - 1);
  if (temp < 0) {
    if (block_idx % capacity == 0) update_block_state(volume, temp, 1, 0);
    return -1;
  }

  return extra_blocks + temp;
}

int32_t create_directory_entry(volume_t *volume, const char *name, uint32_t inode_no, inode_t *inode, 
                               uint32_t parent_inode_no, inode_t *parent_inode, uint8_t empty) {



  // Create directory entry
  dir_entry_t new_entry;
  new_entry.de_inode_no = inode_no;
  new_entry.de_rec_len = volume->block_size;
  new_entry.de_name_len = strlen(name);
  new_entry.de_file_type = (uint8_t) (inode->i_mode & S_IFMT);
  memcpy(new_entry.de_name, name, new_entry.de_name_len);

  if (empty) {
    write_block(volume, get_inode_block_no(volume, parent_inode, 0), 0, new_entry.de_name_len + 8, &new_entry);
    return 0;
  }

  // Calculate length of entry, aligned to 4 bytes
  uint32_t length = (8 + new_entry.de_name_len + 3) / 4 * 4;
  new_entry.de_rec_len = length;

  // Iterate over parent directory, find space to insert the entry
  char buffer[volume->block_size];
  uint32_t block, cur, padding, block_no;
  uint64_t seen = 0, size = inode_file_size(volume, parent_inode);
  dir_entry_t entry;

  while (seen < size) {
    cur = 0;
    block = read_file_content(volume, parent_inode, seen, volume->block_size, buffer);
    while (cur < block) {
      memcpy(&entry, buffer + cur, 8);
      if (entry.de_inode_no) {
        memcpy(entry.de_name, buffer + cur + 8, entry.de_name_len);
        entry.de_name[entry.de_name_len] = '\0';
        padding = (entry.de_rec_len - entry.de_name_len - 8) / 4 * 4;
        if (padding >= length) {
          entry.de_rec_len -= padding; // Decrease current entry rec_len
          new_entry.de_rec_len = padding; // Increase new entry rec_len
          block_no = get_inode_block_no(volume, parent_inode, seen / volume->block_size);
          write_block(volume, block_no, cur + 4, 2, &entry.de_rec_len);
          write_block(volume, block_no, cur + entry.de_rec_len, new_entry.de_name_len + 8, &new_entry);
          return 0;
        }
      } else {
        if (entry.de_rec_len >= length) {
          new_entry.de_rec_len = entry.de_rec_len;
          block_no = get_inode_block_no(volume, parent_inode, seen / volume->block_size);
          write_block(volume, block_no, cur, new_entry.de_name_len + 8, &new_entry);
          return 0;
        }
      }
      cur += entry.de_rec_len;
    }
    seen += block;
  }

  // Need to overflow...
  if ((block_no = overflow_directory(volume, parent_inode, parent_inode_no)) == 0) return -1;
  new_entry.de_rec_len = volume->block_size;
  write_block(volume, block_no, 0, new_entry.de_name_len + 8, &new_entry);
  return 0;
}

int32_t modify_directory_entry(volume_t *volume, const char *newname, uint32_t new_inode_no, 
                               const char *oldname, uint32_t parent_inode_no, inode_t *parent_inode) {



  char buffer[volume->block_size];
  uint8_t oldlen;
  uint32_t block, cur;
  uint64_t seen = 0, size = inode_file_size(volume, parent_inode);
  dir_entry_t entry;

  while (seen < size) {
    cur = 0; // Read at most one block into buffer, iterate over all entires within the block
    block = read_file_content(volume, parent_inode, seen, volume->block_size, buffer);
    while (cur < block) {
      memcpy(&entry, buffer + cur, 8);
      if (entry.de_inode_no) {
        memcpy(entry.de_name, buffer + cur + 8, entry.de_name_len);
        entry.de_name[entry.de_name_len] = '\0';
        if (strcmp(entry.de_name, oldname) == 0) {
          oldlen = entry.de_name_len;
          if (strlen(newname) <= entry.de_rec_len - 8){
            entry.de_inode_no = new_inode_no;
            memset(entry.de_name, 0, entry.de_name_len);
            entry.de_name_len = strlen(newname);
            if (oldlen < entry.de_name_len) oldlen = entry.de_name_len;
            memcpy(entry.de_name, newname, strlen(newname));
          } else {
            inode_t inode;
            read_inode(volume, new_inode_no, &inode);
            if (create_directory_entry(volume, newname, new_inode_no, &inode, parent_inode_no, parent_inode, 0) < 0) return -1;
            entry.de_inode_no = 0;
          }
          uint32_t block_no = get_inode_block_no(volume, parent_inode, seen / volume->block_size);
          write_block(volume, block_no, cur, oldlen + 8, &entry); // Update entry
          return 0;
        }
      }
      cur += entry.de_rec_len;
    }
    seen += block;
  }
  return -1;
}

int32_t remove_directory_entry(volume_t *volume, const char *path) {

  

  char name[256];
  get_last_component(name, path, 255);

  // Read parent directory into buffer
  inode_t inode;
  find_parent_from_path(volume, path, &inode);

  char buffer[volume->block_size];
  uint32_t block, cur;
  uint64_t seen = 0, size = inode_file_size(volume, &inode);
  dir_entry_t entry, prev;

  while (seen < size) {
    cur = 0; // Read at most one block into buffer, iterate over all entires within the block
    block = read_file_content(volume, &inode, seen, volume->block_size, buffer);
    while (cur < block) {
      prev = entry;
      memcpy(&entry, buffer + cur, 8);
      if (entry.de_inode_no) {
        memcpy(entry.de_name, buffer + cur + 8, entry.de_name_len);
        entry.de_name[entry.de_name_len] = '\0';
        if (strcmp(entry.de_name, name) == 0) {
          entry.de_inode_no = 0;
          uint32_t block_no = get_inode_block_no(volume, &inode, seen / volume->block_size);
          if (cur == 0) {
            write_block(volume, block_no, cur, 4, &entry); // Update entry inode number
          } else {
            uint32_t offset = cur - prev.de_rec_len;
            prev.de_rec_len += entry.de_rec_len;
            write_block(volume, block_no, cur, 4, &entry); // Update entry inode number
            write_block(volume, block_no, offset + 4, 2, &prev.de_rec_len); // Update previous entry record length
          }
          return 0;
        }
      }
      cur += entry.de_rec_len;
    }
    seen += block;
  }

  return -1;
}

static int check_empty(const char *name, uint32_t inode_no, void *context) {



  return !(strcmp(name, ".") == 0 || strcmp(name, "..") == 0);
}

int32_t is_directory_empty(volume_t *volume, inode_t *inode) {

  

  return !follow_directory_entries(volume, inode, NULL, NULL, check_empty);
}

int32_t get_last_component(char *name, const char *path, int32_t max_len) {

  

  char *last_slash = strrchr(path, '/');
  if (*(++last_slash) == '\0') return -1;

  int32_t len = strlen(last_slash);
  if (len > max_len) return -1;

  strcpy(name, last_slash);
  return len;
}

int32_t find_zero_bit(volume_t *volume, uint32_t block_no, uint32_t entries_per_group) {

  

  uint8_t buf[(entries_per_group + 7) / 8]; // quick ceiling
  read_block(volume, block_no, 0, (entries_per_group + 7) / 8, buf);

  for (int i = 0; i < entries_per_group; i++)
    if (((buf[i / 8] >> (i % 8)) & 1) == 0) return i;

  return -1;
}

uint32_t find_free_block(volume_t *volume, uint32_t preferred_inode_no) {

  

  if (volume->super.s_free_blocks_count == 0) return 0; // File system has no free blocks remaining

  // First group to check
  uint32_t preferred_group_no = (preferred_inode_no - 1) / volume->super.s_inodes_per_group;

  if (volume->groups[preferred_group_no].bg_free_blocks_count == 0) {
    for (int i = 0; i < volume->num_groups; i++) {
      if (volume->groups[i].bg_free_blocks_count) {
        preferred_group_no = i;
        break;
      }
    }
  }

  uint32_t index;
  uint32_t offset = volume->super.s_first_data_block + preferred_group_no * volume->super.s_blocks_per_group;
  if (preferred_group_no == volume->num_groups - 1)
    index = find_zero_bit(volume, volume->groups[preferred_group_no].bg_block_bitmap, volume->super.s_blocks_count - offset);
  else
    index = find_zero_bit(volume, volume->groups[preferred_group_no].bg_block_bitmap, volume->super.s_blocks_per_group);

  if (index < 0) return 0; // inconsistent file system, could not find free bit

  zero_block(volume, offset + index);
  return offset + index;
}

uint32_t find_free_inode(volume_t *volume, uint32_t preferred_inode_no) {

  if (volume->super.s_free_inodes_count == 0) return 0; // File system has no free inodes remaining

  // First group to check
  uint32_t preferred_group_no = (preferred_inode_no - 1) / volume->super.s_inodes_per_group;

  if (volume->groups[preferred_group_no].bg_free_inodes_count == 0) {
    for (int i = 0; i < volume->num_groups; i++) {
      if (volume->groups[i].bg_free_inodes_count) {
        preferred_group_no = i;
        break;
      }
    }
  }

  uint32_t index;
  uint32_t offset = preferred_group_no * volume->super.s_inodes_per_group;
  if (preferred_group_no == volume->num_groups - 1)
    index = find_zero_bit(volume, volume->groups[preferred_group_no].bg_inode_bitmap, volume->super.s_inodes_count - offset);
  else
    index = find_zero_bit(volume, volume->groups[preferred_group_no].bg_inode_bitmap, volume->super.s_inodes_per_group);

  if (index < 0) return 0; // inconsistent file system, could not find free bit

  return 1 + offset + index;
}

uint32_t find_parent_from_path(volume_t *volume, const char *path, inode_t *dest_inode) {

  

  if (strcmp(path, "/") == 0) return 0;

  char *parent_path = strdup(path);
  char *last_slash = strrchr(parent_path, '/');
  if (last_slash == NULL) {
    free(parent_path);
    return 0;
  }

  if (last_slash - parent_path > 0)
    *(last_slash) = '\0';
  else
    *(last_slash + 1) = '\0';

  uint32_t ret = find_file_from_path(volume, parent_path, dest_inode);
  free(parent_path);
  return ret;
}

uint32_t overflow_directory(volume_t *volume, inode_t *parent_inode, uint32_t parent_inode_no) {

  

  uint32_t size = inode_file_size(volume, parent_inode);
  if (UINT32_MAX - size < volume->block_size) return 0;

  uint32_t block_no = find_free_block(volume, parent_inode_no);
  if (block_no == 0) return 0;
  update_block_state(volume, block_no, -1, 1);

  parent_inode->i_size += volume->block_size;
  parent_inode->i_blocks += volume->block_size / 512;

  // Calculate indirect block sizes
  uint64_t indirect1_size = volume->block_size / 4;
  uint64_t indirect2_size = indirect1_size * indirect1_size;
  uint64_t indirect3_size = indirect1_size * indirect2_size;

  // Calculate indirect block boundries
  uint64_t indirect1 = 12 + indirect1_size;
  uint64_t indirect2 = indirect1 + indirect2_size;
  uint64_t indirect3 = indirect2 + indirect3_size;

  uint32_t extra_counter = 0, temp = 0;
  uint32_t block_idx = parent_inode->i_size / volume->block_size - 1;

  // Retrieve block number
  if (block_idx < 12) {

    parent_inode->i_block[block_idx] = block_no;

  } else if (block_idx < indirect1) {

    // printf("Indirect case!\n");

    if (block_idx == 12) {
      parent_inode->i_block_1ind = find_free_block(volume, parent_inode_no);
      if (parent_inode->i_block_1ind == 0) {
        update_block_state(volume, block_no, 1, 0);
        return 0;
      }
      extra_counter++;
      update_block_state(volume, parent_inode->i_block_1ind, -1, 1);
    }

    temp = create_indirect_block_entry(volume, block_no, parent_inode->i_block_1ind, parent_inode_no, block_idx - 12, 1);
    if (temp < 0) {
      if (block_idx == 12) update_block_state(volume, parent_inode->i_block_1ind, -1, 1);
      update_block_state(volume, block_no, 1, 0);
      return 0;
    }

  } else if (block_idx < indirect2) {

    if (block_idx == indirect1) {
      parent_inode->i_block_2ind = find_free_block(volume, parent_inode_no);
      if (parent_inode->i_block_2ind == 0) {
        update_block_state(volume, block_no, 1, 0);
        return 0;
      }
      extra_counter++;
      update_block_state(volume, parent_inode->i_block_2ind, -1, 1);
    }
    
    temp = create_indirect_block_entry(volume, block_no, parent_inode->i_block_2ind, parent_inode_no, block_idx - indirect1, 2);
    if (temp < 0) {
      update_block_state(volume, block_no, 1, 0);
      return 0;
    }

  } else if (block_idx < indirect3) {

    if (block_idx == indirect2) {
      parent_inode->i_block_3ind = find_free_block(volume, parent_inode_no);
      if (parent_inode->i_block_3ind == 0) {
        update_block_state(volume, block_no, 1, 0);
        return 0;
      }
      extra_counter++;
      update_block_state(volume, parent_inode->i_block_3ind, -1, 1);
    }

    temp = create_indirect_block_entry(volume, block_no, parent_inode->i_block_3ind, parent_inode_no, block_idx - indirect2, 3);
    if (temp < 0) {
      update_block_state(volume, block_no, 1, 0);
      return 0;
    }

  } else {
    update_block_state(volume, block_no, 1, 0);
    return 0;
  }

  // Update i_blocks
  extra_counter += temp;
  parent_inode->i_blocks += extra_counter * volume->block_size / 512;
  write_inode(volume, parent_inode_no, parent_inode);
  return block_no;
}

uint32_t modify_time(volume_t *volume, uint32_t inode_no, inode_t *inode, time_t* ctime, time_t* mtime) {

  

  if (ctime != NULL)
    inode->i_ctime = (uint32_t) *ctime;
  if (mtime != NULL)
    inode->i_mtime = (uint32_t) *mtime;

  write_inode(volume, inode_no, inode);
  return 0;
}