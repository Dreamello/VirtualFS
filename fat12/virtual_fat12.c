#include "virtual_fat12.h"
#include <fuse.h>
#include <stdio.h>
#include <ctype.h>
#include <errno.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/fsuid.h>

/* read_unsigned_le: Reads a little-endian unsigned integer number
   from buffer, starting at position.
   
   Parameters:
     buffer: memory position of the buffer that contains the number to
             be read.
     position: index of the initial position within the buffer where
               the number is to be found.
     num_bytes: number of bytes used by the integer number within the
                buffer. Cannot exceed the size of an int.
   Returns:
     The unsigned integer read from the buffer.
 */
unsigned int read_unsigned_le(const char *buffer, int position, int num_bytes) {
  long number = 0;
  while (num_bytes-- > 0) {
    number = (number << 8) | (buffer[num_bytes + position] & 0xff);
  }
  return number;
}

/* open_volume_file: Opens the specified file and reads the initial
   FAT12 data contained in the file, including the boot sector, file
   allocation table and root directory.
   
   Parameters:
     filename: Name of the file containing the volume data.
   Returns:
     A pointer to a newly allocated fat12volume data structure with
     all fields initialized according to the data in the volume file,
     or NULL if the file is invalid, data is missing, or the file is
     smaller than necessary.
 */
fat12volume *open_volume_file(const char *filename) {
  
  FILE *myfile = fopen(filename, "r+b"); // opens the file for reading
  if (myfile == NULL) return NULL; // failed to open file

  char buffer[BOOT_SECTOR_SIZE];

  if (fread(buffer, sizeof(char), BOOT_SECTOR_SIZE, myfile) != BOOT_SECTOR_SIZE) {
    // failed to read boot sector
    fclose(myfile);
    return NULL;
  }

  // set various file system properties
  fat12volume *volume = (fat12volume*) malloc(sizeof(fat12volume));
  volume->volume_file = myfile;
  volume->sector_size = read_unsigned_le(buffer, 11, 2);
  volume->cluster_size = read_unsigned_le(buffer, 13, 1);
  volume->reserved_sectors = read_unsigned_le(buffer, 14, 2);
  volume->fat_copies = read_unsigned_le(buffer, 16, 1);
  volume->rootdir_entries = read_unsigned_le(buffer, 17, 2);
  volume->fat_num_sectors = read_unsigned_le(buffer, 22, 2);
  volume->hidden_sectors = read_unsigned_le(buffer, 28, 2);
  volume->fat_offset = volume->reserved_sectors + volume->hidden_sectors; // both reserved + hidden come before partition 1
  volume->rootdir_offset = volume->fat_offset + volume->fat_copies * volume->fat_num_sectors;
  volume->rootdir_num_sectors = volume->rootdir_entries * DIR_ENTRY_SIZE / volume->sector_size;
  volume->cluster_offset = volume->rootdir_offset + volume->rootdir_num_sectors - 2 * volume->cluster_size;

  // initialize in memory copy of FAT table
  int fat_size = volume->fat_num_sectors * volume->sector_size;
  volume->fat_array = malloc(fat_size * sizeof(char));
  fseek(myfile, volume->fat_offset * volume->sector_size, SEEK_SET);
  if (fread(volume->fat_array, sizeof(char), fat_size, myfile) != fat_size) {
    // failed to read entire FAT table, free dynamically allocated objects and return
    free(volume->fat_array);
    free(volume);
    fclose(myfile);
    return NULL;
  }

  // initialize in memory copy of root directory
  int rootdir_size = volume->rootdir_entries * DIR_ENTRY_SIZE;
  volume->rootdir_array = malloc(rootdir_size * sizeof(char));
  fseek(myfile, volume->rootdir_offset * volume->sector_size, SEEK_SET);
  if (fread(volume->rootdir_array, sizeof(char), rootdir_size, myfile) != rootdir_size) {
    // failed to read entire root directory, free dynamically allocated objects and return
    free(volume->fat_array);
    free(volume->rootdir_array);
    free(volume);
    fclose(myfile);
    return NULL;
  }

  return volume;
}

/* close_volume_file: Frees and closes all resources used by a FAT12 volume.
   
   Parameters:
     volume: pointer to volume to be freed.
 */
void close_volume_file(fat12volume *volume) {
  free(volume->fat_array);
  free(volume->rootdir_array);
  fclose(volume->volume_file);
  free(volume);
}

/* read_sectors: Reads one or more contiguous sectors from the volume
   file, saving the data in a newly allocated memory space. The caller
   is responsible for freeing the buffer space returned by this
   function.
   
   Parameters:
     volume: pointer to FAT12 volume data structure.
     first_sector: number of the first sector to be read.
     num_sectors: number of sectors to read.
     buffer: address of a pointer variable that will store the
             allocated memory space.
   Returns:
     In case of success, it returns the number of bytes that were read
     from the set of sectors. In that case *buffer will point to a
     malloc'ed space containing the actual data read. If there is no
     data to read (e.g., num_sectors is zero, or the sector is at the
     end of the volume file, or read failed), it returns zero, and
     *buffer will be undefined.
 */
int read_sectors(fat12volume *volume, unsigned int first_sector,
		 unsigned int num_sectors, char **buffer) {

  // check if num_sector is 0
  if (num_sectors == 0)
    return 0;

  // check if read can be completed
  fseek(volume->volume_file, 0, SEEK_END);
  if ((first_sector + num_sectors) * volume->sector_size > ftell(volume->volume_file))
    return 0;

  // read and store the sectors
  int count = 0;
  int curr = 0;
  fseek(volume->volume_file, first_sector * volume->sector_size, SEEK_SET);
  *buffer = malloc(num_sectors * volume->sector_size * sizeof(char));
  for (unsigned int i = 0; i < num_sectors; i++) {
    curr = fread((*buffer) + i * volume->sector_size, sizeof(char), volume->sector_size, volume->volume_file);
    if (curr != volume->sector_size) { // if read failed
      free(*buffer);
      return 0;
    }
    count += curr;
  }
  return count;
}

/* read_cluster: Reads a specific data cluster from the volume file,
   saving the data in a newly allocated memory space. The caller is
   responsible for freeing the buffer space returned by this
   function. Note that, in most cases, the implementation of this
   function involves a single call to read_sectors with appropriate
   arguments.
   
   Parameters:
     volume: pointer to FAT12 volume data structure.
     cluster: number of the cluster to be read (the first data cluster
              is numbered two).
     buffer: address of a pointer variable that will store the
             allocated memory space.
   Returns:
     In case of success, it returns the number of bytes that were read
     from the cluster. In that case *buffer will point to a malloc'ed
     space containing the actual data read. If there is no data to
     read (e.g., the cluster is at the end of the volume file), it
     returns zero, and *buffer will be undefined.
 */
int read_cluster(fat12volume *volume, unsigned int cluster, char **buffer) {
  return read_sectors(volume, volume->cluster_offset + cluster * volume->cluster_size, 
                      volume->cluster_size, buffer);
}

/* get_next_cluster: Finds, in the file allocation table, the number
   of the cluster that follows the given cluster.
   
   Parameters:
     volume: pointer to FAT12 volume data structure.
     cluster: number of the cluster to seek.
   Returns:
     Number of the cluster that follows the given cluster (i.e., whose
     data is the sequence to the data of the current cluster). Returns
     0 if the given cluster is not in use, or a number larger than or
     equal to 0xff8 if the given cluster is the last cluster in a
     file.
 */
unsigned int get_next_cluster(fat12volume *volume, unsigned int cluster) {
  unsigned int remainder = cluster % 2;
  unsigned int index = cluster / 2 * 3;
  unsigned int bytes = read_unsigned_le(volume->fat_array, index, 3);
  return (bytes >> (remainder * 12)) & 0xFFF;
}

/* decode_filename: Reads the filename from a FAT12-formatted directory
   and assigns the filename to a dir_entry data structure.
   
   Parameters:
     data: pointer to the beginning of the directory entry in FAT12
           format. This function assumes that this pointer is at least
           long enough to extract both the name and extension. 
     entry: pointer to a dir_entry structure where the filename will be
            stored.
 */
void decode_filename(const char *data, dir_entry *entry) {
  int j = 0;
  for (int i = 0; i < 8; i++) {
    if (data[i] != 0x20) // if not a whitespace character
      entry->filename[j++] = data[i];
  }
  if (data[8] != 0x20) { // no extension if whitespace is found
    entry->filename[j++] = '.';
    for (int i = 8; i < 11; i++)
      if (data[i] != 0x20)
        entry->filename[j++] = data[i];
  }
  entry->filename[j] = '\0'; // add null terminator
  if (data[0] == 0x05) 
    entry->filename[0] = 0xE5; // special case where first byte is 0x05
}

/* fill_directory_entry: Reads the directory entry from a
   FAT12-formatted directory and assigns its attributes to a dir_entry
   data structure.
   
   Parameters:
     data: pointer to the beginning of the directory entry in FAT12
           format. This function assumes that this pointer is at least
           DIR_ENTRY_SIZE long.
     entry: pointer to a dir_entry structure where the data will be
            stored.
 */
void fill_directory_entry(const char *data, dir_entry *entry) {

  /* OBS: Note that the way that FAT12 represents a year is different
     than the way used by mktime and 'struct tm' to represent a
     year. In particular, both represent it as a number of years from
     a starting year, but the starting year is different between
     them. Make sure to take this into account when saving data into
     the entry. */
  decode_filename(data, entry);
  entry->ctime.tm_hour = (read_unsigned_le(data, 22, 2) >> 11) & 0b11111;
  entry->ctime.tm_min = (read_unsigned_le(data, 22, 2) >> 5) & 0b111111;
  entry->ctime.tm_sec = (read_unsigned_le(data, 22, 2) & 0b11111) * 2;
  entry->ctime.tm_year = ((read_unsigned_le(data, 24, 2) >> 9) & 0b1111111) + 80;
  entry->ctime.tm_mon = (read_unsigned_le(data, 24, 2) >> 5) & 0b1111;
  entry->ctime.tm_mday = read_unsigned_le(data, 24, 2) & 0b11111;
  entry->ctime.tm_isdst = -1;
  entry->size = read_unsigned_le(data, 28, 4);
  entry->first_cluster = read_unsigned_le(data, 26, 2);
  entry->is_directory = (read_unsigned_le(data, 11, 1) >> 4) & 1;
}

/* find_directory_entry: finds the directory entry associated to a
   specific path.
   
   Parameters:
     volume: Pointer to FAT12 volume data structure.
     path: Path of the file to be found. Will always start with a
           forward slash (/). Path components (e.g., subdirectories)
           will be delimited with "/". A path containing only "/"
           refers to the root directory of the FAT12 volume.
     entry: pointer to a dir_entry structure where the data associated
            to the path will be stored.
   Returns:
     In case of success (the provided path corresponds to a valid
     file/directory in the volume), the function will fill the data
     structure entry with the data associated to the path and return
     0. If the path is not a valid file/directory in the volume, the
     function will return -ENOENT, and the data in entry will be
     undefined. If the path contains a component (except the last one)
     that is not a directory, it will return -ENOTDIR, and the data in
     entry will be undefined.
 */
int find_directory_entry(fat12volume *volume, const char *path, dir_entry *entry) {
  
  /* OBS: In the specific case where the path corresponds to the root
     directory ("/"), this function should fill the entry with
     information for the root directory, but this entry will not be
     based on a proper entry in the volume, since the root directory
     is not obtained from such an entry. In particular, the date/time
     for the root directory can be set to Unix time 0 (1970-01-01 0:00
     GMT). */

  // root directory case *** ASK TA
  if (strcmp(path, "/") == 0) {
    entry->filename[0] = '/';
    entry->filename[1] = '\0';
	  entry->ctime.tm_hour = 0;
	  entry->ctime.tm_min = 0;
	  entry->ctime.tm_sec = 0;
	  entry->ctime.tm_year = 70;
	  entry->ctime.tm_mon = 1;
	  entry->ctime.tm_mday = 1;
	  entry->ctime.tm_isdst = -1;
	  entry->size = 0;
	  entry->first_cluster = 0;
	  entry->is_directory = 1;
    return 0;
  }

  // split path into components
  char** path_components = NULL;
  int count = split_path_components(&path_components, path);
  if (count == -ENOENT) return -ENOENT; // return if pathname is invalid

  // resolve pathname
  int ret = resolve_path(volume, volume->rootdir_array, volume->rootdir_entries, path_components, 0, count, entry);
  
  // deallocate dynamically allocated strings
  for (int i = 0; i < count; i++)
    free(path_components[i]);
  free(path_components);

  return ret;
}

/* resolve_path: recursively finds the directory entry associated to a
   specific path.
   
   Parameters:
     volume: Pointer to FAT12 volume data structure.
     directory: Directory to search for the current path component
     dir_size: Size of directory
     path_components: Array of path components
     index: Index of current path component
     count: Count of path components
     entry: pointer to a dir_entry structure where the data associated
            to the path will be stored.
   Returns:
     In case of success (the provided path corresponds to a valid
     file/directory in the volume), the function will fill the data
     structure entry with the data associated to the path and return
     0. If the path is not a valid file/directory in the volume, the
     function will return -ENOENT, and the data in entry will be
     undefined. If the path contains a component (except the last one)
     that is not a directory, it will return -ENOTDIR, and the data in
     entry will be undefined.
 */
int resolve_path(fat12volume *volume, const char *directory, unsigned int dir_size, char** path_components, int index, int count, dir_entry* entry) {

  // iterate over directory entries
  for (unsigned int i = 0; i < dir_size; i++) {
    if (directory[i * DIR_ENTRY_SIZE] == 0) { // end of directory
      return -ENOENT;
    } else if ((directory[i * DIR_ENTRY_SIZE] & 0xFF) == 0xE5) { // deleted directory entry
      continue;
    } else { // valid directory entry
      dir_entry current;
      fill_directory_entry(directory + i * DIR_ENTRY_SIZE, &current);

      // if current filename name matches path component
      if (strcmp(current.filename, path_components[index]) == 0) {

        // at last component of path, successfully found file, update entry attributes and return
        if (index == count - 1) {
          strcpy(entry->filename, current.filename);
          entry->ctime = current.ctime;
          entry->size = current.size;
          entry->is_directory = current.is_directory;
          entry->first_cluster = current.first_cluster;
          return 0;
        }

        // not at last component, but component is not a directory
        if (!current.is_directory) return -ENOTDIR;
        
        // special case when first cluster == 0, which is the root directory *** ASK TA
        if (current.first_cluster == 0)
          return resolve_path(volume, volume->rootdir_array, volume->rootdir_entries, path_components, index + 1, count, entry);

        // load subdirectory into buffer
        char* directory_buffer = NULL;
        int cluster_count = read_all_clusters(volume, current.first_cluster, &directory_buffer);
        if (cluster_count == -1) return -EIO; // cluster read error

        // make recursive call to resolve next path component in subdirectory
        unsigned int next_dir_size = cluster_count * volume->cluster_size * volume->sector_size / DIR_ENTRY_SIZE;
        int ret = resolve_path(volume, directory_buffer, next_dir_size, path_components, index + 1, count, entry);

        // deallocate directory buffer and return
        free(directory_buffer);
        return ret;
      }
    }
  }
  return -ENOENT;
}

int split_path_components(char*** path_components, const char* path) {
  int count = 0; // component counter
  char* copy = strdup(path); // create a copy of pathname since strtok modifies the given string
  char* token = copy;
  char slash[2] = "/";

  // count the number of components
  while(*token) {
    if (*token == slash[0]) {
      if (*(token - 1) == slash[0]) { // invalid pathname: empty path component, eg "//"
        free(copy);
        return -ENOENT;
      }
      count++;
    } 
    token++;
  }
  if (*(token - 1) == slash[0])
    count--; // decrement count if there is a trailing "/"

  // store path components into array
  *path_components = malloc(count * sizeof(char*));
  if (*path_components) {
    int index = 0;
    token = strtok(copy, slash); // NOTE: strtok always returns a non-empty string, first "/" is ignored
    while (token) {
      (*path_components)[index] = strdup(token);
      int i = 0;
      while((*path_components)[index][i]) {
        (*path_components)[index][i] = toupper((*path_components)[index][i]); // convert to uppercase
        i++;
      }
      token = strtok(0, slash); // get next path component
      index++;
    }
  } else { // invalid pathname: component count is 0
    free(copy);
    return -ENOENT;
  }
  free(copy);
  return count;
}

void free_path_components(char** path_components, int count) {
  for (int i = 0; i < count; i++)
    free(path_components[i]);
  free(path_components);
}

/* read_all_clusters: Reads all data cluster belonging to the same
   file from the volume file, saving the data in a newly allocated
   memory space. The caller is responsible for freeing the buffer
   space returned by this function. 
   
   Parameters:
     volume: pointer to FAT12 volume data structure.
     cluster: number of the first cluster to be read (the first data
              cluster is numbered two).
     buffer: address of a pointer variable that will store the
             allocated memory space.
   Returns:
     In case of success, it returns the number of clusters that were
     read. In that case *buffer will point to a malloc'ed space
     containing the actual data read. If reading any clusters in the
     chain failed, it returns -1, and *buffer will be undefined.
 */
int read_all_clusters(fat12volume *volume, unsigned int cluster, char **buffer) {

  // count number of clusters for current file
  int cluster_count = 0;
  int next_cluster = cluster;
  while (next_cluster < 0xFF8) {
    if (next_cluster == 0x0 || next_cluster == 0xFF7) return -1;
    cluster_count++;
    next_cluster = get_next_cluster(volume, next_cluster);
  }

  // allocate buffer based on cluster count
  *buffer = malloc(cluster_count * volume->cluster_size * volume->sector_size * sizeof(char));

  // read all clusters and store file into buffer
  char* cluster_buffer = NULL;
  next_cluster = cluster;
  for (int i = 0; i < cluster_count; i++) {
    if (read_cluster(volume, next_cluster, &cluster_buffer) == 0) {
      free(*buffer);
      return -1; // read failed
    }
    memcpy((*buffer) + i * volume->cluster_size * volume->sector_size, cluster_buffer, volume->cluster_size * volume->sector_size);
    free(cluster_buffer);
    if (next_cluster < 0xFF8) next_cluster = get_next_cluster(volume, next_cluster); // update next cluster number
  }
  return cluster_count;
}




// ==============================================================================================================================
// ALL FUNCTIONS AFTER THIS ARE BONUS FUNCTIONS =================================================================================
// ==============================================================================================================================




/* set_cluster: Changes given cluster's entry in the FAT table to value.
   This affects both the in memory FAT table and the volume file. 
   
   Parameters:
     volume: pointer to FAT12 volume data structure.
     cluster: number of the data cluster that will be changed
     value: new value for the given cluster entry
 */
void set_cluster(fat12volume *volume, unsigned int cluster, unsigned int value) {

  unsigned int remainder = cluster % 2;
  unsigned int index = cluster / 2 * 3;
  unsigned int bytes = read_unsigned_le(volume->fat_array, index, 3);
  unsigned int first = bytes & 0xFFF;
  unsigned int second = (bytes >> 12) & 0xFFF;
  if (remainder)
    second = value;
  else
    first = value;
  bytes = first | (second << 12);

  // printf("Setting cluster %d to value %x\n", cluster, value);

  // Update in memory FAT table
  volume->fat_array[index] = bytes & 0xFF;
  volume->fat_array[index + 1] = (bytes >> 8) & 0xFF;
  volume->fat_array[index + 2] = (bytes >> 16) & 0xFF;

  // Update volume file FAT table
  fseek(volume->volume_file, volume->fat_offset * volume->sector_size + index, SEEK_SET);
  fwrite(volume->fat_array + index, sizeof(char), 3, volume->volume_file);
}

/* clear_all_clusters: Reads all data cluster belonging to the same
   file and marks all of them as free in both the in memory FAT table
   and the volume file. 
   
   Parameters:
     volume: pointer to FAT12 volume data structure.
     cluster: number of the first cluster to be read (the first data
              cluster is numbered two).
   Returns:
     In case of success, it returns 0. If reading any clusters in the
     chain failed, it returns -1.
 */
int clear_all_clusters(fat12volume *volume, unsigned int cluster) {

  // first check if cluster chain is valid
  int next_cluster = cluster;
  while (next_cluster < 0xFF8) {
    if (next_cluster == 0x0 || next_cluster == 0xFF7) return -1;
    next_cluster = get_next_cluster(volume, next_cluster);
  }

  // valid chain, proceed with clear
  next_cluster = cluster;
  while (next_cluster < 0xFF8) {
    cluster = next_cluster;
    next_cluster = get_next_cluster(volume, next_cluster);
    set_cluster(volume, cluster, 0x0);
  }
  return 0;
}

char* find_directory_entry_ptr_and_cluster(fat12volume *volume, const char *path, int* entry_cluster, int* entry_cluster_offset) {
  
  // root directory case
  if (strcmp(path, "/") == 0) {
    return volume->rootdir_array;
  }

  // split path into components
  char** path_components = NULL;
  int count = split_path_components(&path_components, path);
  if (count == -ENOENT) return NULL; // return if pathname is invalid

  // resolve pathname
  char* ret = resolve_path_ptr_and_cluster(volume, volume->rootdir_array, volume->rootdir_entries, path_components, 
                                           0, count, entry_cluster, entry_cluster_offset, 0);
  
  // deallocate dynamically allocated strings
  for (int i = 0; i < count; i++)
    free(path_components[i]);
  free(path_components);
  return ret;
}

char* resolve_path_ptr_and_cluster(fat12volume *volume, char *directory, unsigned int dir_size, char** path_components, 
                                   int index, int count, int* entry_cluster, int* entry_cluster_offset, int parent_cluster) {

  // iterate over directory entries
  for (unsigned int i = 0; i < dir_size; i++) {
    if (directory[i * DIR_ENTRY_SIZE] == 0) { // end of directory
      return NULL;
    } else if ((directory[i * DIR_ENTRY_SIZE] & 0xFF) == 0xE5) { // deleted directory entry
      continue;
    } else { // valid directory entry
      dir_entry current;
      fill_directory_entry(directory + i * DIR_ENTRY_SIZE, &current);

      // if current filename name matches path component
      if (strcmp(current.filename, path_components[index]) == 0) {
        
        // calculate entry cluster and entry cluster offset
        get_entry_cluster(volume, parent_cluster, i, entry_cluster, entry_cluster_offset);

        // at last component of path, successfully found file, return pointer to entry
        if (index == count - 1) {
          if (parent_cluster == 0)
            return directory + i * DIR_ENTRY_SIZE;
          else {
            char* ptr = malloc(DIR_ENTRY_SIZE * sizeof(char));
            memcpy(ptr, directory + i * DIR_ENTRY_SIZE, DIR_ENTRY_SIZE);
            return ptr;
          }
        }

        // not at last component, but component is not a directory
        if (!current.is_directory) return NULL;
        
        // special case when first cluster == 0, which is the root directory *** ASK TA
        if (current.first_cluster == 0) {
          return resolve_path_ptr_and_cluster(volume, volume->rootdir_array, volume->rootdir_entries, path_components, 
                                             index + 1, count, entry_cluster, entry_cluster_offset, current.first_cluster);
        }

        // load subdirectory into buffer
        char* directory_buffer = NULL;
        int cluster_count = read_all_clusters(volume, current.first_cluster, &directory_buffer);
        if (cluster_count == -1) return NULL; // cluster read error

        // make recursive call to resolve next path component in subdirectory
        unsigned int next_dir_size = cluster_count * volume->cluster_size * volume->sector_size / DIR_ENTRY_SIZE;
        char* ret = resolve_path_ptr_and_cluster(volume, directory_buffer, next_dir_size, path_components, index + 1, 
                                                count, entry_cluster, entry_cluster_offset, current.first_cluster);

        // deallocate directory buffer and return
        free(directory_buffer);
        return ret;
      }
    }
  }
  return NULL;
}

int is_directory_empty(fat12volume *volume, dir_entry* entry) {
  
  char* directory = NULL;
  int cluster_count = read_all_clusters(volume, entry->first_cluster, &directory);
  if (cluster_count == -1) return -EIO; // cluster read error

  unsigned int dir_size = cluster_count * volume->cluster_size * volume->sector_size / DIR_ENTRY_SIZE;
  
  // iterate over directory entries
  for (unsigned int i = 0; i < dir_size; i++) {
    if (directory[i * DIR_ENTRY_SIZE] == 0) { // end of directory
      free(directory);
      return 0;
    } else if ((directory[i * DIR_ENTRY_SIZE] & 0xFF) == 0xE5) { // deleted directory entry
      continue;
    } else { // valid directory entry
      dir_entry current;
      fill_directory_entry(directory + i * DIR_ENTRY_SIZE, &current);

      // if current filename name is "." or "..", continue searching
      if (strcmp(current.filename, ".") == 0 || strcmp(current.filename, "..") == 0) continue;

      // current file is not "." or "..", directory not empty
      free(directory);
      return -ENOTEMPTY;
    }
  }

  free(directory);
  return 0;
}

int create_directory(fat12volume *volume, const char *path) {

  // split path into components
  char** path_components = NULL;
  int count = split_path_components(&path_components, path);
  if (count == -ENOENT) return count; // return if pathname is invalid
  if (strlen(path_components[count - 1]) > 8) {
    free_path_components(path_components, count);
    return -ENAMETOOLONG; // return if directory name too long
  }

  // find free cluster to store directory
  int first_cluster = find_free_cluster(volume);
  if (first_cluster == -1) {
    free_path_components(path_components, count);
    return -ENOSPC; // return if all clusters in use
  }

  // zero out new cluster
  zero_cluster(volume, first_cluster, 0);

  // resolve parent directory
  char* directory = NULL;
  unsigned int dir_size;
  int parent_cluster = 0;
  if (count == 1) {
    // parent directory is the root directory
    directory = volume->rootdir_array;
    dir_size = volume->rootdir_entries;
  } else {
    dir_entry entry;
    int ret = resolve_path(volume, volume->rootdir_array, volume->rootdir_entries, path_components, 0, count - 1, &entry);
    if (ret != 0) {
      free_path_components(path_components, count);
      return ret;
    }

    // store parent directory into buffer
    int cluster_count = read_all_clusters(volume, entry.first_cluster, &directory);
    if (cluster_count == -1) {
      free_path_components(path_components, count);
      return -EIO;
    }

    dir_size = cluster_count * volume->cluster_size * volume->sector_size / DIR_ENTRY_SIZE;
    parent_cluster = entry.first_cluster;
  }

  // search parent directory for an empty or deleted entry for new directory
  for (unsigned int i = 0; i < dir_size; i++) {
    if (directory[i * DIR_ENTRY_SIZE] == 0 || (directory[i * DIR_ENTRY_SIZE] & 0xFF) == 0xE5) { // free or delected directory entry

      // calculate entry cluster and entry cluster offset
      int entry_cluster = 0;
      int entry_cluster_offset = 0;
      get_entry_cluster(volume, parent_cluster, i, &entry_cluster, &entry_cluster_offset);

      // create new directory in parent directory
      create_directory_entry(volume, directory + i * DIR_ENTRY_SIZE, path_components[count - 1], 0, first_cluster, entry_cluster, entry_cluster_offset, 1);
      
      // create . and .. directory entries in new directory
      char buf[DIR_ENTRY_SIZE]; // temp buffer to pass to create_directory_entry, since it doesn't actually need to exist
      create_directory_entry(volume, buf, ".", 0, first_cluster, first_cluster, 0, 1);
      create_directory_entry(volume, buf, "..", 0, parent_cluster, first_cluster, DIR_ENTRY_SIZE, 1);

      free_path_components(path_components, count);
      if (count > 1) free(directory);
      return 0;

    } else { // occupied directory entry
      continue;
    }
  }

  printf("\n========================\n");
  printf("\nOVERFLOW DIRECTORY CASE!\n");
  printf("\n========================\n\n");

  // directory is full, need to overflow to a new cluster
  if (parent_cluster == 0) {
    free_path_components(path_components, count);
    return -ENOSPC; // cannot overflow if root directory is full
  }
  int last_cluster = 0;
  int overflow_cluster = 0;
  get_entry_cluster(volume, parent_cluster, dir_size - 1, &last_cluster, &overflow_cluster);
  overflow_cluster = find_free_cluster(volume); // get free cluster for expansion
  set_cluster(volume, last_cluster, overflow_cluster); // set last cluster's entry in FAT table to new cluster
  char buf[DIR_ENTRY_SIZE]; // temp buffer to pass to create_directory_entry, since it doesn't actually need to exist

  // create directory entry in new cluster
  create_directory_entry(volume, buf, path_components[count - 1], 0, first_cluster, overflow_cluster, 0, 1);

  // zero out all entries after new directory
  zero_cluster(volume, overflow_cluster, DIR_ENTRY_SIZE);

  // create . and .. directory entries in new directory
  create_directory_entry(volume, buf, ".", 0, first_cluster, first_cluster, 0, 1);
  create_directory_entry(volume, buf, "..", 0, parent_cluster, first_cluster, DIR_ENTRY_SIZE, 1);

  // deallocate dynamically allocated strings
  free_path_components(path_components, count);
  if (count > 1) free(directory);
  return 0;
}

int create_directory_entry(fat12volume *volume, char* data, char* filename, int filesize, 
                           int first_cluster, int entry_cluster, int entry_cluster_offset, int is_directory) {
  int i;
  for (i = 0; i < 11; i++)
    data[i] = 0x20;
  for (i = 11; i < 32; i++)
    data[i] = 0x00;

  if (is_directory) {
    for (i = 0; i < 8; i++) {
      if (filename[i] == '\0') break;
      data[i] = filename[i];
    }
  } else {

    // TODO: fix this lol...

    for (i = 0; i < 8; i++) {
      if (filename[i] == '.') break;
      data[i] = filename[i];
    }
    i++;
    for (int j = 8; j < 11; j++) {
      if (filename[i] == '\0') break;
      data[j] = filename[i++];
    }
  }

  time_t seconds = time(NULL);
  struct tm ctime = *localtime(&seconds);

  unsigned int dblseconds = ctime.tm_sec / 2;
  unsigned int minutes = ctime.tm_min << 5;
  unsigned int hour = ctime.tm_hour << 11;

  unsigned int year = (ctime.tm_year - 80) << 9;
  unsigned int month = ctime.tm_mon << 5;
  unsigned int day = ctime.tm_mday;

  unsigned int time = dblseconds | minutes | hour;
  unsigned int date = day | month | year;

  data[11] = is_directory << 4;
  data[22] = time & 0xFF;
  data[23] = (time >> 8) & 0xFF;
  data[24] = date & 0xFF;
  data[25] = (date >> 8) & 0xFF;
  data[26] = first_cluster & 0xFF;
  data[27] = (first_cluster >> 8) & 0xFF;
  data[28] = filesize & 0xFF;
  data[29] = (filesize >> 8) & 0xFF;
  data[30] = (filesize >> 16) & 0xFF;
  data[31] = (filesize >> 24) & 0xFF;

  int seek_position; 
  if (entry_cluster == 0)
    seek_position = volume->rootdir_offset * volume->sector_size + entry_cluster_offset;
  else
    seek_position = (volume->cluster_offset + entry_cluster * volume->cluster_size) * volume->sector_size + entry_cluster_offset;
  fseek(volume->volume_file, seek_position, SEEK_SET);
  fwrite(data, sizeof(char), 32, volume->volume_file);
  return 0;
}

int delete_directory_entry(fat12volume *volume, int entry_cluster, int entry_cluster_offset) {
  char buf = 0xE5;
  int seek_position;
  if (entry_cluster == 0)
    seek_position = volume->rootdir_offset * volume->sector_size + entry_cluster_offset;
  else
    seek_position = (volume->cluster_offset + entry_cluster * volume->cluster_size) * volume->sector_size + entry_cluster_offset;
  fseek(volume->volume_file, seek_position, SEEK_SET);
  fwrite(&buf, sizeof(char), 1, volume->volume_file);
  return 0;
}

int move_directory_entry(fat12volume *volume, const char *path, dir_entry* move_entry) {

  // split path into components
  char** path_components = NULL;
  int count = split_path_components(&path_components, path);
  if (count == -ENOENT) return count; // return if pathname is invalid

  // check filename length
  if (move_entry->is_directory) {
    if (strlen(path_components[count - 1]) > 8) {
      free_path_components(path_components, count);
      return -ENAMETOOLONG; // return if directory name too long
    }
  } else {

    // TODO: fix this lol...

    int filename_count = 0;
    int extension_count = 0;
    for (int i = 0; i < strlen(path_components[count - 1]); i++) {
      if (path_components[count - 1][i] == '.') {
        filename_count = i + 1;
      }
      if (path_components[count - 1][i] == '\0') {
        extension_count = i - filename_count - 1;
        break;
      }
    }
    if (filename_count > 8 || extension_count > 3) {
      free_path_components(path_components, count);
      return -ENAMETOOLONG; // return if directory name too long
    }
  }

  // resolve parent directory
  char* directory = NULL;
  unsigned int dir_size;
  int parent_cluster = 0;
  if (count == 1) {
    // parent directory is the root directory
    directory = volume->rootdir_array;
    dir_size = volume->rootdir_entries;
  } else {
    dir_entry entry;
    int ret = resolve_path(volume, volume->rootdir_array, volume->rootdir_entries, path_components, 0, count - 1, &entry);
    if (ret != 0) {
      free_path_components(path_components, count);
      return ret;
    }

    // store parent directory into buffer
    int cluster_count = read_all_clusters(volume, entry.first_cluster, &directory);
    if (cluster_count == -1) {
      free_path_components(path_components, count);
      return -EIO;
    }

    dir_size = cluster_count * volume->cluster_size * volume->sector_size / DIR_ENTRY_SIZE;
    parent_cluster = entry.first_cluster;
  }

  // search parent directory for an empty or deleted entry for new directory
  for (unsigned int i = 0; i < dir_size; i++) {
    if (directory[i * DIR_ENTRY_SIZE] == 0 || (directory[i * DIR_ENTRY_SIZE] & 0xFF) == 0xE5) { // free or delected directory entry

      // calculate entry cluster and entry cluster offset
      int entry_cluster = 0;
      int entry_cluster_offset = 0;
      get_entry_cluster(volume, parent_cluster, i, &entry_cluster, &entry_cluster_offset);

      // create new directory in parent directory
      create_directory_entry(volume, directory + i * DIR_ENTRY_SIZE, path_components[count - 1], move_entry->size, 
                             move_entry->first_cluster, entry_cluster, entry_cluster_offset, move_entry->is_directory);
      
      // overwrite ".." in moved directory
      if (move_entry->is_directory) {
        char buf[DIR_ENTRY_SIZE]; // temp buffer to pass to create_directory_entry, since it doesn't actually need to exist
        create_directory_entry(volume, buf, "..", 0, parent_cluster, move_entry->first_cluster, DIR_ENTRY_SIZE, 1);
      }

      free_path_components(path_components, count);
      if (count > 1) free(directory);
      return 0;

    } else { // occupied directory entry
      continue;
    }
  }

  printf("\n========================\n");
  printf("\nOVERFLOW DIRECTORY CASE!\n");
  printf("\n========================\n\n");

  // directory is full, need to overflow to a new cluster
  if (parent_cluster == 0) {
    free_path_components(path_components, count);
    return -ENOSPC; // cannot overflow if root directory is full
  }
  int last_cluster = 0;
  int overflow_cluster = 0;
  get_entry_cluster(volume, parent_cluster, dir_size - 1, &last_cluster, &overflow_cluster);
  overflow_cluster = find_free_cluster(volume); // get free cluster for expansion
  set_cluster(volume, last_cluster, overflow_cluster); // set last cluster's entry in FAT table to new cluster
  char buf[DIR_ENTRY_SIZE]; // temp buffer to pass to create_directory_entry, since it doesn't actually need to exist
  
  // create directory entry in new cluster
  create_directory_entry(volume, buf, path_components[count - 1], move_entry->size, 
                         move_entry->first_cluster, overflow_cluster, 0, move_entry->is_directory);

  // zero out all entries after new directory
  zero_cluster(volume, overflow_cluster, DIR_ENTRY_SIZE);

  // overwrite ".." in moved directory
  if (move_entry->is_directory) {
    create_directory_entry(volume, buf, "..", 0, parent_cluster, move_entry->first_cluster, DIR_ENTRY_SIZE, 1);
  }

  // deallocate dynamically allocated strings
  free_path_components(path_components, count);
  if (count > 1) free(directory);
  return 0;
}

int find_free_cluster(fat12volume *volume) {
  char buffer[2];

  // read boot sector to find maximum sector size
  fseek(volume->volume_file, 19, SEEK_SET);
  fread(buffer, sizeof(char), 2, volume->volume_file);
  int max_sector = read_unsigned_le(buffer, 0, 2);
  max_sector = (max_sector - volume->cluster_offset - volume->cluster_size * 2) / volume->cluster_size;

  // search all data clusters for a free cluster
  for (int i = 2; i < max_sector; i++) {
    if (get_next_cluster(volume, i) == 0) {
      set_cluster(volume, i, 0xfff);
      return i; 
    }
  }
  return -1;
}

void get_entry_cluster(fat12volume *volume, int parent_cluster, int entry_index, int *entry_cluster, int *entry_cluster_offset) {
  if (parent_cluster != 0) {
    // if parent directory isnt root directory
    *entry_cluster_offset = (entry_index * DIR_ENTRY_SIZE) % (volume->sector_size * volume->cluster_size);
    int num_clusters = entry_index * DIR_ENTRY_SIZE / volume->sector_size / volume->cluster_size;
    *entry_cluster = parent_cluster;
    while (num_clusters-- > 0) {
      *entry_cluster = get_next_cluster(volume, *entry_cluster);
    }
  } else {
    // if parent directory is root directory
    *entry_cluster = parent_cluster;
    *entry_cluster_offset = entry_index * DIR_ENTRY_SIZE;
  }
}

void zero_cluster(fat12volume *volume, int cluster, int offset) {

  // initialize zero byte array
  char zeros[volume->cluster_size * volume->sector_size - offset];
  memset(zeros, 0, volume->cluster_size * volume->sector_size - offset);

  // overwrite cluster with zero bytes
  int seek_position = (volume->cluster_offset + cluster * volume->cluster_size) * volume->sector_size + offset;
  fseek(volume->volume_file, seek_position, SEEK_SET);
  fwrite(zeros, sizeof(char), volume->cluster_size * volume->sector_size - offset, volume->volume_file);
}