#ifndef _VIRTUAL_FAT12_H_
#define _VIRTUAL_FAT12_H_

#include <stdio.h>
#include <time.h>

/* Size of the boot sectore of a FAT12 volume, in bytes */
#define BOOT_SECTOR_SIZE 512
/* Size of each individual entry in a FAT12 directory, in bytes */
#define DIR_ENTRY_SIZE 32

/* Data structure used to store data associated to a FAT12 volume */
typedef struct fat12volume {
  
  /* File pointer to volume file */
  FILE *volume_file;

  /* Sector size in bytes */
  unsigned int sector_size;
  /* Cluster size in sectors */
  unsigned int cluster_size;

  /* Number of reserved sectors in the beginning of the volume
     (including the boot sector) */
  unsigned int reserved_sectors;
  /* Number of hidden sectores in the volume */
  unsigned int hidden_sectors;

  /* First sector number of the first copy of the File Allocation
     Table (FAT) */
  unsigned int fat_offset;
  /* Number of sectors used by each copy of the FAT */
  unsigned int fat_num_sectors;
  /* Number of copies of the FAT found in the volume */
  unsigned int fat_copies;
  /* Copy of the entire FAT in memory */
  char *fat_array;

  /* First sector number of the root directory listing */
  unsigned int rootdir_offset;
  /* Maximum number of directory entries in the root directory */
  unsigned int rootdir_entries;
  /* Number of sectors used by the root directory */
  unsigned int rootdir_num_sectors;
  /* Copy of the entire root directory in memory */
  char *rootdir_array;

  /* Sector number of the data cluster #0. Note that the first data
     cluster is cluster #2, so cluster #0's offset corresponds to two
     clusters before the actual start of the data clusters. */
  unsigned int cluster_offset;
  
} fat12volume;

/* Data structure representing useful information in each entry of a
   FAT12 directory */
typedef struct dir_entry {

  /* Name of the file, as a string in format NAME.EXT. The string must
     be zero-terminated, and should not include any spaces found in
     the file name in the volume. */
  char filename[13];
  /* Creation date/time (check 'man 2 mktime' for information about
     struct tm). Since FAT-12 doesn't distinguish between creation and
     modification time, we'll use this time for both. */
  struct tm ctime;
  /* Size of the file, in bytes */
  unsigned int size;
  /* Number of the first cluster containing data for this
     file. Remaining clusters are found using the File Allocation
     Table (FAT). */
  unsigned int first_cluster : 12;
  /* Flag: 0 if this is a regular file, 1 if it is a directory. */
  unsigned int is_directory : 1;
  
} dir_entry;

fat12volume *open_volume_file(const char *filename);
void close_volume_file(fat12volume *volume);

int read_sectors(fat12volume *volume, unsigned int first_sector, unsigned int num_sectors, char **buffer);
int read_cluster(fat12volume *volume, unsigned int cluster, char **buffer);

unsigned int get_next_cluster(fat12volume *volume, unsigned int cluster);
void fill_directory_entry(const char *data, dir_entry *entry);
int find_directory_entry(fat12volume *volume, const char *path, dir_entry *entry);

// Helpers
void decode_filename(const char *data, dir_entry *entry);
int resolve_path(fat12volume *volume, const char *directory, unsigned int dir_size, char** path_components, int index, int count, dir_entry* entry);
int split_path_components(char*** path_components, const char* path);
void free_path_components(char** path_components, int count);
int read_all_clusters(fat12volume *volume, unsigned int cluster, char **buffer);

// Bonus helpers
void set_cluster(fat12volume *volume, unsigned int cluster, unsigned int value);
int clear_all_clusters(fat12volume *volume, unsigned int cluster);
char* find_directory_entry_ptr_and_cluster(fat12volume *volume, const char *path, int* entry_cluster, int* entry_cluster_offset);
char* resolve_path_ptr_and_cluster(fat12volume *volume, char *directory, unsigned int dir_size, char** path_components, int index, int count, int* entry_cluster, int* entry_cluster_offset, int parent_cluster);
int is_directory_empty(fat12volume *volume, dir_entry* entry);
int create_directory(fat12volume *volume, const char *path);
int create_directory_entry(fat12volume *volume, char* data, char* filename, int filesize, int first_cluster, int parent_cluster, int parent_cluster_offset, int is_directory);
int delete_directory_entry(fat12volume *volume, int entry_cluster, int entry_cluster_offset);
int move_directory_entry(fat12volume *volume, const char *path, dir_entry* move_entry);
int find_free_cluster(fat12volume *volume);
void get_entry_cluster(fat12volume *volume, int parent_cluster, int entry_index, int *entry_cluster, int *entry_cluster_offset);
void zero_cluster(fat12volume *volume, int cluster, int offset);

#endif
