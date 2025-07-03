#ifndef MAP_RECORDS
#define MAP_RECORDS

#include <map>
#include <unordered_map>
#include <mutex>
#include <string>
#include <utility>
#include <iostream>
#include <cassert>
#include <stdio.h>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <memory>
// #include <shared_mutex>
#include "hercules.hpp"

// Structure storing all information related to a certain IMSS.
typedef struct
{

	// IMSS URI.
	char uri_[256];
	// Byte specifying the type of structure.
	char type; // = 'I';
	// Set of ips comforming the IMSS.
	char **ips;
	// Number of IMSS servers.
	int32_t num_storages;
	// Server's dispatcher thread connection port.
	uint16_t conn_port;
} imss_info_;

// In-memory structure storing key-address couples.

class map_records
{
public:
	map_records(const map_records &r);
	map_records() = delete;
	map_records(const uint64_t nsize);
	~map_records();
	void set_size(const uint64_t nsize);
	uint64_t get_size();
	int CheckForMemorySpace(uint64_t required_space);
	int IncreaseMemoryOccupied(uint64_t required_space);
	int DecreaseMemoryOccupied(uint64_t freed_space);
	double get_storage_usage_percentage();
	void print_map();
	void FreeMemory(std::map<std::string, std::pair<void *, uint64_t>>::iterator item);
	// const char *get_head_element();
	std::string get_head_element();
	int32_t erase_head_element();
	// Used in stat_worker threads
	/**
	 * @brief Method deleting a record from the map.
	 * @return Number of elements deleted.
	 * */
	int32_t delete_metadata_stat_worker(std::string key);

	// Method storing a new record.
	int32_t put(std::string key, void *address, uint64_t length, int reused_buffer);
	int32_t put_snapshot(std::string key, int value);
	int32_t put_broadcast(std::string key, void *address, uint64_t length);

	// Gargabe collector functions.
	int32_t put_garbage_collector(std::string key);
	int32_t garbage_collector_pop(std::string key);
	int32_t garbage_collector_search(std::string key);

	// Method retrieving the address associated to a certain record.
	int32_t get(std::string key, void **add_, uint64_t *size_);
	int32_t get_snapshot(std::string key, int *to_copy);
	int32_t get_broadcast(std::string key, void **add_, uint64_t *size_);

	char *GetDataOfFile(std::string file_name, uint64_t *file_size_occupied);
	char *MergeData(__off_t *size_of_data, uint32_t num_of_data_servers, __off_t file_size, uint64_t block_size);

	// Method updating a new record.
	int32_t update(std::string key, void *add_, uint64_t length);
	int32_t update_simple(std::string key, int value);

	// Method renaming from stat_worker
	int32_t rename_metadata_stat_worker(std::string old_key, std::string new_key);
	// Method renaming from srv_worker
	int32_t rename_data_srv_worker(std::string old_key, std::string new_key);
	// Method renaming from srv_worker
	int32_t rename_data_dir_srv_worker(std::string old_dir, std::string rdir_dest);
	// Method renaming from stat_worker
	int32_t rename_metadata_dir_stat_worker(std::string old_dir, std::string rdir_dest);
	// Used in str_worker threads
	// Method deleting the address associated to a certain record.
	int32_t cleaning(char server_type);
	int32_t cleaning_specific(std::string new_key);
	int32_t freeAllMemory();
	int32_t erase_broadcast_element(std::string key);
	int32_t erase_snapshot_element(std::string key);

	int32_t get_broadcast_size();
	int32_t get_buffer_size();

	// int32_t memory2disk(uint64_t block_size, const char *checkpoint_dir, int finish, int server_id);
	// int32_t Checkpoint(uint64_t block_size, const char *checkpoint_dir, int finish, int, char *, struct arguments args);
	int32_t Checkpoint(uint64_t block_size, const char *checkpoint_dir, int finish, int server_id, char *data_hostname, struct arguments args);
	int32_t Snapshot(uint64_t block_size, const char *checkpoint_dir, int finish, int, char *, struct arguments args);

	// Method retrieving a map::begin iterator referencing the first element in the map container.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator begin()
	{
		return buffer.begin();
	}
	// Method retrieving a reference to the end of the map.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator end()
	{
		return buffer.end();
	}

	// Method retrieving the number of elements within the map.
	int32_t size()
	{
		return buffer.size();
	}

private:
	// Map structure tracking stored records (by default sorts keys with '<' op).
	// <key(file uri), <data, lenght>>
	std::map<std::string, std::pair<void *, uint64_t>> buffer;
	std::vector<std::string> buffer_garbage_collector;
	std::map<std::string, int> buffer_snapshot;
	// std::unordered_map<std::string, int> buffer_broadcast;
	std::map<std::string, std::pair<void *, uint64_t>> buffer_broadcast;
	std::map<std::string, std::pair<int, __off_t>> buffer_fd;
	// Mutex restricting access to structure.
	uint64_t total_size;
	uint64_t quantity_occupied = 0;
	std::mutex *mut;
	// std::shared_mutex mut;
};

#endif
