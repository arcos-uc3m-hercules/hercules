#include <map>
#include <mutex>
#include <utility>
#include <iostream>
#include <cassert>
#include <stdio.h>
#include <vector>
#include <algorithm>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <inttypes.h>
#include "records.hpp"
#include <condition_variable>
#include "imss.h"
#include "queue.h"

using std::make_pair;
using std::map;
using std::pair;
using std::string;

extern StsHeader *mem_pool;

// __thread
extern int32_t  current_dataset;   // Dataset whose policy has been set last.
extern dataset_info curr_dataset; // Currently managed dataset.
extern imss curr_imss;

extern std::mutex mtx;
extern std::condition_variable cv;
extern int data_ready;

map_records::map_records(const uint64_t nsize)
{
	total_size = nsize;
	mut = new std::mutex();
}

map_records::~map_records()
{
	fprintf(stderr, "Freeing memory\n");
	freeAllMemory();

	delete mut;
}

void map_records::set_size(const uint64_t nsize)
{
	total_size = nsize;
}

int64_t map_records::get_size()
{
	return total_size;
}

std::string map_records::get_head_element()
{
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	std::map<std::string, std::pair<void *, uint64_t>>::iterator it;
	it = buffer.begin();
	// string key = it->first();
	string key = it->first;
	// fprintf(stderr, "head element = %s", key.c_str());
	// return key.c_str();
	return key;
}

// Method deleting the address associated to a certain record.
int32_t map_records::erase_head_element()
{

	// fprintf(stderr, "GET KEY=%s\n",key.c_str());
	// Map iterator that will be searching for the key.
	// std::map<std::string, std::pair<void *, uint64_t>>::iterator it;
	// Block the access to the map structure.
	// std::unique_lock<std::mutex> lock(*mut);

	// struct utsname detect;
	// uname(&detect);

	// if (buffer.empty())
	// 	return 0;

	// // Search for the address related to the key.
	// it = buffer.find(key);
	// // Check if the value did exist within the map.
	// if (it == buffer.end())
	// {
	// 	// fprintf(stderr,"Nodename-%s NO EXIST=%s\n",detect.nodename, key.c_str());
	// 	// fprintf(stderr,"NO EXIST=%s\n", key.c_str());
	// 	return 0;
	// }

	// fprintf(stderr,"GET-%s \n", key.c_str());
	// fprintf(stderr,"Nodename    - %s	GET-%s \n", detect.nodename, key.c_str());

	// Erase the first element.
	buffer.erase(buffer.begin());

	// Return the address associated to the record.
	return 1;
}

int32_t map_records::erase_broadcast_element(std::string key)
{

	// Map iterator that will be searching for the key.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator it;

	// // Search for the address related to the key.
	it = buffer_broadcast.find(key);
	// // Check if the value did exist within the map.
	if (it == buffer_broadcast.end())
	{
		return 0;
	}

	// Erase the element.
	buffer_broadcast.erase(it);

	return 1;
}

int32_t map_records::erase_snapshot_element(std::string key)
{

	// Map iterator that will be searching for the key.
	std::map<std::string, int>::iterator it;

	// // Search for the address related to the key.
	it = buffer_snapshot.find(key);
	// // Check if the value did exist within the map.
	if (it == buffer_snapshot.end())
	{
		return 0;
	}

	// Erase the element.
	buffer_snapshot.erase(it);

	return 1;
}

void map_records::print_map()
{
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);

	// save partners for later deletion and new insertion of news paths
	// std::vector<string> vec;
	// get_data_location(0, 0, SET);

	slog_info("Datasets in this servers");
	for (const auto &[key, value] : buffer)
	{
		slog_info("key = %s\n", key.c_str());
		int pos = key.find('$');
		string block = key.substr(pos, key.length() + 1);
		string data_uri = key.substr(0, pos);
		fprintf(stderr, "key=%s,\turi=%s,\tblock=%s\n", key.c_str(), data_uri.c_str(), block.c_str());
	}
}

// Method storing a new record.
int32_t map_records::put(std::string key, void *address, uint64_t length)
{
	// Construct a pair object storing the couple of values associated to a key.
	std::pair<void *, uint64_t> value(address, length);
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	// Add a new couple to the map.
	// fprintf(stderr, "total_size=%ld bytes, quantity_occupied=%ld bytes\n",total_size, quantity_occupied);
	if (quantity_occupied + length > total_size && total_size > 0)
	{ // out of space
		fprintf(stderr, "[Map record] Out of space  %ld/%ld.\n", quantity_occupied + length, total_size);
		slog_error("Out of space  %ld/%ld.\n", quantity_occupied + length, total_size);
		return -1;
	}

	// struct utsname detect;
	// uname(&detect);
	// DPRINT("Nodename    - %s add in map=%s\n", detect.nodename, key.c_str());
	// fprintf(stderr, "key=%s, quantity=%ld total size=%ld\n", key.c_str(), quantity_occupied, total_size);
	// if(quantity_occupied % ) {
	// }
	quantity_occupied = quantity_occupied + length;
	buffer.insert({key, value});
	return 0;
}

// Method storing a new record.
int32_t map_records::put_snapshot(std::string key, int value)
{
	// Construct a pair object storing the couple of values associated to a key.
	// std::pair<void *, uint64_t> value(to_copy, 0); // second param is for file size.
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	// Add a new value to the map.
	// fprintf(stderr, "Inserting %s with value %d\n", key.c_str(), value);
	slog_debug("Inserting %s with value %d", key.c_str(), value);
	buffer_snapshot.insert({key, value});
	slog_debug("The value of %s is %d", key.c_str(), buffer_snapshot[key]);
	return 0;
}

int32_t map_records::put_broadcast(std::string key, void *address, uint64_t length)
{
	// Construct a pair object storing the couple of values associated to a key.
	std::pair<void *, uint64_t> value(address, length);
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	// Add a new couple to the map.
	slog_debug("Inserting key %s in the broadcast", key.c_str());
	buffer_broadcast.insert({key, value});
	cv.notify_one();
	return 0;
}

// Method retrieving the address associated to a certain record.
int32_t map_records::get(std::string key, void **add_, uint64_t *size_)
{

	// fprintf(stderr, "GET KEY=%s\n",key.c_str());
	// Map iterator that will be searching for the key.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator it;
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);

	// struct utsname detect;
	// uname(&detect);

	if (buffer.empty()) {
		slog_debug("map is empty");
		return 0;
	}

	// Search for the address related to the key.
	it = buffer.find(key);
	// Check if the value did exist within the map.
	if (it == buffer.end())
	{
		slog_debug("%s not found in the map", key.c_str());
		// size_t len = strlen(key.c_str());
		// if (len > 0 && key.c_str()[len - 1] != '/') 
		// {
		// 	key += '/';
		// 	it = buffer.find(key);
		// }
		// if (it == buffer.end())
		// {
			// fprintf(stderr,"Nodename-%s NO EXIST=%s\n",detect.nodename, key.c_str());
			// fprintf(stderr,"NO EXIST=%s\n", key.c_str());
			return 0;
		// }
	}

	// fprintf(stderr,"GET-%s \n", key.c_str());
	// fprintf(stderr,"Nodename    - %s	GET-%s \n", detect.nodename, key.c_str());

	// Assign the values obtained to the provided references.
	// std::cout <<"Exist " << key << '\n';
	*(add_) = it->second.first;
	*(size_) = it->second.second;

	// Return the address associated to the record.
	return 1;
}

int32_t map_records::get_snapshot(std::string key, int *to_copy)
{

	// Map iterator that will be searching for the key.
	std::map<std::string, int>::iterator it;

	int value = 0;
	// Block the access to the map structure.
	// std::unique_lock<std::mutex> lock(*mut);

	if (buffer_snapshot.empty())
		return 0;

	// Search for the address related to the key.
	it = buffer_snapshot.find(key);
	// Check if the value did exist within the map.
	if (it == buffer_snapshot.end())
	{
		return 0;
	}
	*(to_copy) = it->second;
	// *(file_size) = it->second.second;

	// Return the value.
	return 1;
}

int32_t map_records::get_broadcast(std::string key, void **add_, uint64_t *size_)
{
	// Map iterator that will be searching for the key.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator it;
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);

	if (buffer_broadcast.empty())
		return 0;

	// Search for the address related to the key.
	it = buffer_broadcast.find(key);
	// Check if the value did exist within the map.
	if (it == buffer_broadcast.end())
	{
		return 0;
	}

	// Assign the values obtained to the provided references.
	*(add_) = it->second.first;
	*(size_) = it->second.second;

	return 1;
}

int32_t map_records::get_broadcast_size()
{
	return buffer_broadcast.size();
}

int32_t map_records::get_buffer_size()
{
	return buffer.size();
}

int32_t map_records::update(std::string key, void *add_, uint64_t length)
{

	// Map iterator that will be searching for the key.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator it;
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);

	if (buffer.empty())
		return 0;

	// Search for the address related to the key.
	it = buffer.find(key);
	// Check if the value did exist within the map.
	if (it == buffer.end())
	{
		return 0;
	}

	// Assign the values obtained to the provided references.
	it->second.first = add_;
	it->second.second = length;

	// Return the address associated to the record.
	return 1;
}

int32_t map_records::update_simple(std::string key, int value)
{
	// Map iterator that will be searching for the key.
	std::map<std::string, int>::iterator it;

	std::unique_lock<std::mutex> lock(*mut);

	if (buffer_snapshot.empty())
		return 0;

	// Search for the address related to the key.
	it = buffer_snapshot.find(key);
	// Check if the value did exist within the map.
	if (it == buffer_snapshot.end())
	{
		return 0;
	}

	// Assign the values obtained to the provided references.
	it->second = value;

	// Return the address associated to the record.
	return 1;
}

int32_t map_records::delete_metadata_stat_worker(std::string key)
{
	std::unique_lock<std::mutex> lock(*mut);
	int num_elements_erased = buffer.erase(key);
	if (num_elements_erased == 0)
	{
		//const char *last = key.c_str() + strlen(key.c_str()) - 1;
		//if (last[0] != '/')
		size_t len = strlen(key.c_str());
		if (len > 0 && key.c_str()[len - 1] != '/') 
		{
			key += '/';
			num_elements_erased = delete_metadata_stat_worker(key);
		}
	}

	return num_elements_erased;
}

/***
 * @brief Method renaming from stat_worker
 * @return 1 in case of success or 0 in case of error.
 */
int32_t map_records::rename_metadata_stat_worker(std::string old_key, std::string new_key)
{
	// Map iterator that will be searching for the key.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator it;
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);

	// Search for the address related to the key.
	it = buffer.find(old_key);
	// Check if the value did exist within the map.
	if (it == buffer.end())
	{
		// -1 if the key does not exist.
		return -1;
	}
	else
	{
		imss_info_ *data = (imss_info_ *)it->second.first;
		strcpy(data->uri_, new_key.c_str());
		// Unlinks the node that contains the element pointed to by position and returns a node handle that owns it.
		auto node = buffer.extract(old_key);
		// Update with the new key.
		node.key() = new_key;
		// Insert the node with the new key.
		buffer.insert(std::move(node));
	}

	// 1 if the key exist.
	return 1;
}

// Method renaming from srv_worker
int32_t map_records::rename_data_srv_worker(std::string old_key, std::string new_key)
{
	// Map iterator that will be searching for the key.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator it;
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);

	// save partners for later deletion and new insertion of news paths
	std::vector<string> vec;

	for (const auto &it : buffer)
	{
		string key = it.first;

		int pos = key.find('$');
		string path = key.substr(0, pos);
		string block = key.substr(pos, key.length() + 1);

		if (path.compare(old_key) == 0)
		{
			vec.insert(vec.begin(), key);
		}
	}

	std::vector<string>::iterator i;
	for (i = vec.begin(); i < vec.end(); i++)
	{
		auto item = buffer.find(*i);

		string key = *i;
		int pos = key.find('$');
		string block = key.substr(pos, key.length() + 1);

		string new_path = new_key + block;
		auto node = buffer.extract(key);
		node.key() = new_path;
		buffer.insert(std::move(node));
	}

	// Return the address associated to the record.
	return 1;
}

// Method renaming from srv_worker
int32_t map_records::rename_data_dir_srv_worker(std::string old_dir, std::string rdir_dest)
{
	// printf("rename data_dir_dir_srv_worker old_dir=%s dir_dest=%s\n",old_dir.c_str(), rdir_dest.c_str());
	// Map iterator that will be searching for the key.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator it;
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	std::vector<string> vec;

	struct utsname detect;
	uname(&detect);

	for (const auto &it : buffer)
	{
		string key = it.first;
		int found = key.find(old_dir);
		if (found != std::string::npos)
		{
			vec.insert(vec.begin(), key);

			key.erase(0, old_dir.length() - 1);

			string new_path = rdir_dest;
			new_path.append(key);
		}
	}

	std::vector<string>::iterator i;
	for (i = vec.begin(); i < vec.end(); i++)
	{
		string key = *i;
		// printf("Nodename    - %s	Rename modify original=%s\n",detect.nodename,key.c_str());
		key.erase(0, old_dir.length() - 1);

		string new_path = rdir_dest;
		new_path.append(key);

		auto node = buffer.extract(*i);
		// printf("Nodename    - %s	Rename new=%s\n",detect.nodename, new_path.c_str());
		node.key() = new_path;
		buffer.insert(std::move(node));
	}

	return 1;
}

// Method renaming from stat_worker
int32_t map_records::rename_metadata_dir_stat_worker(std::string old_dir, std::string rdir_dest)
{
	// printf("rename_metadata_dir_dir_stat_worker\n");
	// Map iterator that will be searching for the key.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator it;
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	std::vector<string> vec;

	for (const auto &it : buffer)
	{
		string key = it.first;
		int found = key.find(old_dir);
		if (found != std::string::npos)
		{
			vec.insert(vec.begin(), key);

			key.erase(0, old_dir.length() - 1);

			string new_path = rdir_dest;
			new_path.append(key);

			imss_info_ *data = (imss_info_ *)it.second.first;
			strcpy(data->uri_, new_path.c_str());
		}
	}

	// check if the vector is empty, meaning that the old key is not valid.
	if (vec.size() == 0)
	{
		return -1;
	}

	std::vector<string>::iterator i;
	for (i = vec.begin(); i < vec.end(); i++)
	{
		string key = *i;
		key.erase(0, old_dir.length() - 1);

		string new_path = rdir_dest;
		new_path.append(key);

		auto node = buffer.extract(*i);
		node.key() = new_path;
		buffer.insert(std::move(node));
	}

	return 1;
}

/**
 * Find and set corresponding buffer map memory blocks to be reused.
 */
int32_t map_records::cleaning()
{
	std::vector<string> vec;

	for (const auto &it : buffer)
	{
		string key = it.first;
		// Verify if this is a block 0.
		int found = key.find("$0");

		if (found != std::string::npos)
		{

			// comprobar la estructura st_ulink
			struct stat *st_p = (struct stat *)it.second.first;
			// std::cout << key << "stlink:" <<st_p->st_nlink<<'\n';
			if (st_p->st_nlink == 0)
			{
				// borrar todos los bloques con mismo path/key
				for (const auto &it2 : buffer)
				{
					string partner_key = it2.first;
					if (partner_key.compare(key) != 0)
					{ // para no borrar el actual con el que estoy comparando
						int pos = key.find('$');
						string path = key.substr(0, pos);

						int pos_partner = partner_key.find('$');
						string partner_path = partner_key.substr(0, pos_partner);

						int found_partner = partner_path.compare(path);
						if (found_partner == 0)
						{
							vec.insert(vec.begin(), partner_key);
						}
					}
				}
				vec.insert(vec.begin(), key);
			}
		}
	}

	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	std::vector<string>::iterator i;
	for (i = vec.begin(); i < vec.end(); i++)
	{
		// find the element on all the datasets map.
		auto item = buffer.find(*i);
		// push the memory pointer of this block inside the mem pool to be reused.
		StsQueue.push(mem_pool, item->second.first);
		// erase the dataset information from the map.
		buffer.erase(*i);
	}

	/*for(const auto & it : buffer){
	  string key = it.first;
	  std::cout <<"Garbage Collector: Exist " << key << '\n';
	  }*/
	return 0;
}

int32_t map_records::cleaning_specific(std::string new_key)
{
	std::unique_lock<std::mutex> lock(*mut);
	std::vector<string> vec;

	// borrar todos los bloques con mismo path/key
	for (const auto &it2 : buffer)
	{
		string partner_key = it2.first;

		int pos_partner = partner_key.find('$');
		string partner_path = partner_key.substr(0, pos_partner);

		int found_partner = partner_path.compare(new_key);
		if (found_partner == 0)
		{
			vec.insert(vec.begin(), partner_key);
		}
	}

	if (vec.size() == 0)
	{
		//const char *last = new_key.c_str() + strlen(new_key.c_str()) - 1;
		//if (last[0] != '/')
		size_t len = strlen(new_key.c_str());
		if (len > 0 && new_key.c_str()[len - 1] != '/') 
		{
			new_key += '/';
			cleaning_specific(new_key);
		}
		return -1;
	}

	// Block the access to the map structure.
	std::vector<string>::iterator i;
	for (i = vec.begin(); i != vec.end(); i++)
	{
		// std::cout << "Garbage Collector: Deleting " << *i << "\n";
		auto item = buffer.find(*i);
		// push the memory pointer of this block inside the mem pool to be reused.
		StsQueue.push(mem_pool, item->second.first);
		quantity_occupied = quantity_occupied - item->second.second;
		// fprintf(stderr, "quantity_occupied = %lu\n", quantity_occupied);
		// free(item->second.first); // free the memory of this block.
		// erase the dataset information from the map.
		// fprintf(stderr, "Erasing element with key %s\n", i);
		slog_debug("Erasing element with key %s", item->first.c_str());
		buffer.erase(*i);
		buffer_snapshot.erase(*i);
	}

	/*for(const auto & it : buffer){
	  string key = it.first;
	  std::cout <<"Garbage Collector: Exist " << key << '\n';
	  }*/
	return 0;
}

/**
 * Find and set corresponding buffer map memory blocks to be reused.
 */
int32_t map_records::freeAllMemory()
{
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	ssize_t free_memory_count = 0;
	for (const auto &it : buffer)
	{
		// fprintf(stderr,"Deleting %s\n", it.first.c_str());
		free(it.second.first);
		quantity_occupied = quantity_occupied - it.second.second;
		free_memory_count += it.second.second;
	}
	free_memory_count /= (1024 * 1024 * 1024); // Bytes to GiB.
	printf("Hercules has release %lu GB of memory\n", free_memory_count);
	buffer.clear();

	// std::vector<string>::iterator i;
	// for (i = vec.begin(); i < vec.end(); i++)
	// {
	// 	// find the element on all the datasets map.
	// 	auto item = buffer.find(*i);
	// 	// push the memory pointer of this block inside the mem pool to be reused.
	// 	StsQueue.push(mem_pool, item->second.first);
	// 	// erase the dataset information from the map.
	// 	buffer.erase(*i);
	// }

	/*for(const auto & it : buffer){
	  string key = it.first;
	  std::cout <<"Garbage Collector: Exist " << key << '\n';
	  }*/
	return 0;
}

// Function to extract the number after the '$' symbol
int extractNumber(const std::string &key)
{
	size_t pos = key.find('$');
	if (pos != std::string::npos)
	{
		return std::stoi(key.substr(pos + 1));
	}
	return -1;
}

/**
 * @brief Fetch all data related to a file name.
 */
char *map_records::GetDataOfFile(string file_name, uint64_t *file_size_occupied)
{
	std::vector<string> vec;
	// uint64_t file_size_occupied = 0;
	uint32_t extra_size = 0;
	int block_number = 0;

	for (const auto &it2 : buffer)
	{
		string partner_key = it2.first;

		int found = partner_key.find("$0");
		if (found != std::string::npos)
		{
			// skip block 0.
			slog_warn("Block with key %s has not been added to the collected data for file %s", partner_key.c_str(), file_name.c_str());
			continue;
		}

		int pos_partner = partner_key.find('$');
		string partner_path = partner_key.substr(0, pos_partner);
		slog_debug("partner_key=%s, partner_path=%s, file_name=%s", partner_key.c_str(), partner_path.c_str(), file_name.c_str());
		int found_partner = partner_path.compare(file_name);
		if (found_partner == 0)
		{
			vec.insert(vec.begin(), partner_key);
			*file_size_occupied = *file_size_occupied + it2.second.second;
			extra_size += sizeof(int);
		}
	}

	slog_debug("file_name=%s, file_size_occupied=%lu", file_name.c_str(), *file_size_occupied);

	if (*file_size_occupied <= 0)
	{
		slog_debug("There is not data for file %s", file_name.c_str());
		return NULL;
	}

	// std::sort(vec.begin(), vec.end());
	// sort elements by the block number.
	std::sort(vec.begin(), vec.end(), [](const std::string &a, const std::string &b)
			  { return extractNumber(a) < extractNumber(b); });

	// Sum the extra size for the block numbers represented as integers.
	*file_size_occupied += extra_size;

	char *chucks_of_file_buffer = (char *)malloc((*file_size_occupied + 1) * sizeof(char));
	char *aux_buf = chucks_of_file_buffer;
	char *address_ = NULL;
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	std::vector<string>::iterator i;
	uint64_t block_size_rtvd = 0;
	off_t total_written = 0;
	string data_uri, block_file_name;
	for (i = vec.begin(); i != vec.end(); i++)
	{
		// std::cout << "Garbage Collector: Deleting " << *i << "\n";
		auto item = buffer.find(*i);
		// strncpy(aux_buf, (const char *)item->second.first, item->second.second);
		getBlockInformation(item->first, &block_number, &data_uri, &block_file_name);

		block_size_rtvd = item->second.second;
		address_ = (char *)item->second.first;

		memcpy((char *)aux_buf + total_written, &block_number, sizeof(int));
		memcpy((char *)aux_buf + total_written + sizeof(int), address_, block_size_rtvd);

		total_written = total_written + block_size_rtvd + sizeof(int);
	}
	chucks_of_file_buffer[total_written] = '\0';
	return chucks_of_file_buffer;
}

// TODO: make a function to allow each server to write the collected data.

/**
 * @brief Merge all data comming from the data servers following a Posix format.
 * @param size_of_data: size of the data that has been merged. This pointer is updated with the accumulated size of data comming from all data servers.
 * @param num_of_data_servers: number of data servers that will merge their data.
 * @param file_size: size of the original file according to the block 0.
 * @param block_size: size of the block used by Hercules when this file was stored.
 */
char *map_records::MergeData(off_t *size_of_data, uint32_t num_of_data_servers, off_t file_size, uint64_t block_size)
{
	int32_t find = 0;
	uint64_t block_size_rtvd = 0;
	off_t total_written_acumulated = 0, total_written_per_server = 0;
	off_t block_offset = 0;
	char expected_key_format[PATH_MAX];
	char *reconstructed_data_file = NULL;

	if (file_size <= 0)
	{
		perror("HERCULES_ERR_MERGE_DATA_INVALID_FILE_SIZE");
		return NULL;
	}

	reconstructed_data_file = (char *)malloc(file_size * sizeof(char));
	if (reconstructed_data_file == NULL)
	{
		perror("HERCULES_ERR_RECONSTRUCTED_DATA_FILE_MEM_FAILED");
		slog_error("HERCULES_ERR_RECONSTRUCTED_DATA_FILE_MEM_FAILED");
		return NULL;
	}

	int32_t indx = 0;
	// We used a control flag to iterate over the incomming data on the broadcast map.
	// This loop will break when certain number of data servers that send its data has been reached.
	int flag = 1;
	while (flag)
	{
		char *buffer_address = NULL;
		uint64_t storage_buffer_size = 0;

		// std::string expected_key = expected_key_format;
		std::string expected_key;

		// if broadcast map is empty, we wait
		std::unique_lock<std::mutex> lk(mtx);
		cv.wait(lk, [this]
				{ return get_broadcast_size() > 0; });
		// get the first element of the broadcast map.
		auto firstElement = buffer_broadcast.begin();
		expected_key = firstElement->first;
		buffer_address = (char *)firstElement->second.first;
		storage_buffer_size = firstElement->second.second;

		slog_debug("key %s has been find, storage_buffer_size=%lu", expected_key.c_str(), storage_buffer_size);
		// iterate the buffer to get the blocks data.
		int block_number = -1;
		total_written_per_server = 0;
		off_t aux_counter = 0;
		while (aux_counter < storage_buffer_size)
		{
			// Each block number has been added to the data string.
			// TODO: block number should be a unsigned long integer.
			memcpy(&block_number, buffer_address, sizeof(int));
			block_offset = (block_number - 1) * block_size;

			// This helps to write the last block with the remaining data
			// preventing writing the entire block.
			if ((block_size + block_offset) > file_size)
			{
				block_size_rtvd = file_size - block_offset;
			}
			else
			{
				block_size_rtvd = block_size;
			}

			memcpy((char *)reconstructed_data_file + block_offset, buffer_address + sizeof(int), block_size_rtvd);
			// Move the pointer by the data size copied plus the int size (block number as integer).
			buffer_address = buffer_address + block_size_rtvd + sizeof(int);
			total_written_per_server += block_size_rtvd;
			aux_counter += block_size_rtvd + sizeof(int);
			slog_debug("block number=%d, block_offset=%d, block_size=%d, file_size=%ld, block_size_rtvd=%lu, total_written_per_server=%ld", block_number, block_offset, block_size, file_size, block_size_rtvd, total_written_per_server);
		}
		total_written_acumulated += total_written_per_server;
		slog_debug("total_written_per_server=%ld, total_written_acumulated=%ld, file_size=%ld", total_written_per_server, total_written_acumulated, file_size);
		total_written_per_server = 0;
		// erase the element from the broadcast map.
		find = erase_broadcast_element(expected_key);
		indx++;
		// if all data servers has sent its data, we break the loop.
		if (indx >= num_of_data_servers)
		{
			flag = 0;
		}
		if (find == 0)
		{
			slog_debug("key %s has not been deleted in broadcast", expected_key.c_str());
			continue;
		}
		else
		{
			slog_debug("key %s has been deleted in broadcast", expected_key.c_str());
		}
	}
	*size_of_data = total_written_acumulated;
	return reconstructed_data_file;
}

/**
 * @brief Copy all data stored in Hercules FOLLOWING a Posix format file.
 */
int32_t map_records::Snapshot(uint64_t block_size, const char *snapshot_dir, int finish, int server_id, char *data_hostname, struct arguments args)
{
	clock_t t;
	double parcial_time_taken = 0.0, time_taken_for_writting = 0.0, time_taken_for_merge = 0.0, time_taken_for_collecting = 0.0;
	int pos = 0, ret = 0, fd = -1, block_number = 0, continue_exe = 0;
	u_int32_t number_active_storage_servers = 0;
	string key, block, file_name, data_uri;
	char expected_uri[PATH_MAX];
	char expected_key_format[PATH_MAX + sizeof(int) + 1];
	void *address_ = NULL;
	void *address_block_0 = NULL;
	uint64_t block_size_rtvd = 0;
	int origin_server_id = 0;
	struct stat *stats = NULL;
	int32_t file_desc = 0;
	std::size_t found = 0;
	size_t iteration = 0;
	int is_shared_memory = 0;
	__off_t file_size = 0;

	char *POLICY = args.policy;
	const int64_t number_of_data_servers = args.num_data_servers;
	number_active_storage_servers = (u_int32_t)get_number_of_active_nodes(args.hercules_path);

	if (!strcmp(POLICY, "LOCAL") || !strcmp(POLICY, "ZCOPY"))
	{
		is_shared_memory = 1;
	}

	char *reconstructed_data_file = NULL;
	off_t block_offset = 0;

	// in order to avoid locks and syncronizations:
	// server 0 checks for a file to be snapshoting.
	// server 0 sends a signal to all servers to find and "reduce" all its
	// data about this file.
	// all servers sends its data to server 0.
	// Server 0 find and "reduce" all its data about the file.
	// Server 0 collects and re-structure all the data comming from others servers and write the final file.

	for (const auto &it : buffer_snapshot)
	{
		key = it.first;
		if (key.empty())
		{
			fprintf(stderr, "Key is missing\n");
			continue;
		}
		origin_server_id = it.second;

		pos = key.find('$') + 1; // +1 to skip '$' on the block number.
		if (pos == std::string::npos)
		{
			perror("HERCULES_ERR_MISSFORMAT_KEY");
			slog_error("HERCULES_ERR_MISSFORMAT_KEY");
			continue;
		}
		slog_debug("key=%s, origin_server_id=%d, iteration=%d", key.c_str(), origin_server_id, iteration);
		if (origin_server_id == -1)
		{
			block = key.substr(pos, key.length() + 1); // substract the block number from the key.
			if (block.empty())
			{
				fprintf(stderr, "Block number is missing in %s\n", key.c_str());
				continue;
			}
			block_number = stoi(block, 0, 10); //  string to number.
			pos -= 1;						   // -1 to skip '$' on the data uri.
			data_uri = key.substr(0, pos);	   // substract the data uri from the key.
			file_name = data_uri.substr(strlen("imss://"));
			sprintf(expected_uri, "imss://%s", file_name.c_str());

			// We need at least one iteartion to ensure "curr_dataset" is not
			// empty.
			if (iteration > 0)
			{
				// if the expected uri is different from the previous iteration,
				// we are in a block of another dataset. So, we need to get the
				// information of this dataset and to check it is ready to be
				// copied to disk or not.
				if (!strcmp(curr_dataset.uri_, expected_uri) && curr_dataset.n_open > 0)
				{
					slog_debug("current_dataset.uri=%s, expecred_uri=%s, n_open=%d", curr_dataset.uri_, expected_uri, curr_dataset.n_open);
					// Skip all blocks of this dataset because is not ready.
					continue;
				}
			}

			// To check if this is a block 0.
			found = key.find("$0");
			if (found != std::string::npos)
			{ // checks if block 0 is for regular file or directory.
				slog_debug("Block 0 for key %s", key.c_str());
				ret = get(key, &address_, &block_size_rtvd);
				if (ret == 0)
				{
					fprintf(stderr, "key %s not found for snapshot\n", key.c_str());
					continue;
				}
				if (!is_shared_memory)
				{
					stats = (struct stat *)address_;
				}
				else
				{ // get block 0 from shared memory.
					size_t memory_offset = 0;
					uint32_t stored_block_size = 0;
					sscanf((const char *)address_, "%lu %d", &memory_offset, &stored_block_size);
					fprintf(stderr, "memory offset=%lu, stored_block_size=%u\n", memory_offset, stored_block_size);
					stats = (struct stat *)((char *)args.pool_memory + memory_offset);
				}
				if (S_ISDIR(stats->st_mode)) // directory case.
				{
					fprintf(stderr, "%s is a directory\n", key.c_str());
					Make_directory(file_name.c_str());
				}
				// Send a message to all servers telling this servers needs the information.

				// Deletes all information related to this uri from the local arrays. it ensures the dataset information will be updated from the remote metadata server.
				clear_dataset(expected_uri);

				// To get dataset info from the metadata server. here
				// "curr_dataset" is filled.
				file_desc = open_dataset(expected_uri, 0);
				if (file_desc < 0)
				{
					continue;
				}
				iteration++;

				// Checks if there are still processes with the file opened.
				// Also, if "hercules stop" was called, we ignore this condition to copy the remaining data.
				// if (curr_dataset.n_open > 0 && finish != 1)
				slog_debug("curr_dataset.n_open=%d", curr_dataset.n_open);
				if (curr_dataset.n_open > 0)
				{
					fprintf(stderr, "Dataset %s is not ready, n_open=%d\n", curr_dataset.uri_, curr_dataset.n_open);
					slog_debug("Dataset %s is not ready, n_open=%d", curr_dataset.uri_, curr_dataset.n_open);
					continue;
				}

				// Send the message to all servers.
				char broadcast_request[PATH_MAX + 1024];
				sprintf(broadcast_request, "BROADCAST %s %d", expected_uri, args.id);
				SendBroadcastMessage(args.id, number_active_storage_servers, broadcast_request);

				file_size = stats->st_size;

				uint64_t file_size_occupied = 0;
				// This server add their data.
				fprintf(stderr, "Performing Snapshopt from file %s in data server %d\n", expected_uri, args.id);
				t = clock();
				char *data_ = GetDataOfFile(expected_uri, &file_size_occupied);
				if (data_ != NULL)
				{
					// TODO: add the error condition.
					sprintf(expected_key_format, "%s$%d", expected_uri, args.id);
					put_broadcast((string)expected_key_format, data_, file_size_occupied);
				}
				t = clock() - t;
				time_taken_for_collecting = ((double)t) / (CLOCKS_PER_SEC);

				off_t size_of_merge_data = 0;
				slog_debug("Merge data of file %s with size %ld", file_name.c_str(), file_size);
				t = clock();
				// Merge the data from all servers.
				char *full_data_from_file = MergeData(&size_of_merge_data, number_active_storage_servers, file_size, block_size);
				if (full_data_from_file == NULL)
				{
					fprintf(stderr, "Data from file %s has not been merge in server %d\n", file_name.c_str(), args.id);
					slog_error("HERCULES_ERR_MERGE_DATA_SNAPSHOT");
					perror("HERCULES_ERR_MERGE_DATA_SNAPSHOT");
					if (data_ != NULL)
					{
						free(data_);
					}
					continue;
				}
				t = clock() - t;
				time_taken_for_merge = ((double)t) / (CLOCKS_PER_SEC);

				t = clock();
				int fd = Open_file(snapshot_dir, data_hostname);
				slog_debug("writting %lu bytes to disk with the name %s", size_of_merge_data, data_hostname);
				ssize_t written_bytes_in_disk = Write_2_disk(fd, full_data_from_file, size_of_merge_data, 0);
				fprintf(stderr, "Writting %lu bytes to disk from %d servers with the name %s, written_bytes_in_disk=%zd\n", size_of_merge_data, number_active_storage_servers, data_hostname, written_bytes_in_disk);

				Close_file(fd);

				t = clock() - t;
				time_taken_for_writting = ((double)t) / (CLOCKS_PER_SEC);

				if (full_data_from_file != NULL)
				{
					free(full_data_from_file);
				}

				continue_exe = 1;

				slog_time("%d,%d,%s,%lu,%f,%f,%f,%f,%f,%f,%lu,%f,%f,%s",
						  number_active_storage_servers,
						  server_id,
						  data_hostname,
						  written_bytes_in_disk,
						  (double)written_bytes_in_disk / 1024 / 1024,
						  (double)written_bytes_in_disk / 1024 / 1024 / 1024,
						  time_taken_for_writting,
						  (double)written_bytes_in_disk / time_taken_for_writting,
						  (double)written_bytes_in_disk / time_taken_for_writting / 1024 / 1024,
						  (double)written_bytes_in_disk / time_taken_for_writting / 1024 / 1024 / 1024,
						  block_size,
						  time_taken_for_collecting,
						  time_taken_for_merge,
						  POLICY);

				int find = erase_snapshot_element(key);
				if (find)
				{
					slog_debug("Element %s has been deleted", key.c_str());
					break;
				}
				else
				{
					slog_warn("Element %s has NOT been deleted", key.c_str());
				}
			}
		}
		else
		{
			// Other blocks differents to 0 in this map
			// means that there are a server waiting for
			// the data.
			uint64_t file_size_occupied = 0;
			t = clock();
			char *data_ = GetDataOfFile(key, &file_size_occupied);
			if (data_ == NULL)
			{
				fprintf(stderr, "Data from file %s has not been get in server %d\n", key.c_str(), args.id);
				slog_error("HERCULES_ERR_GET_DATA_OF_SNAPSHOT");
				perror("HERCULES_ERR_GET_DATA_OF_SNAPSHOT");
				continue;
			}
			t = clock() - t;
			time_taken_for_collecting = ((double)t) / (CLOCKS_PER_SEC);

			// char key_[REQUEST_SIZE];
			int n_server_ = origin_server_id;
			// Deletes all information related to this uri from the local arrays. it ensures the dataset information will be updated from the remote metadata server.
			clear_dataset((char *)key.c_str());

			// To get dataset info from the metadata server. here
			// "curr_dataset" is filled.
			file_desc = open_dataset((char *)key.c_str(), 0);
			if (file_desc < 0)
			{
				continue;
			}

			// buffer_broadcast
			if (set_data_server_reduce(server_id, n_server_, data_, file_size_occupied, key.c_str()) < 0)
			{
				perror("HERCULES_ERR_SET_DATA_SERVER_REDUCE");
				slog_error("HERCULES_ERR_SET_DATA_SERVER_REDUCE");
				return -1;
			}

			slog_debug("Data sent to server %d", n_server_);

			continue_exe = 1;

			int find = erase_snapshot_element(key);
			if (find)
			{
				slog_debug("Element %s has been deleted", key.c_str());
				break;
			}
			else
			{
				slog_warn("Element %s has NOT been deleted", key.c_str());
			}
		}
	}
	slog_debug("Ending Snapshot");

	return continue_exe;
}

int32_t map_records::Checkpoint(uint64_t block_size, const char *checkpoint_dir, int finish, int server_id, char *data_hostname, struct arguments args)
{
	clock_t t;
	double parcial_time_taken = 0.0, time_taken_for_writting = 0.0, time_taken_for_merge = 0.0, time_taken_for_collecting = 0.0;
	int pos = 0, ret = 0, fd = -1, block_number = 0, continue_exe = 0;
	u_int32_t number_active_storage_servers = 0;
	string key, block, file_name, data_uri;
	char expected_uri[PATH_MAX];
	char expected_key_format[PATH_MAX + sizeof(int) + 1];
	void *address_ = NULL;
	void *address_block_0 = NULL;
	uint64_t block_size_rtvd = 0;
	int origin_server_id = 0;
	struct stat *stats = NULL;
	int32_t file_desc = 0;
	std::size_t found = 0;
	size_t iteration = 0;
	int is_shared_memory = 0;
	__off_t file_size = 0;

	char *POLICY = args.policy;
	const int64_t number_of_data_servers = args.num_data_servers;
	number_active_storage_servers = (u_int32_t)get_number_of_active_nodes(args.hercules_path);

	if (!strcmp(POLICY, "LOCAL") || !strcmp(POLICY, "ZCOPY"))
	{
		is_shared_memory = 1;
	}

	char *reconstructed_data_file = NULL;
	off_t block_offset = 0;

	// in order to avoid locks and syncronizations:
	// server 0 checks for a file to be snapshoting.
	// server 0 sends a signal to all servers to find and "reduce" all its
	// data about this file.
	// all servers sends its data to server 0.
	// Server 0 find and "reduce" all its data about the file.
	// Server 0 collects and re-structure all the data comming from others servers and write the final file.

	for (const auto &it : buffer_snapshot)
	{
		key = it.first;
		if (key.empty())
		{
			fprintf(stderr, "Key is missing\n");
			continue;
		}
		origin_server_id = it.second;

		pos = key.find('$') + 1; // +1 to skip '$' on the block number.
		if (pos == std::string::npos)
		{
			perror("HERCULES_ERR_MISSFORMAT_KEY");
			slog_error("HERCULES_ERR_MISSFORMAT_KEY");
			continue;
		}
		slog_debug("key=%s, origin_server_id=%d, iteration=%d", key.c_str(), origin_server_id, iteration);
		if (origin_server_id == -1)
		{
			block = key.substr(pos, key.length() + 1); // substract the block number from the key.
			if (block.empty())
			{
				fprintf(stderr, "Block number is missing in %s\n", key.c_str());
				continue;
			}
			block_number = stoi(block, 0, 10); //  string to number.
			pos -= 1;						   // -1 to skip '$' on the data uri.
			data_uri = key.substr(0, pos);	   // substract the data uri from the key.
			file_name = data_uri.substr(strlen("imss://"));
			sprintf(expected_uri, "imss://%s", file_name.c_str());

			// We need at least one iteartion to ensure "curr_dataset" is not
			// empty.
			if (iteration > 0)
			{
				// if the expected uri is different from the previous iteration,
				// we are in a block of another dataset. So, we need to get the
				// information of this dataset and to check it is ready to be
				// copied to disk or not.
				if (!strcmp(curr_dataset.uri_, expected_uri) && curr_dataset.n_open > 0)
				{
					slog_debug("current_dataset.uri=%s, expecred_uri=%s, n_open=%d", curr_dataset.uri_, expected_uri, curr_dataset.n_open);
					// Skip all blocks of this dataset because is not ready.
					continue;
				}
			}

			// To check if this is a block 0.
			found = key.find("$0");
			if (found != std::string::npos)
			{ // checks if block 0 is for regular file or directory.
				slog_debug("Block 0 for key %s", key.c_str());
				ret = get(key, &address_, &block_size_rtvd);
				if (ret == 0)
				{
					fprintf(stderr, "key %s not found for snapshot\n", key.c_str());
					continue;
				}
				if (!is_shared_memory)
				{
					stats = (struct stat *)address_;
				}
				else
				{ // get block 0 from shared memory.
					size_t memory_offset = 0;
					uint32_t stored_block_size = 0;
					sscanf((const char *)address_, "%lu %d", &memory_offset, &stored_block_size);
					fprintf(stderr, "memory offset=%lu, stored_block_size=%u\n", memory_offset, stored_block_size);
					stats = (struct stat *)((char *)args.pool_memory + memory_offset);
				}
				if (S_ISDIR(stats->st_mode)) // directory case.
				{
					fprintf(stderr, "%s is a directory\n", key.c_str());
					Make_directory(file_name.c_str());
				}
				// Send a message to all servers telling this servers needs the information.

				// Deletes all information related to this uri from the local arrays. it ensures the dataset information will be updated from the remote metadata server.
				clear_dataset(expected_uri);

				// To get dataset info from the metadata server. here
				// "curr_dataset" is filled.
				file_desc = open_dataset(expected_uri, 0);
				if (file_desc < 0)
				{
					continue;
				}
				iteration++;

				// Checks if there are still processes with the file opened.
				// Also, if "hercules stop" was called, we ignore this condition to copy the remaining data.
				// if (curr_dataset.n_open > 0 && finish != 1)
				slog_debug("curr_dataset.n_open=%d", curr_dataset.n_open);
				if (curr_dataset.n_open > 0)
				{
					fprintf(stderr, "Dataset %s is not ready, n_open=%d\n", curr_dataset.uri_, curr_dataset.n_open);
					slog_debug("Dataset %s is not ready, n_open=%d", curr_dataset.uri_, curr_dataset.n_open);
					continue;
				}

				// Send the message to all servers.
				char broadcast_request[PATH_MAX + 1024];
				sprintf(broadcast_request, "BROADCAST %s %d", expected_uri, args.id);
				SendBroadcastMessage(args.id, number_active_storage_servers, broadcast_request);

				file_size = stats->st_size;

				uint64_t file_size_occupied = 0;
				// This server add their data.
				fprintf(stderr, "Performing Snapshopt from file %s in data server %d\n", expected_uri, args.id);
				t = clock();
				char *data_ = GetDataOfFile(expected_uri, &file_size_occupied);
				if (data_ != NULL)
				{
					// TODO: add the error condition.
					sprintf(expected_key_format, "%s$%d", expected_uri, args.id);
					put_broadcast((string)expected_key_format, data_, file_size_occupied);
				}
				t = clock() - t;
				time_taken_for_collecting = ((double)t) / (CLOCKS_PER_SEC);

				off_t size_of_merge_data = 0;
				slog_debug("Merge data of file %s with size %ld", file_name.c_str(), file_size);
				t = clock();
				// Merge the data from all servers.
				char *full_data_from_file = MergeData(&size_of_merge_data, number_active_storage_servers, file_size, block_size);
				if (full_data_from_file == NULL)
				{
					fprintf(stderr, "Data from file %s has not been merge in server %d\n", file_name.c_str(), args.id);
					slog_error("HERCULES_ERR_MERGE_DATA_SNAPSHOT");
					perror("HERCULES_ERR_MERGE_DATA_SNAPSHOT");
					if (data_ != NULL)
					{
						free(data_);
					}
					continue;
				}
				t = clock() - t;
				time_taken_for_merge = ((double)t) / (CLOCKS_PER_SEC);

				t = clock();
				int fd = Open_file(checkpoint_dir, data_hostname);
				slog_debug("writting %lu bytes to disk with the name %s", size_of_merge_data, data_hostname);
				ssize_t written_bytes_in_disk = Write_2_disk(fd, full_data_from_file, size_of_merge_data, 0);
				fprintf(stderr, "Writting %lu bytes to disk from %d servers with the name %s, written_bytes_in_disk=%zd\n", size_of_merge_data, number_active_storage_servers, data_hostname, written_bytes_in_disk);

				Close_file(fd);

				t = clock() - t;
				time_taken_for_writting = ((double)t) / (CLOCKS_PER_SEC);

				if (full_data_from_file != NULL)
				{
					free(full_data_from_file);
				}

				continue_exe = 1;

				slog_time("%d,%d,%s,%lu,%f,%f,%f,%f,%f,%f,%lu,%f,%f,%s",
						  number_active_storage_servers,
						  server_id,
						  data_hostname,
						  written_bytes_in_disk,
						  (double)written_bytes_in_disk / 1024 / 1024,
						  (double)written_bytes_in_disk / 1024 / 1024 / 1024,
						  time_taken_for_writting,
						  (double)written_bytes_in_disk / time_taken_for_writting,
						  (double)written_bytes_in_disk / time_taken_for_writting / 1024 / 1024,
						  (double)written_bytes_in_disk / time_taken_for_writting / 1024 / 1024 / 1024,
						  block_size,
						  time_taken_for_collecting,
						  time_taken_for_merge,
						  POLICY);

				int find = erase_snapshot_element(key);
				if (find)
				{
					slog_debug("Element %s has been deleted", key.c_str());
					break;
				}
				else
				{
					slog_warn("Element %s has NOT been deleted", key.c_str());
				}
			}
		}
		else
		{
			// Other blocks differents to 0 in this map
			// means that there are a server waiting for
			// the data.
			uint64_t file_size_occupied = 0;
			t = clock();
			char *data_ = GetDataOfFile(key, &file_size_occupied);
			if (data_ == NULL)
			{
				fprintf(stderr, "Data from file %s has not been get in server %d\n", key.c_str(), args.id);
				slog_error("HERCULES_ERR_GET_DATA_OF_SNAPSHOT");
				perror("HERCULES_ERR_GET_DATA_OF_SNAPSHOT");
				continue;
			}
			t = clock() - t;
			time_taken_for_collecting = ((double)t) / (CLOCKS_PER_SEC);

			// char key_[REQUEST_SIZE];
			int n_server_ = origin_server_id;
			// Deletes all information related to this uri from the local arrays. it ensures the dataset information will be updated from the remote metadata server.
			clear_dataset((char *)key.c_str());

			// To get dataset info from the metadata server. here
			// "curr_dataset" is filled.
			file_desc = open_dataset((char *)key.c_str(), 0);
			if (file_desc < 0)
			{
				continue;
			}

			// buffer_broadcast
			if (set_data_server_reduce(server_id, n_server_, data_, file_size_occupied, key.c_str()) < 0)
			{
				perror("HERCULES_ERR_SET_DATA_SERVER_REDUCE");
				slog_error("HERCULES_ERR_SET_DATA_SERVER_REDUCE");
				return -1;
			}

			slog_debug("Data sent to server %d", n_server_);

			continue_exe = 1;

			int find = erase_snapshot_element(key);
			if (find)
			{
				slog_debug("Element %s has been deleted", key.c_str());
				break;
			}
			else
			{
				slog_warn("Element %s has NOT been deleted", key.c_str());
			}
		}
	}
	slog_debug("Ending Snapshot");

	return continue_exe;
}