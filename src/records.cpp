#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <iostream>
#include <cassert>
#include <string.h>
#include <stdio.h>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include "records.hpp"
#include "imss.h"
#include "queue.h"

using std::make_pair;
using std::map;
using std::pair;
using std::string;

extern StsHeader *mem_pool;

extern int32_t __thread current_dataset;   // Dataset whose policy has been set last.
extern dataset_info __thread curr_dataset; // Currently managed dataset.
extern imss __thread curr_imss;

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

// Method retrieving the address associated to a certain record.
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
		return -1;
	}

	// struct utsname detect;
	// uname(&detect);
	// DPRINT("Nodename    - %s add in map=%s\n", detect.nodename, key.c_str());
	// fprintf(stderr, "key=%s, quantity=%ld total size=%ld\n", key.c_str(), quantity_occupied, total_size);
	quantity_occupied = quantity_occupied + length;
	buffer.insert({key, value});
	return 0;
}

// Method storing a new record.
int32_t map_records::put_simple(std::string key, int value)
{
	// Construct a pair object storing the couple of values associated to a key.
	// std::pair<void *, uint64_t> value(to_copy, 0); // second param is for file size.
	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	// Add a new value to the map.
	// fprintf(stderr, "Inserting %s with value %d\n", key.c_str(), value);
	buffer_checkpoint.insert({key, value});
	return 0;
}

// Method retrieving the address associated to a certain record.
int32_t map_records::get(std::string key, void **add_, uint64_t *size_)
{

	// fprintf(stderr, "GET KEY=%s\n",key.c_str());
	// Map iterator that will be searching for the key.
	std::map<std::string, std::pair<void *, uint64_t>>::iterator it;
	// Block the access to the map structure.
	// std::unique_lock<std::mutex> lock(*mut);

	// struct utsname detect;
	// uname(&detect);

	if (buffer.empty())
		return 0;

	// Search for the address related to the key.
	it = buffer.find(key);
	// Check if the value did exist within the map.
	if (it == buffer.end())
	{
		// fprintf(stderr,"Nodename-%s NO EXIST=%s\n",detect.nodename, key.c_str());
		// fprintf(stderr,"NO EXIST=%s\n", key.c_str());
		return 0;
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

int32_t map_records::get_simple(std::string key, uint64_t *to_copy)
{

	// Map iterator that will be searching for the key.
	std::map<std::string, int>::iterator it;
	// std::map<std::string, std::pair<uint64_t, uint64_t>>::iterator it;

	int value = 0;
	// Block the access to the map structure.
	// std::unique_lock<std::mutex> lock(*mut);

	if (buffer_checkpoint.empty())
		return 0;

	// Search for the address related to the key.
	it = buffer_checkpoint.find(key);
	// Check if the value did exist within the map.
	if (it == buffer_checkpoint.end())
	{
		return 0;
	}
	*(to_copy) = it->second;
	// *(file_size) = it->second.second;

	// Return the value.
	return 1;
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

	if (buffer_checkpoint.empty())
		return 0;

	// Search for the address related to the key.
	it = buffer_checkpoint.find(key);
	// Check if the value did exist within the map.
	if (it == buffer_checkpoint.end())
	{
		return 0;
	}

	// Assign the values obtained to the provided references.
	it->second = value;

	// Return the address associated to the record.
	return 1;
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
		// 0 if the key does not exist.
		return 0;
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

	// Block the access to the map structure.
	std::unique_lock<std::mutex> lock(*mut);
	std::vector<string>::iterator i;
	for (i = vec.begin(); i != vec.end(); i++)
	{
		// std::cout << "Garbage Collector: Deleting " << *i << "\n";
		auto item = buffer.find(*i);
		// push the memory pointer of this block inside the mem pool to be reused.
		StsQueue.push(mem_pool, item->second.first);
		quantity_occupied = quantity_occupied - item->second.second;
		// fprintf(stderr, "quantity_occupied = %lu\n", quantity_occupied);
		// free(item->second.first);
		// erase the dataset information from the map.
		// fprintf(stderr, "Erasing element with key %s\n", i);
		buffer.erase(*i);
		buffer_checkpoint.erase(*i);
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

// Used in str_worker threads.
int32_t map_records::memory2disk(uint64_t block_size, const char *checkpoint_dir, int finish, int server_id, char *data_hostname)
{
	clock_t t;
	double parcial_time_taken = 0.0, total_time_taken = 0.0;
	int pos = 0, ret = 0, fd = -1, block_number = 0, skip = 0, number_active_storage_servers = 0;
	size_t offset = 0;
	string key, inner_key, block, file_name, inner_file_name, data_uri;
	char key_block_0[PATH_MAX];
	char expected_uri[PATH_MAX];
	char old_expected_uri[PATH_MAX];
	void *address_ = NULL;
	void *address_block_0 = NULL;
	uint64_t copy_to_disk = 0, block_size_rtvd = 0, block_0_size = 0, total_written = 0;
	struct stat *stats = NULL;
	int32_t file_desc = 0;
	std::size_t found = 0;
	size_t iteration = 0;
	int to_free = 0;
	__off_t file_size = 0;

	if (!finish)
	{
		return 0;
	}

	// fprintf(stderr, "Buffer size = %lu, number of block in primary map=%lu\n", buffer_checkpoint.size(), buffer.size());
	for (const auto &it : buffer_checkpoint)
	{
		key = it.first;
		// fprintf(stderr, "key=%s\n", key.c_str());
		if (key.empty())
		{
			fprintf(stderr, "Key is missing\n");
			continue;
		}
		copy_to_disk = it.second;

		pos = key.find('$') + 1; // +1 to skip '$' on the block number.
		if (pos == std::string::npos)
		{
			perror("HERCULES_ERR_MISSFORMAT_KEY");
			slog_error("HERCULES_ERR_MISSFORMAT_KEY");
			continue;
		}

		if (copy_to_disk == 1)
		{
			block = key.substr(pos, key.length() + 1); // substract the block number from the key.
			if (block.empty())
			{
				fprintf(stderr, "Block number is missing in %s\n", key.c_str());
				continue;
			}
			// block_number = std::stoi(block);
			block_number = stoi(block, 0, 10); //  string to number.
			pos -= 1;						   // -1 to skip '$' on the data uri.
			data_uri = key.substr(0, pos);	   // substract the data uri from the key.
			// fprintf(stderr,"data_uri=%s\n", data_uri.c_str());
			file_name = data_uri.substr(strlen("imss://"));
			sprintf(expected_uri, "imss://%s", file_name.c_str());

			// We need at least one iteartion to ensure "curr_dataset" is not
			// empty.
			if (iteration > 0)
				// if the expected uri is different from the previous iteration,
				// we are in a block of another dataset. So, we need to get the
				// information of this dataset and to check it is ready to be
				// copied to disk or not.
				if (!strcmp(curr_dataset.uri_, expected_uri) && curr_dataset.n_open > 0)
				{
					// Skip all blocks of this dataset because is not ready.
					continue;
				}

			found = key.find("$0");
			if (found != std::string::npos)
			{ // checks if block 0 is for regular file or directory.
				ret = get(key, &address_, &block_size_rtvd);
				if (ret == 0)
				{
					fprintf(stderr, "key %s not found for checkpointing\n", key.c_str());
					continue;
				}
				stats = (struct stat *)address_;
				if (S_ISDIR(stats->st_mode)) // directory case.
				{
					fprintf(stderr, "%s is a directory\n", key.c_str());
					Make_directory(file_name.c_str());
					// set "copy_to_disk" to 0.
					buffer_checkpoint[key] = 0;
				}
				continue;
			}

			// deletes all information related to this uri from the local
			// arrays. it ensures the dataset information will be updated from
			// the remote metadata server.
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
			// Also, if "hercules stop" was called, we ignore this condition to
			// copy the remaining data.
			if (curr_dataset.n_open > 0 && finish != 1)
			{
				fprintf(stderr, "Dataset %s is not ready, n_open=%d\n", curr_dataset.uri_, curr_dataset.n_open);
				slog_debug("Dataset %s is not ready, n_open=%d", curr_dataset.uri_, curr_dataset.n_open);
				continue;
			}

			ret = get(key, &address_, &block_size_rtvd);
			if (ret == 0)
			{
				fprintf(stderr, "key %s not found for checkpointing\n", key.c_str());
				continue;
			}

			// Checks if the file was opened.
			std::map<std::string, std::pair<int, __off_t>>::iterator it_fd;
			it_fd = buffer_fd.find(file_name);
			if (it_fd == buffer_fd.end())
			{
				// Open the file if it does not exists.
				fd = Open_file(checkpoint_dir, file_name.c_str());
				if (fd < 0)
				{
					// On error, we skip this block.
					continue;
				}
				else
				{
					// On success, the fd is inserted on the buffer_fd map.
					// We also try to get block 0 from local map to know the
					// file size. If the block is not here we make a request to
					// the corresponding data server.
					sprintf(key_block_0, "%s$0", data_uri.c_str());
					to_free = 0;
					ret = get(key_block_0, &address_block_0, &block_0_size);
					if (ret == 0)
					{ // block 0 is not on the local map.
						address_block_0 = (void *)malloc(block_size * sizeof(char));
						ret = get_ndata(file_desc, 0, address_block_0, 0, 0);
						to_free = 1;
					}

					if (ret < 0)
					{
						char err_msg[MAX_ERR_MSG_LEN];
						sprintf(err_msg, "HERCULES_ERR_GET_NDATA_CHECKPOINT: %s", key_block_0);
						perror(err_msg);
						slog_error("HERCULES_ERR_GET_NDATA_CHECKPOINT: %s", err_msg);
						// return -1;
						// On error, we set the file size as 0
						// indicating we were not able to retrive
						// the correct file size, for example due a
						// network fail.
						file_size = 0;
					}
					stats = (struct stat *)address_block_0;
					if (stats == NULL)
					{
						perror("HERCULES_ERR_BLOCK_0_GET_ERROR");
						slog_error("HERCULES_ERR_BLOCK_0_GET_ERROR");
						if (to_free)
							free(address_block_0);
						continue;
					}
					file_size = stats->st_size;
					buffer_fd.insert({file_name, {fd, file_size}});
				}
			}
			else
			{
				fd = it_fd->second.first;
				file_size = it_fd->second.second;
			}

			offset = block_size * (block_number - 1); // -1 due block 0.
			if (file_size != 0)
			{
				// This helps to write files smaller than the block size with
				// the corresponding size.
				if (file_size < block_size && file_size != 0)
				{
					block_size_rtvd = file_size;
				}

				// This helps to write the last block with the remaining data
				// preventing writing the entire block.
				if (block_size_rtvd + offset > file_size)
				{
					block_size_rtvd = file_size - offset;
				}
			}

			// fprintf(stderr, "key.c_str(): %s, block_size_rtvd=%lu, offset=%lu, stats->st_size=%lu\n", key.c_str(), block_size_rtvd, offset, stats->st_size);
			slog_debug("key.c_str(): %s, block_size_rtvd=%lu, offset=%lu, file_size=%lu", key.c_str(), block_size_rtvd, offset, file_size);
			t = clock();
			block_size_rtvd = Write_2_disk(fd, address_, block_size_rtvd, offset);
			t = clock() - t;
			parcial_time_taken = ((double)t) / (CLOCKS_PER_SEC);
			total_time_taken += parcial_time_taken;
			if (block_size_rtvd == -1)
			{
				continue;
			}
			total_written += block_size_rtvd;
			// set "copy_to_disk" to 0 to prevent this block to be copy to disk
			// twice.
			buffer_checkpoint[key] = 0;

			if (to_free)
			{
				free(address_block_0);
				to_free = 0;
			}
		}
	}
	// Close all file descriptors.
	for (const auto &it : buffer_fd)
	{
		fd = it.second.first;
		Close_file(fd);
	}
	// To remove all the elements from the file descriptors map.
	if (buffer_fd.size() > 0)
	{
		buffer_fd.clear();
	}

	if (total_time_taken > 0)
	{
		slog_time("ServerID,%d,Hostname,%s,Total-writen,%lu B,%f MB,%f GB,Time-taken,%f s,Troughput,%f B/s,%f MB/s,%f GB/s,Blocksize,%lu KB",
				  server_id,
				  data_hostname,
				  total_written,
				  (double)total_written / 1024 / 1024,
				  (double)total_written / 1024 / 1024 / 1024,
				  total_time_taken,
				  (double)total_written / total_time_taken,
				  (double)total_written / total_time_taken / 1024 / 1024,
				  (double)total_written / total_time_taken / 1024 / 1024 / 1024,
				  block_size);
	}

	return total_time_taken;
}