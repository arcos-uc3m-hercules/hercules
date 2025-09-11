#include <map>
#include <iostream>
#include <vector>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <memory>

extern uint64_t IMSS_BLKSIZE;
#define KB 1024
#define GB 1073741824

std::mutex mtx;
struct PrefetchItem
{
  int32_t first_block;
  int32_t last_block;
  // char *buf;
  std::shared_ptr<std::string> buf;
};

using std::string;
typedef std::map<std::string, struct PrefetchItem> MapPrefetch;

extern "C"
{

  void *map_create_prefetch()
  {
    return reinterpret_cast<void *>(new MapPrefetch);
  }

  char *map_get_buffer_prefetch(void *map, char *path, int *first_block, int *last_block)
  {
    // critical section (exclusive access to std::cout signaled by lifetime of lck):
    std::unique_lock<std::mutex> lck(mtx);
    MapPrefetch *m = reinterpret_cast<MapPrefetch *>(map);

    auto search = m->find(std::string(path));

    if (search != m->end())
    {
      *(first_block) = search->second.first_block;
      *(last_block) = search->second.last_block;
      return (char *)search->second.buf->c_str();
    }
    return NULL;
  }

  void map_update_prefetch(void *map, char *path, int first_block, int last_block)
  {
    // critical section (exclusive access to std::cout signaled by lifetime of lck):
    std::unique_lock<std::mutex> lck(mtx);
    MapPrefetch *m = reinterpret_cast<MapPrefetch *>(map);

    auto search = m->find(std::string(path));
    if (search != m->end())
    {
      search->second.first_block = first_block;
      search->second.last_block = last_block;
    }
  }

  void map_release_prefetch(void *map, const char *path)
  {
    // critical section (exclusive access to std::cout signaled by lifetime of lck):
    std::unique_lock<std::mutex> lck(mtx);
    MapPrefetch *m = reinterpret_cast<MapPrefetch *>(map);
    auto search = m->find(std::string(path));

    if (search != m->end())
    {
      // free(search->second.buf);
      search->second.buf.reset();
    }

    m->erase(path);
  }

  // void map_init_prefetch(void *map, const char *path, char *buff)
  void map_init_prefetch(void *map, const char *path, long int buf_size)
  {
    // critical section (exclusive access to std::cout signaled by lifetime of lck):
    std::unique_lock<std::mutex> lck(mtx);
    MapPrefetch *m = reinterpret_cast<MapPrefetch *>(map);

    struct PrefetchItem pitem;
    pitem.first_block = 0;
    pitem.last_block = 0;
    pitem.buf = std::make_shared<std::string>(buf_size, '\0');
    // pitem.buf->reserve(buf_size);
    // pitem.buf = buff;

    m->insert(std::pair<std::string, struct PrefetchItem>(std::string(path), pitem));
  }

  int map_rename_prefetch(void *map, const char *oldname, const char *newname)
  {
    // critical section (exclusive access to std::cout signaled by lifetime of lck):
    std::unique_lock<std::mutex> lck(mtx);
    MapPrefetch *m = reinterpret_cast<MapPrefetch *>(map);
    auto node = m->extract(oldname);
    node.key() = newname;
    m->insert(std::move(node));

    return 1;
  }

  int map_rename_dir_dir_prefetch(void *map, const char *old_dir, const char *rdir_dest)
  {
    // critical section (exclusive access to std::cout signaled by lifetime of lck):
    std::unique_lock<std::mutex> lck(mtx);
    MapPrefetch *m = reinterpret_cast<MapPrefetch *>(map);

    std::vector<string> vec;

    for (auto it = m->cbegin(); it != m->cend(); ++it)
    {
      string key = it->first;
      int found = key.find(old_dir);
      if (found != std::string::npos)
      {
        vec.insert(vec.begin(), key);
      }
    }

    std::vector<string>::iterator i;
    for (i = vec.begin(); i < vec.end(); i++)
    {
      string key = *i;
      key.erase(0, strlen(old_dir) - 1);

      string new_path = rdir_dest;
      new_path.append(key);
      auto node = m->extract(*i);
      node.key() = new_path;
      m->insert(std::move(node));
    }

    return 1;
  }

  int free_prefetch(void *map)
  {
    // Block the access to the map structure.
    std::unique_lock<std::mutex> lock(mtx);

    MapPrefetch *m = reinterpret_cast<MapPrefetch *>(map);
    // ssize_t free_memory_count = 0;

    // for (auto it = m->cbegin(); it != m->cend(); ++it)
    // {
    //   it->second.buf.reset();
    // }

    
    // free_memory_count /= (1024 * 1024 * 1024); // Bytes to GiB.
    // printf("Hercules has release %lu GB of memory\n", free_memory_count);

    m->clear();

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

} // extern "C"
