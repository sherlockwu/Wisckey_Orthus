// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <atomic>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"
namespace leveldb {

Cache::~Cache() {
}

namespace {
//Kan: for buckets
bool print_cache_behavior = false;
std::atomic<uint32_t> cache_access(0);
std::atomic<uint32_t> cache_miss(0);

typedef struct bucket_struct_ {
  uint32_t bucket_id;
  uint32_t usage_;
  uint32_t gen_;     // to identify whether the map from key to this bucket is up to date
} bucket_struct;


typedef struct object_location_ {
  // for hash table
  struct object_location_* next_hash;
  size_t key_length;
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  
  // for bucket addressing
  uint32_t bucket_;
  uint32_t in_bucket_offset_;
  uint32_t length_;     // to identify whether the map from key to this bucket is up to date
  uint32_t gen_;     // to identify whether the map from key to this bucket is up to date
  
  
  char key_data[1];   // Beginning of key
  
  
  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    //if (next == this) {
    //  return *(reinterpret_cast<Slice*>(value));
    //} else {
    return Slice(key_data, key_length);
    //}
  }
} object_location;

// LRU cache implementation

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {
  void* value;    // point to a persist cache bucket structure if it's a persist buffer
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  size_t key_length;
  uint32_t refs;
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  char key_data[1];   // Beginning of key

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != NULL) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// Kan: for bucket use
class ObjectTable {
 public:
  ObjectTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~ObjectTable() { delete[] list_; }

  object_location* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  object_location* Insert(object_location* h) {
    object_location** ptr = FindPointer(h->key(), h->hash);
    object_location* old = *ptr;
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  object_location* Remove(const Slice& key, uint32_t hash) {
    object_location** ptr = FindPointer(key, hash);
    object_location* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  object_location** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  object_location** FindPointer(const Slice& key, uint32_t hash) {
    object_location** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    object_location** new_list = new object_location*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      object_location* h = list_[i];
      while (h != NULL) {
        object_location* next = h->next_hash;
        uint32_t hash = h->hash;
        object_location** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};


// A single shard of sharded cache.
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { 
    capacity_ = capacity; 
    
  }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);

  // Kan: For buckets related
  size_t capacity_buckets_;
  size_t usage_buckets_;
  int shard_fd_;
  uint64_t bucket_size = 64 * 1024;    // Bucket size in KB
  
  //LRUHandle* buffer_bucket = NULL;
  LRUHandle* buffer_bucket_lsm = NULL;
  LRUHandle* buffer_bucket_vlog = NULL;
  
  LRUHandle* bucket_handles = NULL;  // array of LRU_handles for every bucket
  ObjectTable object_table_;                // map object(eg. a LSM block, a vlog page) key to the object_location struct(bucket, gen, in bucket offset)
  
  Cache::Handle* BucketInsert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* BucketLookup(const Slice& key, uint32_t hash, void * scratch);
 
 
  void SetBackedFile(int fd_to_set) { 
    shard_fd_ = fd_to_set;
    // Kan: to set for persist cache
    capacity_buckets_ = capacity_ / bucket_size;
    usage_buckets_ = 0;
    bucket_handles = (LRUHandle *)malloc(capacity_buckets_ * sizeof(LRUHandle)); 
  }
 
 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  void Unref(LRUHandle* e);


  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  port::Mutex mutex_;
  size_t usage_;
  
  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  LRUHandle lru_;

  HandleTable table_;
};

LRUCache::LRUCache()
    : usage_(0) {
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

LRUCache::~LRUCache() {
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->refs == 1);  // Error if caller has an unreleased handle
    Unref(e);
    e = next;
  }
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs <= 0) {
    usage_ -= e->charge;
    (*e->deleter)(e->key(), e->value);
    free(e);
  } 
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* e) {
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != NULL) {
    e->refs++;
    LRU_Remove(e);
    LRU_Append(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);

  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 2;  // One from LRUCache, one for the returned handle
  memcpy(e->key_data, key.data(), key.size());
  LRU_Append(e);
  usage_ += charge;

  LRUHandle* old = table_.Insert(e);
  if (old != NULL) {
    LRU_Remove(old);
    Unref(old);
  }
  //std::cout << this << " Insert " << charge << ", used: " << usage_  << ", capacity: " << capacity_ << std::endl; 
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    LRU_Remove(old);
    //std::cout << "This is to evict " << old->refs << std::endl;
    table_.Remove(old->key(), old->hash);
    Unref(old);
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Remove(key, hash);
  if (e != NULL) {
    LRU_Remove(e);
    Unref(e);
  }
}
  

Cache::Handle* LRUCache::BucketLookup(const Slice& key, uint32_t hash, void * scratch) {
  MutexLock l(&mutex_);
  // 1. check the key to bucket map
  object_location* e = object_table_.Lookup(key, hash);
  if (e == NULL) {
    return NULL;
  } 
  
  LRUHandle* to_check_bucket = &bucket_handles[e->bucket_];
  bucket_struct * bucket_info = (bucket_struct *)(to_check_bucket->value);
  
  uint32_t bucket_id, in_bucket_offset, object_len, object_gen;
  bucket_id = e->bucket_;
  in_bucket_offset = e->in_bucket_offset_;
  object_len = e->length_;
  object_gen = e->gen_;
  
  // check the generation of this bucket
  if (e->gen_ != bucket_info->gen_) {
    if (print_cache_behavior)
      std::cout << "    == cache item's bucket was evicted, (bucket_id, in_bucket_offset, gen) " << bucket_id << ":" << in_bucket_offset << ":" << object_gen <<  "\n";
    // delete this map
    object_table_.Remove(e->key(), e->hash);
    return NULL;
  } 
  
  if (print_cache_behavior) 
    std::cout << "    == found the map in object_table, (bucket_id, in_bucket_offset, gen) " << bucket_id << ":" << in_bucket_offset << ":" << object_gen <<  "\n";
  
  // 2. move the bucket to the LRU list head
  to_check_bucket->refs++;
  LRU_Remove(to_check_bucket);
  LRU_Append(to_check_bucket);
  
  // 3. read data from the bucket
  //std::cout << "Read from cache " << shard_fd_ << ", " << bucket_id << " : " << in_bucket_offset << ", " << bucket_id * bucket_size + in_bucket_offset << " : " << object_len << std::endl;
  ssize_t r = pread(shard_fd_, scratch, object_len, bucket_id * bucket_size + in_bucket_offset);
  if (r != object_len) {
    std::cout << "pread from cache file failed!\n";
    exit(1);
  }

  
  return reinterpret_cast<Cache::Handle*>(to_check_bucket);
}

Cache::Handle* LRUCache::BucketInsert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);

  if (charge > bucket_size) {
    std::cout << "Inserting a huge value!" << std::endl;
    exit(1);
  }

  // find the correct bucket to hold this data
  bucket_struct * bucket_info;

  
  LRUHandle** buffer_bucket;
  if (key.size() == 16) {
    //std::cout << "The key size is 16, use lsm bucket\n";
    buffer_bucket = &buffer_bucket_lsm;
  } else {
    //std::cout << "The key size is " << key.size() << ", use vlog bucket\n";
    buffer_bucket = &buffer_bucket_vlog;
  }


  if (*buffer_bucket != NULL) {
    bucket_info = (bucket_struct *)((*buffer_bucket)->value);
    if (bucket_info->usage_ + charge > bucket_size) {
      if (print_cache_behavior)
        std::cout << "    The previous buffer bucket is fully utilized: " << bucket_info->usage_ << std::endl;
      *buffer_bucket = NULL;
    } 
  }

  if (*buffer_bucket == NULL) {
    if (print_cache_behavior) 
      std::cout << "    == Need to find a buffer_bucket \n";
    //check if this shard is full
    if (usage_buckets_ < capacity_buckets_) {
      // set the new buffer_bucket
      bucket_info = (bucket_struct *) malloc(sizeof(bucket_struct));
      bucket_info->bucket_id = usage_buckets_++;
      bucket_info->usage_ = 0;
      bucket_info->gen_ = 0;
      
      *buffer_bucket = &(bucket_handles[bucket_info->bucket_id]);
      (*buffer_bucket)->value = (void *)bucket_info;
      (*buffer_bucket)->deleter = deleter;
      (*buffer_bucket)->refs = 2;  // One from LRUCache, one for the returned handle
      LRU_Append(*buffer_bucket);
      if (print_cache_behavior)
        std::cout << "      shard not fully utilized, find bucket " << bucket_info->bucket_id << " to buffer\n";
    } else {
      //if full, reuse a bucket
      if (lru_.next == &lru_) {
        //std::cout << "Get some cannot fixed error\n";
        return NULL;
      }
      LRUHandle* to_reuse = lru_.next;
      to_reuse->refs++;
      LRU_Remove(to_reuse);
      LRU_Append(to_reuse);
      *buffer_bucket = to_reuse;
      bucket_info = (bucket_struct *)((*buffer_bucket)->value);
      bucket_info->usage_ = 0;
      bucket_info->gen_ += 1;
      if (print_cache_behavior)
        std::cout << "      reuse a bucket " << bucket_info->bucket_id << " to buffer\n";
    }
  } else {
    //buffer_bucket->refs++; // TODO?
    LRU_Remove(*buffer_bucket);
    LRU_Append(*buffer_bucket);
    if (print_cache_behavior)
      std::cout << "    Has a buffer bucket, inserting into " << ((bucket_struct *)((*buffer_bucket)->value))->bucket_id << " \n";
  }

  // map key to the (bucket, gen, in-bucket offset, length)
  object_location* e = reinterpret_cast<object_location*>(
      malloc(sizeof(object_location)-1 + key.size()));
  e->key_length = key.size();
  e->hash = hash;
  memcpy(e->key_data, key.data(), key.size());
  
  e->bucket_ = bucket_info->bucket_id;
  e->in_bucket_offset_ = bucket_info->usage_;
  bucket_info->usage_ += charge;
  e->length_ = charge;
  e->gen_ = bucket_info->gen_;
  
  object_table_.Insert(e);
  if (print_cache_behavior)
    std::cout << "    == Creat a map from key to bucket, (bucket_id, in_bucket_offset, gen) " << e->bucket_ << ":" << e->in_bucket_offset_ << ":" << e->gen_ << " \n";
  
  // write the data to the backed_file
  //std::cout << "Write to cache " << shard_fd_ << ", " << e->bucket_ << " : " << e->in_bucket_offset_ << ", " << e->bucket_ * bucket_size + e->in_bucket_offset_ << " : " << charge << std::endl;
  ssize_t r = pwrite(shard_fd_, value, charge, e->bucket_ * bucket_size + e->in_bucket_offset_);
  if (r != charge) {
    std::cout << "pwrite to cache file failed!\n";
    exit(1);
  }

  return reinterpret_cast<Cache::Handle*>(*buffer_bucket);
}

static const int kNumShardBits = 4;
//static const int kNumShardBits = 1;   // TODO Kan: just for test
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Insert(const Slice& key, uint64_t charge, char * scratch) {}; // this is for persist cache
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual Handle* Lookup(const Slice& key, char * scratch) {}; // this is for persistent cache
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
};

static void DeleteNothing(const Slice& key, void* value) {
}

class ShardedBucketLRUCache : public Cache {

 private:
  LRUCache shard_[kNumShards];
  port::Mutex id_mutex_;
  uint64_t last_id_;
 

	  
  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }


  // Kan: for persist cache
  int fds_[kNumShards];                // Backed fds for each shard
 
 
 public:
  explicit ShardedBucketLRUCache(size_t capacity)
      : last_id_(0) {
    
    // set the capacity for each shard
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }

    // set backed cache file for each shard
    for (int s = 0; s < kNumShards; s++) {
      // open backed file
      std::string cache_file_name = "/mnt/optane/cache_dir/file_" + std::to_string(s);
      std::cout << "shard " << s << " is going to set a cached file with " << cache_file_name << std::endl;
      fds_[s] = open(cache_file_name.c_str(), O_RDWR | O_CREAT, 0);
      int td = ftruncate(fds_[s], per_shard + 1024*1024);
      if (fds_[s]<0 || td<0) {
        std::cout << "unable to create file" << fds_[s] << " : " << td << std::endl;
        exit(1);
      }

      shard_[s].SetBackedFile(fds_[s]);
    }
    
  }
  virtual ~ShardedBucketLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Insert(const Slice& key, uint64_t charge, char * scratch) {
    const uint32_t hash = HashSlice(key);
    if (print_cache_behavior)
      std::cout << "  == BucketInsert to shard " << Shard(hash) << ", " << charge << "\n";
    Handle * bucket_handle = shard_[Shard(hash)].BucketInsert(key, hash, scratch, charge, &DeleteNothing);
    return bucket_handle;
  }
  virtual Handle* Lookup(const Slice& key, char *scratch) {
    const uint32_t hash = HashSlice(key);
    if (print_cache_behavior)
      std::cout << "  == BucketLookup in shard " << Shard(hash) << "\n";
    int bucket = Shard(hash);
    //Handle * bucket_handle = shard_[Shard(hash)].BucketLookup(key, hash, scratch);
    Handle * bucket_handle = shard_[bucket].BucketLookup(key, hash, scratch);
    
    // Update cache statistics
    cache_access += 1;
    if (bucket_handle == NULL) {
      cache_miss += 1;
    }

    if (bucket == 0 && cache_access > 1000000) {
      std::cout << "Cache access: " << cache_access << ", miss: " << cache_miss << ", miss ratio: " << (double) cache_miss / cache_access * 100 << "\n";
      cache_access = 0;
      cache_miss = 0;
    }
    return bucket_handle;
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
};


}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

Cache* NewPersistLRUCache(size_t capacity) {
  std::cout << "========== here is to NewPersistLRUCache\n";
  return new ShardedBucketLRUCache(capacity);
}

}  // namespace leveldb
