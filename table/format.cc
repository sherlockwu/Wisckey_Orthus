// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/random.h"
#include "leveldb/cache.h"


#include <bits/stdc++.h>
namespace leveldb {

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
#ifndef NDEBUG
  const size_t original_size = dst->size();
#endif
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
}

Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

Status ReadBlock(RandomAccessFile* file,
                 const ReadOptions& options,
                 const BlockHandle& handle,
                 BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kBlockTrailerSize];
  Slice contents;

  //ll: output
  //  fprintf(stdout, "read size: %zu \n", n);

  Status s;
  Cache::Handle* cache_handle = NULL;
  Cache* persist_block_cache = (Cache *) (file->persist_block_cache);

  //std::cout << "== ReadBlock: " << file->persist_cache_id << " " << handle.offset() << " " << n + kBlockTrailerSize<< "\n";
  //if (file->backed_file != NULL && (fastrand()%100) < 0) {
    //s = (file->backed_file)->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
 
  // TODO how to handle very large requests 
  if (true && (file->persist_block_cache != NULL) && (file->persist_cache_id != -1) && n+kBlockTrailerSize < 8000) {
    // creat the key for cache lookup
    char cache_key_buffer[16];    // perhaps we could use 16 vs 24 to identify it's value or LSM page
    EncodeFixed64(cache_key_buffer, file->persist_cache_id);     // how to get the cache_id ???????
    EncodeFixed64(cache_key_buffer+8, handle.offset());
    
    Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      
    //look up the cache
    
    cache_handle = persist_block_cache->Lookup(key, buf);
    if (cache_handle == NULL) {
      // read the pages from flash
      s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
        /*
	std::cout << "Insert cache for " << file->persist_cache_id << " " << handle.offset() << " " << n + kBlockTrailerSize <<  "\n"; 
        for (int i = 0; i < 30; i++) {
          int val = int(buf[i]); 
          std::cout << val << " " ;
	}
        std::cout << "\n"; 
        */
      // TODO decide whether to admit
      if (true) {
        // insert the pages into the cache 
        cache_handle = persist_block_cache->Insert(key, n + kBlockTrailerSize, buf);
      }
    } else {
      contents = Slice(buf, n + kBlockTrailerSize);
      /*
      char* buf_to_check = new char[n + kBlockTrailerSize];
      s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf_to_check);
      
      if (strcmp(buf, buf_to_check) != 0) {
        std::cout << "The data from flash is different from what is cached! " << file->persist_cache_id << " " << handle.offset() << "\n";
        for (int i = 0; i < n + kBlockTrailerSize; i++) {
          if (buf[i] != buf_to_check[i]) {
	    std::cout << i << " ";
	  } 
        }
        std::cout << "\n";
        for (int i = 0; i < 30; i++) {
	  std::cout << int(buf[i]) << " ";
	}
        std::cout << "\n";
        for (int i = 0; i < 30; i++) {
	  std::cout << int(buf_to_check[i]) << " ";
	}
        std::cout << "\n";
	//exit(1);
      }
      std::cout << "The data from flash is same as what is cached! " << handle.offset() << "\n";
      */
    }
    
    //s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  } else {
    s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
    
    /*std::cout << "Read from file " << file->persist_cache_id << " " << handle.offset() << " " << n + kBlockTrailerSize << "\n";
    //for (int i = 0; i < n + kBlockTrailerSize - 10; i+=4) {
        //std::cout << int(buf[i]) << " ";
    //    std::cout << *((int*)(&(buf[i]))) << " ";
    //}
    std::cout << "\n";*/
  }
  if (!s.ok()) {
    std::cout << "Didn't find this page!\n";
    exit(1);
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    std::cout << "contents is not correct!\n";
    exit(1);
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents.data();    // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      std::cout << "checksum mismatch\n";
      exit(1);
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      if (data != buf) {
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] buf;
        result->data = Slice(data, n);
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
      } else {
        result->data = Slice(buf, n);
        result->heap_allocated = true;
        result->cachable = true;
      }

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      std::cout << "bad block type\n";
      exit(1);
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

}  // namespace leveldb
