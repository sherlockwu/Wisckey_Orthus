// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VLOG_WRITER_H_
#define STORAGE_LEVELDB_DB_VLOG_WRITER_H_

#include <stdint.h>
#include "db/vlog_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

#include "leveldb/write_batch.h"

#include "db/dbformat.h"

//#define ALWAYSON_GC
#define MAX_VLOG_SIZE 400*1024*1024*1024
#ifdef ALWAYSON_GC
#define GC_THRESHOLD 0.01
#else
#define GC_THRESHOLD 0.9
#endif
#define GC_CHUNK_SIZE 4*1024*1024
#define GC_CHUNK_NUM 256 

namespace leveldb {

class WritableFile;

namespace vlog {

class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(WritableFile* dest);
  ~Writer();

  Status AddRecord(const Slice& slice, WriteBatch* batch);

  void SetVlogSB(const char* scratch); // set sb with buffer reading from vlog 
  void WriteVlogSB(bool isnew); // write an initial sb to vlog file 
  uint64_t GetHead();
  uint64_t GetTail();
  void SetTail(uint64_t tail); 
 
  uint64_t MaxVlogSize(); 
  bool NeedGC(); 
  bool WaitForSpace();
  Status PunchHole(uint64_t off, uint64_t len);
  void Sync(); 
  Status ReadCache(uint64_t offset, size_t n, Slice* result, char* scratch) const;
 private:
  WritableFile* dest_;
  SuperBlock sb_; 

  std::string values; // write buffer for values 

  int block_offset_;       // Current offset in block

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];

  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  // No copying allowed
  Writer(const Writer&);
  void operator=(const Writer&);
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VLOG_WRITER_H_
