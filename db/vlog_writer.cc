// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/vlog_writer.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "db/dbformat.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
// copy from write_batch.cc 
static const size_t kHeader = 12;

namespace vlog {

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0),
      cur_offset_(0){
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

Writer::~Writer() {
}

//ll: given the key/value slice, write values to vlog, generate 
//new (key, addr/size) for keys in LSM, kUpdates 
Status Writer::AddRecord(const Slice& slice, WriteBatch* kUpdates) {

  Slice input(slice); 
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  int count = DecodeFixed32(input.data() + 8);
  input.remove_prefix(kHeader);

  Slice key, value, new_value; 
  std::string values; 
  uint64_t vaddr;
  uint32_t vsize; 

  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {

	  //handler->Put(key, value);

	  //later, can pack two together as one uint64_t 
	  std::string addr_size; 
	  vaddr = cur_offset_; 
	  vsize = static_cast<uint32_t>(value.size());

	  //addr_size string contains addr and size of value in vlog 
	  PutFixed64(&addr_size, vaddr);
	  PutFixed32(&addr_size, vsize);
	  new_value = Slice(addr_size);
	  //new (key, addr_size) for a new writebatch; key/new_value copied 
	  kUpdates->Put(key, new_value); 

	  //copy value to a new string; fixme 
	  values.append(value.data(), value.size()); 
	  cur_offset_ += value.size();
	  
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
	  //          handler->Delete(key);
	  //ll: fixme; handle later; 
	  //kUpdates->Delete(key, new_value); 

        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }

  if (found != count) {
    return Status::Corruption("WriteBatch has wrong count");
  } 
  
  //write value_slice to vlog file 
  Slice value_slice(values); 
  const char* ptr = value_slice.data();
  size_t left = value_slice.size();

  //  fprintf(stdout, "value_slice size: %zu\n", left);

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);

    //    fprintf(stdout, "emitphysicalrecord size: %zu\n", fragment_length); 

    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);

  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, n));
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + n;
  return s;
}

}  // namespace log
}  // namespace leveldb
