// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/vlog_writer.h"

#include <stdint.h>
//ll: for sleep
#include <unistd.h>

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
      cur_offset_(0),
      block_offset_(0) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

Writer::~Writer() {
}

void Writer::SetOffset(uint64_t offset) {
  cur_offset_ = offset; 
}

//given the key/value slice, write values to vlog, generate 
//new (key, addr/size) for keys in LSM, kUpdates 
Status Writer::AddRecord(const Slice& slice, WriteBatch* kUpdates) {

  Slice input(slice); 
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  int count = DecodeFixed32(input.data() + 8);
  input.remove_prefix(kHeader);

  Slice key, value, new_value; 
  uint64_t offset; 
  std::string values; 
  uint64_t vaddr;
  uint32_t ksize, vsize; 
  int found = 0;

  offset = cur_offset_; 
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {

	  //vlog format: (ksize, vsize, key, value)
	  ksize = static_cast<uint32_t>(key.size());
	  vsize = static_cast<uint32_t>(value.size());
	  PutFixed32(&values, ksize);
	  PutFixed32(&values, vsize);
	  values.append(key.data(), key.size());
	  values.append(value.data(), value.size());
	  
	  //addr_size string contains addr and size of value in vlog 
	  std::string addr_size; 
	  vaddr = offset + 8 + key.size(); 
	  PutFixed64(&addr_size, vaddr);
	  PutFixed32(&addr_size, vsize);
	  new_value = Slice(addr_size);

	  //new (key, addr_size) for a new writebatch; key/new_value copied 
	  kUpdates->Put(key, new_value); 
	  offset += 8 + ksize + vsize;
	  /*
          fprintf(stdout, "ksize: %lu, key: %.16s, vsize: %lu, vaddr: %llu, offset: %llu \n", 
		  (unsigned long)ksize, key.data(), (unsigned long)vsize, 
		  (unsigned long long)vaddr, (unsigned long long)offset);  
          sleep(5); 
	  */
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;

      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
	  //for delete, just keep the original kType and key
	  //no need to write anything for values 
	  
	  fprintf(stdout, "deletion record ! \n"); 
	  kUpdates->Delete(key);

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
  Status s;

//ll: my output 
//  fprintf(stdout, "vlog file cur_offset_: %llu \n", (unsigned long long)cur_offset_);
//  sleep(5); 

  s = dest_->Append(value_slice);
  if (s.ok()) {
    s = dest_->Flush();
  }

  cur_offset_ += value_slice.size();
  return s;
}

}  // namespace log
}  // namespace leveldb


