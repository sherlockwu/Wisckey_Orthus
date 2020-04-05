// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/vlog_writer.h"

#include <stdint.h>
//ll: for sleep
#include <unistd.h>
#include <time.h>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "db/dbformat.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
// copy from write_batch.cc 
static const size_t kHeader = 12;

// flush values in batch fashion 
static const size_t kBatch = 300*1024;

namespace vlog {

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

Writer::~Writer() {

  time_t start = time(NULL);
  Sync();
  time_t end = time(NULL);

  fprintf(stdout, "vlog writer fsync: %.3f \n", difftime(end, start));

  //before close vlog file, flush sb to vlog file 
  WriteVlogSB(false);
  dest_->Sync();
}

void Writer::Sync() {

  //flush vlog file user buffer
  if (values.size() > 0) {
    Slice value_slice(values); 
    Status s = dest_->Append(value_slice);
    if (s.ok()) {
      s = dest_->Flush();
    }
    values.clear();   
  }

  dest_->Sync();
}

void Writer::SetVlogSB(const char* scratch) {
  //mcpy(&sb_, scratch, sizeof(SuperBlock));
  sb_ = *(struct SuperBlock *)scratch; 
}

//write an sb to vlog file 
void Writer::WriteVlogSB(bool isnew) {
  
  if (isnew) {
    //initial sb for a new vlog file 
    sb_.first = sizeof(SuperBlock);
    sb_.last = (uint64_t)MAX_VLOG_SIZE;
    sb_.free = sb_.last - sb_.first; 
    sb_.head = sb_.tail = sb_.first;
  } 

  Status s; 
  std::string data; 
  data.append((char*)&sb_, sizeof(SuperBlock)); 
  Slice slice(data); 

  //seek to the begin of vlog file, write the superblock 
  dest_->SeekToOffset(0);
  s = dest_->Append(slice);
  if (s.ok()) {
    s = dest_->Flush();
  } else {
    fprintf(stdout, "WriteVlogSB(): write sb failed ! \n"); 
  }
  //seek back to head for appending 
  dest_->SeekToOffset(sb_.head);
  /*
  fprintf(stdout, "WriteVlogSB(): head: %llu, tail: %llu \n",
	  (unsigned long long)sb_.head,
	  (unsigned long long)sb_.tail); 
  */
}

uint64_t Writer::GetHead() {
  return sb_.head;
}
  
uint64_t Writer::GetTail() {
  return sb_.tail;
}

void Writer::SetTail(uint64_t addr) {
  sb_.tail = addr;
}

uint64_t Writer::MaxVlogSize() {
  return (sb_.last - sb_.first); 
}

//return whether vlog needs gc now
bool Writer::NeedGC() {
  //threshold based policy now 
  uint64_t used_thre = GC_THRESHOLD * (sb_.last - sb_.first);
  uint64_t used = (sb_.head - sb_.tail);
  return (used >= used_thre) ? true : false; 
}

bool Writer::WaitForSpace() {
  if (sb_.head - sb_.tail >= MaxVlogSize())
    return true;
  else 
    return false; 
}

Status Writer::PunchHole(uint64_t off, uint64_t len){
  return dest_->PunchHole(off, len);
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

  Status s;
  Slice key, value, new_value; 
  uint64_t offset; 
  uint64_t vaddr;
  uint32_t ksize, vsize; 
  int found = 0;

  offset = sb_.head; 
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {

	  //later, can use varintlength() and store varint for ksize/vsize ! 
	  //vlog format: (ksize, vsize, key, value)
	  ksize = static_cast<uint32_t>(key.size());
	  vsize = static_cast<uint32_t>(value.size());
	  
	  //Kan: padding something, to make sure: the (end offset - start offset) / 4096 (aligned up) = size / 4096 (aligned up)
	  int total_size = 8 + ksize + vsize;
	  //std::cout << "Start from " << offset / 4096 << ", end at " << (offset + total_size - 1) /4096 << std::endl;
	  if ((offset + total_size -1) / 4096 - offset / 4096 + 1 > (total_size + 4095) / 4096) {
	    //std::cout << "Start from " << offset << ", end at " << offset + total_size - 1 << ", hence padding" << std::endl;
	    uint32_t padding_bytes = 4096 - offset % 4096;
	    // padding values
	    values.append(padding_bytes, 'X'); 
	    offset += padding_bytes;
	  }
	  
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

          //std::cout << "AddRecord to " << offset << std::endl; 
	  //new (key, addr_size) for a new writebatch; key/new_value copied 
	  kUpdates->Put(key, new_value); 
	  offset += 8 + ksize + vsize;
	  //update vlog superblock; lock ??? 
	  sb_.head = offset;
	  sb_.free -= (8 + ksize + vsize);
	 
	  /*
          fprintf(stdout, "ksize: %lu, key: %.16s, vsize: %lu, vaddr: %llu, offset: %llu \n", 
		  (unsigned long)ksize, key.data(), (unsigned long)vsize, 
		  (unsigned long long)vaddr, (unsigned long long)offset);  
	  fflush(stdout);
          sleep(3); 
	  */
	  
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;

      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
	  //for delete, just keep the original kType and key
	  //no need to write anything for values 
	  
	  kUpdates->Delete(key);
	  //	  fprintf(stdout, "delete key: %s ! \n", key.data()); 

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

  //when size over kBatch, flush to vlog file 
  if (values.size() >= kBatch) {
    Slice value_slice(values); 
    s = dest_->Append(value_slice);
    if (s.ok()) {
      s = dest_->Flush();
    }
    //    fprintf(stdout, "flush a batch, size: %zu \n", values.size()); 
    values.clear();   
  } else {
    //    fprintf(stdout, "no flush, size: %zu \n", values.size()); 
  }
  
  //fixme: update vlog superblock; ??? 

  return s;
}

Status Writer::ReadCache(uint64_t offset, size_t n, Slice* result, char* scratch) const {
	unsigned long long buffer_end = sb_.head - 1;
	unsigned long long buffer_start = sb_.head - values.size();
	if (offset >= buffer_start && offset <= buffer_end) {
		if (n > buffer_end - offset + 1) {
			// This situation will only happen if a crash resulted
			// in key-offset being inserted in the LSM, but not in
			// the Vlog.
			n = buffer_end - offset + 1;
		}
		memcpy(scratch, values.data() + (offset - buffer_start), n);
		*result = Slice(scratch, n);
		return Status::OK();
	} else {
		assert(offset + n - 1 < buffer_start);
		return Status::NotFound(Slice());
	}
}



}  // namespace log
}  // namespace leveldb


