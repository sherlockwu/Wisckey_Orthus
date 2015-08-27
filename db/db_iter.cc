// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/filename.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

#include <unistd.h>
#include "Queue.h"

namespace leveldb {

//ll: code; prefetch numbers
static const size_t kPrefetch = 32; 
static bool preworker_on_ = false; 
static pthread_t preworker[kPrefetch];
DBImpl *prefetch_db_ = NULL;
  
#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter: public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction {
    kForward,
    kReverse
  };

  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
         uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        rnd_(seed),
	//ll: code;
	prefetch_on_(false),
	next_time_(0),
	counter_(0),
        bytes_counter_(RandomPeriod()) {
    
    //ll: code; start prefetch workers for the first time 
    if (!preworker_on_) {
      preworker_on_ = true;
      prefetch_db_ = db_;
      //      fprintf(stdout, "Prefetch db name is: %s \n", prefetch_db_->dbname_.c_str());
      StartPreWorker();
    }
  }
  virtual ~DBIter() {
    delete iter_;

    fprintf(stdout, "next_time_ : %f seconds \n", next_time_ * 1e-6);
  }
  virtual bool Valid() const { return valid_; }
  virtual Slice key() const {
    assert(valid_);
    //ll: output
    //    fprintf(stdout, "\ndbiter key() \n");
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  virtual Slice value() const {
    assert(valid_);

    //ll: code; get the lsm value, read from vlog, construct a new value 
    Status s; 
    uint64_t vaddr;
    uint32_t vsize;
    //this is lsm value return ! 
    Slice lsm_value = (direction_ == kForward) ? iter_->value() : saved_value_;

    vaddr = DecodeFixed64(lsm_value.data());
    vsize = DecodeFixed32(lsm_value.data() + 8);
    
    //    fprintf(stdout, "key: %s, vaddr: %llu, vsize: %lu \n",
    //	    ExtractUserKey(iter_->key()).data(),
    //	    (unsigned long long)vaddr, (unsigned long)vsize); 
    
    //read the real value from vlog file 
    size_t n = static_cast<size_t>(vsize);
    Slice real_value;

    //now, just use a shared buffer for scan; assume one thread ! 
    //fixme: for multipthread, just allocate a buffer locally 
    char* buf = db_->Buffer(); 
    s = db_->ReadVlog(vaddr, n, &real_value, buf); 
    if (!s.ok()) {
      fprintf(stdout, "db_iter::value(): failed ! \n");
    }

    return real_value; 
  }
  virtual Status status() const {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  virtual void Next();
  virtual void Prev();
  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();

  //ll: code; 
  void StartPreWorker(); 

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  //ll: code; prefetch related functions
  bool NeedPrefetch(); 
  void PrefetchNext(); 

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  // Pick next gap with average value of config::kReadBytesPeriod.
  ssize_t RandomPeriod() {
    return rnd_.Uniform(2*config::kReadBytesPeriod);
  }

  DBImpl* db_;
  const Comparator* const user_comparator_;
  Iterator* const iter_;
  SequenceNumber const sequence_;

  Status status_;
  std::string saved_key_;     // == current key when direction_==kReverse
  std::string saved_value_;   // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;

  Random rnd_;
  ssize_t bytes_counter_;

  //ll: code; prefetch related variables
  bool prefetch_on_;
  ssize_t counter_; 

  uint64_t lsm_time_;
  uint64_t next_time_;

  // No copying allowed
  DBIter(const DBIter&);
  void operator=(const DBIter&);
};

inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key();
  ssize_t n = k.size() + iter_->value().size();
  bytes_counter_ -= n;
  while (bytes_counter_ < 0) {
    bytes_counter_ += RandomPeriod();
    db_->RecordReadSample(k);
  }
  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }

    // saved_key_ already contains the key to skip past.
  } else {
    // Store in saved_key_ the current key so we skip it below.
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
  }

  FindNextUserEntry(true, &saved_key_);

  //ll: code; trigger the prefetcher 
  if (!prefetch_on_ && NeedPrefetch()) {
    PrefetchNext();     
  }
}

void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          SaveKey(ikey.user_key, skip);
          skipping = true;
          break;
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
            // Entry hidden
          } else {
            valid_ = true;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false;
}

void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()),
                                    saved_key_) < 0) {
        break;
      }
    }
    direction_ = kReverse;

    //ll: code; reset prefetch counter to 0
    counter_ = 0; 
  }

  FindPrevUserEntry();
}

void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
          // We encountered a non-deleted value in entries for previous keys,
          break;
        }
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;
  }
}

void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(
      &saved_key_, ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  FindPrevUserEntry();
}

//ll: code;
#define MAXVALSIZE (512 * 1024)

struct prefetch_entry {
	uint64_t address;
	uint32_t size;
};

Queue<prefetch_entry> prefetch_q; 

// update the counter and check whether prefetch or not 
bool DBIter::NeedPrefetch() {
  counter_++; 
  if (counter_ >= kPrefetch) {
    counter_ = 0;
    return true; 
  }
  return false;
}

// prefetch next kPrefetch values from vlog file 
void DBIter::PrefetchNext() {

  const uint64_t start_micros = prefetch_db_->env_->NowMicros();

  prefetch_on_ = true;   
  std::string cur_key = ExtractUserKey(iter_->key()).ToString();

  //  fprintf(stdout, "PrefetchNext(): cur_key: %s\n", cur_key.data()); 

  //get addr/size, add them to queue
  for(int i = 0; i < kPrefetch * 2; i++) {
    Next(); 
    if (!Valid())
      break;

    uint64_t vaddr;
    uint32_t vsize;
    Slice lsm_value = (direction_ == kForward) ? iter_->value() : saved_value_;
    vaddr = DecodeFixed64(lsm_value.data());
    vsize = DecodeFixed32(lsm_value.data() + 8);

    //    fprintf(stdout, "PrefetchNext(): add address: %llu, size: %lu to queue \n",
    //	    (unsigned long long)vaddr, (unsigned long)vsize); 

    struct prefetch_entry entry;
    entry.address = vaddr;
    entry.size = vsize;

    prefetch_q.push(entry);    
  } 

  //seek back to the user's position 
  Seek(Slice(cur_key)); 
  prefetch_on_ = false; 

  next_time_ += prefetch_db_->env_->NowMicros() - start_micros; 
}


void *worker(void *arg) {
  char* buf = new char[MAXVALSIZE]; 
  Slice real_value;
  Status s;
  while (1) {
    //    fprintf(stdout, "preworker waiting, db name: %s \n", prefetch_db_->dbname_.c_str());
    struct prefetch_entry entry = prefetch_q.pop();
    assert(entry.size < MAXVALSIZE);
    //    fprintf(stdout, "preworker %lu: read from addr: %llu, size: %lu \n", 
    //	    pthread_self(), (unsigned long long)entry.address, (unsigned long)entry.size);    
    s = prefetch_db_->ReadVlog(entry.address, entry.size, &real_value, buf); 
    assert(s.ok());
  }
}

void DBIter::StartPreWorker() {
  for (int i = 0; i < kPrefetch; i++) {
    int ret = pthread_create(&preworker[i], NULL, worker, NULL);
    assert(ret == 0);
  }
}

}  // anonymous namespace

Iterator* NewDBIterator(
    DBImpl* db,
    const Comparator* user_key_comparator,
    Iterator* internal_iter,
    SequenceNumber sequence,
    uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

}  // namespace leveldb
