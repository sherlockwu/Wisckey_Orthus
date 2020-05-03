// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include <chrono>
//ll: for sleep 
#include <unistd.h>
#include <sstream> 
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
//ll: code; 
#include "db/vlog_writer.h"

#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

//ll: code; global variable for table time statistics 
struct Table_Time table_time_;

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

// Kan: for monitoring load of optane SSD
uint64_t perf_counter()
{
  uint32_t lo, hi;
  // take time stamp counter, rdtscp does serialize by itself, and is much cheaper than using CPUID
  //"rdtscp" : "=a"(lo), "=d"(hi)
  __asm__ __volatile__ (
      "rdtscp" : "=a"(lo), "=d"(hi)
      :: "%rcx");
  return ((uint64_t)lo) | (((uint64_t)hi) << 32);
}


void parse_counters(char * string_to_parse, std::vector<uint64_t> & parsed_elements) {
  std::string s;
  std::stringstream ss(string_to_parse);
  
  parsed_elements.clear();
  while (getline(ss, s, ' ')) {
    if (s.size() == 0)
      continue;
    
    parsed_elements.push_back(std::stoll(s));
  } 
  return;
}

void * monitor_func(void *vargp) {
  // create a monitor file on Optane SSD to detect
  int fd_optane = open("/sys/block/nvme1n1/stat", O_RDONLY);
  if (fd_optane < 0) {
    std::cout << "unable to open file" << fd_optane << " " << errno << std::endl;
    exit(1);
  }
  
  int fd_flash = open("/sys/block/nvme0n1/stat", O_RDONLY);
  if (fd_flash < 0) {
    std::cout << "unable to open file" << fd_flash << " " << errno << std::endl;
    exit(1);
  }
  
  char * optane_buf = (char *) malloc(sizeof(char) * 1024);
  char * flash_buf = (char *) malloc(sizeof(char) * 1024);
  
  
  int ret;

  std::vector<uint64_t> stats_optane, stats_flash, last_stats_optane, last_stats_flash;
  float last_throughput, detected_throughput, optane_read_throughput, optane_write_throughput, flash_read_throughput;
  int last_action, this_action;
  
  int steps = 0;
  // init state
  last_throughput = -1;
  last_action = -5;
  load_admit_ratio = 100;
  data_admit_ratio = 100;

  while(true) {
    if (flag_monitor) {
      
      steps += 1;

      //monitor the load of Optane SSD and Flash SSD
      ret = pread(fd_optane, optane_buf, 1024, 0);
      assert(ret > 0);
      ret = pread(fd_flash, flash_buf, 1024, 0);
      assert(ret > 0);
      
      parse_counters(optane_buf, stats_optane);
      parse_counters(flash_buf, stats_flash);

      if (last_stats_flash.size()!=15) {  // first second
        last_stats_flash = stats_flash;
        last_stats_optane = stats_optane;
        continue;
      }
      
      int optane_ticks, flash_ticks;
      optane_ticks = stats_optane[9] - last_stats_optane[9];
      flash_ticks = stats_flash[9] - last_stats_flash[9];

      // detected Optane and Flash throughput

      detected_throughput = 0.0;
      if (optane_ticks > 0) {
        detected_throughput += (stats_optane[2] - last_stats_optane[2]) / (2.0 * optane_ticks);
      }	
      if (flash_ticks > 0) {
        detected_throughput += (stats_flash[2] - last_stats_flash[2]) / (2.0 * flash_ticks);
      }
      
      if (steps % 50 == 0) {
        std::cout << "  Optane read throughput: " << (stats_optane[2] - last_stats_optane[2]) / (2.0 * optane_ticks) << ";";
        std::cout << "  Optane write throughput: " << (stats_optane[6] - last_stats_optane[6]) / (2.0 * optane_ticks) << ";";
        std::cout << "  Flash read throughput: " << (stats_flash[2] - last_stats_flash[2]) / (2.0 * flash_ticks) << ";";
        std::cout << "  Overall throughput observed: " << detected_throughput << "\n";
      }

      //TODO reset the cache scheduler
      //if (optane_ticks == 0 || (stats_optane[2] - last_stats_optane[2]) / (2.0 * optane_ticks) < 0.5 * 2500) {
      if (optane_ticks == 0) {
        load_admit_ratio = 100;
        data_admit_ratio = 100;
        goto next_loop;
      }

      // compare to last throughput
      if (detected_throughput > last_throughput) {
        this_action = last_action;
      } else {
        this_action = -1 * last_action;
      }
      
      if (this_action > 0) {
        if (flag_tune_load_admit && load_admit_ratio < 100) {
	  load_admit_ratio = (load_admit_ratio + this_action > 100)?100:load_admit_ratio + this_action;
	} else {
	  data_admit_ratio = (data_admit_ratio + this_action > 100)?100:data_admit_ratio + this_action;
	}
      } else {
        if (data_admit_ratio > 0) {
	  data_admit_ratio = (data_admit_ratio + this_action < 0)?0:data_admit_ratio + this_action;
	} else if (flag_tune_load_admit) {
	  load_admit_ratio = (load_admit_ratio + this_action < 0)?0:load_admit_ratio + this_action;
	}
      }
	      
     next_loop:
      last_stats_flash = stats_flash;
      last_stats_optane = stats_optane;
      last_throughput = detected_throughput;      
      last_action = this_action;
      if (steps % 50 == 0) {
        std::cout << "  Data admit ratio: " << data_admit_ratio << " Load admit ratio: " << load_admit_ratio << "\n";
      } 
      usleep(10000);
    } else {
      usleep(1000000);
    }
  }
  return NULL;
}


DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(new MemTable(internal_comparator_)),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      //ll: code; init my variables 
      vlog_write_(NULL),
      vlog_(NULL),
      vlog_read_(NULL),
      vlog_gc_read_(NULL),
      bg_gc_cv_(&mutex_),
      bg_gc_scheduled_(false),
      log_time_(0),
      mem_time_(0),
      wait_time_(0),
      vlog_time_(0),
      seed_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      manual_compaction_(NULL) {
  mem_->Ref();
  has_imm_.Release_Store(NULL);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_);

  //ll: code; init read buffer
  buf_ = new char[512*1024]; 

  //by default, not write to lsm log 
  write_lsm_log_ = false;


  //Kan: to use the persist block cache for vlog
  persist_block_cache = options_.persist_block_cache;
  //persist_block_cache = options_.persist_vlog_cache;
  if (persist_block_cache != NULL) {
    vlog_cache_id = persist_block_cache->NewId();
  }
  
  // TODO start a thread to monitor the load of Optane SSD
  pthread_t monitor_thread; 
  pthread_create(&monitor_thread, NULL, monitor_func, NULL);

  //usleep(1000000000000);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  //ll: code; wait gc finish
  mutex_.Lock();
  while (bg_gc_scheduled_) {
    fprintf(stdout, "watiting gc quit ! \n");
    fflush(stdout); 
    bg_gc_cv_.Wait();
  }
  fprintf(stdout, "gc quits ! \n");
  mutex_.Unlock();

  //if no lsm log write
  if (!write_lsm_log_) {
    //flush imm_ first 
    if (imm_ != NULL) {
      fprintf(stdout, "flush imm_ at end \n");
      CompactMemTableNoLog();
    }
    //then flush mem_ 
    if (mem_ != NULL && mem_->ApproximateMemoryUsage() > 0) {
      imm_ = mem_;
      mem_ = NULL; 
      has_imm_.Release_Store(imm_);
      fprintf(stdout, "flush mem_ at end \n");
      CompactMemTableNoLog();
    }    
  }


  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;

  //ll: code; 
  delete vlog_; 
  //  fprintf(stdout, "~DBImpl(): end of delete vlog_ \n");
  delete vlog_write_;
  delete vlog_read_;
  delete vlog_gc_read_;
  //  fprintf(stdout, "~DBImpl(): end of delete vlog_read_ and vlog_write_ \n");
  delete buf_; 

  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }

  //  fprintf(stdout, "~DBImpl(): end \n");
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    //ll: NOT use write buffer for manifest log file 
    log::Writer log(file, false);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover();
  if (s.ok()) {
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    const uint64_t min_log = versions_->LogNumber();
    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);
    if (!s.ok()) {
      return s;
    }
    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        expected.erase(number);
        if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
          logs.push_back(number);
      }
    }
    if (!expected.empty()) {
      char buf[50];
      snprintf(buf, sizeof(buf), "%d missing files; e.g.",
               static_cast<int>(expected.size()));
      return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) {
      s = RecoverLogFile(logs[i], edit, &max_sequence);

      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(logs[i]);
    }

    if (s.ok()) {
      if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
      }
    }
  }

  return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number,
                              VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      status = WriteLevel0Table(mem, edit, NULL);
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
      mem->Unref();
      mem = NULL;
    }
  }

  if (status.ok() && mem != NULL) {
    status = WriteLevel0Table(mem, edit, NULL);
    // Reflect errors immediately so that conditions like full
    // file-systems cause the DB::Open() to fail.
  }

  if (mem != NULL) mem->Unref();
  delete file;
  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != NULL) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
      //ll: add my output
      //     Log(options_.info_log, "new table to level:  %d", level); 
    }
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

//ll: code; my own memtable compaction funtion; 
//call this when shut down a db, and we have to flush memtable to
//table when we did not write to lsm log 
void DBImpl::CompactMemTableNoLog() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}


void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {

    fprintf(stdout, "compactmem(): shutdown error ! \n");

    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

//ll: code; return db's internal read buffer for range query usage 
char* DBImpl::Buffer() {
  return buf_;
}

//ll: code; return current vlog head and tail
void DBImpl::GetVlogHT(uint64_t *head, uint64_t *tail) {
  *head = vlog_->GetHead();
  *tail = vlog_->GetTail(); 
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      bg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == NULL &&
             manual_compaction_ == NULL &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  bg_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != NULL) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}


Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  //ll: add my last output entry 
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes level-%d",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes),
      compact->compaction->level() + 1
      );

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}
  
//ll: code; garbage collection entry function 
void DBImpl::MaybeScheduleGC() {
  //  mutex_.AssertHeld();
  if (bg_gc_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_gc_error_.ok()) {
    // Already got an error; no more changes
  } else if (!vlog_->NeedGC()) {
    // No work to be done
    fprintf(stdout, "gc is sleep !!! \n");    
  } else {
    fprintf(stdout, "gc is triggered !!! \n");
    bg_gc_scheduled_ = true;
    env_->GCSchedule(&DBImpl::GCWork, this);
  }
}

void DBImpl::GCWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->GCCall();
}

void DBImpl::GCCall() {
  //  MutexLock l(&mutex_);
  assert(bg_gc_scheduled_);

  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_gc_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundGC();
  }

  bg_gc_scheduled_ = false;

  // Previous gc may not produce enough free space,
  // so reschedule another gc if needed.
  MaybeScheduleGC();

  //fixme; later, in makeroomforwrite(), foreground write will wait(), 
  //and this will wake them up after gc frees spaces 

  //wake up waiting at ~DBImpl() ! 
  MutexLock l(&mutex_);
  bg_gc_cv_.SignalAll();
}

//garbage collection thread main function
void DBImpl::BackgroundGC() {
  //  mutex_.AssertHeld();

  Status status;

  // do 1GB each time when gc is running 
  for (int i=0; i < GC_CHUNK_NUM; i++) {

    status = DoGCWork();

    if (shutting_down_.Acquire_Load()) {
      break;
    }

    if (status.ok()) {
      // Done
    }  else {
      Log(options_.info_log,
	  "GC error: %s", status.ToString().c_str());
    }
  }
}

//fixme; handle rewind cases; update superblock to vlog, punch hole, etc 
//do the real garbage collection work 
Status DBImpl::DoGCWork() {
  Status s; 
  uint32_t target = GC_CHUNK_SIZE; 
  uint32_t total_reads = 0, total_writes = 0; 
  uint64_t read_offset = vlog_->GetTail(); 
  uint64_t start_offset = read_offset; 
  size_t read_size, header_size = 8;
  char kv_buf[512*1024]; //maximal kv pair size 
  WriteBatch wb; 

  //  mutex_.Unlock(); 

  while(total_reads < target) {     
    //read one kv pair 
    uint32_t ksize, vsize;
    Slice read_data; 
 
    //read (ksize, vsize) header 
    read_size = header_size; 
    s = GCReadVlog(read_offset, read_size, &read_data, kv_buf); 
    read_offset += read_size;
    if (s.ok()) {
      ksize = DecodeFixed32(read_data.data());
      vsize = DecodeFixed32(read_data.data() + 4);

      //      fprintf(stdout, "gc, readoff: %llu, ksize: %lu, vsize: %lu \n",
      //      (unsigned long long)read_offset, 
      //      (unsigned long)ksize, (unsigned long)vsize); 

      //read (key, value) data 
      read_size = ksize + vsize; 
      assert(read_size < 512 * 1024); 
      s = GCReadVlog(read_offset, read_size, &read_data, kv_buf + header_size); 

      read_offset += read_size;
      total_reads += (header_size + ksize + vsize); 
    }
    if (s.ok()) {
      ReadOptions opts;
      opts.internal = true; 
      Slice key(read_data.data(), ksize);
      Slice value(read_data.data() + ksize, vsize);
      std::string lsm_value; 

      //read (key, vaddr/vsize) from lsm 
      s = Get(opts, key, &lsm_value);  
      //found the lsm key/value ! 
      if (s.ok()) {
	uint64_t vaddr;
	vaddr = DecodeFixed64(lsm_value.c_str());
	if (vaddr == (read_offset - vsize)) {
	  //valid kv, write it back to vlog file in a batch fashion
	  wb.Put(key, value);

	  //	  fprintf(stdout, "gc write, key: %s \n", key.data());

	  total_writes += (header_size + ksize + vsize); 
	}
      }
    }
  }

  //if gc needs to write some valid data 
  if (WriteBatchInternal::Count(&wb)) {
    WriteBatch offsets; 

    // writing to vlog file only 
    {
      WriteOptions opt; 
      opt.internal = true; 
      opt.vlog_only = true;
      opt.off_updates = &offsets; 
      s = Write(opt, &wb);
      assert(s.ok());
      vlog_->Sync();
    }

    // writing to lsm only 
    {
      WriteOptions opt; 
      opt.internal = true; 
      opt.lsm_only = true;
      opt.sync = true;   // have to sync lsm log ! 
      offsets.Put("VLOG_TAIL", "XXX");  //store current tail to lsm 
      s = Write(opt, &offsets);
      assert(s.ok());
    } 

    Log(options_.info_log, "gc: writebatch size: %zu\n", 
	WriteBatchInternal::ByteSize(&wb));
  }

  // gc may not write any valid data, we still needs to punch hole 
  // punch hole in vlog file to reclaim space 
  {
    vlog_->PunchHole(start_offset, read_offset - start_offset);
    vlog_->Sync();		      
  }  

  //  delete kv_buf;
  //  mutex_.Lock();

  // notfound is because gc read only invalid data ! 
  if (s.ok() || s.IsNotFound()) {
    //update vlog file for space; now, not punch hole, just overwrite 
    vlog_->SetTail(read_offset); 

    /*
    if(total_reads > total_writes)
      bg_gc_cv_.SignalAll();
    */
    Log(options_.info_log, "tail: %llu \n", (unsigned long long)read_offset); 

    //output gc statistics 
    Log(options_.info_log, "gc: total_reads: %lu, total writes: %lu\n", 
	(unsigned long)total_reads, (unsigned long)total_writes);
  }
  
  return s.ok() ? s : Status::OK(); 
}

static void DeleteNothing(const Slice& key, void* value) {
}

//ll: code; random read vlog file
Status DBImpl::ReadVlog(uint64_t offset, size_t n, Slice* result, char* scratch) {
  
  //std::cout << "ReadVlog: " << offset << " : " << n << std::endl;	
  Status ret;
  ret = vlog_->ReadCache(offset, n, result, scratch);
  if (ret.ok()) {
    return ret;
  }

  // Kan: add block cache logic
  Status s;
  Cache::Handle* cache_handle = NULL;
  if (true && persist_block_cache != NULL) { // not pinned into DRAM, && (fastrand()%100) < 50
      // creat the key for cache lookup
      uint64_t start_page, end_page, in_page_offset;
      start_page = offset/4096;
      end_page = (offset + n - 1) / 4096;
      in_page_offset = offset - start_page * 4096;  // start from 0

      char cache_key_buffer[24];
      EncodeFixed64(cache_key_buffer, vlog_cache_id);
      EncodeFixed64(cache_key_buffer+8, start_page);       // start page and end page uniquely identified pages that should be cached
      EncodeFixed64(cache_key_buffer+16, end_page);
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      
      //std::cout << "\n\n== Lookup " << offset << ":" << offset + n - 1 << ", in page cache: " 
	//        << vlog_cache_id << ", " << start_page << "," << end_page << ":" << in_page_offset << std::endl;
      
      
      // look up the cache
      char* page_buf = new char[(end_page - start_page + 1) * 4096];
      cache_handle = persist_block_cache->Lookup(key, page_buf);
      if (cache_handle == NULL) {
	// read the pages from the real vlog file
	s = vlog_read_->Read(start_page * 4096, (end_page - start_page + 1) * 4096, result, page_buf);
	// decide whether to admit
        //if (flag_admit) {
        if ((fastrand()%100) < data_admit_ratio) {
	  // insert the pages into the cache 
          cache_handle = persist_block_cache->Insert(key, (end_page - start_page + 1) * 4096, page_buf);
	}
      }
      
      memcpy((void *)scratch, (void *)(page_buf + in_page_offset), n);
      *result = Slice(scratch, n);
      delete [] page_buf;
  } else {
    s = vlog_read_->Read(offset, n, result, scratch);
  }	  
	  
  return s;
}

// sequential read vlog file
Status DBImpl::GCReadVlog(uint64_t offset, size_t n, Slice* result, char* scratch) {
  return vlog_gc_read_->Read(offset, n, result, scratch);
}

//read vlog sb from file to memory
void DBImpl::ReadVlogSB() {
  Status s; 
  Slice result; 
  char* scratch = new char[sizeof(SuperBlock)];

  s = vlog_read_->Read(0, sizeof(SuperBlock), &result, scratch);
  if (s.ok()){
    //init sb by reading from vlog file 

    memcpy(scratch, result.data(), result.size());
    vlog_->SetVlogSB(scratch);
    delete scratch; 
  } else {
    fprintf(stdout, "ReadVlogSb(): read sb failed ! \n");
  }
}


Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

#ifdef ALWAYSON_GC
  //forground thread and need gc for space
  if (!options.internal && vlog_->NeedGC()) {
    MaybeScheduleGC(); 
  }
#endif

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }

    //ll: code; value contains (addr,size)
    //if found the key, and not internal, then read its value in vlog file 
    if (s.ok() && !options.internal) {
      //internal is true for garbage collection lookup 

      uint64_t vaddr;
      uint32_t vsize;
      Slice lsm_value(*value);
      vaddr = DecodeFixed64(lsm_value.data());
      vsize = DecodeFixed32(lsm_value.data() + 8);
      /*
      sleep(3); 
      fprintf(stdout, "Get(): key: %.16s, vaddr: %llu, vsize: %lu \n",
	      key.data(), (unsigned long long)vaddr, (unsigned long)vsize); 
      */

      //read the real value from vlog file; 
      //read the whole tuple (ksize, vsize, key, value)
      size_t n = static_cast<size_t>(4 + 4 + key.size() + vsize);
      char* buf = new char[n];
      Slice tuple;
      uint64_t tuple_addr = vaddr - key.size() - 8; 
      s = ReadVlog(tuple_addr, n, &tuple, buf); 
      if (s.ok()) {
	value->assign(tuple.data()+8+key.size(), vsize);
	delete [] buf;
	//	fprintf(stdout, "Get(): value read size: %u\n", (unsigned)value->size()); 
      } else {
        std::cout << "ReadVlog falied!" << std::endl;
        exit(1);
      }
    }

    //lock move to here after reading from vlog file 
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}

//ll: entry function for iterator of db; called by users ! 
Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  //ll: merge three interators together 
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  //ll: finally, DBItertator process results returned from merge iterators 
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

//ll: Put()/Delete()-> Write(); key/value are in my_batch 
Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  //forground thread and need gc for space
  if (!options.internal && vlog_->NeedGC()) {
    MaybeScheduleGC(); 
    //sleep foreground thread when used space bigger than a threshold 
    while (vlog_->WaitForSpace()) {  
      Log(options_.info_log, "vlog is on pressure, wait ... \n");
      //      bg_gc_cv_.Wait();
      env_->SleepForMicroseconds(1000);
    }
    //    Log(options_.info_log, "vlog is not on pressure, wakeup ... \n");
  }

  //ll: mutex and conditional variable to use together; learn later 
  MutexLock l(&mutex_);
  writers_.push_back(&w);
  //ll: only allow the front writer to enter; singler writer 
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  //ll: when wake up, the writer's job may be done already, due to BuildBatchGroup() ! 
  if (w.done) {
    return w.status;
  }

  const uint64_t start_wait = env_->NowMicros();
  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == NULL);

  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;

  //ll: code; make kUpdates have the same sequence number as updates
  uint64_t my_sequence;

  //ll: handle put() and delete() 
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions

    //ll: try to merge several writers together; each writer is a separate thread; 
    //updates is the combined writebatch; last_write is the last combined writer 
    //fixme: should not combine gc's write with foreground writes 
    WriteBatch* updates = BuildBatchGroup(&last_writer);

    //ll: code; for gc's vlog only write, no need for sequence number 
    if (!options.vlog_only) {    
      //ll: set a sequence number for this combined batch 
      WriteBatchInternal::SetSequence(updates, last_sequence + 1);
      //ll: code;
      my_sequence = last_sequence + 1; 
      //ll: sequence number is the number of Put(), not combined batches 
      last_sequence += WriteBatchInternal::Count(updates);
    }

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();  

      uint64_t start_vlog = 0; 
      uint64_t start_log = 0; 
      uint64_t start_mem = 0; 
      bool sync_error = false;

      wait_time_ += env_->NowMicros() - start_wait; 

      //ll: code; write values to vlog, construct kUpdates (key, addre/size)
      WriteBatch *kUpdates; 
      if (options.vlog_only) {
	// gc's vlog write only 
	kUpdates = options.off_updates; // gc set options.off_updates; 
	start_vlog = env_->NowMicros();
	status = vlog_->AddRecord(WriteBatchInternal::Contents(updates), kUpdates);	
	vlog_time_ += env_->NowMicros() - start_vlog; 
      } else {
	WriteBatch tmp; 
	if (options.lsm_only) {
	  // gc's lsm only write 
	  kUpdates = updates; 
	} else {
	  // user's writes 
	  kUpdates = &tmp; 
	  start_vlog = env_->NowMicros();
	  status = vlog_->AddRecord(WriteBatchInternal::Contents(updates), kUpdates);
	  vlog_time_ += env_->NowMicros() - start_vlog; 
	}

	//set the correct sequence number for kupdates
	WriteBatchInternal::SetSequence(kUpdates, my_sequence);

	uint64_t start_log = 0;

	//if user require lsm log or gc's lsm write, then write lsm log 
	if (write_lsm_log_ || options.lsm_only) {
	  start_log = env_->NowMicros();  
	  status = log_->AddRecord(WriteBatchInternal::Contents(kUpdates));
	  if (status.ok() && options.sync) {
	    status = logfile_->Sync();
	    if (!status.ok()) {
	      sync_error = true;
	    }
	  }	 
	  log_time_ += env_->NowMicros() - start_log; 
	}

	start_mem = env_->NowMicros();
	if (status.ok()) {      
	  //ll: code; write new key/values to memtable 
	  status = WriteBatchInternal::InsertInto(kUpdates, mem_);
	}
	mem_time_ += env_->NowMicros() - start_mem; 
      }

      mutex_.Lock();

      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();
    //ll: update last_sequence number 
    versions_->SetLastSequence(last_sequence);
  }

  //ll: due to combined batch, need to mark individual writer done 
  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  //ll: this writer is done, wake up the next front writer 
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
//ll: to improve performance, try to merge several writers together to be
//a large batch. 
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      //ll: combine the writebatch 
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;

  //ll: fixme; later, I should stop foreground write traffic if there is not 
  //enough space in vlog file; and gc will wake this up after it freed spaces 
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {

      //ll: add my log output
      //      Log(options_.info_log, "L0 files more than 8, sleep 1ms \n");

      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      bg_cv_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      bg_cv_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }

      //ll: my output
      uint64_t logsize;
      env_->GetFileSize(LogFileName(dbname_, logfile_number_), &logsize);
      Log(options_.info_log, "delete log file, size: %d\n", static_cast<int>(logsize));

      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      //ll: code; use write buffer for lsm log file
      log_ = new log::Writer(lfile, true);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

//ll: this is useful for my debugging or collect statistics 
bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                 Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }

    //ll: code; add my own output too ! 
    snprintf(buf, sizeof(buf), "\nwait: %.3f, vlog: %.3f, log: %.3f, mem: %.3f \n",
             wait_time_ * 1e-6, vlog_time_ * 1e-6, 
	     log_time_ * 1e-6, mem_time_ * 1e-6);

    value->append(buf);

    snprintf(buf, sizeof(buf), "\nindex: %.3f, meta: %.3f, block: %.3f\n",
             table_time_.index_time * 1e-6, table_time_.meta_time * 1e-6, 
	     table_time_.block_time * 1e-6);    

    value->append(buf);

    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

//ll: when open a new DB, need to create the vlog file;
//fixme; open an existing DB, modify Recover() to handle vlog 
Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;
  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;

  Status s = impl->Recover(&edit); // Handles create_if_missing, error_if_exists
  if (s.ok()) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;

      //ll: code; use write buffer for lsm log file
      impl->log_ = new log::Writer(lfile, true);
    }

    //ll: code; init vlog read and write files, similar to log file 
    WritableFile* wfile;
    uint64_t wfile_size;

    s = options.env->NewWritableFile(vLogFileName(dbname), &wfile);
    if (s.ok()) {
      impl->vlog_write_ = wfile;
      impl->vlog_ = new vlog::Writer(wfile);

      //seek to the end of the vlog file for appending 
      s = options.env->GetFileSize(vLogFileName(dbname), &wfile_size);
    }

    RandomAccessFile* rfile;
    RandomAccessFile* gcfile;
    s = options.env->NewReadAccessFile(vLogFileName(dbname), &rfile, true);
    // use mmap() for sequential range query 
    //    s = options.env->NewRandomAccessFile(vLogFileName(dbname), &rfile);
    s = options.env->NewReadAccessFile(vLogFileName(dbname), &gcfile, false);
    if (s.ok()) {
      impl->vlog_read_ = rfile;
      impl->vlog_gc_read_ = gcfile;

      if(wfile_size > 0) {
	//if vlog file exists, read and init the superblock 
	impl->ReadVlogSB();
      } else {
	//for a new vlog file, init and write the superblock
	impl->vlog_->WriteVlogSB(true); 
      }

      wfile->SeekToOffset(impl->vlog_->GetHead());
      
      fprintf(stdout, "vlog size: %llu MB, ", 
	      (unsigned long long)wfile_size/(1024*1024));
      fprintf(stdout, "head: %llu, tail: %llu \n",
	      (unsigned long long)impl->vlog_->GetHead(),
	      (unsigned long long)impl->vlog_->GetTail());

      s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
    
      
    }

    //ll: after open a db, it will start to do compaction if db needs !!! 
    if (s.ok()) {
      impl->DeleteObsoleteFiles();    
      impl->MaybeScheduleCompaction();
    }


    //Kan: start a thread to monitor the load of Optane SSD
    std::cout << "========== Here is in DB::Open, to start a thread to monitor Optane\n";




  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
  }

  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
