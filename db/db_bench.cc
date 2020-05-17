// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#include <iostream>

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      open          -- cost of opening a DB
//      crc32c        -- repeated crc32c of 4K of data
//      acquireload   -- load N*1000 times
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)
static const char* FLAGS_benchmarks =
    "fillseq,"
    "fillsync,"
    "fillrandom,"
    "gcload,"
    "gctest,"
    "overwrite,"
    "readrandom,"
    "readrandom_1,"
    "readrandom_warmup,"
    "readrandom,"  // Extra run to allow previous compactions to quiesce
    "readseq,"
    "readreverse,"
    "compact,"
    "readrandom,"
    "readseq,"
    "readreverse,"
    "fill100K,"
    "crc32c,"
    "snappycomp,"
    "snappyuncomp,"
    "acquireload,"
    // Kan: for benchmark
    "ycsb,"  // it includes a warmup phase
    "ycsb_warmup,"
    "mixgraph,"
    ;

// Number of key/values to place in database
static int FLAGS_num = 1000000;

//ll: code; input current db's key/value numbers
static int FLAGS_db_num = 1000000;

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

// Number of concurrent threads to run.
static int FLAGS_threads = 1;

// Size of each value
static int FLAGS_value_size = 100;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 0.5;

// For gc workloads. Ratio of free space that the garbage collector will find when cleaning a chunk.
static double FLAGS_gc_freeratio = 0.5;

// Print histogram of operation timings
static bool FLAGS_histogram = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
static int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

// Use the db with the following name.
static const char* FLAGS_db = NULL;


// Kan: For general benchmark
int num_threads_measure = 1;
static bool FLAGS_monitor = false;

// Kan: For zippydb
//  key:
//static int FLAGS_keyrange_num = 10;
//static int FLAGS_keyrange_num = 3;
//static int FLAGS_keyrange_num = 5;
static int FLAGS_keyrange_num = 1;
// -keyrange_dist_a=14.18 -keyrange_dist_b=-2.917 -keyrange_dist_c=0.0164 -keyrange_dist_d=-0.08082
static double FLAGS_keyrange_dist_a = 14.18;
static double FLAGS_keyrange_dist_b = -2.917;
static double FLAGS_keyrange_dist_c = 0.0164;
static double FLAGS_keyrange_dist_d = -0.08082;
    
//  query type
static double FLAGS_mix_get_ratio = 0.85;
//static double FLAGS_mix_get_ratio = 1.0;
//static double FLAGS_mix_get_ratio = 1.00;
static double FLAGS_mix_put_ratio = 0.14;
//static double FLAGS_mix_put_ratio = 0.15;
static double FLAGS_mix_seek_ratio = 0.01;

//  value:
static double FLAGS_value_k = 0.2615;
static double FLAGS_value_sigma = 25.45;

// scan:
static double FLAGS_iter_k = 2.517;
static double FLAGS_iter_sigma = 25.45;

// QPS: -sine_a=1000 -sine_b=0.000073 -sine_d=4500
//      FLAGS_sine_a*sin((FLAGS_sine_b*x) + FLAGS_sine_c) + FLAGS_sine_d;
//static double FLAGS_sine_a = 1000;
static double FLAGS_sine_a = 12;
static double FLAGS_sine_b = 0.000073;
static double FLAGS_sine_c = 0;
//static double FLAGS_sine_d = 4500;
static double FLAGS_sine_d = 20;
//static uint64_t FLAGS_sine_mix_rate_interval_milliseconds = 5000;
static uint64_t FLAGS_sine_mix_rate_interval_milliseconds = 1000;
static uint64_t throughput_report_interval = 100; // 100 ms report once

int running_threads = 24;



namespace leveldb {

namespace {

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit-1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

class Stats {
 public:
  int tid;
 private:
  double start_;
  uint64_t sine_interval_;
  double finish_;
  double seconds_;
  int done_;
  int last_done_;
  int done_put_;
  int last_done_put_;
  int done_scan_;
  int last_done_scan_;
  int next_report_;
  int64_t bytes_;
  double last_op_finish_;
  Histogram hist_;
  std::string message_;

 public:
  Stats() { Start(); }

  void Start() {
    next_report_ = 10000;
    last_op_finish_ = start_;
    hist_.Clear();
    done_ = 0;
    last_done_ = 0;
    done_put_ = last_done_put_ = 0;
    done_scan_ = last_done_scan_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = Env::Default()->NowMicros();
    sine_interval_ = Env::Default()->NowMicros();
    finish_ = start_;
    message_.clear();
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = Env::Default()->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) {
    AppendWithSpace(&message_, msg);
  }

  void ClearStats() {
    last_done_ = done_ = 0;
    done_put_ = last_done_put_ = 0;
    done_scan_ = last_done_scan_ = 0;
    next_report_ = 10000;
  }

  void ResetSineInterval() {
    sine_interval_ = Env::Default()->NowMicros();
  }

  uint64_t GetSineInterval() {
    return sine_interval_;
  }

  uint64_t GetStart() {
    return start_;
  }
  
  void FinishedSingleOp(int type = 0) {   // type = 0 : read, 1 : scan, 2: write
    if (FLAGS_histogram) {
      double now = Env::Default()->NowMicros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    if (type == 0)
      done_++;
    if (type == 2)
      done_scan_++;
    if (type == 1)
      done_put_++;


      
     
    uint64_t now = Env::Default()->NowMicros();
    uint64_t usecs_since_last;
    if (now > last_op_finish_) {
      usecs_since_last = now - last_op_finish_;
    } else {
      usecs_since_last = 0;
    }

    if (usecs_since_last >
        (throughput_report_interval * uint64_t{1000})) {
      
      double micros = now - last_op_finish_;
      last_op_finish_ = now;
      
      int last_finished = done_ - last_done_;
      int last_finished_put = done_put_ - last_done_put_;
      int last_finished_scan = done_scan_ - last_done_scan_;
      last_done_ = done_;
      last_done_put_ = done_put_;
      last_done_scan_ = done_scan_;

      double speed_get = last_finished / micros * 1000000 * (int) running_threads;
      double speed_put = last_finished_put / micros * 1000000 * (int) running_threads;
      double speed_scan = last_finished_scan / micros * 1000000 * (int) running_threads;
      //fprintf(stdout, "... thread %d finished %d ops, %.1f ops/s%30s\n", tid, done_, speed, "");
      
      if (tid == 0) {
        //fprintf(stdout, "... %d threads finished at time %ld ms, %.1f ops/s%30s\n", FLAGS_threads, now/ 1000, speed * FLAGS_threads, "");
        //fprintf(stdout, "... %d threads finished %d ops, at time %ld ms, %.1f ops/s %.1f puts/s %.1f scans/s %40s\n", running_threads, done_, now/ 1000, speed * (int) running_threads, speed_put, speed_scan, "");
        fprintf(stdout, "... %d threads, finished %d ops, at time %ld ms, %.1f ops/s, %.1f gets/s, %.1f puts/s, %.1f scans/s, data admission ratio: %d, load admission ratio: %d %30s\n", running_threads, done_, now/ 1000, speed_get + speed_put + speed_scan, speed_get, speed_put, speed_scan, data_admit_ratio, load_admit_ratio, "");
      }
    }
    return ;
    if (done_ >= next_report_) {
      double now = Env::Default()->NowMicros();
      double micros = now - last_op_finish_;
      int last_finished = done_ - last_done_;
      last_op_finish_ = now;
      last_done_ = done_;

      double speed = last_finished / micros * 1000000;
      //std::cout << micros << " " << last_finished << "\n";

      //if      (next_report_ < 1000)   next_report_ += 100;
      //else if (next_report_ < 5000)   next_report_ += 500;
      //else
      if (next_report_ < 10000)  next_report_ += 5000;
      else if (next_report_ < 50000)  next_report_ += 5000;
      else if (next_report_ < 100000) next_report_ += 10000;
      else if (next_report_ < 500000) next_report_ += 50000;
      else                            next_report_ += 100000;
      //fprintf(stderr, "... finished %d ops%30s\r", done_, "");
      //fprintf(stderr, "... finished %d ops, %.1f ops/s%30s\r", done_, speed, "");
      //fflush(stderr);
      fprintf(stdout, "... thread %d finished %d ops, %.1f ops/s%30s\n", tid, done_, speed, "");
      fflush(stdout);
    }
  }
  
  int CheckInterval() {
    int finished_last_interval = done_ - last_done_;
    last_done_ = done_;

    return finished_last_interval;
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      std::cout << "ran for " << elapsed << " s, fetched " << bytes_ << " B\n";
      std::cout << "=========== Rate: " << rate << "\n";
      extra = rate;
    }
    AppendWithSpace(&extra, message_);

    //ll: code; add total seconds output 
    fprintf(stdout, "%-12s : %d ops; %11.3f ops/s;%11.3f micros/op;%s%s;%10.3f\n",
            name.ToString().c_str(),
            done_,
	    done_ / (seconds_/ FLAGS_threads),
	    seconds_ * 1e6 / done_,
            (extra.empty() ? "" : " "),
            extra.c_str(), 
	    seconds_);
    std::cout << "Kan!!!! " << extra << " " << seconds_ << std::endl;
    if (FLAGS_histogram) {
      fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
    }
    fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized;
  int num_done;
  bool start;

  SharedState() : cv(&mu) { }
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
  Random rand;         // Has different seeds for different threads
  Stats stats;
  SharedState* shared;

  ThreadState(int index)
      : tid(index),
        rand(1000 + index) {
    stats.tid = tid;
  }
};

}  // namespace

class Benchmark {
 private:
  Cache* cache_;
  const FilterPolicy* filter_policy_;
  DB* db_;
  int num_;
  int value_size_;
  int entries_per_batch_;
  WriteOptions write_options_;
  int reads_;
  int heap_counter_;

  void PrintHeader() {
    const int kKeySize = 16;
    PrintEnvironment();
    fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
    fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
            FLAGS_value_size,
            static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    fprintf(stdout, "Entries:    %d\n", num_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_)
             / 1048576.0));
    fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
            (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_)
             / 1048576.0));
    PrintWarnings();
    fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stdout,
            "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
            );
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

    // See if snappy is working by attempting to compress a compressible string
    const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    std::string compressed;
    if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
      fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
    } else if (compressed.size() >= sizeof(text)) {
      fprintf(stdout, "WARNING: Snappy compression is not effective\n");
    }
  }

  void PrintEnvironment() {
    fprintf(stderr, "LevelDB:    version %d.%d\n",
            kMajorVersion, kMinorVersion);

#if defined(__linux)
    time_t now = time(NULL);
    fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != NULL) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != NULL) {
        const char* sep = strchr(line, ':');
        if (sep == NULL) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

 public:
  Benchmark()
  : cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : NULL),
    filter_policy_(FLAGS_bloom_bits >= 0
                   ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                   : NULL),
    db_(NULL),
    num_(FLAGS_num),
    value_size_(FLAGS_value_size),
    entries_per_batch_(1),
    reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
    heap_counter_(0) {
    std::vector<std::string> files;
    Env::Default()->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        Env::Default()->DeleteFile(std::string(FLAGS_db) + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, Options());
    }
  }

  ~Benchmark() {
    delete db_;
    delete cache_;
    delete filter_policy_;
  }

  void Run() {
    PrintHeader();
    Open();

    const char* benchmarks = FLAGS_benchmarks;
    
    int threads_ratio = 1;

    while (benchmarks != NULL) {
      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == NULL) {
        name = benchmarks;
        benchmarks = NULL;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      // Reset parameters that may be overridden below
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      value_size_ = FLAGS_value_size;
      entries_per_batch_ = 1;
      write_options_ = WriteOptions();

      void (Benchmark::*method)(ThreadState*) = NULL;
      bool fresh_db = false;
      
      
      int num_threads = FLAGS_threads;
      running_threads = FLAGS_threads;

      if (name == Slice("open")) {
        method = &Benchmark::OpenBench;
        num_ /= 10000;
        if (num_ < 1) num_ = 1;
      } else if (name == Slice("fillseq")) {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillbatch")) {
        fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillrandom")) {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("gcload")) {
        fresh_db = true;
        method = &Benchmark::DoGCLoad;
      } else if (name == Slice("gctest")) {
        fresh_db = false;
        method = &Benchmark::DoGCTest;
      } else if (name == Slice("overwrite")) {
        fresh_db = false;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fillsync")) {
        fresh_db = true;
	//ll: comment out this line
        //num_ /= 1000;
        write_options_.sync = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fill100K")) {
        fresh_db = true;
        num_ /= 1000;
        value_size_ = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("readseq")) {
        method = &Benchmark::ReadSequential;
      } else if (name == Slice("readreverse")) {
        method = &Benchmark::ReadReverse;
      } else if (name == "mixgraph") {
        method = &Benchmark::MixGraph;
      } else if (name == Slice("ycsb")) {
	num_threads_measure = num_threads;
	num_threads = 32;
	reads_ = reads_ / num_threads;
	
	flag_monitor = false;
	flag_tune_load_admit = true;
        method = &Benchmark::YCSB;
      } else if (name == Slice("ycsb_warmup")) {
        std::cout << "====== This is to warm up for YCSB\n";
	flag_monitor = false;
	flag_tune_load_admit = true;
	
	reads_ = 500000;
        method = &Benchmark::YCSB;
      } else if (name == Slice("readrandom")) {
	flag_monitor = true;
	//flag_tune_load_admit = true;
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("readrandom_1")) {
	flag_monitor = true;
        method = &Benchmark::ReadRandomChangeWorkset;
      } else if (name == Slice("readrandom_warmup")) {
        std::cout << "====== This is to warm up for random reads\n";
	num_threads = 32;
	reads_ = 300000;
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("readmissing")) {
        method = &Benchmark::ReadMissing;
      } else if (name == Slice("seekrandom")) {
        method = &Benchmark::SeekRandom;
      } else if (name == Slice("readhot")) {
        method = &Benchmark::ReadHot;
      } else if (name == Slice("readrandomsmall")) {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("deleteseq")) {
        method = &Benchmark::DeleteSeq;
      } else if (name == Slice("deleterandom")) {
        method = &Benchmark::DeleteRandom;
      } else if (name == Slice("readwhilewriting")) {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == Slice("compact")) {
        method = &Benchmark::Compact;
      } else if (name == Slice("crc32c")) {
        method = &Benchmark::Crc32c;
      } else if (name == Slice("acquireload")) {
        method = &Benchmark::AcquireLoad;
      } else if (name == Slice("snappycomp")) {
        method = &Benchmark::SnappyCompress;
      } else if (name == Slice("snappyuncomp")) {
        method = &Benchmark::SnappyUncompress;
      } else if (name == Slice("heapprofile")) {
        HeapProfile();
      } else if (name == Slice("stats")) {
        PrintStats("leveldb.stats");
      } else if (name == Slice("sstables")) {
        PrintStats("leveldb.sstables");
      } else {
        if (name != Slice()) {  // No error message for empty name
          fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
        }
      }

      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                  name.ToString().c_str());
          method = NULL;
        } else {
          delete db_;
          db_ = NULL;
          DestroyDB(FLAGS_db, Options());
          Open();
        }
      }

      if (method != NULL) {
        RunBenchmark(num_threads, name, method);
      }
    }
  }

 private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    thread->stats.Start();
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  void RunBenchmark(int n, Slice name,
                    void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
      arg[i].thread->shared = &shared;
      Env::Default()->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();

    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report(name);

    //ll: code; add db stats output 
    PrintStats("leveldb.stats");

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;

    //ll: my output 
    //    fprintf(stdout, "end of RunBenchMark() \n");
  }

  void Crc32c(ThreadState* thread) {
    // Checksum about 500MB of data total
    const int size = 4096;
    const char* label = "(4K per op)";
    std::string data(size, 'x');
    int64_t bytes = 0;
    uint32_t crc = 0;
    while (bytes < 500 * 1048576) {
      crc = crc32c::Value(data.data(), size);
      thread->stats.FinishedSingleOp();
      bytes += size;
    }
    // Print so result is not dead
    fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
  }

  void AcquireLoad(ThreadState* thread) {
    int dummy;
    port::AtomicPointer ap(&dummy);
    int count = 0;
    void *ptr = NULL;
    thread->stats.AddMessage("(each op is 1000 loads)");
    while (count < 100000) {
      for (int i = 0; i < 1000; i++) {
        ptr = ap.Acquire_Load();
      }
      count++;
      thread->stats.FinishedSingleOp();
    }
    if (ptr == NULL) exit(1); // Disable unused variable warning.
  }

  void SnappyCompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
      produced += compressed.size();
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "(output: %.1f%%)",
               (produced * 100.0) / bytes);
      thread->stats.AddMessage(buf);
      thread->stats.AddBytes(bytes);
    }
  }

  void SnappyUncompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    std::string compressed;
    bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
    int64_t bytes = 0;
    char* uncompressed = new char[input.size()];
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok =  port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                    uncompressed);
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }
    delete[] uncompressed;

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      thread->stats.AddBytes(bytes);
    }
  }

  void Open() {
    assert(db_ == NULL);
    Options options;
    options.create_if_missing = !FLAGS_use_existing_db;
    options.block_cache = cache_;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_open_files = FLAGS_open_files;
    options.filter_policy = filter_policy_;
    
    //Kan: for persist cache
    //options.use_persist_cache = false;
    //options.persist_block_cache = NULL;
    
    options.use_persist_cache = true;
    options.persist_block_cache = NewPersistLRUCache(((size_t)34)*1024*1024*1024);
    //options.persist_block_cache = NewPersistLRUCache(((size_t)100)*1024*1024*1024);
    
    // zippydb is so skewed, use a 1/10 cache size 
    //options.persist_block_cache = NewPersistLRUCache(((size_t)4)*1024*1024*1024);
    
    //options.persist_vlog_cache = NewPersistLRUCache(((size_t)2)*1024*1024*1024);  // need to setup the db_impl code to separate lsm and vlog cache

    Status s = DB::Open(options, FLAGS_db, &db_);
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  void OpenBench(ThreadState* thread) {
    for (int i = 0; i < num_; i++) {
      delete db_;
      Open();
      thread->stats.FinishedSingleOp();
    }
  }

  void WriteSeq(ThreadState* thread) {
    DoWrite(thread, true);
  }

  void WriteRandom(ThreadState* thread) {
    DoWrite(thread, false);
  }

  void DoGCLoad(ThreadState* thread) {
    DoWrite(thread, true);
    const double freeratio = FLAGS_gc_freeratio;
    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    for (double i = 0; i < num_; i += 1.0 / freeratio) { 
      batch.Clear();
      const int k = int(i);
      char key[100];
      snprintf(key, sizeof(key), "%016d", k);
      batch.Delete(key);

      //      fprintf(stdout, "dogcload(): delete key : %s \n", key);

      thread->stats.FinishedSingleOp();
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        fprintf(stderr, "del error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
  }

  void DoGCTest(ThreadState* thread) {
    DoWrite(thread, false, 100 * 1024 * 1024);
  }

  void DoWrite(ThreadState* thread, bool seq, const int start = 0) {
    if (num_ != FLAGS_num) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", num_);
      thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    int64_t bytes = 0;
    
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = (seq ? i+j : (thread->rand.Next() % FLAGS_num)) + start;
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        batch.Put(key, gen.Generate(value_size_));
        bytes += value_size_ + strlen(key);
        thread->stats.FinishedSingleOp();
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
    thread->stats.AddBytes(bytes);
  }

  void ReadSequential(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {

      //ll: code; change to access the value to avoid mmap() based read in leveldb 
      //bytes += iter->key().size() + iter->value().size();   
      int64_t ksize = 0, vsize = 0;
      ksize = iter->key().ToString().size();
      vsize = iter->value().ToString().size();
      bytes += ksize + vsize;
      //fprintf(stdout, "readseq(): ksize: %llu, vsize: %llu \n",  
      //      (unsigned long long)ksize, (unsigned long long)vsize); 
      //      fprintf(stdout, "readseq(): key is : %s \n", iter->key().ToString().c_str());
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadReverse(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  struct KeyrangeUnit {
    int64_t keyrange_start;
    int64_t keyrange_access;
    int64_t keyrange_keys;
  };

  // From our observations, the prefix hotness (key-range hotness) follows
  // the two-term-exponential distribution: f(x) = a*exp(b*x) + c*exp(d*x).
  // However, we cannot directly use the inverse function to decide a
  // key-range from a random distribution. To achieve it, we create a list of
  // KeyrangeUnit, each KeyrangeUnit occupies a range of integers whose size is
  // decided based on the hotness of the key-range. When a random value is
  // generated based on uniform distribution, we map it to the KeyrangeUnit Vec
  // and one KeyrangeUnit is selected. The probability of a  KeyrangeUnit being
  // selected is the same as the hotness of this KeyrangeUnit. After that, the
  // key can be randomly allocated to the key-range of this KeyrangeUnit, or we
  // can based on the power distribution (y=ax^b) to generate the offset of
  // the key in the selected key-range. In this way, we generate the keyID
  // based on the hotness of the prefix and also the key hotness distribution.
  class GenerateTwoTermExpKeys {
   public:
    int64_t keyrange_rand_max_;
    int64_t keyrange_size_;
    int64_t keyrange_num_;
    bool initiated_;
    std::vector<KeyrangeUnit> keyrange_set_;

    GenerateTwoTermExpKeys() {
      keyrange_rand_max_ = FLAGS_num;
      initiated_ = false;
    }

    ~GenerateTwoTermExpKeys() {}

    // Initiate the KeyrangeUnit vector and calculate the size of each
    // KeyrangeUnit.
    Status InitiateExpDistribution(int64_t total_keys, double prefix_a,
                                   double prefix_b, double prefix_c,
                                   double prefix_d) {
      int64_t amplify = 0;
      int64_t keyrange_start = 0;
      initiated_ = true;
      if (FLAGS_keyrange_num <= 0) {
        keyrange_num_ = 1;
      } else {
        keyrange_num_ = FLAGS_keyrange_num;
      }
      keyrange_size_ = total_keys / keyrange_num_;

      // Calculate the key-range shares size based on the input parameters
      for (int64_t pfx = keyrange_num_; pfx >= 1; pfx--) {
        // Step 1. Calculate the probability that this key range will be
        // accessed in a query. It is based on the two-term expoential
        // distribution
        double keyrange_p = prefix_a * std::exp(prefix_b * pfx) +
                            prefix_c * std::exp(prefix_d * pfx);
        if (keyrange_p < std::pow(10.0, -16.0)) {
          keyrange_p = 0.0;
        }
        // Step 2. Calculate the amplify
        // In order to allocate a query to a key-range based on the random
        // number generated for this query, we need to extend the probability
        // of each key range from [0,1] to [0, amplify]. Amplify is calculated
        // by 1/(smallest key-range probability). In this way, we ensure that
        // all key-ranges are assigned with an Integer that  >=0
        if (amplify == 0 && keyrange_p > 0) {
          amplify = static_cast<int64_t>(std::floor(1 / keyrange_p)) + 1;
        }

        // Step 3. For each key-range, we calculate its position in the
        // [0, amplify] range, including the start, the size (keyrange_access)
        KeyrangeUnit p_unit;
        p_unit.keyrange_start = keyrange_start;
        if (0.0 >= keyrange_p) {
          p_unit.keyrange_access = 0;
        } else {
          p_unit.keyrange_access =
              static_cast<int64_t>(std::floor(amplify * keyrange_p));
        }
        p_unit.keyrange_keys = keyrange_size_;
        keyrange_set_.push_back(p_unit);
        keyrange_start += p_unit.keyrange_access;
      }
      keyrange_rand_max_ = keyrange_start;

      // Step 4. Shuffle the key-ranges randomly
      // Since the access probability is calculated from small to large,
      // If we do not re-allocate them, hot key-ranges are always at the end
      // and cold key-ranges are at the begin of the key space. Therefore, the
      // key-ranges are shuffled and the rand seed is only decide by the
      // key-range hotness distribution. With the same distribution parameters
      // the shuffle results are the same.
      //Random64 rand_loca(keyrange_rand_max_);
      Random rand_loca(keyrange_rand_max_);
      for (int64_t i = 0; i < FLAGS_keyrange_num; i++) {
        int64_t pos = rand_loca.Next() % FLAGS_keyrange_num;
        assert(i >= 0 && i < static_cast<int64_t>(keyrange_set_.size()) &&
               pos >= 0 && pos < static_cast<int64_t>(keyrange_set_.size()));
        std::swap(keyrange_set_[i], keyrange_set_[pos]);
      }

      // Step 5. Recalculate the prefix start postion after shuffling
      int64_t offset = 0;
      for (auto& p_unit : keyrange_set_) {
        p_unit.keyrange_start = offset;
        offset += p_unit.keyrange_access;
      }

      return Status::OK();
    }

    // Generate the Key ID according to the input ini_rand and key distribution
    int64_t DistGetKeyID(int64_t ini_rand, double key_dist_a,
                         double key_dist_b) {
      //return ini_rand % FLAGS_num;
      
      int64_t keyrange_rand = ini_rand % keyrange_rand_max_;

      // Calculate and select one key-range that contains the new key
      int64_t start = 0, end = static_cast<int64_t>(keyrange_set_.size());
      while (start + 1 < end) {
        int64_t mid = start + (end - start) / 2;
        assert(mid >= 0 && mid < static_cast<int64_t>(keyrange_set_.size()));
        if (keyrange_rand < keyrange_set_[mid].keyrange_start) {
          end = mid;
        } else {
          start = mid;
        }
      }
      int64_t keyrange_id = start;

      // Select one key in the key-range and compose the keyID
      int64_t key_offset = 0, key_seed;
      if (key_dist_a == 0.0 || key_dist_b == 0.0) {
        key_offset = ini_rand % keyrange_size_;
      } else {
        double u =
            static_cast<double>(ini_rand % keyrange_size_) / keyrange_size_;
        key_seed = static_cast<int64_t>(
            ceil(std::pow((u / key_dist_a), (1 / key_dist_b))));
        //Random64 rand_key(key_seed);
        Random rand_key(key_seed);
        key_offset = rand_key.Next() % keyrange_size_;
      }
      return keyrange_size_ * keyrange_id + key_offset;
    }
  };
  
  

  int get_query_type(int64_t rand_num) {
    
    double ratio = (rand_num % 100) / 100.0; 
    if (ratio < FLAGS_mix_get_ratio)
      return 0;
    if (ratio < FLAGS_mix_get_ratio + FLAGS_mix_put_ratio)
      return 1;
    return 2;
  }

  int64_t ParetoCdfInversion(double u, double theta, double k, double sigma) {
    double ret;
    if (k == 0.0) {
      ret = theta - sigma * std::log(u);
    } else {
      ret = theta + sigma * (std::pow(u, -1 * k) - 1) / k;
    }
    return static_cast<int64_t>(ceil(ret));
  } 

  double SineRate(double x) {
    return FLAGS_sine_a*sin((FLAGS_sine_b*x) + FLAGS_sine_c) + FLAGS_sine_d;
  }
  
  
  // The social graph wokrload mixed with Get, Put, Iterator queries.
  // The value size and iterator length follow Pareto distribution.
  // The overall key access follow power distribution. If user models the
  // workload based on different key-ranges (or different prefixes), user
  // can use two-term-exponential distribution to fit the workload. User
  // needs to decides the ratio between Get, Put, Iterator queries before
  // starting the benchmark.
  void MixGraph(ThreadState* thread) {
    int64_t read = 0;  // including single gets and Next of iterators
    int64_t gets = 0;
    int64_t puts = 0;
    int64_t found = 0;
    int64_t seek = 0;
    int64_t seek_found = 0;
    int64_t bytes = 0;
    const int64_t default_value_max = 1 * 1024 * 1024;
    int64_t value_max = default_value_max;
    //int64_t scan_len_max = FLAGS_mix_max_scan_len;
    //int64_t scan_len_max = 1000;
    int64_t scan_len_max = 10;
    double write_rate = 1000000.0;
    double read_rate = 1000000.0;
    bool use_prefix_modeling = true;

    //std::vector<double> ratio{0.85, 0.14, 0.01};  // gen 
    //std::vector<double> ratio{FLAGS_mix_get_ratio, FLAGS_mix_put_ratio, FLAGS_mix_seek_ratio};  // gen 
    
    
    ReadOptions options;
    std::string value;
    char key[100];
    
    
    char value_buffer[default_value_max];
    /*
    RandomGenerator gen;

    // the limit of qps initiation
    if (FLAGS_sine_a != 0 || FLAGS_sine_d != 0) {
      thread->shared->read_rate_limiter.reset(NewGenericRateLimiter(
          static_cast<int64_t>(read_rate), 100000 , 10 ,
          RateLimiter::Mode::kReadsOnly));
      thread->shared->write_rate_limiter.reset(
          NewGenericRateLimiter(static_cast<int64_t>(write_rate)));
    }

    gen_exp.InitiateExpDistribution(
          FLAGS_num, FLAGS_keyrange_dist_a, FLAGS_keyrange_dist_b,
          FLAGS_keyrange_dist_c, FLAGS_keyrange_dist_d);
    
    */
    
    //init the random key generation module for prefix modeling
    
    GenerateTwoTermExpKeys gen_exp;
    gen_exp.InitiateExpDistribution(
          FLAGS_db_num, FLAGS_keyrange_dist_a, FLAGS_keyrange_dist_b,
          FLAGS_keyrange_dist_c, FLAGS_keyrange_dist_d);
    
    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    
    
    //TODO init the qps rate limiter
    double mix_rate_with_noise = 32;
    //double mix_rate_with_noise = 24;


    // warmup phase
    flag_monitor = false;
    for (int i = 0; i < 200000; i++) {
    //for (int i = 0; i < 500000; i++) {
      int64_t ini_rand, key_rand, rand_v;
      ini_rand = thread->rand.Next();
      key_rand = gen_exp.DistGetKeyID(ini_rand, 0.0, 0.0);
      snprintf(key, sizeof(key), "%016d", key_rand);

      if (db_->Get(options, key, &value).ok()) {
        found++;
	thread->stats.AddBytes(value.length());
      } 
      thread->stats.FinishedSingleOp();
    }

    std::cout << "==== thread" << thread->tid << " warming up finished\n";
    
    flag_monitor = FLAGS_monitor;
    //flag_monitor = true;
    //flag_monitor = true;

    //for (int i = 0; i < reads_; i++) {
    while (true) {
      // TODO Step 0: decide the QPS
      
      uint64_t now = Env::Default()->NowMicros();
      uint64_t usecs_since_last;
      if (now > thread->stats.GetSineInterval()) {
        usecs_since_last = now - thread->stats.GetSineInterval();
      } else {
        usecs_since_last = 0;
      }
      
      if (usecs_since_last >
          (FLAGS_sine_mix_rate_interval_milliseconds * uint64_t{1000})) {
    
	double usecs_since_start = static_cast<double>(now - thread->stats.GetStart());
         thread->stats.ResetSineInterval();
	//double mix_rate_with_noise = SineRate(usecs_since_start / 1000000.0);
        //mix_rate_with_noise = SineRate(usecs_since_start / 1000.0);
        mix_rate_with_noise = SineRate(usecs_since_start / 500.0);
        //mix_rate_with_noise = 24;
	if (thread -> tid == 0)
          std::cout << "=== interval " << usecs_since_start << " " << mix_rate_with_noise << "\n";
        //std::cout << usecs_since_start << " " << mix_rate_with_noise << "\n";
      }

      running_threads = mix_rate_with_noise;
      if (thread->tid >= mix_rate_with_noise) {
        //std::cout << "thread " << thread->tid << "continue\n";
        continue;
      }
	

      // Start the query
      
      // Step 1: generate random key
      int64_t ini_rand, key_rand, rand_v;
      ini_rand = thread->rand.Next();
      key_rand = gen_exp.DistGetKeyID(ini_rand, 0.0, 0.0);
      snprintf(key, sizeof(key), "%016d", key_rand);

      rand_v = ini_rand % FLAGS_db_num;
      double u = static_cast<double>(rand_v) / FLAGS_db_num;

      
      // Step 2: decide what type of request it is
      int query_type = get_query_type(ini_rand);
      
      if (query_type == 0) {
	//std::cout << "This will be a Get\n";
        // the Get query
        gets++;
        read++;
	
        if (db_->Get(options, key, &value).ok()) {
          found++;
	  thread->stats.AddBytes(value.length());
        } else {
	  std::cout << key_rand << " Failed!\n";
	  exit(1);
	}
	
        /*
	if (thread->shared->read_rate_limiter.get() != nullptr &&
            read % 256 == 255) {
          thread->shared->read_rate_limiter->Request(
              256, Env::IO_HIGH, nullptr ,
              RateLimiter::OpType::kRead);
        }*/
        //thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db, 1, kRead);
      
        thread->stats.FinishedSingleOp(0);
      } else if (query_type == 1) {
        
	//std::cout << "This will be a put\n";
	// the Put query
	puts++;
        
        //int64_t val_size = ParetoCdfInversion(
        //    u, 0.0, FLAGS_value_k, FLAGS_value_sigma);
        
	// slightly larger value size to get rid of memory influence
	int64_t val_size = ParetoCdfInversion(
            u, 4096, FLAGS_value_k, FLAGS_value_sigma);
        if (val_size < 0) {
          val_size = 10;
        } else if (val_size > value_max) {
          val_size = val_size % value_max;
        }
	
	//continue;      
	//std::cout << val_size << std::endl;
        
	batch.Clear();
        batch.Put(key, gen.Generate(val_size));
        bytes += value_size_ + strlen(key);
       
        s = db_->Write(write_options_, &batch);
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        
	thread->stats.FinishedSingleOp(1);

	// TODO tune the write rate?
        /*
	if (thread->shared->write_rate_limiter) {
          thread->shared->write_rate_limiter->Request(
              key.size() + val_size, Env::IO_HIGH, nullptr ,
              RateLimiter::OpType::kWrite);
        }
        thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db, 1, kWrite);
        */
      
      
      } else if (query_type == 2) {
	//std::cout << "This will be a Scan\n";
        
	/*// Seek query
        Iterator* single_iter = nullptr;
          single_iter = db_with_cfh->db->NewIterator(options);
          if (single_iter != nullptr) {
            single_iter->Seek(key);
            seek++;
            read++;
            if (single_iter->Valid() && single_iter->key().compare(key) == 0) {
              seek_found++;
            }
            int64_t scan_length =
                ParetoCdfInversion(u, FLAGS_iter_theta, FLAGS_iter_k,
                                   FLAGS_iter_sigma) %
                scan_len_max;
            for (int64_t j = 0; j < scan_length && single_iter->Valid(); j++) {
              Slice value = single_iter->value();
              memcpy(value_buffer, value.data(),
                     std::min(value.size(), sizeof(value_buffer)));
              bytes += single_iter->key().size() + single_iter->value().size();
              single_iter->Next();
              assert(single_iter->status().ok());
            }
          }
          delete single_iter;
        }
        thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db, 1, kSeek);
        */
    
	
        Iterator* iter = db_->NewIterator(options);
	//continue;      
      
        if (iter != nullptr) {
          iter->Seek(key);
          seek++;
          read++;
          if (iter->Valid() && iter->key() == key) found++;
      
          int64_t scan_length = ParetoCdfInversion(u, 0.0, FLAGS_iter_k, FLAGS_iter_sigma) % scan_len_max;
	  for (int j = 0; j < scan_length && iter->Valid(); j++) {
            int64_t ksize = 0, vsize = 0;
            ksize = iter->key().ToString().size();
            vsize = iter->value().ToString().size();
            bytes += ksize + vsize;
            iter->Next();	
	  }
          thread->stats.FinishedSingleOp(2);
        }
        delete iter;
      }
    
    }
        
    
    char msg[256];
    snprintf(msg, sizeof(msg), "(Gets: %ld, Puts: %ld, Seek:%ld, %d of %ld found)", gets, puts, seek, found, gets);

    //snprintf(msg, sizeof(msg),
    //         "( Gets:%" int " Puts:%" PRIu64 " Seek:%" PRIu64 " of %" PRIu64
    //         " in %" PRIu64 " found)\n",
    //         gets, puts, seek, found, read);

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(msg);

  }
  
  
  
  void YCSB(ThreadState* thread) {
    
    ReadOptions options;
    std::string value;
    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    
    
    int found = 0;
    int64_t bytes = 0;
    // init the zipfian random generator
    double g_zipfian_theta = 0.9;
    ZipfianRandom zipfian_rng(FLAGS_db_num, g_zipfian_theta, 1237 + thread->tid); 

    // warmup phase
    for (int i = 0; i < reads_; i++) {
    //for (int i = 0; i < 0; i++) {
      char key[100];

      //Kan: for zipfian accesses
      //const int k = thread->rand.Next() % (FLAGS_db_num / 3);
      const int k = zipfian_rng.next() % (FLAGS_db_num);
      //const int k = zipfian_rng.next() % (FLAGS_db_num/3);
      //std::cout << "key: " << k << "\n";
      snprintf(key, sizeof(key), "%016d", k);
      

      if (db_->Get(options, key, &value).ok()) {
        found++;
        thread->stats.AddBytes(value.length());
      }
      thread->stats.FinishedSingleOp();
    }
    
    if (thread->tid >= num_threads_measure) {
      std::cout << "Thread " << thread->tid << " finished warming up\n";
      return; 
    }
    thread->stats.ClearStats();

    // TO test classic cache or tuned cache
    flag_monitor = true;
    
    double read_ratio = 1.0;
    double scan_ratio = 0.0;
    double write_ratio = 0.5;
    bool rmw = true;    // whether the write is read modify write

    // measurement phase
    // handle write speically
    if (false && write_ratio == 0.5 && thread->tid == 0) {
      while (true) {
        {
          MutexLock l(&thread->shared->mu);
          if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
            // Other threads have finished
            break;
          }
        }

        //const int k = thread->rand.Next() % FLAGS_num;
        //const int k = zipfian_rng.next() % (FLAGS_db_num);
        const int k = zipfian_rng.next() % (FLAGS_db_num/3);
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        
	
	if (rmw) {
	  db_->Get(options, key, &value);
	}
	
	s = db_->Put(write_options_, key, gen.Generate(value_size_));
	
	if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        //
        thread->stats.AddBytes(value.length());
        thread->stats.FinishedSingleOp();
      }
    
      return;
    }
    
    
    //for (int i = 0; i < (reads_*32/num_threads_measure/4); i++) {
    std::cout << reads_ << " : " << num_threads_measure << " " << (reads_*32/num_threads_measure/8) << std::endl;
    for (int i = 0; i < (reads_*32/num_threads_measure/8); i++) {
      char key[100];

      //Kan: decide key to access, zipfian distributions
      //const int k = thread->rand.Next() % (FLAGS_db_num / 3);
      const int k = zipfian_rng.next() % (FLAGS_db_num);
      snprintf(key, sizeof(key), "%016d", k);

      //TODO decide operation type
      double query_ratio = 0.0;
      
      if (true || write_ratio != 0.5)
        query_ratio = (thread->rand.Next() % 100) / 100.0;

      if (query_ratio < read_ratio) {
	//std::cout << "This is to do Get, thread " << thread->tid << "\n";
        // get
	if (db_->Get(options, key, &value).ok()) {
          found++;
          thread->stats.AddBytes(value.length());
        }
        thread->stats.FinishedSingleOp();
      } else if (query_ratio < read_ratio + scan_ratio) {
        // TODO scan
        Iterator* iter = db_->NewIterator(options);
	//continue;      
      
        if (iter != nullptr) {
          iter->Seek(key);
          if (iter->Valid() && iter->key() == key) found++;
      
          int64_t scan_length = 10;
	  for (int j = 0; j < scan_length && iter->Valid(); j++) {
            int64_t ksize = 0, vsize = 0;
            ksize = iter->key().ToString().size();
            vsize = iter->value().ToString().size();
            bytes += ksize + vsize;
            iter->Next();	
	  }
          thread->stats.FinishedSingleOp();
        }
        delete iter;
      } else {
	// put
	//std::cout << "This is to do Put, thread " << thread->tid << "\n";
	//const int k = thread->rand.Next()  + FLAGS_db_num;
	//const int k = 666;
        //const int k = zipfian_rng.next() % (FLAGS_db_num);
        //snprintf(key, sizeof(key), "%016d", k);
	
	if (rmw) {
	  db_->Get(options, key, &value);
	}
	
	s = db_->Put(write_options_, key, gen.Generate(value_size_));
	
	if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        //
        thread->stats.AddBytes(value.length());
        thread->stats.FinishedSingleOp();
      }
	
    }
    
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }
  
  void ReadRandom(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    int found = 0;
    int64_t bytes = 0;
    for (int i = 0; i < reads_; i++) {
      char key[100];

      //ll: code; change this to db size 
      //const int k = thread->rand.Next() % FLAGS_db_num;
      
      //Kan: for skewed accesses
      const int k = thread->rand.Next() % (FLAGS_db_num / 3);
      //const int k = thread->rand.Next() % (FLAGS_db_num / 3);
      //const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d", k);

      if (db_->Get(options, key, &value).ok()) {
        found++;
        thread->stats.AddBytes(value.length());
      }
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }
  
  void ReadRandomChangeWorkset(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    int found = 0;
    int64_t bytes = 0;
    for (int i = 0; i < reads_; i++) {
      char key[100];

      //const int k = FLAGS_db_num / 6 + thread->rand.Next() % (FLAGS_db_num / 3);
      //const int k = FLAGS_db_num / 9 + thread->rand.Next() % (FLAGS_db_num / 3);
      const int k = FLAGS_db_num / 3 + thread->rand.Next() % (FLAGS_db_num / 3);
      //const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d", k);

      if (db_->Get(options, key, &value).ok()) {
        found++;
        thread->stats.AddBytes(value.length());
      }
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void ReadMissing(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d.", k);
      db_->Get(options, key, &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void ReadHot(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    const int range = (FLAGS_num + 99) / 100;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % range;
      snprintf(key, sizeof(key), "%016d", k);
      db_->Get(options, key, &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void SeekRandom(ThreadState* thread) {
    ReadOptions options;
    int found = 0;
    for (int i = 0; i < reads_; i++) {
      Iterator* iter = db_->NewIterator(options);
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d", k);
      iter->Seek(key);
      if (iter->Valid() && iter->key() == key) found++;
      delete iter;
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void DoDelete(ThreadState* thread, bool seq) {
    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i+j : (thread->rand.Next() % FLAGS_num);
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        batch.Delete(key);
        thread->stats.FinishedSingleOp();
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        fprintf(stderr, "del error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
  }

  void DeleteSeq(ThreadState* thread) {
    DoDelete(thread, true);
  }

  void DeleteRandom(ThreadState* thread) {
    DoDelete(thread, false);
  }

  void ReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      // Special thread that keeps writing until other threads are done.
      RandomGenerator gen;
      while (true) {
        {
          MutexLock l(&thread->shared->mu);
          if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
            // Other threads have finished
            break;
          }
        }

        const int k = thread->rand.Next() % FLAGS_num;
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
      }

      // Do not count any of the preceding work/delay in stats.
      thread->stats.Start();
    }
  }

  void Compact(ThreadState* thread) {
    db_->CompactRange(NULL, NULL);
  }

  void PrintStats(const char* key) {
    std::string stats;
    if (!db_->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    fprintf(stdout, "\n%s\n", stats.c_str());
  }

  static void WriteToFile(void* arg, const char* buf, int n) {
    reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
  }

  void HeapProfile() {
    char fname[100];
    snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db, ++heap_counter_);
    WritableFile* file;
    Status s = Env::Default()->NewWritableFile(fname, &file);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return;
    }
    bool ok = port::GetHeapProfile(WriteToFile, file);
    delete file;
    if (!ok) {
      fprintf(stderr, "heap profiling not supported\n");
      Env::Default()->DeleteFile(fname);
    }
  }
};

}  // namespace leveldb

int main(int argc, char** argv) {
  FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
  FLAGS_open_files = leveldb::Options().max_open_files;
  std::string default_db_path;

  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    char junk;
    if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--gc_freeratio=%lf%c", &d, &junk) == 1) {
      FLAGS_gc_freeratio = d;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_existing_db = n;
    } else if (sscanf(argv[i], "--monitor=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_monitor = n;
      if (FLAGS_monitor)
        std::cout << "Will monitor the cache!\n";
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
      //ll: code; add db_num 
    } else if (sscanf(argv[i], "--db_num=%d%c", &n, &junk) == 1) {
      FLAGS_db_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--cache_size=%d%c", &n, &junk) == 1) {
      FLAGS_cache_size = n;
    } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      FLAGS_bloom_bits = n;
    } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
      FLAGS_open_files = n;
    } else if (sscanf(argv[i], "--frequency=%d%c", &n, &junk) == 1) {
      leveldb::scheduler_frequency = n;
      std::cout << "Scheduler frequency: " << leveldb::scheduler_frequency << "\n";
    } else if (sscanf(argv[i], "--keyrange_num=%d%c", &n, &junk) == 1) {
      FLAGS_keyrange_num = n;
      std::cout << "MixGraph keyrange num: " << FLAGS_keyrange_num << "\n";
    } else if (sscanf(argv[i], "--step=%d%c", &n, &junk) == 1) {
      leveldb::scheduler_step = n;
      std::cout << "Scheduler step size: " << leveldb::scheduler_step << "\n";
    } else if (strncmp(argv[i], "--db=", 5) == 0) {
      FLAGS_db = argv[i] + 5;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == NULL) {
      leveldb::Env::Default()->GetTestDirectory(&default_db_path);
      default_db_path += "/dbbench";
      FLAGS_db = default_db_path.c_str();
  }

  leveldb::Benchmark benchmark;
  benchmark.Run();
  return 0;
}
