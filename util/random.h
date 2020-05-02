// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_RANDOM_H_
#define STORAGE_LEVELDB_UTIL_RANDOM_H_

#include <stdint.h>
#include <cmath>
#include <iostream>

namespace leveldb {

// Kan: uniformrandom and zipfian random number generating for YCSB
class UniformRandom {
 public:
  UniformRandom() : seed_(0) {}
  explicit UniformRandom(uint64_t seed) : seed_(seed) {}

  /**
   * In TPCC terminology, from=x, to=y.
   * NOTE both from and to are _inclusive_.
   */
  uint32_t uniform_within(uint32_t from, uint32_t to) {
    assert(from <= to);
    if (from == to) {
      return from;
    }
    return from + (next_uint32() % (to - from + 1));
  }
  /**
   * Same as uniform_within() except it avoids the "except" value.
   * Make sure from!=to.
   */
  uint32_t uniform_within_except(uint32_t from, uint32_t to, uint32_t except) {
    while (true) {
      uint32_t val = uniform_within(from, to);
      if (val != except) {
        return val;
      }
    }
  }

  /**
   * @brief Non-Uniform random (NURand) in TPCC spec (see Sec 2.1.6).
   * @details
   * In TPCC terminology, from=x, to=y.
   *  NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
   */
  uint32_t non_uniform_within(uint32_t A, uint32_t from, uint32_t to) {
    uint32_t C = get_c(A);
    return  (((uniform_within(0, A) | uniform_within(from, to)) + C) % (to - from + 1)) + from;
  }

  uint64_t get_current_seed() const {
    return seed_;
  }
  void set_current_seed(uint64_t seed) {
    seed_ = seed;
  }
  uint64_t next_uint64() {
    return (static_cast<uint64_t>(next_uint32()) << 32) | next_uint32();
  }
  uint32_t next_uint32() {
    seed_ = seed_ * 0xD04C3175 + 0x53DA9022;
    return (seed_ >> 32) ^ (seed_ & 0xFFFFFFFF);
  }

 private:
  uint64_t seed_;

  /**
   * C is a run-time constant randomly chosen within [0 .. A] that can be
   * varied without altering performance. The same C value, per field
   * (C_LAST, C_ID, and OL_I_ID), must be used by all emulated terminals.
   * constexpr, but let's not bother C++11.
   */
  uint32_t get_c(uint32_t A) const {
    // yes, I'm lazy. but this satisfies the spec.
    const uint64_t kCSeed = 0x734b00c6d7d3bbdaULL;
    return kCSeed % (A + 1);
  }
};
	
class ZipfianRandom {
 private:
  double zeta(uint64_t n) {
    double sum = 0;
    for (uint64_t i = 0; i < n; i++) {
      sum += 1 / std::pow(i + 1, theta_);
    }
    return sum;
  }

 public:
  void init(uint64_t items, double theta, uint64_t urnd_seed) {
    max_ = items - 1;
    theta_ = theta;
    zetan_ = zeta(items);
    alpha_ = 1.0 / (1.0 - theta_);
    eta_ = (1 - std::pow(2.0 / items, 1 - theta_)) / (1 - zeta(2) / zetan_);
    urnd_.set_current_seed(urnd_seed);
  }

  ZipfianRandom(uint64_t items, double theta, uint64_t urnd_seed) {
    init(items, theta, urnd_seed);
  }

  ZipfianRandom() {}

  uint64_t next() {
    double u = urnd_.uniform_within(0, max_) / static_cast<double>(max_);
    double uz = u * zetan_;
    if (uz < 1.0) {
      return 0;
    }

    if (uz < 1.0 + std::pow(0.5, theta_)) {
      return 1;
    }

    uint64_t ret = static_cast<uint64_t>(max_ * std::pow(eta_ * u - eta_ + 1, alpha_));
    return ret;
  }

  uint64_t  get_current_seed() const { return urnd_.get_current_seed(); }
  void      set_current_seed(uint64_t seed) { urnd_.set_current_seed(seed); }

 private:
  UniformRandom urnd_;
  uint64_t max_;
  uint64_t base_;
  double theta_;
  double zetan_;
  double alpha_;
  double eta_;
};



// A very simple random number generator.  Not especially good at
// generating truly random bits, but good enough for our needs in this
// package.

static unsigned int g_seed = 2333; 
inline int fastrand() { 
  g_seed = (214013*g_seed+2531011); 
  return (g_seed>>16)&0x7FFF; 
} 	

	
class Random {
 private:
  uint32_t seed_;
 public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
    // Avoid bad seeds.
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
  }
  uint32_t Next() {
    static const uint32_t M = 2147483647L;   // 2^31-1
    static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }
  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return (Next() % n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_RANDOM_H_
