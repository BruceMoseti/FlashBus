#pragma once

// Latency recording and benchmark reporting.
//
// Two rules drive this file:
//   1. Recording a sample must not allocate and must not print. Printing a
//      latency per event measures the printer, not the system.
//   2. Every number that leaves the process carries the conditions that
//      produced it, so a result can be reproduced or dismissed on evidence.

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

#include "flashbus/platform.hpp"

namespace flashbus {

/// A counter with exactly one writing thread and any number of readers.
///
/// The relaxed load-add-store compiles to a plain `mov`; `fetch_add` would emit
/// a `lock xadd`, which is tens of cycles and is pure waste when there is only
/// one writer. At a few million events per second that difference is visible.
class Counter {
 public:
  void increment(uint64_t by = 1) noexcept {
    value_.store(value_.load(std::memory_order_relaxed) + by, std::memory_order_relaxed);
  }
  void set_max(uint64_t candidate) noexcept {
    if (candidate > value_.load(std::memory_order_relaxed)) {
      value_.store(candidate, std::memory_order_relaxed);
    }
  }
  [[nodiscard]] uint64_t get() const noexcept { return value_.load(std::memory_order_relaxed); }

 private:
  std::atomic<uint64_t> value_{0};
};

/// HDR-style logarithmic histogram: fixed memory, bounded relative error, no
/// allocation after construction. With 8 significant bits each bucket spans at
/// most 1/128 of its value, so reported percentiles are within 0.8% of the
/// true sample. Values are reported as the bucket's highest equivalent value,
/// clamped to the exact observed maximum, so a latency is never under-reported.
class Histogram {
 public:
  static constexpr int kSubBits = 8;
  static constexpr uint64_t kSubCount = uint64_t{1} << kSubBits;
  static constexpr size_t kExpCount = 64 - kSubBits + 1;
  static constexpr size_t kSlots = kExpCount * static_cast<size_t>(kSubCount);

  Histogram() : counts_(kSlots, 0) {}

  void record(uint64_t value) noexcept {
    ++counts_[slot_of(value)];
    ++count_;
    sum_ += value;
    min_ = std::min(min_, value);
    max_ = std::max(max_, value);
  }

  void merge(const Histogram& other) noexcept {
    for (size_t i = 0; i < kSlots; ++i) counts_[i] += other.counts_[i];
    count_ += other.count_;
    sum_ += other.sum_;
    min_ = std::min(min_, other.min_);
    max_ = std::max(max_, other.max_);
  }

  void clear() noexcept {
    std::fill(counts_.begin(), counts_.end(), uint64_t{0});
    count_ = 0;
    sum_ = 0;
    min_ = UINT64_MAX;
    max_ = 0;
  }

  [[nodiscard]] uint64_t percentile(double p) const noexcept {
    if (count_ == 0) return 0;
    const auto scaled = (p / 100.0) * static_cast<double>(count_) + 0.5;
    const uint64_t target = std::clamp<uint64_t>(static_cast<uint64_t>(scaled), 1, count_);
    uint64_t cumulative = 0;
    for (size_t i = 0; i < kSlots; ++i) {
      cumulative += counts_[i];
      if (cumulative >= target) return std::min(slot_upper_bound(i), max_);
    }
    return max_;
  }

  [[nodiscard]] uint64_t count() const noexcept { return count_; }
  [[nodiscard]] uint64_t min() const noexcept { return count_ ? min_ : 0; }
  [[nodiscard]] uint64_t max() const noexcept { return max_; }
  [[nodiscard]] double mean() const noexcept {
    return count_ ? static_cast<double>(sum_) / static_cast<double>(count_) : 0.0;
  }

  /// Non-empty buckets as (highest_equivalent_value, count), for plotting a
  /// distribution without shipping every raw sample.
  template <typename Fn>
  void for_each_bucket(Fn&& fn) const {
    for (size_t i = 0; i < kSlots; ++i) {
      if (counts_[i] != 0) fn(slot_upper_bound(i), counts_[i]);
    }
  }

  static size_t slot_of(uint64_t value) noexcept {
    if (value < kSubCount) return static_cast<size_t>(value);
    const int msb = 63 - std::countl_zero(value);
    const int exponent = msb - kSubBits + 1;
    const uint64_t sub = (value >> exponent) & (kSubCount - 1);
    return static_cast<size_t>(exponent) * static_cast<size_t>(kSubCount) +
           static_cast<size_t>(sub);
  }

  static uint64_t slot_upper_bound(size_t slot) noexcept {
    const size_t exponent = slot / static_cast<size_t>(kSubCount);
    const uint64_t sub = static_cast<uint64_t>(slot % static_cast<size_t>(kSubCount));
    if (exponent == 0) return sub;
    return ((sub + 1) << exponent) - 1;
  }

 private:
  std::vector<uint64_t> counts_;
  uint64_t count_ = 0;
  uint64_t sum_ = 0;
  uint64_t min_ = UINT64_MAX;
  uint64_t max_ = 0;
};

/// One row of results. Everything here is either measured or a parameter of
/// the run; nothing is estimated.
struct BenchmarkResult {
  std::string benchmark;
  std::string variant;

  uint32_t payload_bytes = 0;
  uint32_t producers = 1;
  uint32_t consumers = 1;
  uint64_t capacity = 0;
  uint64_t target_rate = 0;  // 0 = unthrottled
  double warmup_s = 0.0;

  uint64_t messages_sent = 0;
  uint64_t messages_received = 0;
  uint64_t messages_dropped = 0;
  uint64_t sequence_gaps = 0;
  double duration_s = 0.0;
  double throughput_msg_s = 0.0;
  double cpu_cores_used = 0.0;

  uint64_t min_ns = 0, p50_ns = 0, p95_ns = 0, p99_ns = 0, p999_ns = 0, max_ns = 0;
  double mean_ns = 0.0;

  uint64_t heap_allocations = 0;
  uint64_t voluntary_ctx_switches = 0;
  uint64_t involuntary_ctx_switches = 0;
  uint64_t minor_faults = 0;
  uint64_t major_faults = 0;

  void fill_latency(const Histogram& h) {
    min_ns = h.min();
    p50_ns = h.percentile(50.0);
    p95_ns = h.percentile(95.0);
    p99_ns = h.percentile(99.0);
    p999_ns = h.percentile(99.9);
    max_ns = h.max();
    mean_ns = h.mean();
  }

  void fill_cost(const Rusage& delta, double wall_s) {
    cpu_cores_used = wall_s > 0.0 ? (delta.user_s + delta.sys_s) / wall_s : 0.0;
    voluntary_ctx_switches = delta.voluntary_ctx_switches;
    involuntary_ctx_switches = delta.involuntary_ctx_switches;
    minor_faults = delta.minor_faults;
    major_faults = delta.major_faults;
  }
};

/// Appends results to a CSV, writing the header plus the machine description on
/// first use so a file is self-describing.
class ResultWriter {
 public:
  explicit ResultWriter(std::string path);
  void add(const BenchmarkResult& result);

 private:
  std::string path_;
  bool header_written_ = false;
};

void print_result(std::ostream& os, const BenchmarkResult& result);
void print_env(std::ostream& os);

}  // namespace flashbus
