#pragma once

// The reference clock for every FlashBus latency number.
//
// steady_clock is the default deliberately: on Linux/x86_64 it is
// CLOCK_MONOTONIC through the vDSO, so it costs tens of nanoseconds, never
// goes backwards, and needs no calibration to be correct. The TSC path below
// is cheaper but is treated as an experiment that has to be validated against
// steady_clock before any result leans on it (see benchmarks/clock_bench.cpp).

#include <chrono>
#include <cstdint>
#include <thread>

#if defined(__x86_64__) || defined(__i386__)
#include <x86intrin.h>
#endif

namespace flashbus {

[[nodiscard]] inline uint64_t now_ns() noexcept {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch())
                                   .count());
}

/// Paces a load generator at a target rate.
///
/// Deadlines are computed from a fixed origin rather than accumulated per
/// event, so a late event does not push every later deadline back: the
/// generator stays on its nominal schedule and queueing delay shows up in the
/// latency numbers instead of being silently absorbed into the send rate.
class Pacer {
 public:
  /// `rate_per_second` of 0 means unthrottled.
  explicit Pacer(uint64_t rate_per_second)
      : interval_ns_(rate_per_second == 0 ? 0 : 1'000'000'000.0 / static_cast<double>(rate_per_second)),
        origin_ns_(now_ns()) {}

  /// Blocks until event number `index` (0-based) is due.
  void wait_for(uint64_t index) const noexcept {
    if (interval_ns_ == 0.0) return;
    const uint64_t due = origin_ns_ + static_cast<uint64_t>(static_cast<double>(index) * interval_ns_);
    uint64_t current = now_ns();
    // Hand the CPU back for long waits; spin for short ones, where a sleep
    // would overshoot by more than the interval itself.
    while (due > current + 200'000) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      current = now_ns();
    }
    while (now_ns() < due) {
    }
  }

  [[nodiscard]] uint64_t origin_ns() const noexcept { return origin_ns_; }

 private:
  double interval_ns_;
  uint64_t origin_ns_;
};

/// Timestamp-counter clock. `cycles()` is a single serialising instruction;
/// conversion to nanoseconds uses a Q32 fixed-point factor so the hot path has
/// no floating point.
class TscClock {
 public:
  /// Calibrated once, on first use, against steady_clock.
  static const TscClock& instance() noexcept;

  [[nodiscard]] static uint64_t cycles() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    unsigned aux;
    return __rdtscp(&aux);  // rdtscp waits for prior loads, unlike rdtsc
#else
    return now_ns();
#endif
  }

  /// Converts a cycle *delta*, not an absolute counter: the Q32 multiply
  /// overflows past a few seconds' worth of cycles.
  [[nodiscard]] uint64_t delta_to_ns(uint64_t cycle_delta) const noexcept {
    return (cycle_delta * ns_per_cycle_q32_) >> 32;
  }

  /// False when the CPU lacks constant_tsc + nonstop_tsc, i.e. when the
  /// counter can change rate or stop in idle states and must not be trusted.
  [[nodiscard]] bool usable() const noexcept { return usable_; }
  [[nodiscard]] double ghz() const noexcept { return ghz_; }

 private:
  TscClock() noexcept;

  uint64_t ns_per_cycle_q32_ = 0;
  double ghz_ = 0.0;
  bool usable_ = false;
};

}  // namespace flashbus
