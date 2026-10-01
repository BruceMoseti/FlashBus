#include "flashbus/metrics.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

#include "flashbus/clock.hpp"

namespace flashbus {
namespace {

/// Exact percentile from sorted samples, using the same 1-based rank rule as
/// Histogram, so the two can be compared directly.
uint64_t exact_percentile(std::vector<uint64_t> samples, double p) {
  std::sort(samples.begin(), samples.end());
  const auto scaled = (p / 100.0) * static_cast<double>(samples.size()) + 0.5;
  const auto rank = std::clamp<size_t>(static_cast<size_t>(scaled), 1, samples.size());
  return samples[rank - 1];
}

TEST(Histogram, EmptyHistogramReportsZeros) {
  const Histogram h;
  EXPECT_EQ(h.count(), 0u);
  EXPECT_EQ(h.percentile(50.0), 0u);
  EXPECT_EQ(h.min(), 0u);
  EXPECT_EQ(h.max(), 0u);
  EXPECT_DOUBLE_EQ(h.mean(), 0.0);
}

TEST(Histogram, SmallValuesAreExact) {
  Histogram h;
  for (uint64_t v = 0; v < Histogram::kSubCount; ++v) h.record(v);
  // Below the sub-bucket count every value has its own slot.
  for (uint64_t v = 0; v < Histogram::kSubCount; ++v) {
    EXPECT_EQ(Histogram::slot_upper_bound(Histogram::slot_of(v)), v);
  }
  EXPECT_EQ(h.min(), 0u);
  EXPECT_EQ(h.max(), Histogram::kSubCount - 1);
}

TEST(Histogram, LargeValuesStayWithinTheStatedError) {
  // The documented bound is 1/128 of the value. Check it across nine decades.
  for (uint64_t value = Histogram::kSubCount; value < (uint64_t{1} << 48); value *= 3) {
    const uint64_t upper = Histogram::slot_upper_bound(Histogram::slot_of(value));
    ASSERT_GE(upper, value) << "a latency must never be reported lower than observed";
    const double error = static_cast<double>(upper - value) / static_cast<double>(value);
    ASSERT_LT(error, 1.0 / 128.0) << "value " << value << " -> " << upper;
  }
}

TEST(Histogram, MinMaxAndMeanAreExact) {
  Histogram h;
  h.record(5);
  h.record(1000000);
  h.record(15);
  EXPECT_EQ(h.min(), 5u);
  EXPECT_EQ(h.max(), 1000000u);
  EXPECT_NEAR(h.mean(), (5.0 + 1000000.0 + 15.0) / 3.0, 1e-9);
  EXPECT_EQ(h.count(), 3u);
}

TEST(Histogram, PercentilesMatchExactWithinTheErrorBound) {
  std::mt19937_64 rng(99);
  // A long-tailed distribution, which is the shape that matters here: mostly
  // a few microseconds with occasional millisecond outliers.
  std::vector<uint64_t> samples;
  samples.reserve(200000);
  for (int i = 0; i < 200000; ++i) {
    const bool tail = (rng() % 1000) == 0;
    samples.push_back(tail ? 500000 + rng() % 2000000 : 2000 + rng() % 4000);
  }

  Histogram h;
  for (const uint64_t v : samples) h.record(v);

  for (const double p : {50.0, 95.0, 99.0, 99.9, 100.0}) {
    const uint64_t expected = exact_percentile(samples, p);
    const uint64_t actual = h.percentile(p);
    EXPECT_GE(actual, expected) << "p" << p << " must not under-report";
    const double error = static_cast<double>(actual - expected) / static_cast<double>(expected);
    EXPECT_LT(error, 1.0 / 128.0) << "p" << p << " expected " << expected << " got " << actual;
  }
}

TEST(Histogram, PercentilesNeverExceedTheObservedMaximum) {
  Histogram h;
  for (uint64_t i = 0; i < 1000; ++i) h.record(1000 + i);
  EXPECT_LE(h.percentile(99.9), h.max());
  EXPECT_EQ(h.percentile(100.0), h.max());
}

TEST(Histogram, MergeIsEquivalentToRecordingBoth) {
  std::mt19937_64 rng(7);
  Histogram a, b, combined;
  for (int i = 0; i < 50000; ++i) {
    const uint64_t left = rng() % 100000;
    const uint64_t right = rng() % 100000;
    a.record(left);
    b.record(right);
    combined.record(left);
    combined.record(right);
  }
  a.merge(b);
  EXPECT_EQ(a.count(), combined.count());
  EXPECT_EQ(a.min(), combined.min());
  EXPECT_EQ(a.max(), combined.max());
  for (const double p : {50.0, 99.0, 99.9}) {
    EXPECT_EQ(a.percentile(p), combined.percentile(p));
  }
}

TEST(Histogram, RecordDoesNotAllocate) {
  Histogram h;
  std::mt19937_64 rng(1);
  reset_heap_allocation_count();
  const uint64_t before = heap_allocation_count();
  for (int i = 0; i < 1000000; ++i) h.record(rng() % 10000000);
  EXPECT_EQ(heap_allocation_count(), before);
}

TEST(Histogram, ClearResetsEverything) {
  Histogram h;
  for (uint64_t i = 0; i < 100; ++i) h.record(i * 1000);
  h.clear();
  EXPECT_EQ(h.count(), 0u);
  EXPECT_EQ(h.max(), 0u);
  EXPECT_EQ(h.percentile(99.0), 0u);
}

TEST(Histogram, BucketIterationCoversEverySample) {
  Histogram h;
  for (uint64_t i = 0; i < 10000; ++i) h.record(i);
  uint64_t total = 0;
  h.for_each_bucket([&](uint64_t, uint64_t count) { total += count; });
  EXPECT_EQ(total, h.count());
}

// --- clock ------------------------------------------------------------------

TEST(Clock, SteadyClockIsMonotonic) {
  uint64_t previous = now_ns();
  for (int i = 0; i < 100000; ++i) {
    const uint64_t current = now_ns();
    ASSERT_GE(current, previous);
    previous = current;
  }
}

TEST(Clock, TscAgreesWithSteadyClockWhenUsable) {
  const auto& tsc = TscClock::instance();
  if (!tsc.usable()) GTEST_SKIP() << "CPU lacks constant_tsc/nonstop_tsc";

  const uint64_t ns_start = now_ns();
  const uint64_t cycles_start = TscClock::cycles();
  // Busy-wait rather than sleep: a sleep measures the scheduler.
  while (now_ns() - ns_start < 50'000'000) {
  }
  const uint64_t cycles_ns = tsc.delta_to_ns(TscClock::cycles() - cycles_start);
  const uint64_t steady_ns = now_ns() - ns_start;

  const double ratio = static_cast<double>(cycles_ns) / static_cast<double>(steady_ns);
  EXPECT_NEAR(ratio, 1.0, 0.01) << "TSC-derived " << cycles_ns << " ns vs steady " << steady_ns
                                << " ns; the TSC path is not trustworthy here";
}

}  // namespace
}  // namespace flashbus
