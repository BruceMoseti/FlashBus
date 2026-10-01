// What the clock costs, and whether the TSC path can be trusted here.
//
// This runs first in the benchmark suite for a reason: every latency number in
// the project is two clock reads wide. If a clock read costs 25 ns, a claimed
// 100 ns handoff is really a 50 ns handoff plus the instrument, and the honest
// thing is to know which.

#include <iomanip>
#include <iostream>
#include <vector>

#include "flashbus/cli.hpp"
#include "flashbus/clock.hpp"
#include "flashbus/metrics.hpp"
#include "flashbus/platform.hpp"

namespace {

using namespace flashbus;

/// Average cost of one read, from a long loop: a per-call measurement would be
/// measuring the clock with the clock.
double average_cost_ns(uint64_t samples, uint64_t (*read)()) {
  const uint64_t start = now_ns();
  uint64_t sink = 0;
  for (uint64_t i = 0; i < samples; ++i) sink += read();
  const uint64_t elapsed = now_ns() - start;
  if (sink == 0) std::cerr << "";  // keep the loop alive
  return static_cast<double>(elapsed) / static_cast<double>(samples);
}

/// Distribution of the gap between two consecutive reads. The minimum is a
/// decent estimate of the read cost; the tail shows the interference the
/// machine is under, which matters when reading p99.9 numbers later.
Histogram successive_gaps(uint64_t samples) {
  Histogram histogram;
  uint64_t previous = now_ns();
  for (uint64_t i = 0; i < samples; ++i) {
    const uint64_t current = now_ns();
    histogram.record(current - previous);
    previous = current;
  }
  return histogram;
}

}  // namespace

int main(int argc, char** argv) {
  const cli::Args args(argc, argv);
  args.reject_unknown({"samples", "validate-ms", "help"});
  if (args.flag("help")) {
    std::cout << "usage: clock_bench [--samples N] [--validate-ms N]\n";
    return 0;
  }
  const auto samples = args.number<uint64_t>("samples", 20'000'000);
  const auto validate_ms = args.number<uint64_t>("validate-ms", 200);

  print_env(std::cout);
  std::cout << std::fixed << std::setprecision(2);

  const double steady_cost = average_cost_ns(samples, [] { return now_ns(); });
  std::cout << "\nsteady_clock::now()  " << steady_cost << " ns per read\n";

  const auto& tsc = TscClock::instance();
  if (tsc.usable()) {
    const double tsc_cost = average_cost_ns(samples, [] { return TscClock::cycles(); });
    std::cout << "rdtscp               " << tsc_cost << " ns per read (calibrated at "
              << std::setprecision(3) << tsc.ghz() << " GHz)\n"
              << std::setprecision(2);

    // Validate before trusting: a TSC that disagrees with CLOCK_MONOTONIC over
    // a long interval is a TSC that must not be used for latency.
    const uint64_t ns_start = now_ns();
    const uint64_t cycles_start = TscClock::cycles();
    while (now_ns() - ns_start < validate_ms * 1'000'000) {
    }
    const uint64_t steady_elapsed = now_ns() - ns_start;
    const uint64_t tsc_elapsed = tsc.delta_to_ns(TscClock::cycles() - cycles_start);
    const double drift =
        100.0 * (static_cast<double>(tsc_elapsed) - static_cast<double>(steady_elapsed)) /
        static_cast<double>(steady_elapsed);
    std::cout << "agreement over " << validate_ms << " ms: steady " << steady_elapsed
              << " ns, tsc " << tsc_elapsed << " ns, drift " << drift << "%\n";
    if (drift > 0.5 || drift < -0.5) {
      std::cout << "  -> more than 0.5% apart. The TSC path is NOT usable for latency here.\n";
    } else {
      std::cout << "  -> within 0.5%. The TSC path would be usable, and would save about "
                << steady_cost - tsc_cost << " ns per read.\n";
    }
  } else {
    const auto& info = env_info();
    std::cout << "rdtscp               unusable: constant_tsc=" << info.tsc_constant
              << " nonstop_tsc=" << info.tsc_nonstop << '\n'
              << "  -> the counter can change rate or stop, so it cannot measure time here.\n";
  }

  const Histogram gaps = successive_gaps(std::min<uint64_t>(samples, 5'000'000));
  const auto ns = [](uint64_t value) { return value; };
  std::cout << "\ngap between consecutive now_ns() calls, ns:\n"
            << "  min " << ns(gaps.min()) << "  p50 " << ns(gaps.percentile(50)) << "  p99 "
            << ns(gaps.percentile(99)) << "  p99.9 " << ns(gaps.percentile(99.9)) << "  max "
            << ns(gaps.max()) << '\n'
            << "  min is the clock read itself; the tail is this machine interfering with the\n"
            << "  measurement, and it bounds how small a p99.9 claim can be believed.\n";
  return 0;
}
