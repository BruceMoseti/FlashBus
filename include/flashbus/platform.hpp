#pragma once

// Linux/machine facts that a benchmark result is meaningless without, plus the
// two OS knobs FlashBus actually uses: CPU affinity and resource accounting.

#include <cstddef>
#include <cstdint>
#include <string>

namespace flashbus {

/// Every x86_64 and AArch64 target FlashBus runs on has 64-byte lines. Used for
/// padding hot state apart and for aligning ring slots.
constexpr size_t kCacheLineSize = 64;

struct EnvInfo {
  std::string cpu_model;
  unsigned cpu_count = 0;
  std::string kernel;
  std::string os;
  std::string compiler;
  std::string compiler_flags;
  std::string build_type;
  std::string sanitizer;
  std::string boost_version;
  /// "unavailable" inside VMs that expose no cpufreq interface. A missing
  /// governor means frequency behaviour is outside our control and outside our
  /// measurement, which is worth printing rather than hiding.
  std::string scaling_governor = "unavailable";
  bool tsc_constant = false;
  bool tsc_nonstop = false;
  /// True only if a hardware PMU event can actually be opened. Virtualised
  /// hosts usually expose no vPMU, which makes `perf stat -e cycles` report
  /// "<not supported>" and makes cache-miss claims unsupportable.
  bool hw_pmu_available = false;
};

const EnvInfo& env_info();

/// Pins the calling thread to one logical CPU. Returns false when the
/// container or cpuset forbids it; callers must treat affinity as advisory.
bool pin_to_cpu(unsigned cpu);
/// Logical CPU the caller is running on right now, or -1.
int current_cpu();

struct Rusage {
  uint64_t voluntary_ctx_switches = 0;
  uint64_t involuntary_ctx_switches = 0;
  uint64_t minor_faults = 0;
  uint64_t major_faults = 0;
  double user_s = 0.0;
  double sys_s = 0.0;
};

Rusage rusage_now();
Rusage operator-(const Rusage& a, const Rusage& b);

/// Number of global `operator new` calls so far. Returns 0 unless the target
/// links the `flashbus_alloc_counter` object library, which is how the
/// "no heap allocation on the hot path" claim is checked instead of asserted.
uint64_t heap_allocation_count();
void reset_heap_allocation_count();

}  // namespace flashbus
