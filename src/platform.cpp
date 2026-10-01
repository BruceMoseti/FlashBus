#include "flashbus/platform.hpp"

#include <linux/perf_event.h>
#include <pthread.h>
#include <sched.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <fstream>
#include <sstream>
#include <thread>

#include <boost/version.hpp>

#include "flashbus/clock.hpp"

namespace flashbus {
namespace {

std::string read_first_line(const char* path) {
  std::ifstream in(path);
  std::string line;
  if (in && std::getline(in, line)) return line;
  return {};
}

/// /proc/cpuinfo's "model name" and "flags" for the first core.
void read_cpuinfo(std::string& model, bool& constant_tsc, bool& nonstop_tsc) {
  std::ifstream in("/proc/cpuinfo");
  std::string line;
  while (std::getline(in, line)) {
    if (model.empty() && line.rfind("model name", 0) == 0) {
      const auto colon = line.find(':');
      if (colon != std::string::npos) {
        model = line.substr(colon + 1);
        const auto start = model.find_first_not_of(" \t");
        model = start == std::string::npos ? std::string{} : model.substr(start);
      }
    } else if (line.rfind("flags", 0) == 0) {
      constant_tsc = line.find(" constant_tsc") != std::string::npos;
      nonstop_tsc = line.find(" nonstop_tsc") != std::string::npos;
      if (!model.empty()) break;
    }
  }
}

/// Opens a hardware cycle counter and immediately closes it. A virtualised
/// host without a vPMU fails here, which is exactly the condition that makes
/// `perf stat -e cycles` print "<not supported>".
bool probe_hw_pmu() {
  perf_event_attr attr{};
  attr.size = sizeof(attr);
  attr.type = PERF_TYPE_HARDWARE;
  attr.config = PERF_COUNT_HW_CPU_CYCLES;
  attr.disabled = 1;
  attr.exclude_kernel = 1;
  attr.exclude_hv = 1;
  const long fd = syscall(__NR_perf_event_open, &attr, 0, -1, -1, 0);
  if (fd < 0) return false;
  ::close(static_cast<int>(fd));
  return true;
}

EnvInfo build_env_info() {
  EnvInfo info;
  read_cpuinfo(info.cpu_model, info.tsc_constant, info.tsc_nonstop);
  if (info.cpu_model.empty()) info.cpu_model = "unknown";
  info.cpu_count = static_cast<unsigned>(sysconf(_SC_NPROCESSORS_ONLN));
  info.cpu_available = available_cpu_count();

  utsname uts{};
  if (uname(&uts) == 0) {
    info.kernel = uts.release;
    info.os = std::string(uts.sysname) + " " + uts.machine;
  }

  const std::string pretty = [] {
    std::ifstream in("/etc/os-release");
    std::string line;
    while (std::getline(in, line)) {
      if (line.rfind("PRETTY_NAME=", 0) == 0) {
        auto value = line.substr(12);
        if (!value.empty() && value.front() == '"') value = value.substr(1, value.size() - 2);
        return value;
      }
    }
    return std::string{};
  }();
  if (!pretty.empty()) info.os += " / " + pretty;

  std::ostringstream compiler;
#if defined(__clang__)
  compiler << "clang++ " << __clang_major__ << "." << __clang_minor__ << "." << __clang_patchlevel__;
#elif defined(__GNUC__)
  compiler << "g++ " << __GNUC__ << "." << __GNUC_MINOR__ << "." << __GNUC_PATCHLEVEL__;
#else
  compiler << "unknown";
#endif
  info.compiler = compiler.str();

#ifdef FLASHBUS_CXX_FLAGS
  info.compiler_flags = FLASHBUS_CXX_FLAGS;
#endif
#ifdef FLASHBUS_BUILD_TYPE
  info.build_type = FLASHBUS_BUILD_TYPE;
#endif
#ifdef FLASHBUS_SANITIZER_NAME
  info.sanitizer = FLASHBUS_SANITIZER_NAME;
#endif
  info.boost_version = std::to_string(BOOST_VERSION / 100000) + "." +
                       std::to_string(BOOST_VERSION / 100 % 1000) + "." +
                       std::to_string(BOOST_VERSION % 100);

  const auto governor = read_first_line("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor");
  if (!governor.empty()) info.scaling_governor = governor;

  info.hw_pmu_available = probe_hw_pmu();
  return info;
}

}  // namespace

const EnvInfo& env_info() {
  static const EnvInfo info = build_env_info();
  return info;
}

unsigned available_cpu_count() {
  cpu_set_t set;
  CPU_ZERO(&set);
  if (sched_getaffinity(0, sizeof(set), &set) == 0) {
    const int count = CPU_COUNT(&set);
    if (count > 0) return static_cast<unsigned>(count);
  }
  const long online = sysconf(_SC_NPROCESSORS_ONLN);
  return online > 0 ? static_cast<unsigned>(online) : 1u;
}

bool pin_to_cpu(unsigned cpu) {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
}

int current_cpu() { return sched_getcpu(); }

Rusage rusage_now() {
  rusage usage{};
  Rusage out;
  if (getrusage(RUSAGE_SELF, &usage) != 0) return out;
  out.voluntary_ctx_switches = static_cast<uint64_t>(usage.ru_nvcsw);
  out.involuntary_ctx_switches = static_cast<uint64_t>(usage.ru_nivcsw);
  out.minor_faults = static_cast<uint64_t>(usage.ru_minflt);
  out.major_faults = static_cast<uint64_t>(usage.ru_majflt);
  out.user_s = static_cast<double>(usage.ru_utime.tv_sec) +
               static_cast<double>(usage.ru_utime.tv_usec) * 1e-6;
  out.sys_s = static_cast<double>(usage.ru_stime.tv_sec) +
              static_cast<double>(usage.ru_stime.tv_usec) * 1e-6;
  return out;
}

Rusage operator-(const Rusage& a, const Rusage& b) {
  return Rusage{a.voluntary_ctx_switches - b.voluntary_ctx_switches,
                a.involuntary_ctx_switches - b.involuntary_ctx_switches,
                a.minor_faults - b.minor_faults,
                a.major_faults - b.major_faults,
                a.user_s - b.user_s,
                a.sys_s - b.sys_s};
}

// Defined weakly so that targets which do not link flashbus_alloc_counter
// still link, and simply report zero allocations.
__attribute__((weak)) uint64_t heap_allocation_count() { return 0; }
__attribute__((weak)) void reset_heap_allocation_count() {}

TscClock::TscClock() noexcept {
  const auto& info = env_info();
  usable_ = info.tsc_constant && info.tsc_nonstop;
  if (!usable_) return;

  // Two samples 20 ms apart. Enough to get well under 0.1% error on the
  // frequency without making every process that touches the clock slow.
  const uint64_t ns0 = now_ns();
  const uint64_t c0 = cycles();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  const uint64_t ns1 = now_ns();
  const uint64_t c1 = cycles();

  const auto cycle_delta = static_cast<double>(c1 - c0);
  const auto ns_delta = static_cast<double>(ns1 - ns0);
  if (cycle_delta <= 0.0 || ns_delta <= 0.0) {
    usable_ = false;
    return;
  }
  const double ns_per_cycle = ns_delta / cycle_delta;
  ghz_ = 1.0 / ns_per_cycle;
  ns_per_cycle_q32_ = static_cast<uint64_t>(ns_per_cycle * 4294967296.0);
}

const TscClock& TscClock::instance() noexcept {
  static const TscClock clock;
  return clock;
}

}  // namespace flashbus
