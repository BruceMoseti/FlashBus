#include "flashbus/metrics.hpp"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>

namespace flashbus {
namespace {

constexpr const char* kColumns =
    "benchmark,variant,payload_bytes,producers,consumers,capacity,target_rate,warmup_s,"
    "messages_sent,messages_received,messages_dropped,sequence_gaps,duration_s,"
    "throughput_msg_s,cpu_cores_used,min_ns,p50_ns,p95_ns,p99_ns,p999_ns,max_ns,mean_ns,"
    "heap_allocations,voluntary_ctx_switches,involuntary_ctx_switches,minor_faults,"
    "major_faults,cpu_model,cpu_count,cpu_available,kernel,os,compiler,compiler_flags,"
    "build_type,"
    "sanitizer,boost_version,scaling_governor,hw_pmu_available";

std::string quote(const std::string& field) {
  std::string out = "\"";
  for (const char c : field) {
    if (c == '"') out += '"';
    out += c;
  }
  out += '"';
  return out;
}

}  // namespace

ResultWriter::ResultWriter(std::string path) : path_(std::move(path)) {
  const std::filesystem::path file(path_);
  if (file.has_parent_path()) std::filesystem::create_directories(file.parent_path());
  header_written_ = std::filesystem::exists(file) && std::filesystem::file_size(file) > 0;
}

void ResultWriter::add(const BenchmarkResult& r) {
  std::ofstream out(path_, std::ios::app);
  if (!out) return;
  if (!header_written_) {
    out << kColumns << '\n';
    header_written_ = true;
  }
  const auto& e = env_info();
  out << std::fixed << std::setprecision(3);
  out << quote(r.benchmark) << ',' << quote(r.variant) << ',' << r.payload_bytes << ','
      << r.producers << ',' << r.consumers << ',' << r.capacity << ',' << r.target_rate << ','
      << r.warmup_s << ',' << r.messages_sent << ',' << r.messages_received << ','
      << r.messages_dropped << ',' << r.sequence_gaps << ',' << r.duration_s << ','
      << r.throughput_msg_s << ',' << r.cpu_cores_used << ',' << r.min_ns << ',' << r.p50_ns << ','
      << r.p95_ns << ',' << r.p99_ns << ',' << r.p999_ns << ',' << r.max_ns << ',' << r.mean_ns
      << ',' << r.heap_allocations << ',' << r.voluntary_ctx_switches << ','
      << r.involuntary_ctx_switches << ',' << r.minor_faults << ',' << r.major_faults << ','
      << quote(e.cpu_model) << ',' << e.cpu_count << ',' << e.cpu_available << ','
      << quote(e.kernel) << ',' << quote(e.os)
      << ',' << quote(e.compiler) << ',' << quote(e.compiler_flags) << ',' << quote(e.build_type)
      << ',' << quote(e.sanitizer) << ',' << quote(e.boost_version) << ','
      << quote(e.scaling_governor) << ',' << (e.hw_pmu_available ? 1 : 0) << '\n';
}

void print_env(std::ostream& os) {
  const auto& e = env_info();
  os << "CPU:          " << e.cpu_model << " (" << e.cpu_count << " logical, "
     << e.cpu_available << " available to this process)\n"
     << "Kernel:       " << e.kernel << "\n"
     << "OS:           " << e.os << "\n"
     << "Compiler:     " << e.compiler << " [" << e.build_type << "]\n"
     << "Flags:        " << e.compiler_flags << "\n"
     << "Sanitizer:    " << e.sanitizer << "\n"
     << "Boost:        " << e.boost_version << "\n"
     << "Governor:     " << e.scaling_governor << "\n"
     << "TSC:          " << (e.tsc_constant ? "constant" : "variable") << ", "
     << (e.tsc_nonstop ? "nonstop" : "halts") << "\n"
     << "Hardware PMU: " << (e.hw_pmu_available ? "available" : "NOT available (no vPMU)")
     << "\n";
}

void print_result(std::ostream& os, const BenchmarkResult& r) {
  const auto us = [](uint64_t ns) { return static_cast<double>(ns) / 1000.0; };
  os << std::fixed;
  os << "-- " << r.benchmark << " / " << r.variant << '\n';
  os << std::setprecision(0) << "   payload       " << r.payload_bytes << " B, " << r.producers
     << "P/" << r.consumers << "C, capacity " << r.capacity;
  if (r.target_rate) os << ", target " << r.target_rate << " msg/s";
  os << '\n';
  os << std::setprecision(3) << "   duration      " << r.duration_s << " s (warmup "
     << r.warmup_s << " s)\n";
  os << "   throughput    " << r.throughput_msg_s / 1e6 << " M msg/s\n";
  os << "   latency us    p50 " << us(r.p50_ns) << "  p95 " << us(r.p95_ns) << "  p99 "
     << us(r.p99_ns) << "  p99.9 " << us(r.p999_ns) << "  max " << us(r.max_ns) << '\n';
  os << "   sent/recv     " << std::setprecision(0) << static_cast<double>(r.messages_sent) << " / "
     << static_cast<double>(r.messages_received) << "   dropped " << r.messages_dropped
     << "   gaps " << r.sequence_gaps << '\n';
  os << std::setprecision(2) << "   cpu           " << r.cpu_cores_used << " cores"
     << "   ctx-sw " << r.voluntary_ctx_switches << "v/" << r.involuntary_ctx_switches << "i"
     << "   faults " << r.minor_faults << "m/" << r.major_faults << "M"
     << "   heap allocs " << r.heap_allocations << '\n';
}

}  // namespace flashbus
