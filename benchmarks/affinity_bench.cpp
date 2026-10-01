// Where threads run, and whether it matters.
//
// No claim is made that pinning helps. The experiment runs the same SPSC
// handoff under several placements and reports what happened, including the
// placements that make things worse.

#include <atomic>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "flashbus/cli.hpp"
#include "flashbus/clock.hpp"
#include "flashbus/metrics.hpp"
#include "flashbus/platform.hpp"
#include "flashbus/ring_buffer.hpp"

namespace {

using namespace flashbus;

struct Item {
  uint64_t timestamp_ns = 0;
  uint64_t sequence = 0;
  std::byte payload[48]{};
};

struct Placement {
  std::string name;
  int producer_cpu;
  int consumer_cpu;
  /// Overridden for the pathological same-CPU case, where progress depends on
  /// the scheduler preempting a spinning thread and the run takes orders of
  /// magnitude longer per message.
  uint64_t messages = 0;
};

std::string read_file(const std::string& path) {
  std::ifstream in(path);
  std::string contents;
  std::getline(in, contents);
  return contents;
}

/// Finds a CPU that shares a physical core with CPU 0, i.e. an SMT sibling.
/// Returns nothing when SMT is off or the topology is hidden, which is common
/// in virtual machines and is worth saying rather than guessing.
std::optional<unsigned> smt_sibling_of_cpu0() {
  const std::string siblings =
      read_file("/sys/devices/system/cpu/cpu0/topology/thread_siblings_list");
  // Format is like "0,4" or "0-1" when SMT is on, "0" when it is not.
  for (size_t i = 0; i < siblings.size(); ++i) {
    if (siblings[i] == ',' || siblings[i] == '-') {
      return static_cast<unsigned>(std::stoul(siblings.substr(i + 1)));
    }
  }
  return std::nullopt;
}

BenchmarkResult run(const Placement& placement, uint64_t messages, size_t capacity) {
  SpscRing<Item> ring(capacity);
  Histogram latency;

  const Rusage usage_before = rusage_now();
  const uint64_t started = now_ns();

  std::thread producer([&] {
    if (placement.producer_cpu >= 0) pin_to_cpu(static_cast<unsigned>(placement.producer_cpu));
    Item item;
    for (uint64_t i = 0; i < messages; ++i) {
      item.sequence = i;
      item.timestamp_ns = now_ns();
      while (!ring.try_push(item)) {
      }
    }
  });

  if (placement.consumer_cpu >= 0) pin_to_cpu(static_cast<unsigned>(placement.consumer_cpu));
  Item item;
  uint64_t received = 0;
  while (received < messages) {
    if (!ring.try_pop(item)) continue;
    const uint64_t now = now_ns();
    if (now > item.timestamp_ns) latency.record(now - item.timestamp_ns);
    ++received;
  }
  producer.join();

  const double elapsed = static_cast<double>(now_ns() - started) / 1e9;
  const Rusage usage_after = rusage_now();

  BenchmarkResult result;
  result.benchmark = "affinity";
  result.variant = placement.name;
  result.payload_bytes = sizeof(Item);
  result.capacity = ring.capacity();
  result.messages_sent = messages;
  result.messages_received = received;
  result.duration_s = elapsed;
  result.throughput_msg_s = elapsed > 0.0 ? static_cast<double>(received) / elapsed : 0.0;
  result.fill_latency(latency);
  result.fill_cost(usage_after - usage_before, elapsed);
  return result;
}

}  // namespace

int main(int argc, char** argv) {
  const cli::Args args(argc, argv);
  args.reject_unknown({"messages", "capacity", "csv", "help"});
  if (args.flag("help")) {
    std::cout << "usage: affinity_bench [--messages N] [--capacity N] [--csv PATH]\n";
    return 0;
  }
  const auto messages = args.number<uint64_t>("messages", 5'000'000);
  const auto capacity = args.number<size_t>("capacity", 4096);
  const std::string csv = args.string("csv", "");

  print_env(std::cout);
  const unsigned cpus = env_info().cpu_count;
  if (!pin_to_cpu(0)) {
    std::cout << "\nthis process is not allowed to set CPU affinity; only the unpinned "
                 "result is meaningful\n";
  }

  std::vector<Placement> placements;
  placements.push_back({"unpinned (scheduler decides)", -1, -1, messages});
  if (cpus >= 2) placements.push_back({"adjacent cpus 0,1", 0, 1, messages});
  if (cpus >= 4) {
    placements.push_back(
        {"distant cpus 0," + std::to_string(cpus - 1), 0, static_cast<int>(cpus - 1), messages});
  }
  placements.push_back({"same cpu 0 (both threads share one core)", 0, 0,
                        std::min<uint64_t>(messages, 200'000)});
  if (const auto sibling = smt_sibling_of_cpu0()) {
    placements.push_back({"smt siblings 0," + std::to_string(*sibling), 0,
                          static_cast<int>(*sibling), messages});
  } else {
    std::cout << "\nno SMT sibling for cpu0: either SMT is off or the topology is not exposed, "
                 "so the\nhyperthread-pairing comparison cannot be run on this machine.\n";
  }

  std::cout << '\n';
  std::unique_ptr<ResultWriter> writer;
  if (!csv.empty()) writer = std::make_unique<ResultWriter>(csv);
  for (const Placement& placement : placements) {
    const BenchmarkResult result = run(placement, placement.messages, capacity);
    print_result(std::cout, result);
    if (writer != nullptr) writer->add(result);
  }
  return 0;
}
