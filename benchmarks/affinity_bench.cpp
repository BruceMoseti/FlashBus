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

BenchmarkResult run(const Placement& placement, uint64_t messages, size_t capacity,
                    uint64_t rate) {
  SpscRing<Item> ring(capacity);
  Histogram latency;

  const Rusage usage_before = rusage_now();
  const uint64_t started = now_ns();

  // Both sides run on threads of their own. Pinning the calling thread would
  // stick for the rest of the process and be inherited by every later thread,
  // so one pinned placement would silently contaminate all the placements after
  // it -- including the unpinned baseline.
  std::thread producer([&] {
    if (placement.producer_cpu >= 0) pin_to_cpu(static_cast<unsigned>(placement.producer_cpu));
    Item item;
    const Pacer pacer(rate);
    for (uint64_t i = 0; i < messages; ++i) {
      if (rate != 0) pacer.wait_for(i);
      item.sequence = i;
      item.timestamp_ns = now_ns();
      while (!ring.try_push(item)) {
      }
    }
  });

  uint64_t received = 0;
  std::thread consumer([&] {
    if (placement.consumer_cpu >= 0) pin_to_cpu(static_cast<unsigned>(placement.consumer_cpu));
    Item item;
    while (received < messages) {
      if (!ring.try_pop(item)) continue;
      const uint64_t now = now_ns();
      if (now > item.timestamp_ns) latency.record(now - item.timestamp_ns);
      ++received;
    }
  });
  producer.join();
  consumer.join();

  const double elapsed = static_cast<double>(now_ns() - started) / 1e9;
  const Rusage usage_after = rusage_now();

  BenchmarkResult result;
  result.benchmark = rate == 0 ? "affinity_saturated" : "affinity_paced";
  result.variant = placement.name;
  result.payload_bytes = sizeof(Item);
  result.capacity = ring.capacity();
  result.target_rate = rate;
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
  args.reject_unknown({"messages", "capacity", "rate", "repeat", "csv", "help"});
  if (args.flag("help")) {
    std::cout << "usage: affinity_bench [--messages N] [--capacity N] [--rate MSG/S]\n"
                 "                      [--repeat N] [--csv PATH]\n"
                 "  Runs each placement twice: saturated for throughput, and paced at --rate\n"
                 "  so the queue stays shallow and the latency figure is handoff cost.\n";
    return 0;
  }
  const auto messages = args.number<uint64_t>("messages", 5'000'000);
  const auto capacity = args.number<size_t>("capacity", 4096);
  const auto rate = args.number<uint64_t>("rate", 1'000'000);
  const unsigned repeat = std::max(1u, args.number<unsigned>("repeat", 5));
  const std::string csv = args.string("csv", "");

  print_env(std::cout);
  const unsigned cpus = env_info().cpu_count;

  // Probe inside a throwaway thread. Calling pin_to_cpu here would pin this
  // thread for the rest of the process, and since the consumer runs on this
  // thread and new threads inherit the affinity mask, the "unpinned" baseline
  // would silently run both threads on CPU 0 and report a quarter of the real
  // throughput.
  bool can_pin = false;
  std::thread probe([&] { can_pin = pin_to_cpu(0); });
  probe.join();
  if (!can_pin) {
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
  // Repeated, because the spread between runs on this machine is wide enough
  // that a single run cannot distinguish one placement from another.
  const auto measure = [&](const Placement& placement, uint64_t paced_rate) {
    std::vector<BenchmarkResult> runs;
    for (unsigned i = 0; i < repeat; ++i) {
      runs.push_back(run(placement, placement.messages, capacity, paced_rate));
      if (writer != nullptr) writer->add(runs.back());
    }
    std::sort(runs.begin(), runs.end(),
              [](const BenchmarkResult& a, const BenchmarkResult& b) {
                return a.throughput_msg_s < b.throughput_msg_s;
              });
    print_result(std::cout, runs[runs.size() / 2]);
    std::cout << "   spread        " << repeat << " runs, throughput "
              << runs.front().throughput_msg_s / 1e6 << " to "
              << runs.back().throughput_msg_s / 1e6 << " M msg/s (median above)\n";
  };

  std::cout << "--- saturated: how fast the handoff goes. The latency column here is just\n"
               "    ring capacity divided by throughput, so read the throughput.\n";
  for (const Placement& placement : placements) measure(placement, 0);

  std::cout << "\n--- paced at " << rate << " msg/s: the queue stays shallow, so the latency\n"
               "    column is the handoff cost and placement is the only thing varying.\n";
  for (const Placement& placement : placements) {
    // The same-core placement cannot reach the paced rate at all, so pacing it
    // would measure the scheduler's timeslice rather than a handoff.
    if (placement.producer_cpu == placement.consumer_cpu && placement.producer_cpu >= 0) continue;
    measure(placement, rate);
  }
  return 0;
}
