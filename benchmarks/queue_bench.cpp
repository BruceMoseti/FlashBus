// Queue microbenchmark: the mutex baseline against the SPSC ring, plus the
// cache-layout variants.
//
// Two modes, because they answer different questions and one contaminates the
// other:
//
//   throughput  no timestamps at all. How fast can the handoff go.
//   latency     a clock read on both sides of every item. What the handoff
//               costs end to end, *including* two clock reads. At this scale
//               the clock is not a rounding error, so the cost of a clock read
//               is printed next to the result instead of being quietly
//               subtracted.

#include <atomic>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "flashbus/cli.hpp"
#include "flashbus/clock.hpp"
#include "flashbus/metrics.hpp"
#include "flashbus/mutex_queue.hpp"
#include "flashbus/platform.hpp"
#include "flashbus/ring_buffer.hpp"

namespace {

using namespace flashbus;

template <size_t PayloadBytes>
struct Item {
  uint64_t timestamp_ns = 0;
  uint64_t sequence = 0;
  std::byte payload[PayloadBytes]{};
};

struct Options {
  uint64_t messages = 10'000'000;
  size_t capacity = 4096;
  int producer_cpu = -1;
  int consumer_cpu = -1;
  bool latency_mode = true;
  bool throughput_mode = true;
};

/// Measures the cost of one `now_ns()` so the latency numbers can be read with
/// the measurement overhead in view.
uint64_t clock_read_cost_ns() {
  constexpr uint64_t kSamples = 2'000'000;
  const uint64_t start = now_ns();
  uint64_t sink = 0;
  for (uint64_t i = 0; i < kSamples; ++i) sink += now_ns();
  const uint64_t elapsed = now_ns() - start;
  // Keep the loop from being optimised away without perturbing the timing.
  if (sink == 0) std::cerr << "";
  return elapsed / kSamples;
}

template <typename Queue, typename ItemType>
BenchmarkResult run(const std::string& variant, const Options& options, bool record_latency) {
  Queue queue(options.capacity);
  Histogram latency;
  std::atomic<uint64_t> push_retries{0};

  const Rusage usage_before = rusage_now();
  const uint64_t started = now_ns();

  std::thread producer([&] {
    if (options.producer_cpu >= 0) pin_to_cpu(static_cast<unsigned>(options.producer_cpu));
    ItemType item;
    uint64_t retries = 0;
    for (uint64_t i = 0; i < options.messages; ++i) {
      item.sequence = i;
      if (record_latency) item.timestamp_ns = now_ns();
      while (!queue.try_push(item)) ++retries;
    }
    push_retries.store(retries, std::memory_order_relaxed);
  });

  uint64_t received = 0;
  uint64_t order_violations = 0;
  {
    if (options.consumer_cpu >= 0) pin_to_cpu(static_cast<unsigned>(options.consumer_cpu));
    ItemType item;
    while (received < options.messages) {
      if (!queue.try_pop(item)) continue;
      if (item.sequence != received) ++order_violations;
      if (record_latency) {
        const uint64_t now = now_ns();
        if (now > item.timestamp_ns) latency.record(now - item.timestamp_ns);
      }
      ++received;
    }
  }
  producer.join();

  const uint64_t finished = now_ns();
  const Rusage usage_after = rusage_now();
  const double elapsed = static_cast<double>(finished - started) / 1e9;

  BenchmarkResult result;
  result.benchmark = record_latency ? "queue_latency" : "queue_throughput";
  result.variant = variant;
  result.payload_bytes = static_cast<uint32_t>(sizeof(ItemType));
  result.capacity = queue.capacity();
  result.messages_sent = options.messages;
  result.messages_received = received;
  result.sequence_gaps = order_violations;
  result.duration_s = elapsed;
  result.throughput_msg_s = elapsed > 0.0 ? static_cast<double>(received) / elapsed : 0.0;
  result.fill_latency(latency);
  result.fill_cost(usage_after - usage_before, elapsed);
  return result;
}

template <size_t PayloadBytes>
void run_payload(const Options& options, ResultWriter* writer) {
  using ItemType = Item<PayloadBytes>;
  std::vector<BenchmarkResult> results;

  const auto collect = [&](BenchmarkResult result) {
    print_result(std::cout, result);
    if (writer != nullptr) writer->add(result);
  };

  if (options.throughput_mode) {
    collect(run<MutexQueue<ItemType>, ItemType>("mutex-queue", options, false));
    collect(run<SpscRing<ItemType>, ItemType>("spsc-padded", options, false));
    collect(run<SpscRingUnpadded<ItemType>, ItemType>("spsc-unpadded", options, false));
  }
  if (options.latency_mode) {
    collect(run<MutexQueue<ItemType>, ItemType>("mutex-queue", options, true));
    collect(run<SpscRing<ItemType>, ItemType>("spsc-padded", options, true));
    collect(run<SpscRingUnpadded<ItemType>, ItemType>("spsc-unpadded", options, true));
  }
}

void dispatch(size_t payload_bytes, const Options& options, ResultWriter* writer) {
  switch (payload_bytes) {
    case 16: run_payload<16>(options, writer); return;
    case 32: run_payload<32>(options, writer); return;
    case 48: run_payload<48>(options, writer); return;
    case 64: run_payload<64>(options, writer); return;
    case 112: run_payload<112>(options, writer); return;
    case 240: run_payload<240>(options, writer); return;
    case 1008: run_payload<1008>(options, writer); return;
    default:
      std::cerr << "queue_bench: --payload must be one of 16 32 48 64 112 240 1008 "
                   "(these are the user bytes; each item carries 16 more for a timestamp and "
                   "sequence, so the slot sizes come out at 32 48 64 80 128 256 1024)\n";
      std::exit(2);
  }
}

}  // namespace

int main(int argc, char** argv) {
  const cli::Args args(argc, argv);
  args.reject_unknown({"messages", "capacity", "payload", "producer-cpu", "consumer-cpu", "csv",
                       "mode", "help"});
  if (args.flag("help")) {
    std::cout << "usage: queue_bench [--messages N] [--capacity N] [--payload BYTES]\n"
                 "                   [--producer-cpu N] [--consumer-cpu N] "
                 "[--mode both|latency|throughput]\n"
                 "                   [--csv PATH]\n"
                 "  --payload may be repeated as a comma-separated list.\n";
    return 0;
  }

  Options options;
  options.messages = args.number<uint64_t>("messages", options.messages);
  options.capacity = args.number<size_t>("capacity", options.capacity);
  options.producer_cpu = args.number<int>("producer-cpu", -1);
  options.consumer_cpu = args.number<int>("consumer-cpu", -1);
  const std::string mode = args.string("mode", "both");
  options.latency_mode = (mode == "both" || mode == "latency");
  options.throughput_mode = (mode == "both" || mode == "throughput");
  if (!options.latency_mode && !options.throughput_mode) {
    std::cerr << "queue_bench: --mode must be both, latency or throughput\n";
    return 2;
  }

  const std::string csv = args.string("csv", "");
  std::unique_ptr<ResultWriter> writer;
  if (!csv.empty()) writer = std::make_unique<ResultWriter>(csv);

  print_env(std::cout);
  std::cout << "one now_ns() costs about " << clock_read_cost_ns()
            << " ns; latency-mode numbers include two of them\n\n";

  std::string payload_list = args.string("payload", "48");
  size_t start = 0;
  while (start <= payload_list.size()) {
    const size_t comma = payload_list.find(',', start);
    const std::string token = payload_list.substr(
        start, comma == std::string::npos ? std::string::npos : comma - start);
    if (!token.empty()) dispatch(std::stoul(token), options, writer.get());
    if (comma == std::string::npos) break;
    start = comma + 1;
  }
  return 0;
}
