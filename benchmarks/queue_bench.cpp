// Queue microbenchmark: the mutex baseline against the SPSC ring, plus the
// cache-layout variants. This is performance case study 1.
//
// Three things here exist because the first version of this benchmark was
// misleading without them.
//
// Modes. In throughput mode there is no clock in the loop at all; in latency
// mode there is a clock read on both sides of every item. A clock read costs
// about as much as an SPSC handoff on this hardware, so one mode cannot answer
// both questions, and the cost of a clock read is printed next to the result
// rather than quietly subtracted from it.
//
// Pacing. With an unthrottled producer, a queue that cannot keep up simply
// stays full, and the measured latency becomes capacity divided by throughput
// -- a throughput result wearing a latency costume. Paced below saturation the
// queue stays shallow and the number is the handoff cost. Both are reported,
// because the saturated case is a real operating regime and worth seeing.
//
// Repetition. This is a shared virtual machine. One run of anything here can be
// off by a factor of two, so every configuration runs several times and the
// median is what gets reported, with the spread alongside it.

#include <algorithm>
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
  uint64_t messages = 20'000'000;
  size_t capacity = 4096;
  uint64_t rate = 0;
  unsigned repeat = 5;
  int producer_cpu = -1;
  int consumer_cpu = -1;
  bool latency_mode = true;
  bool throughput_mode = true;
};

double clock_read_cost_ns() {
  constexpr uint64_t kSamples = 2'000'000;
  const uint64_t start = now_ns();
  uint64_t sink = 0;
  for (uint64_t i = 0; i < kSamples; ++i) sink += now_ns();
  const uint64_t elapsed = now_ns() - start;
  if (sink == 0) std::cerr << "";  // keep the loop from being optimised away
  return static_cast<double>(elapsed) / static_cast<double>(kSamples);
}

template <typename Queue, typename ItemType>
BenchmarkResult run_once(const std::string& variant, const Options& options,
                         bool record_latency) {
  Queue queue(options.capacity);
  Histogram latency;
  // Thread startup and the first touch of every page belong to setup, not to
  // the steady state, so the clock starts once the warmup has gone through.
  const uint64_t warmup = options.messages / 10;
  std::atomic<uint64_t> warmed{0};
  std::atomic<uint64_t> measured_start_ns{0};

  std::thread producer([&] {
    if (options.producer_cpu >= 0) pin_to_cpu(static_cast<unsigned>(options.producer_cpu));
    ItemType item;
    const Pacer pacer(options.rate);
    for (uint64_t i = 0; i < options.messages; ++i) {
      if (options.rate != 0) pacer.wait_for(i);
      item.sequence = i;
      if (record_latency) item.timestamp_ns = now_ns();
      while (!queue.try_push(item)) {
      }
    }
  });

  uint64_t received = 0;
  uint64_t order_violations = 0;
  {
    if (options.consumer_cpu >= 0) pin_to_cpu(static_cast<unsigned>(options.consumer_cpu));
    ItemType item;
    while (received < options.messages) {
      if (!queue.try_pop(item)) continue;
      if (item.sequence != received) ++order_violations;
      ++received;
      if (received == warmup) {
        latency.clear();
        measured_start_ns.store(now_ns(), std::memory_order_relaxed);
      } else if (received > warmup && record_latency) {
        const uint64_t now = now_ns();
        if (now > item.timestamp_ns) latency.record(now - item.timestamp_ns);
      }
    }
  }
  const uint64_t finished = now_ns();
  producer.join();

  const uint64_t started = measured_start_ns.load(std::memory_order_relaxed);
  const double elapsed =
      started != 0 ? static_cast<double>(finished - started) / 1e9 : 0.0;
  const uint64_t counted = options.messages - warmup;

  BenchmarkResult result;
  result.benchmark = record_latency ? "queue_latency" : "queue_throughput";
  result.variant = variant;
  result.payload_bytes = static_cast<uint32_t>(sizeof(ItemType));
  result.capacity = queue.capacity();
  result.target_rate = options.rate;
  result.messages_sent = options.messages;
  result.messages_received = received;
  result.sequence_gaps = order_violations;
  result.duration_s = elapsed;
  result.throughput_msg_s = elapsed > 0.0 ? static_cast<double>(counted) / elapsed : 0.0;
  result.fill_latency(latency);
  return result;
}

/// Median by throughput. Taking the median of each column separately would
/// invent a row that no run produced; this returns a row that actually happened.
BenchmarkResult median_by_throughput(std::vector<BenchmarkResult> results) {
  std::sort(results.begin(), results.end(), [](const BenchmarkResult& a, const BenchmarkResult& b) {
    return a.throughput_msg_s < b.throughput_msg_s;
  });
  return results[results.size() / 2];
}

template <typename Queue, typename ItemType>
void run_repeated(const std::string& variant, const Options& options, bool record_latency,
                  ResultWriter* writer) {
  std::vector<BenchmarkResult> results;
  const Rusage usage_before = rusage_now();
  const uint64_t wall_start = now_ns();
  for (unsigned i = 0; i < options.repeat; ++i) {
    results.push_back(run_once<Queue, ItemType>(variant, options, record_latency));
    if (writer != nullptr) writer->add(results.back());
  }
  const double wall = static_cast<double>(now_ns() - wall_start) / 1e9;

  BenchmarkResult median = median_by_throughput(results);
  median.fill_cost(rusage_now() - usage_before, wall);
  print_result(std::cout, median);

  const auto lowest = std::min_element(
      results.begin(), results.end(), [](const BenchmarkResult& a, const BenchmarkResult& b) {
        return a.throughput_msg_s < b.throughput_msg_s;
      });
  const auto highest = std::max_element(
      results.begin(), results.end(), [](const BenchmarkResult& a, const BenchmarkResult& b) {
        return a.throughput_msg_s < b.throughput_msg_s;
      });
  std::cout << "   spread        " << options.repeat << " runs, throughput "
            << lowest->throughput_msg_s / 1e6 << " to " << highest->throughput_msg_s / 1e6
            << " M msg/s (median reported above)\n";
  if (median.sequence_gaps != 0) {
    std::cout << "   ORDERING      " << median.sequence_gaps
              << " items arrived out of order -- this is a bug, not a slow run\n";
  }
}

template <size_t PayloadBytes>
void run_payload(const Options& options, ResultWriter* writer) {
  using ItemType = Item<PayloadBytes>;
  if (options.throughput_mode) {
    run_repeated<MutexQueue<ItemType>, ItemType>("mutex-queue", options, false, writer);
    run_repeated<SpscRing<ItemType>, ItemType>("spsc-padded", options, false, writer);
    run_repeated<SpscRingUnpadded<ItemType>, ItemType>("spsc-unpadded", options, false, writer);
  }
  if (options.latency_mode) {
    run_repeated<MutexQueue<ItemType>, ItemType>("mutex-queue", options, true, writer);
    run_repeated<SpscRing<ItemType>, ItemType>("spsc-padded", options, true, writer);
    run_repeated<SpscRingUnpadded<ItemType>, ItemType>("spsc-unpadded", options, true, writer);
  }
}

void dispatch(size_t payload_bytes, const Options& options, ResultWriter* writer) {
  switch (payload_bytes) {
    case 16: run_payload<16>(options, writer); return;
    case 48: run_payload<48>(options, writer); return;
    case 112: run_payload<112>(options, writer); return;
    case 240: run_payload<240>(options, writer); return;
    case 1008: run_payload<1008>(options, writer); return;
    default:
      std::cerr << "queue_bench: --payload must be one of 16 48 112 240 1008. Those are the\n"
                   "user bytes; each item carries 16 more for a timestamp and a sequence, so\n"
                   "the slot sizes come out at the round numbers 32 64 128 256 1024.\n";
      std::exit(2);
  }
}

}  // namespace

int main(int argc, char** argv) {
  const cli::Args args(argc, argv);
  args.reject_unknown({"messages", "capacity", "payload", "rate", "repeat", "producer-cpu",
                       "consumer-cpu", "csv", "mode", "help"});
  if (args.flag("help")) {
    std::cout << "usage: queue_bench [--messages N] [--capacity N] [--payload LIST] "
                 "[--rate MSG/S]\n"
                 "                   [--repeat N] [--mode both|latency|throughput]\n"
                 "                   [--producer-cpu N] [--consumer-cpu N] [--csv PATH]\n"
                 "  --payload takes a comma-separated list of 16,48,112,240,1008.\n"
                 "  --rate 0 saturates the queue, which turns the latency result into\n"
                 "  capacity/throughput; pace below saturation to measure handoff cost.\n";
    return 0;
  }

  Options options;
  options.messages = args.number<uint64_t>("messages", options.messages);
  options.capacity = args.number<size_t>("capacity", options.capacity);
  options.rate = args.number<uint64_t>("rate", options.rate);
  options.repeat = std::max(1u, args.number<unsigned>("repeat", options.repeat));
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
  std::cout << "\none now_ns() costs about " << clock_read_cost_ns()
            << " ns; latency-mode numbers include two of them\n"
            << (options.rate == 0
                    ? "producer unthrottled: the queue saturates, so latency is dominated by\n"
                      "queueing depth rather than by handoff cost\n"
                    : "producer paced, so the queue stays shallow and latency is handoff cost\n")
            << '\n';

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
