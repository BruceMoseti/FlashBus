// flashbus-bench: end-to-end benchmark through the real TCP path.
//
// Broker, publishers and subscribers all run in this process, on their own
// threads and their own sockets. Nothing is simulated: every event is encoded,
// written to a socket, routed by the dispatcher, fanned out, and decoded again.

#include <atomic>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

#include "flashbus/cli.hpp"
#include "flashbus/clock.hpp"
#include "flashbus/metrics.hpp"
#include "flashbus/publisher.hpp"
#include "flashbus/subscriber.hpp"
#include "flashbus/transport.hpp"

namespace {

using namespace flashbus;

struct Options {
  uint32_t producers = 1;
  uint32_t consumers = 1;
  uint64_t messages = 1'000'000;
  size_t payload = 64;
  uint64_t rate = 0;
  double warmup_s = 0.5;
  size_t publisher_batch = 1;
  size_t egress_batch = 32;
  bool adaptive_batching = false;
  size_t egress_capacity = 4096;
  size_t ingress_capacity = 4096;
  OverflowPolicy policy = OverflowPolicy::kDropNewest;
  int network_cpu = -1;
  int dispatcher_cpu = -1;
  int first_producer_cpu = -1;
  int first_consumer_cpu = -1;
  std::string csv;
  std::string variant = "tcp-end-to-end";
};

struct ConsumerResult {
  Histogram latency;
  uint64_t received = 0;
  uint64_t recorded = 0;
  uint64_t gaps = 0;
  uint64_t missing = 0;
  uint64_t first_recorded_ns = 0;
  uint64_t last_recorded_ns = 0;
};

/// Waits until the broker reports `expected` subscriptions, so that no event is
/// published before there is somewhere to route it.
bool await_subscriptions(const Server& server, uint64_t expected, double timeout_s) {
  const uint64_t deadline = now_ns() + static_cast<uint64_t>(timeout_s * 1e9);
  while (now_ns() < deadline) {
    if (server.stats().subscriptions >= expected) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return false;
}

}  // namespace

int main(int argc, char** argv) {
  const cli::Args args(argc, argv);
  args.reject_unknown({"producers", "consumers", "messages", "payload", "rate", "warmup",
                       "publisher-batch", "egress-batch", "adaptive-batching", "egress-capacity",
                       "ingress-capacity", "policy", "network-cpu", "dispatcher-cpu",
                       "producer-cpu", "consumer-cpu", "csv", "variant", "help"});
  if (args.flag("help")) {
    std::cout
        << "usage: flashbus-bench [--producers N] [--consumers N] [--messages N] "
           "[--payload BYTES]\n"
           "                      [--rate MSG/S] [--warmup MILLISECONDS] "
           "[--publisher-batch N]\n"
           "                      [--egress-batch N] [--adaptive-batching] "
           "[--egress-capacity N]\n"
           "                      [--ingress-capacity N] [--policy NAME] [--network-cpu N]\n"
           "                      [--dispatcher-cpu N] [--producer-cpu N] [--consumer-cpu N]\n"
           "                      [--csv PATH] [--variant NAME]\n"
           "  --messages is the total published across all producers; each consumer is\n"
           "  subscribed to the same topic, so each one should receive all of them.\n"
           "  --producer-cpu/--consumer-cpu give the first CPU of a consecutive range.\n";
    return 0;
  }

  Options options;
  options.producers = args.number<uint32_t>("producers", options.producers);
  options.consumers = args.number<uint32_t>("consumers", options.consumers);
  options.messages = args.number<uint64_t>("messages", options.messages);
  options.payload = args.number<size_t>("payload", options.payload);
  options.rate = args.number<uint64_t>("rate", options.rate);
  options.warmup_s = static_cast<double>(args.number<uint64_t>("warmup", 500)) / 1000.0;
  options.publisher_batch = args.number<size_t>("publisher-batch", options.publisher_batch);
  options.egress_batch = args.number<size_t>("egress-batch", options.egress_batch);
  options.adaptive_batching = args.flag("adaptive-batching");
  options.egress_capacity = args.number<size_t>("egress-capacity", options.egress_capacity);
  options.ingress_capacity = args.number<size_t>("ingress-capacity", options.ingress_capacity);
  options.network_cpu = args.number<int>("network-cpu", -1);
  options.dispatcher_cpu = args.number<int>("dispatcher-cpu", -1);
  options.first_producer_cpu = args.number<int>("producer-cpu", -1);
  options.first_consumer_cpu = args.number<int>("consumer-cpu", -1);
  options.csv = args.string("csv", "");
  options.variant = args.string("variant", options.variant);

  const std::string policy_name = args.string("policy", "drop-newest");
  if (const auto policy = parse_policy(policy_name)) {
    options.policy = *policy;
  } else {
    std::cerr << "flashbus-bench: unknown --policy '" << policy_name << "'\n";
    return 2;
  }
  if (options.payload > kMaxInlinePayload) {
    std::cerr << "flashbus-bench: --payload above the inline limit of " << kMaxInlinePayload
              << " bytes\n";
    return 2;
  }
  if (options.producers == 0 || options.consumers == 0) {
    std::cerr << "flashbus-bench: need at least one producer and one consumer\n";
    return 2;
  }

  ServerConfig server_config;
  server_config.port = 0;  // let the kernel choose, so parallel runs never clash
  server_config.ingress_capacity = options.ingress_capacity;
  server_config.egress_capacity = options.egress_capacity;
  server_config.egress_batch = options.egress_batch;
  server_config.adaptive_batching = options.adaptive_batching;
  server_config.policy = options.policy;
  server_config.network_cpu = options.network_cpu;
  server_config.dispatcher_cpu = options.dispatcher_cpu;

  Server server(server_config);
  try {
    server.start();
  } catch (const std::exception& error) {
    std::cerr << "flashbus-bench: " << error.what() << '\n';
    return 1;
  }
  std::thread network_thread([&server] { server.run(); });
  const uint16_t port = server.port();

  const uint64_t per_producer = options.messages / options.producers;
  const uint64_t total_published = per_producer * options.producers;
  const uint64_t warmup_deadline_offset = static_cast<uint64_t>(options.warmup_s * 1e9);

  std::vector<ConsumerResult> results(options.consumers);
  std::atomic<uint32_t> ready{0};
  std::atomic<bool> publishers_done{false};
  std::atomic<bool> abort{false};

  std::vector<std::thread> consumer_threads;
  for (uint32_t index = 0; index < options.consumers; ++index) {
    consumer_threads.emplace_back([&, index] {
      if (options.first_consumer_cpu >= 0) {
        pin_to_cpu(static_cast<unsigned>(options.first_consumer_cpu) + index);
      }
      ConsumerResult& result = results[index];
      try {
        SubscriberConfig config;
        config.blocking = false;  // so the thread can notice shutdown
        Subscriber subscriber("127.0.0.1", port, config);
        subscriber.subscribe(kTopicTrades);
        ready.fetch_add(1, std::memory_order_release);

        uint64_t warmup_until = 0;
        uint64_t idle_since = 0;
        while (!abort.load(std::memory_order_relaxed) && subscriber.connected()) {
          const size_t frames = subscriber.poll(
              [&](const MessageHeader& header, const std::byte*, size_t) {
                const uint64_t received_ns = now_ns();
                if (warmup_until == 0) warmup_until = received_ns + warmup_deadline_offset;
                if (received_ns < warmup_until) return;
                if (received_ns > header.timestamp_ns) {
                  result.latency.record(received_ns - header.timestamp_ns);
                }
                ++result.recorded;
                if (result.first_recorded_ns == 0) result.first_recorded_ns = received_ns;
                result.last_recorded_ns = received_ns;
              });
          if (frames != 0) {
            idle_since = 0;
            if (subscriber.received() >= total_published) break;
            continue;
          }
          // Publishers are finished and nothing has arrived for a while: any
          // remaining events were dropped, which the counters will show.
          if (publishers_done.load(std::memory_order_acquire)) {
            const uint64_t current = now_ns();
            if (idle_since == 0) idle_since = current;
            if (current - idle_since > 500'000'000) break;
          }
        }
        result.received = subscriber.received();
        result.gaps = subscriber.gaps();
        result.missing = subscriber.missing();
      } catch (const std::exception& error) {
        std::cerr << "flashbus-bench: consumer " << index << ": " << error.what() << '\n';
        abort.store(true, std::memory_order_relaxed);
        ready.fetch_add(1, std::memory_order_release);
      }
    });
  }

  while (ready.load(std::memory_order_acquire) < options.consumers) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (abort.load(std::memory_order_relaxed) ||
      !await_subscriptions(server, options.consumers, 5.0)) {
    std::cerr << "flashbus-bench: subscribers failed to register\n";
    abort.store(true, std::memory_order_relaxed);
    for (std::thread& thread : consumer_threads) thread.join();
    server.stop();
    network_thread.join();
    return 1;
  }

  const Rusage usage_before = rusage_now();
  const uint64_t allocations_before = heap_allocation_count();
  const uint64_t started_ns = now_ns();

  std::vector<std::thread> producer_threads;
  std::atomic<uint64_t> published{0};
  for (uint32_t index = 0; index < options.producers; ++index) {
    producer_threads.emplace_back([&, index] {
      if (options.first_producer_cpu >= 0) {
        pin_to_cpu(static_cast<unsigned>(options.first_producer_cpu) + index);
      }
      try {
        PublisherConfig config;
        config.batch_frames = options.publisher_batch;
        Publisher publisher("127.0.0.1", port, config);
        std::vector<std::byte> payload(options.payload);
        std::memset(payload.data(), static_cast<int>('a' + index % 26), options.payload);

        const Pacer pacer(options.rate / options.producers);
        for (uint64_t i = 0; i < per_producer && !abort.load(std::memory_order_relaxed); ++i) {
          pacer.wait_for(i);
          if (options.payload >= sizeof(uint64_t)) {
            const uint64_t stamp = i + 1;
            std::memcpy(payload.data(), &stamp, sizeof(stamp));
          }
          if (!publisher.publish(kTopicTrades, payload.data(), options.payload)) break;
        }
        publisher.flush();
        published.fetch_add(publisher.sent(), std::memory_order_relaxed);
      } catch (const std::exception& error) {
        std::cerr << "flashbus-bench: producer " << index << ": " << error.what() << '\n';
        abort.store(true, std::memory_order_relaxed);
      }
    });
  }
  for (std::thread& thread : producer_threads) thread.join();
  publishers_done.store(true, std::memory_order_release);

  for (std::thread& thread : consumer_threads) thread.join();
  const uint64_t finished_ns = now_ns();
  const Rusage usage_after = rusage_now();

  const ServerStats server_stats = server.stats();
  server.stop();
  network_thread.join();

  // Aggregate. Throughput is measured over the window in which latency was
  // actually recorded, so the warmup does not dilute it.
  Histogram latency;
  uint64_t received = 0, recorded = 0, gaps = 0, missing = 0;
  uint64_t window_start = UINT64_MAX, window_end = 0;
  for (const ConsumerResult& result : results) {
    latency.merge(result.latency);
    received += result.received;
    recorded += result.recorded;
    gaps += result.gaps;
    missing += result.missing;
    if (result.first_recorded_ns != 0) {
      window_start = std::min(window_start, result.first_recorded_ns);
      window_end = std::max(window_end, result.last_recorded_ns);
    }
  }
  const double window_s = window_end > window_start
                              ? static_cast<double>(window_end - window_start) / 1e9
                              : 0.0;
  const double total_s = static_cast<double>(finished_ns - started_ns) / 1e9;

  BenchmarkResult result;
  result.benchmark = "end_to_end";
  result.variant = options.variant;
  result.payload_bytes = static_cast<uint32_t>(options.payload);
  result.producers = options.producers;
  result.consumers = options.consumers;
  result.capacity = options.egress_capacity;
  result.target_rate = options.rate;
  result.warmup_s = options.warmup_s;
  result.messages_sent = published.load(std::memory_order_relaxed);
  result.messages_received = received;
  result.messages_dropped = server_stats.frames_dropped + server_stats.frames_rejected;
  result.sequence_gaps = gaps;
  result.duration_s = window_s;
  result.throughput_msg_s =
      window_s > 0.0 ? static_cast<double>(recorded) / window_s : 0.0;
  result.fill_latency(latency);
  result.fill_cost(usage_after - usage_before, total_s);
  // Allocations during the run only. Startup allocates plenty: rings, buffers,
  // histograms. What matters is that the steady state adds (nearly) nothing.
  result.heap_allocations = heap_allocation_count() - allocations_before;

  const auto us = [](uint64_t ns) { return static_cast<double>(ns) / 1000.0; };
  std::cout << "FlashBus Benchmark\n";
  print_env(std::cout);
  std::cout << std::fixed;
  std::cout << "Payload:        " << options.payload << " B\n"
            << "Topology:       " << options.producers << " publishers -> broker -> "
            << options.consumers << " subscribers\n"
            << "Messages:       " << total_published << " published, " << received
            << " received across subscribers\n"
            << "Egress batch:   " << options.egress_batch
            << (options.adaptive_batching ? " (adaptive)" : "") << ", publisher batch "
            << options.publisher_batch << ", policy " << to_string(options.policy) << '\n'
            << std::setprecision(3) << "Throughput:     "
            << result.throughput_msg_s / 1e6 << " M msg/s delivered (over " << window_s
            << " s measured window)\n"
            << "Latency:\n"
            << "  p50           " << us(result.p50_ns) << " us\n"
            << "  p95           " << us(result.p95_ns) << " us\n"
            << "  p99           " << us(result.p99_ns) << " us\n"
            << "  p99.9         " << us(result.p999_ns) << " us\n"
            << "  max           " << us(result.max_ns) << " us\n"
            << "Dropped:        " << result.messages_dropped << " (" << server_stats.frames_dropped
            << " egress overflow, " << server_stats.frames_rejected << " ingress overflow)\n"
            << "Sequence gaps:  " << gaps << " (" << missing << " events)\n"
            << "Unroutable:     " << server_stats.frames_unroutable << '\n'
            << "Egress depth:   " << server_stats.egress_high_water << " high water of "
            << options.egress_capacity << '\n'
            << std::setprecision(2) << "CPU:            " << result.cpu_cores_used
            << " cores, ctx-sw " << result.voluntary_ctx_switches << "v/"
            << result.involuntary_ctx_switches << "i, faults " << result.minor_faults << "m/"
            << result.major_faults << "M\n"
            << "Heap allocs:    " << result.heap_allocations << " during the run\n";
  if (server_stats.block_spins != 0) {
    std::cout << "Block spins:    " << server_stats.block_spins << '\n';
  }

  if (!options.csv.empty()) {
    ResultWriter(options.csv).add(result);
    std::cout << "wrote " << options.csv << '\n';
  }
  return 0;
}
