// Sustained-load benchmark.
//
// A five-second microbenchmark cannot see the failure modes that matter: p99
// creeping upward, memory growing, an allocation count that is not actually
// zero, a queue whose depth trends up instead of oscillating. This runs the
// full TCP path at a fixed rate and reports one row per interval so that drift
// is visible as drift rather than averaged away.

#include <atomic>
#include <fstream>
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

/// Resident set size in KiB, from /proc/self/statm. Cheap enough to sample once
/// per interval, and it is the number that shows a leak.
uint64_t resident_kib() {
  std::ifstream in("/proc/self/statm");
  uint64_t total_pages = 0, resident_pages = 0;
  if (!(in >> total_pages >> resident_pages)) return 0;
  return resident_pages * 4;  // 4 KiB pages
}

struct Interval {
  double at_s = 0.0;
  uint64_t received = 0;
  double throughput = 0.0;
  uint64_t p50 = 0, p99 = 0, p999 = 0, max = 0;
  uint64_t dropped = 0;
  uint64_t egress_depth = 0;
  uint64_t allocations = 0;
  uint64_t resident_kib = 0;
};

}  // namespace

int main(int argc, char** argv) {
  const cli::Args args(argc, argv);
  args.reject_unknown({"seconds", "rate", "payload", "interval", "egress-capacity",
                       "egress-batch", "csv", "help"});
  if (args.flag("help")) {
    std::cout << "usage: sustained_bench [--seconds N] [--rate MSG/S] [--payload BYTES]\n"
                 "                       [--interval SECONDS] [--egress-capacity N]\n"
                 "                       [--egress-batch N] [--csv PATH]\n";
    return 0;
  }

  const auto seconds = args.number<uint64_t>("seconds", 60);
  const auto rate = args.number<uint64_t>("rate", 300'000);
  const auto payload_size = args.number<size_t>("payload", 64);
  const auto interval_s = args.number<uint64_t>("interval", 5);
  const std::string csv = args.string("csv", "");

  ServerConfig server_config;
  server_config.port = 0;
  server_config.egress_capacity = args.number<size_t>("egress-capacity", 4096);
  server_config.egress_batch = args.number<size_t>("egress-batch", 32);

  Server server(server_config);
  server.start();
  std::thread network_thread([&server] { server.run(); });
  const uint16_t port = server.port();

  std::atomic<bool> subscribed{false};
  std::atomic<bool> stop{false};
  std::vector<Interval> intervals;
  std::atomic<uint64_t> total_received{0};

  std::thread consumer_thread([&] {
    SubscriberConfig config;
    config.blocking = false;
    Subscriber subscriber("127.0.0.1", port, config);
    subscriber.subscribe(kTopicTrades);
    subscribed.store(true, std::memory_order_release);

    Histogram window;
    const uint64_t started = now_ns();
    uint64_t next_report = started + interval_s * 1'000'000'000;
    uint64_t window_start = started;
    uint64_t window_received = 0;
    uint64_t allocations_at_window_start = heap_allocation_count();

    while (!stop.load(std::memory_order_relaxed) && subscriber.connected()) {
      subscriber.poll([&](const MessageHeader& header, const std::byte*, size_t) {
        const uint64_t received_ns = now_ns();
        if (received_ns > header.timestamp_ns) window.record(received_ns - header.timestamp_ns);
        ++window_received;
      });

      const uint64_t current = now_ns();
      if (current < next_report) continue;

      const ServerStats stats = server.stats();
      const double span = static_cast<double>(current - window_start) / 1e9;
      Interval interval;
      interval.at_s = static_cast<double>(current - started) / 1e9;
      interval.received = window_received;
      interval.throughput = span > 0.0 ? static_cast<double>(window_received) / span : 0.0;
      interval.p50 = window.percentile(50);
      interval.p99 = window.percentile(99);
      interval.p999 = window.percentile(99.9);
      interval.max = window.max();
      interval.dropped = stats.frames_dropped + stats.frames_rejected;
      interval.egress_depth = stats.egress_high_water;
      interval.allocations = heap_allocation_count() - allocations_at_window_start;
      interval.resident_kib = resident_kib();
      intervals.push_back(interval);

      window.clear();
      window_received = 0;
      window_start = current;
      allocations_at_window_start = heap_allocation_count();
      next_report = current + interval_s * 1'000'000'000;
    }
    total_received.store(subscriber.received(), std::memory_order_relaxed);
  });

  while (!subscribed.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  while (server.stats().subscriptions == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  print_env(std::cout);
  std::cout << "\nsustained run: " << seconds << " s at " << rate << " msg/s, " << payload_size
            << " B payloads, reporting every " << interval_s << " s\n\n";

  std::thread producer_thread([&] {
    Publisher publisher("127.0.0.1", port);
    std::vector<std::byte> payload(payload_size, std::byte{0x5A});
    const Pacer pacer(rate);
    const uint64_t deadline = now_ns() + seconds * 1'000'000'000;
    uint64_t index = 0;
    while (now_ns() < deadline && publisher.connected()) {
      pacer.wait_for(index++);
      publisher.publish(kTopicTrades, payload.data(), payload_size);
    }
    publisher.flush();
  });

  producer_thread.join();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  stop.store(true, std::memory_order_relaxed);
  consumer_thread.join();
  server.stop();
  network_thread.join();

  std::cout << std::fixed << std::setprecision(2);
  std::cout << "   t(s)   recv      Mmsg/s   p50us   p99us  p99.9us   maxus  dropped  depth"
               "  allocs   rss(MiB)\n";
  for (const Interval& interval : intervals) {
    std::cout << std::setw(7) << interval.at_s << std::setw(9) << interval.received
              << std::setw(12) << interval.throughput / 1e6 << std::setw(8)
              << static_cast<double>(interval.p50) / 1000.0 << std::setw(8)
              << static_cast<double>(interval.p99) / 1000.0 << std::setw(9)
              << static_cast<double>(interval.p999) / 1000.0 << std::setw(8)
              << static_cast<double>(interval.max) / 1000.0 << std::setw(9) << interval.dropped
              << std::setw(7) << interval.egress_depth << std::setw(8) << interval.allocations
              << std::setw(11) << static_cast<double>(interval.resident_kib) / 1024.0 << '\n';
  }

  if (intervals.size() >= 2) {
    const Interval& first = intervals.front();
    const Interval& last = intervals.back();
    const auto drift = [](uint64_t from, uint64_t to) {
      return from == 0 ? 0.0
                       : 100.0 * (static_cast<double>(to) - static_cast<double>(from)) /
                             static_cast<double>(from);
    };
    std::cout << "\ndrift from the first interval to the last:\n"
              << "  p50   " << drift(first.p50, last.p50) << " %\n"
              << "  p99   " << drift(first.p99, last.p99) << " %\n"
              << "  p99.9 " << drift(first.p999, last.p999) << " %\n"
              << "  rss   " << drift(first.resident_kib, last.resident_kib) << " %  ("
              << static_cast<double>(first.resident_kib) / 1024.0 << " -> "
              << static_cast<double>(last.resident_kib) / 1024.0 << " MiB)\n"
              << "  allocations per interval: " << first.allocations << " -> "
              << last.allocations << '\n';
  }
  std::cout << "total received " << total_received.load(std::memory_order_relaxed) << '\n';

  if (!csv.empty()) {
    std::ofstream out(csv);
    out << "at_s,received,throughput_msg_s,p50_ns,p99_ns,p999_ns,max_ns,dropped,egress_depth,"
           "allocations,resident_kib\n";
    out << std::fixed << std::setprecision(3);
    for (const Interval& interval : intervals) {
      out << interval.at_s << ',' << interval.received << ',' << interval.throughput << ','
          << interval.p50 << ',' << interval.p99 << ',' << interval.p999 << ',' << interval.max
          << ',' << interval.dropped << ',' << interval.egress_depth << ','
          << interval.allocations << ',' << interval.resident_kib << '\n';
    }
    std::cout << "wrote " << csv << '\n';
  }
  return 0;
}
