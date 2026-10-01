// Synthetic exchange -> FlashBus -> order-book consumer.
//
// Steady or bursty load, through the real TCP path, with a consumer that keeps
// a live book. Reports end-to-end latency, sequence gaps, drops, and the book
// state at the end, so a run can be checked rather than just admired.

#include <atomic>
#include <optional>
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
#include "market_data.hpp"
#include "order_book.hpp"

namespace {

using namespace flashbus;
using namespace flashbus::market;

struct Phase {
  uint64_t rate = 0;
  uint64_t duration_ms = 0;
};

/// Markets are bursty; a uniform rate is the one load pattern a real feed never
/// produces. These phases are the default because the interesting numbers only
/// appear when the arrival rate changes faster than the consumer can adapt.
std::vector<Phase> burst_profile(uint64_t base_rate) {
  return {{base_rate, 1000},
          {base_rate * 5, 200},
          {base_rate, 1000},
          {base_rate * 10, 100},
          {base_rate, 1000}};
}

}  // namespace

int main(int argc, char** argv) {
  const cli::Args args(argc, argv);
  args.reject_unknown({"events", "instruments", "rate", "burst", "egress-capacity",
                       "egress-batch", "adaptive-batching", "policy", "pool", "csv",
                       "slow-consumer-us", "help"});
  if (args.flag("help")) {
    std::cout << "usage: market-data-demo [--events N] [--instruments N] [--rate MSG/S]\n"
                 "                        [--burst] [--egress-capacity N] [--egress-batch N]\n"
                 "                        [--adaptive-batching] [--policy NAME] [--pool|--pool=0]"
                 "\n                        [--slow-consumer-us N] [--csv PATH]\n"
                 "  --burst replaces the steady rate with alternating normal and burst "
                 "phases.\n"
                 "  --pool=0 makes the order book use new/delete per order instead of a pool.\n";
    return 0;
  }

  const auto events = args.number<uint64_t>("events", 2'000'000);
  const auto instruments = args.number<uint32_t>("instruments", 8);
  const auto rate = args.number<uint64_t>("rate", 500'000);
  const bool burst = args.flag("burst");
  const bool pooled = args.string("pool", "1") != "0";
  const auto slow_consumer_us = args.number<unsigned>("slow-consumer-us", 0);
  const std::string csv = args.string("csv", "");

  ServerConfig server_config;
  server_config.port = 0;
  server_config.egress_capacity = args.number<size_t>("egress-capacity", 8192);
  server_config.egress_batch = args.number<size_t>("egress-batch", 32);
  server_config.adaptive_batching = args.flag("adaptive-batching");
  if (const auto policy = parse_policy(args.string("policy", "drop-newest"))) {
    server_config.policy = *policy;
  } else {
    std::cerr << "market-data-demo: unknown --policy\n";
    return 2;
  }

  Server server(server_config);
  server.start();
  // Before the network thread starts: port() touches the acceptor, which the
  // io thread also owns.
  const uint16_t port = server.port();
  std::thread network_thread([&server] { server.run(); });

  constexpr size_t kMaxLiveOrders = 8192;
  Histogram latency;
  std::atomic<bool> subscribed{false};
  std::atomic<bool> publisher_done{false};
  uint64_t received = 0, gaps = 0, missing = 0, book_applied = 0, unknown_orders = 0;
  uint64_t store_high_water = 0, store_exhausted = 0;
  std::vector<std::optional<int64_t>> final_spreads;

  std::thread consumer_thread([&] {
    SubscriberConfig config;
    config.blocking = false;
    Subscriber subscriber("127.0.0.1", port, config);
    subscriber.subscribe(kTopicTrades);
    subscribed.store(true, std::memory_order_release);

    OrderBook<PooledOrderStore> pooled_book(kMaxLiveOrders, instruments);
    OrderBook<HeapOrderStore> heap_book(kMaxLiveOrders, instruments);

    uint64_t idle_since = 0;
    while (subscriber.connected()) {
      const size_t frames =
          subscriber.poll([&](const MessageHeader& header, const std::byte* payload, size_t size) {
            const uint64_t received_ns = now_ns();
            if (received_ns > header.timestamp_ns) {
              latency.record(received_ns - header.timestamp_ns);
            }
            if (size < kMarketEventWireSize) return;
            const MarketEvent event = decode(payload);
            if (pooled) {
              pooled_book.apply(event);
            } else {
              heap_book.apply(event);
            }
          });
      if (frames != 0) {
        idle_since = 0;
        if (slow_consumer_us != 0) {
          std::this_thread::sleep_for(std::chrono::microseconds(slow_consumer_us));
        }
        continue;
      }
      if (publisher_done.load(std::memory_order_acquire)) {
        const uint64_t current = now_ns();
        if (idle_since == 0) idle_since = current;
        if (current - idle_since > 500'000'000) break;
      }
    }

    received = subscriber.received();
    gaps = subscriber.gaps();
    missing = subscriber.missing();
    if (pooled) {
      book_applied = pooled_book.stats().applied;
      unknown_orders = pooled_book.stats().unknown_orders;
      store_exhausted = pooled_book.stats().store_exhausted;
      store_high_water = pooled_book.store().high_water();
      for (uint32_t i = 0; i < instruments; ++i) final_spreads.push_back(pooled_book.spread(i));
    } else {
      book_applied = heap_book.stats().applied;
      unknown_orders = heap_book.stats().unknown_orders;
      store_exhausted = heap_book.stats().store_exhausted;
      for (uint32_t i = 0; i < instruments; ++i) final_spreads.push_back(heap_book.spread(i));
    }
  });

  while (!subscribed.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  while (server.stats().subscriptions == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  const Rusage usage_before = rusage_now();
  const uint64_t started_ns = now_ns();
  uint64_t published = 0;
  {
    Publisher publisher("127.0.0.1", port);
    Generator generator(Generator::Config{instruments, kMaxLiveOrders, 100'000, 0x5EED});
    std::byte payload[kMarketEventWireSize];

    const std::vector<Phase> phases = burst ? burst_profile(rate) : std::vector<Phase>{};
    uint64_t remaining = events;
    if (phases.empty()) {
      const Pacer pacer(rate);
      for (uint64_t i = 0; i < events && publisher.connected(); ++i) {
        pacer.wait_for(i);
        const MarketEvent event = generator.next();
        encode(payload, event);
        publisher.publish(kTopicTrades, payload, sizeof(payload));
      }
    } else {
      // Cycle through the phases until the event budget is spent, so a longer
      // --events gives more bursts rather than a longer single phase.
      while (remaining > 0 && publisher.connected()) {
        for (const Phase& phase : phases) {
          if (remaining == 0 || !publisher.connected()) break;
          const Pacer pacer(phase.rate);
          const uint64_t phase_end = now_ns() + phase.duration_ms * 1'000'000;
          uint64_t index = 0;
          while (remaining > 0 && now_ns() < phase_end && publisher.connected()) {
            pacer.wait_for(index++);
            const MarketEvent event = generator.next();
            encode(payload, event);
            publisher.publish(kTopicTrades, payload, sizeof(payload));
            --remaining;
          }
        }
      }
    }
    publisher.flush();
    published = publisher.sent();
  }
  publisher_done.store(true, std::memory_order_release);
  consumer_thread.join();
  const uint64_t finished_ns = now_ns();
  const Rusage usage_after = rusage_now();

  const ServerStats server_stats = server.stats();
  server.stop();
  network_thread.join();

  const double elapsed = static_cast<double>(finished_ns - started_ns) / 1e9;
  const auto us = [](uint64_t ns) { return static_cast<double>(ns) / 1000.0; };

  BenchmarkResult result;
  result.benchmark = "market_data";
  result.variant = std::string(burst ? "burst" : "steady") + (pooled ? "+pool" : "+heap") +
                   (slow_consumer_us != 0 ? "+slow-consumer" : "");
  result.payload_bytes = kMarketEventWireSize;
  result.producers = 1;
  result.consumers = 1;
  result.capacity = server_config.egress_capacity;
  result.target_rate = rate;
  result.messages_sent = published;
  result.messages_received = received;
  result.messages_dropped = server_stats.frames_dropped + server_stats.frames_rejected;
  result.sequence_gaps = gaps;
  result.duration_s = elapsed;
  result.throughput_msg_s = elapsed > 0.0 ? static_cast<double>(received) / elapsed : 0.0;
  result.fill_latency(latency);
  result.fill_cost(usage_after - usage_before, elapsed);

  std::cout << "FlashBus market-data demo\n";
  print_env(std::cout);
  std::cout << std::fixed << std::setprecision(3)
            << "Profile:       " << (burst ? "bursty" : "steady") << ", base rate " << rate
            << " msg/s, order store " << (pooled ? "pool" : "new/delete") << '\n'
            << "Events:        " << published << " published, " << received << " received, "
            << book_applied << " applied to the book\n"
            << "Throughput:    " << result.throughput_msg_s / 1e6 << " M msg/s over " << elapsed
            << " s\n"
            << "Latency us:    p50 " << us(result.p50_ns) << "  p95 " << us(result.p95_ns)
            << "  p99 " << us(result.p99_ns) << "  p99.9 " << us(result.p999_ns) << "  max "
            << us(result.max_ns) << '\n'
            << "Dropped:       " << result.messages_dropped << "   sequence gaps " << gaps << " ("
            << missing << " events)\n"
            << "Egress depth:  " << server_stats.egress_high_water << " of "
            << server_config.egress_capacity << '\n'
            << "Order store:   high water " << store_high_water << ", exhausted "
            << store_exhausted << ", unknown-order events " << unknown_orders << '\n';
  std::cout << "Final spreads: ";
  for (const std::optional<int64_t>& spread : final_spreads) {
    if (!spread) {
      std::cout << "n/a ";
    } else if (*spread < 0) {
      // The generator moves each side from a random walk independently, so a
      // stale bid can sit above a fresh ask. Real feeds cross transiently too;
      // saying so beats printing a bare negative number.
      std::cout << *spread << "(crossed) ";
    } else {
      std::cout << *spread << ' ';
    }
  }
  std::cout << "ticks\n";

  // A gap means FlashBus dropped events for this subscriber. The book then
  // legitimately sees cancels for orders it never got, and saying so is the
  // point of tracking it.
  if (unknown_orders != 0 && gaps == 0) {
    std::cerr << "market-data-demo: the book saw unknown orders with no sequence gap; that is a "
                 "bug, not packet loss\n";
    return 1;
  }

  if (!csv.empty()) {
    ResultWriter(csv).add(result);
    std::cout << "wrote " << csv << '\n';
  }
  return 0;
}
