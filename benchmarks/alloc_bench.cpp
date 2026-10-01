// Allocation benchmark: new/delete per order against a preallocated pool.
//
// The workload is the order-book consumer, because that is where FlashBus
// actually has objects with independent lifetimes. The two variants differ in
// exactly one thing — where an Order comes from — so the difference in the tail
// is attributable.
//
// Events are generated up front and replayed from memory: generating them
// inside the measured loop would measure the generator.

#include <iostream>
#include <string>
#include <vector>

#include "flashbus/cli.hpp"
#include "flashbus/clock.hpp"
#include "flashbus/metrics.hpp"
#include "flashbus/platform.hpp"
#include "market_data.hpp"
#include "order_book.hpp"

namespace {

using namespace flashbus;
using namespace flashbus::market;

template <typename Store>
BenchmarkResult run(const std::string& variant, const std::vector<MarketEvent>& events,
                    size_t capacity, uint32_t instruments) {
  OrderBook<Store> book(capacity, instruments);
  Histogram latency;

  // Warm up the book so the measured window is steady state, not the ramp from
  // an empty book to a full one.
  const size_t warmup = std::min<size_t>(events.size() / 10, 200'000);
  for (size_t i = 0; i < warmup; ++i) book.apply(events[i]);

  const uint64_t allocations_before = heap_allocation_count();
  const Rusage usage_before = rusage_now();
  const uint64_t started = now_ns();
  for (size_t i = warmup; i < events.size(); ++i) {
    const uint64_t before = now_ns();
    book.apply(events[i]);
    latency.record(now_ns() - before);
  }
  const uint64_t finished = now_ns();
  const Rusage usage_after = rusage_now();
  const uint64_t allocations = heap_allocation_count() - allocations_before;

  const double elapsed = static_cast<double>(finished - started) / 1e9;
  const uint64_t measured = events.size() - warmup;

  BenchmarkResult result;
  result.benchmark = "order_book_apply";
  result.variant = variant;
  result.payload_bytes = kMarketEventWireSize;
  result.capacity = capacity;
  result.messages_sent = measured;
  result.messages_received = book.stats().applied - warmup;
  result.duration_s = elapsed;
  result.throughput_msg_s = elapsed > 0.0 ? static_cast<double>(measured) / elapsed : 0.0;
  result.fill_latency(latency);
  result.fill_cost(usage_after - usage_before, elapsed);
  result.heap_allocations = allocations;

  print_result(std::cout, result);
  std::cout << "   book          live " << book.live_orders() << ", new " << book.stats().new_orders
            << ", cancels " << book.stats().cancels << ", trades " << book.stats().trades
            << ", quotes " << book.stats().quote_updates << '\n'
            << "   store         high water " << book.store().high_water() << ", exhausted "
            << book.store().exhaustions() << ", unknown-order events "
            << book.stats().unknown_orders << '\n';
  return result;
}

}  // namespace

int main(int argc, char** argv) {
  const cli::Args args(argc, argv);
  args.reject_unknown({"events", "instruments", "capacity", "csv", "help"});
  if (args.flag("help")) {
    std::cout << "usage: alloc_bench [--events N] [--instruments N] [--capacity N] "
                 "[--csv PATH]\n";
    return 0;
  }

  const auto event_count = args.number<size_t>("events", 5'000'000);
  const auto instruments = args.number<uint32_t>("instruments", 8);
  const auto capacity = args.number<size_t>("capacity", 8192);
  const std::string csv = args.string("csv", "");

  print_env(std::cout);
  std::cout << "generating " << event_count << " events...\n";
  std::vector<MarketEvent> events;
  events.reserve(event_count);
  Generator generator(
      Generator::Config{instruments, static_cast<uint32_t>(capacity), 100'000, 0x5EED});
  for (size_t i = 0; i < event_count; ++i) events.push_back(generator.next());
  std::cout << "generated; " << generator.live_orders() << " orders live at the end\n\n";

  std::unique_ptr<ResultWriter> writer;
  if (!csv.empty()) writer = std::make_unique<ResultWriter>(csv);

  const BenchmarkResult heap = run<HeapOrderStore>("heap-new-delete", events, capacity, instruments);
  const BenchmarkResult pooled = run<PooledOrderStore>("memory-pool", events, capacity, instruments);
  if (writer != nullptr) {
    writer->add(heap);
    writer->add(pooled);
  }

  const auto ratio = [](uint64_t from, uint64_t to) {
    return to == 0 ? 0.0 : static_cast<double>(from) / static_cast<double>(to);
  };
  std::cout << "\npool vs heap: p50 x" << ratio(heap.p50_ns, pooled.p50_ns) << ", p99 x"
            << ratio(heap.p99_ns, pooled.p99_ns) << ", p99.9 x"
            << ratio(heap.p999_ns, pooled.p999_ns) << ", max x"
            << ratio(heap.max_ns, pooled.max_ns) << " (above 1.0 means the pool is faster)\n"
            << "heap allocations during the measured window: " << heap.heap_allocations
            << " (heap) vs " << pooled.heap_allocations << " (pool)\n";
  return 0;
}
