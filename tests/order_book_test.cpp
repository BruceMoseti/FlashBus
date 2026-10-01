#include "order_book.hpp"

#include <gtest/gtest.h>

#include <unordered_set>
#include <vector>

#include "flashbus/platform.hpp"
#include "market_data.hpp"

namespace flashbus::market {
namespace {

MarketEvent make_event(EventType type, uint64_t order_id, int64_t price, uint32_t quantity,
                       Side side = Side::kBid, uint32_t instrument = 0) {
  MarketEvent event;
  event.type = type;
  event.order_id = order_id;
  event.price = price;
  event.quantity = quantity;
  event.side = side;
  event.instrument_id = instrument;
  return event;
}

TEST(MarketEventWire, RoundTrips) {
  MarketEvent in;
  in.timestamp_ns = 0x0102030405060708;
  in.sequence = 42;
  in.order_id = 0xFEDCBA9876543210;
  in.price = -123456;
  in.instrument_id = 7;
  in.quantity = 999;
  in.type = EventType::kTrade;
  in.side = Side::kAsk;

  std::byte bytes[kMarketEventWireSize];
  encode(bytes, in);
  const MarketEvent out = decode(bytes);

  EXPECT_EQ(out.timestamp_ns, in.timestamp_ns);
  EXPECT_EQ(out.sequence, in.sequence);
  EXPECT_EQ(out.order_id, in.order_id);
  EXPECT_EQ(out.price, in.price);
  EXPECT_EQ(out.instrument_id, in.instrument_id);
  EXPECT_EQ(out.quantity, in.quantity);
  EXPECT_EQ(out.type, in.type);
  EXPECT_EQ(out.side, in.side);
}

TEST(MarketEventWire, FitsInOneFlashBusFrame) {
  EXPECT_LE(kMarketEventWireSize, kMaxInlinePayload);
}

template <typename Store>
void run_basic_lifecycle() {
  OrderBook<Store> book(64, 2);
  book.apply(make_event(EventType::kNewOrder, 100, 9900, 10, Side::kBid));
  book.apply(make_event(EventType::kNewOrder, 101, 10100, 20, Side::kAsk));
  EXPECT_EQ(book.live_orders(), 2u);
  EXPECT_EQ(book.best_bid(0), 9900);
  EXPECT_EQ(book.best_ask(0), 10100);
  EXPECT_EQ(book.spread(0), 200);

  book.apply(make_event(EventType::kCancelOrder, 100, 0, 0));
  EXPECT_EQ(book.live_orders(), 1u);
  book.apply(make_event(EventType::kTrade, 101, 10100, 20, Side::kAsk));
  EXPECT_EQ(book.live_orders(), 0u);

  EXPECT_EQ(book.stats().new_orders, 2u);
  EXPECT_EQ(book.stats().cancels, 1u);
  EXPECT_EQ(book.stats().trades, 1u);
  EXPECT_EQ(book.stats().unknown_orders, 0u);
}

TEST(OrderBook, LifecycleWithPool) { run_basic_lifecycle<PooledOrderStore>(); }
TEST(OrderBook, LifecycleWithHeap) { run_basic_lifecycle<HeapOrderStore>(); }

TEST(OrderBook, ReportsUnknownOrdersRatherThanCorruptingState) {
  OrderBook<PooledOrderStore> book(16, 1);
  book.apply(make_event(EventType::kCancelOrder, 999, 0, 0));
  EXPECT_EQ(book.stats().unknown_orders, 1u);
  EXPECT_EQ(book.live_orders(), 0u);
}

TEST(OrderBook, QuoteUpdatesTrackTheTopOfBook) {
  OrderBook<PooledOrderStore> book(16, 2);
  book.apply(make_event(EventType::kBidUpdate, 0, 9950, 5, Side::kBid, 1));
  book.apply(make_event(EventType::kAskUpdate, 0, 9960, 5, Side::kAsk, 1));
  EXPECT_EQ(book.best_bid(1), 9950);
  EXPECT_EQ(book.best_ask(1), 9960);
  EXPECT_EQ(book.spread(1), 10);
  // A later quote replaces the earlier one; a quote is a statement about now.
  book.apply(make_event(EventType::kBidUpdate, 0, 9900, 5, Side::kBid, 1));
  EXPECT_EQ(book.best_bid(1), 9900);
  EXPECT_EQ(book.spread(1), 60);
}

TEST(OrderBook, SpreadIsEmptyUntilBothSidesHaveAPrice) {
  OrderBook<PooledOrderStore> book(16, 1);
  EXPECT_FALSE(book.spread(0).has_value());
  book.apply(make_event(EventType::kBidUpdate, 0, 100, 1, Side::kBid));
  EXPECT_FALSE(book.spread(0).has_value()) << "one side priced is not a spread";
  book.apply(make_event(EventType::kAskUpdate, 0, 104, 1, Side::kAsk));
  EXPECT_EQ(book.spread(0), 4);
}

// A crossed book has a genuinely negative spread, which is why the accessor
// returns optional rather than using a negative sentinel for "no spread".
TEST(OrderBook, CrossedBookReportsANegativeSpread) {
  OrderBook<PooledOrderStore> book(16, 1);
  book.apply(make_event(EventType::kBidUpdate, 0, 110, 1, Side::kBid));
  book.apply(make_event(EventType::kAskUpdate, 0, 108, 1, Side::kAsk));
  ASSERT_TRUE(book.spread(0).has_value());
  EXPECT_EQ(*book.spread(0), -2);
}

TEST(OrderBook, KeepsEveryOrderDistinctAcrossHashCollisions) {
  // More orders than buckets forces chaining, which is where an intrusive hash
  // table goes wrong if the unlink is sloppy.
  constexpr size_t kOrders = 4096;
  OrderBook<PooledOrderStore> book(kOrders, 1);
  for (uint64_t i = 0; i < kOrders; ++i) {
    // Ids spaced by a power of two to force collisions in any masked hash.
    book.apply(make_event(EventType::kNewOrder, i * 4096, 100 + static_cast<int64_t>(i), 1));
  }
  EXPECT_EQ(book.live_orders(), kOrders);
  for (uint64_t i = 0; i < kOrders; ++i) {
    book.apply(make_event(EventType::kCancelOrder, i * 4096, 0, 0));
  }
  EXPECT_EQ(book.live_orders(), 0u);
  EXPECT_EQ(book.stats().unknown_orders, 0u);
}

TEST(OrderBook, PoolStoreExhaustsInsteadOfGrowing) {
  OrderBook<PooledOrderStore> book(4, 1);
  for (uint64_t i = 0; i < 10; ++i) {
    book.apply(make_event(EventType::kNewOrder, i + 1, 100, 1));
  }
  EXPECT_EQ(book.live_orders(), 4u);
  EXPECT_EQ(book.stats().store_exhausted, 6u);
}

TEST(OrderBook, PooledApplyDoesNotAllocate) {
  constexpr size_t kCapacity = 4096;
  OrderBook<PooledOrderStore> book(kCapacity, 4);
  Generator generator(Generator::Config{4, kCapacity, 100000, 7});
  std::vector<MarketEvent> events;
  events.reserve(200000);
  for (int i = 0; i < 200000; ++i) events.push_back(generator.next());

  // Warm up first, so the measured window is steady state.
  for (size_t i = 0; i < 50000; ++i) book.apply(events[i]);
  reset_heap_allocation_count();
  const uint64_t before = heap_allocation_count();
  for (size_t i = 50000; i < events.size(); ++i) book.apply(events[i]);
  EXPECT_EQ(heap_allocation_count(), before);
}

TEST(Generator, ProducesASelfConsistentStream) {
  constexpr size_t kCapacity = 1024;
  Generator generator(Generator::Config{4, kCapacity, 100000, 99});
  OrderBook<PooledOrderStore> book(kCapacity, 4);
  for (int i = 0; i < 500000; ++i) book.apply(generator.next());

  // Every cancel and trade referred to an order the book knew about: the
  // generator never invents an id, so a non-zero count here would mean the book
  // lost one.
  EXPECT_EQ(book.stats().unknown_orders, 0u);
  EXPECT_EQ(book.stats().store_exhausted, 0u);
  EXPECT_EQ(book.live_orders(), generator.live_orders());
  EXPECT_GT(book.stats().new_orders, 0u);
  EXPECT_GT(book.stats().cancels, 0u);
  EXPECT_GT(book.stats().trades, 0u);
  EXPECT_GT(book.stats().quote_updates, 0u);
}

TEST(Generator, SequencesAreContiguous) {
  Generator generator;
  uint64_t expected = 1;
  for (int i = 0; i < 10000; ++i) {
    EXPECT_EQ(generator.next().sequence, expected++);
  }
}

TEST(Generator, OrderIdsAreNeverReused) {
  Generator generator(Generator::Config{4, 512, 100000, 5});
  std::unordered_set<uint64_t> seen;
  for (int i = 0; i < 200000; ++i) {
    const MarketEvent event = generator.next();
    if (event.type != EventType::kNewOrder) continue;
    EXPECT_TRUE(seen.insert(event.order_id).second) << "order id " << event.order_id << " reused";
  }
}

}  // namespace
}  // namespace flashbus::market
