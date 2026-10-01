#pragma once

// An order book maintained from the FlashBus event stream.
//
// This is the consumer that makes the benchmark mean something: it keeps real
// per-order state, so its latency includes a lookup, an insert or an erase, and
// a best-price update, rather than just a counter bump.
//
// Order objects come from a Store policy. The two stores differ only in where
// the memory comes from, which is what makes the allocation case study an A/B
// test rather than a comparison of two different programs.

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>
#include <vector>

#include "flashbus/memory_pool.hpp"
#include "market_data.hpp"

namespace flashbus::market {

/// Lives in an intrusive hash chain, so lookup needs no side table and
/// deletion needs no tombstones or rehash.
struct Order {
  uint64_t id = 0;
  int64_t price = 0;
  uint32_t quantity = 0;
  Side side = Side::kBid;
  Order* hash_next = nullptr;
};

/// `new`/`delete` per order: the baseline.
class HeapOrderStore {
 public:
  explicit HeapOrderStore(size_t /*capacity*/) {}
  Order* acquire() { return new Order(); }
  void release(Order* order) { delete order; }
  [[nodiscard]] size_t exhaustions() const { return 0; }
  [[nodiscard]] size_t high_water() const { return 0; }
};

/// Preallocated blocks, O(1) acquire and release, no syscalls, no growth.
class PooledOrderStore {
 public:
  explicit PooledOrderStore(size_t capacity) : pool_(capacity) {}
  Order* acquire() { return pool_.acquire(); }
  void release(Order* order) { pool_.release(order); }
  [[nodiscard]] size_t exhaustions() const { return pool_.exhaustions(); }
  [[nodiscard]] size_t high_water() const { return pool_.high_water(); }

 private:
  MemoryPool<Order> pool_;
};

template <typename Store>
class OrderBook {
 public:
  struct Stats {
    uint64_t applied = 0;
    uint64_t new_orders = 0;
    uint64_t cancels = 0;
    uint64_t trades = 0;
    uint64_t quote_updates = 0;
    /// Cancels and trades for an order the book never saw. Should be zero for
    /// the generator's stream; non-zero means events were lost upstream.
    uint64_t unknown_orders = 0;
    uint64_t store_exhausted = 0;
  };

  /// `capacity` bounds the live order count. Buckets are sized from it and
  /// allocated once.
  explicit OrderBook(size_t capacity, uint32_t instruments = 8)
      : store_(capacity),
        buckets_(std::bit_ceil(capacity * 2), nullptr),
        bucket_mask_(buckets_.size() - 1),
        books_(instruments) {}

  void apply(const MarketEvent& event) {
    ++stats_.applied;
    switch (event.type) {
      case EventType::kNewOrder: add(event); return;
      case EventType::kCancelOrder: remove(event, /*is_trade=*/false); return;
      case EventType::kTrade: remove(event, /*is_trade=*/true); return;
      case EventType::kBidUpdate:
      case EventType::kAskUpdate: quote(event); return;
    }
  }

  [[nodiscard]] int64_t best_bid(uint32_t instrument) const {
    return book(instrument).best_bid;
  }
  [[nodiscard]] int64_t best_ask(uint32_t instrument) const {
    return book(instrument).best_ask;
  }
  /// Negative when one side is empty, which is a real state and should not be
  /// papered over with a zero.
  [[nodiscard]] int64_t spread(uint32_t instrument) const {
    const Book& b = book(instrument);
    if (b.best_bid == kNoPrice || b.best_ask == kNoPrice) return -1;
    return b.best_ask - b.best_bid;
  }
  [[nodiscard]] size_t live_orders() const { return live_orders_; }
  [[nodiscard]] const Stats& stats() const { return stats_; }
  [[nodiscard]] const Store& store() const { return store_; }

  static constexpr int64_t kNoPrice = std::numeric_limits<int64_t>::min();

 private:
  struct Book {
    int64_t best_bid = kNoPrice;
    int64_t best_ask = kNoPrice;
    int64_t bid_quantity = 0;
    int64_t ask_quantity = 0;
  };

  [[nodiscard]] const Book& book(uint32_t instrument) const {
    return books_[instrument % books_.size()];
  }
  [[nodiscard]] Book& book(uint32_t instrument) { return books_[instrument % books_.size()]; }

  [[nodiscard]] size_t bucket_of(uint64_t id) const {
    // Fibonacci hashing: one multiply and a shift, and it tolerates the
    // sequentially-biased ids a feed produces.
    return static_cast<size_t>((id * 0x9E3779B97F4A7C15ULL) >> 32) & bucket_mask_;
  }

  void add(const MarketEvent& event) {
    Order* order = store_.acquire();
    if (order == nullptr) {
      ++stats_.store_exhausted;
      return;
    }
    order->id = event.order_id;
    order->price = event.price;
    order->quantity = event.quantity;
    order->side = event.side;
    Order*& head = buckets_[bucket_of(event.order_id)];
    order->hash_next = head;
    head = order;
    ++live_orders_;
    ++stats_.new_orders;
    touch_best(event.instrument_id, event.side, event.price);
  }

  void remove(const MarketEvent& event, bool is_trade) {
    Order** link = &buckets_[bucket_of(event.order_id)];
    while (*link != nullptr && (*link)->id != event.order_id) link = &(*link)->hash_next;
    if (*link == nullptr) {
      ++stats_.unknown_orders;
      return;
    }
    Order* order = *link;
    *link = order->hash_next;
    store_.release(order);
    --live_orders_;
    if (is_trade) {
      ++stats_.trades;
    } else {
      ++stats_.cancels;
    }
  }

  void quote(const MarketEvent& event) {
    ++stats_.quote_updates;
    Book& b = book(event.instrument_id);
    if (event.side == Side::kBid) {
      b.best_bid = event.price;
      b.bid_quantity = event.quantity;
    } else {
      b.best_ask = event.price;
      b.ask_quantity = event.quantity;
    }
  }

  void touch_best(uint32_t instrument, Side side, int64_t price) {
    Book& b = book(instrument);
    if (side == Side::kBid) {
      if (b.best_bid == kNoPrice || price > b.best_bid) b.best_bid = price;
    } else {
      if (b.best_ask == kNoPrice || price < b.best_ask) b.best_ask = price;
    }
  }

  Store store_;
  std::vector<Order*> buckets_;
  size_t bucket_mask_;
  std::vector<Book> books_;
  size_t live_orders_ = 0;
  Stats stats_;
};

}  // namespace flashbus::market
