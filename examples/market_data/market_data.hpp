#pragma once

// A synthetic market-data feed.
//
// The point is not to model a market. The point is a workload with the shape
// that matters for this kind of system: small fixed-width binary events, a
// realistic mix of message types, bursts, and a consumer that has to keep real
// state rather than just count bytes.

#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>

#include "flashbus/protocol.hpp"

namespace flashbus::market {

enum class EventType : uint8_t {
  kNewOrder = 1,
  kCancelOrder = 2,
  kTrade = 3,
  kBidUpdate = 4,
  kAskUpdate = 5,
};

enum class Side : uint8_t { kBid = 0, kAsk = 1 };

/// Fixed width, little-endian on the wire, 48 bytes. Prices are integer ticks:
/// a float price in a feed is a decision to be imprecise on purpose.
struct MarketEvent {
  uint64_t timestamp_ns = 0;
  uint64_t sequence = 0;
  uint64_t order_id = 0;
  int64_t price = 0;
  uint32_t instrument_id = 0;
  uint32_t quantity = 0;
  EventType type = EventType::kNewOrder;
  Side side = Side::kBid;
};

constexpr size_t kMarketEventWireSize = 48;

inline void encode(std::byte* dst, const MarketEvent& event) noexcept {
  store_le64(dst + 0, event.timestamp_ns);
  store_le64(dst + 8, event.sequence);
  store_le64(dst + 16, event.order_id);
  store_le64(dst + 24, static_cast<uint64_t>(event.price));
  store_le32(dst + 32, event.instrument_id);
  store_le32(dst + 36, event.quantity);
  dst[40] = static_cast<std::byte>(event.type);
  dst[41] = static_cast<std::byte>(event.side);
  store_le16(dst + 42, 0);
  store_le32(dst + 44, 0);
}

inline MarketEvent decode(const std::byte* src) noexcept {
  MarketEvent event;
  event.timestamp_ns = load_le64(src + 0);
  event.sequence = load_le64(src + 8);
  event.order_id = load_le64(src + 16);
  event.price = static_cast<int64_t>(load_le64(src + 24));
  event.instrument_id = load_le32(src + 32);
  event.quantity = load_le32(src + 36);
  event.type = static_cast<EventType>(src[40]);
  event.side = static_cast<Side>(src[41]);
  return event;
}

/// Generates a self-consistent stream: every cancel and trade refers to an
/// order that is currently live, so a consumer that rejects an unknown order id
/// is reporting a real bug rather than an artefact of the generator.
class Generator {
 public:
  struct Config {
    uint32_t instruments = 8;
    /// Order ids are sparse, as they are in a real feed, so the consumer needs
    /// a real lookup structure rather than an array index.
    uint32_t max_live_orders = 8192;
    int64_t opening_price = 100'000;  // ticks
    uint64_t seed = 0x5EED;
  };

  Generator() : Generator(Config{}) {}

  explicit Generator(Config config)
      : config_(config), rng_(config.seed), mid_(config.instruments, config.opening_price) {
    live_.reserve(config.max_live_orders);
  }

  MarketEvent next() {
    MarketEvent event;
    event.sequence = ++sequence_;
    event.instrument_id = static_cast<uint32_t>(rng_() % config_.instruments);
    int64_t& mid = mid_[event.instrument_id];
    // Random walk, floored so the price never goes non-positive.
    mid += static_cast<int64_t>(rng_() % 3) - 1;
    if (mid < 100) mid = 100;

    const unsigned roll = static_cast<unsigned>(rng_() % 100);
    const bool can_remove = !live_.empty();
    if (roll < 45) {
      event.type = (roll % 2 == 0) ? EventType::kBidUpdate : EventType::kAskUpdate;
      event.side = (event.type == EventType::kBidUpdate) ? Side::kBid : Side::kAsk;
      event.price = mid + (event.side == Side::kBid ? -1 : 1);
      event.quantity = static_cast<uint32_t>(1 + rng_() % 1000);
    } else if (roll < 75 || !can_remove) {
      event.type = EventType::kNewOrder;
      event.side = (rng_() % 2 == 0) ? Side::kBid : Side::kAsk;
      event.price = mid + (event.side == Side::kBid ? -(1 + static_cast<int64_t>(rng_() % 10))
                                                    : (1 + static_cast<int64_t>(rng_() % 10)));
      event.quantity = static_cast<uint32_t>(1 + rng_() % 500);
      // Sparse ids, with gaps, like a real exchange.
      event.order_id = next_order_id_;
      next_order_id_ += 1 + rng_() % 7;
      if (live_.size() < config_.max_live_orders) {
        live_.push_back(event.order_id);
      } else {
        // At capacity: turn this into a cancel of an existing order instead, so
        // the live set stays bounded without inventing an unknown id.
        const size_t index = rng_() % live_.size();
        event.type = EventType::kCancelOrder;
        event.order_id = live_[index];
        live_[index] = live_.back();
        live_.pop_back();
      }
    } else {
      const size_t index = rng_() % live_.size();
      event.order_id = live_[index];
      live_[index] = live_.back();
      live_.pop_back();
      event.type = (roll < 95) ? EventType::kCancelOrder : EventType::kTrade;
      event.quantity = static_cast<uint32_t>(1 + rng_() % 500);
      event.price = mid;
    }
    return event;
  }

  [[nodiscard]] size_t live_orders() const { return live_.size(); }

 private:
  Config config_;
  std::mt19937_64 rng_;
  std::vector<int64_t> mid_;
  std::vector<uint64_t> live_;
  uint64_t sequence_ = 0;
  uint64_t next_order_id_ = 1;
};

}  // namespace flashbus::market
