#pragma once

// In-memory message types. The wire encoding lives in protocol.hpp; nothing
// here is memcpy'd to a socket, so these layouts are free to suit the CPU
// rather than the wire.

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "flashbus/platform.hpp"

namespace flashbus {

constexpr uint16_t kMagic = 0xFB55;
constexpr uint8_t kProtocolVersion = 1;
constexpr size_t kHeaderSize = 32;

/// Hard ceiling on a frame's payload, enforced by the decoder. A bound here is
/// what stops a corrupt or hostile length field from becoming a huge
/// allocation or an out-of-bounds read.
constexpr uint32_t kMaxPayloadSize = 64 * 1024;

/// Largest payload that fits inline in a ring slot. Ring slots are fixed size
/// so that a queued frame needs no indirection, no refcount and no allocation;
/// the cost of that choice is this ceiling.
constexpr size_t kMaxInlinePayload = 1024;

/// Topics are numeric on the hot path so routing is an array index, never a
/// string comparison. Ids are bounded so the routing table can be a flat
/// vector; the broker rejects anything above this.
constexpr uint32_t kMaxTopicId = 1023;

constexpr uint32_t kTopicTrades = 1;
constexpr uint32_t kTopicQuotes = 2;
constexpr uint32_t kTopicOrders = 3;
constexpr uint32_t kTopicSystem = 4;

/// Resolves a command-line topic argument: one of the well-known names, or a
/// plain decimal id. Names are resolved once, at startup, never per message.
/// There is no hashing here on purpose — a silent hash collision between two
/// topic names would be a miserable bug to find.
[[nodiscard]] inline std::optional<uint32_t> resolve_topic(std::string_view name) {
  if (name == "trades") return kTopicTrades;
  if (name == "quotes") return kTopicQuotes;
  if (name == "orders") return kTopicOrders;
  if (name == "system") return kTopicSystem;
  uint32_t id = 0;
  const auto* end = name.data() + name.size();
  const auto result = std::from_chars(name.data(), end, id);
  if (result.ec != std::errc{} || result.ptr != end || id > kMaxTopicId) return std::nullopt;
  return id;
}

[[nodiscard]] inline std::string topic_name(uint32_t id) {
  switch (id) {
    case kTopicTrades: return "trades";
    case kTopicQuotes: return "quotes";
    case kTopicOrders: return "orders";
    case kTopicSystem: return "system";
    default: return std::to_string(id);
  }
}

enum class MessageType : uint8_t {
  kData = 1,       ///< publisher -> broker -> subscriber
  kSubscribe = 2,  ///< subscriber -> broker, payload empty, topic in header
  kHeartbeat = 3,  ///< either direction, payload empty
};

[[nodiscard]] constexpr bool is_known_type(uint8_t raw) noexcept {
  return raw >= static_cast<uint8_t>(MessageType::kData) &&
         raw <= static_cast<uint8_t>(MessageType::kHeartbeat);
}

/// Mirrors the 32-byte wire header field for field.
struct MessageHeader {
  uint16_t magic = kMagic;
  uint8_t version = kProtocolVersion;
  MessageType type = MessageType::kData;
  uint32_t topic = 0;
  uint32_t payload_size = 0;
  uint16_t flags = 0;
  uint16_t reserved = 0;
  /// Per-stream, starts at 1. On the publisher->broker hop this is the
  /// publisher's own count; the broker overwrites it with a per-topic broker
  /// sequence before fan-out, so subscribers can tell "publisher never sent it"
  /// apart from "FlashBus dropped it for me".
  uint64_t sequence = 0;
  /// Publisher's steady_clock reading at publish, in nanoseconds. Carried
  /// end to end; a subscriber subtracts it from its own reading to get latency.
  uint64_t timestamp_ns = 0;
};

static_assert(sizeof(MessageHeader) == kHeaderSize, "header mirrors the wire layout");

/// A frame as it sits in a ring slot: header plus inline payload, cache-line
/// aligned so one frame never straddles into a neighbour's line.
template <size_t PayloadCapacity>
struct alignas(kCacheLineSize) FrameT {
  static constexpr size_t kPayloadCapacity = PayloadCapacity;
  MessageHeader header;
  std::byte payload[PayloadCapacity];
};

using Frame = FrameT<kMaxInlinePayload>;

}  // namespace flashbus
