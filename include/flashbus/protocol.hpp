#pragma once

// Wire encoding and the streaming decoder.
//
// TCP delivers a byte stream, not a message stream: one recv() may contain
// three frames and half of a fourth, or four bytes of a header. The decoder
// below is a state machine over whatever arrives, and it is tested a byte at a
// time (tests/protocol_test.cpp).
//
// Byte order is little-endian, explicitly. The accessors are byte-wise so a
// big-endian host stays correct; on a little-endian host the compiler folds
// each one back into a single load or store.

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "flashbus/message.hpp"

namespace flashbus {

inline void store_le16(std::byte* p, uint16_t v) noexcept {
  p[0] = static_cast<std::byte>(v & 0xFF);
  p[1] = static_cast<std::byte>((v >> 8) & 0xFF);
}
inline void store_le32(std::byte* p, uint32_t v) noexcept {
  store_le16(p, static_cast<uint16_t>(v & 0xFFFF));
  store_le16(p + 2, static_cast<uint16_t>((v >> 16) & 0xFFFF));
}
inline void store_le64(std::byte* p, uint64_t v) noexcept {
  store_le32(p, static_cast<uint32_t>(v & 0xFFFFFFFF));
  store_le32(p + 4, static_cast<uint32_t>((v >> 32) & 0xFFFFFFFF));
}
[[nodiscard]] inline uint16_t load_le16(const std::byte* p) noexcept {
  return static_cast<uint16_t>(static_cast<uint16_t>(p[0]) |
                               static_cast<uint16_t>(static_cast<uint16_t>(p[1]) << 8));
}
[[nodiscard]] inline uint32_t load_le32(const std::byte* p) noexcept {
  return static_cast<uint32_t>(load_le16(p)) | (static_cast<uint32_t>(load_le16(p + 2)) << 16);
}
[[nodiscard]] inline uint64_t load_le64(const std::byte* p) noexcept {
  return static_cast<uint64_t>(load_le32(p)) | (static_cast<uint64_t>(load_le32(p + 4)) << 32);
}

enum class DecodeError : uint8_t {
  kOk = 0,
  kBadMagic,
  kBadVersion,
  kBadType,
  kPayloadTooLarge,
  kReservedNonZero,
};

[[nodiscard]] inline const char* to_string(DecodeError error) noexcept {
  switch (error) {
    case DecodeError::kOk: return "ok";
    case DecodeError::kBadMagic: return "bad magic";
    case DecodeError::kBadVersion: return "unsupported version";
    case DecodeError::kBadType: return "unknown message type";
    case DecodeError::kPayloadTooLarge: return "payload too large";
    case DecodeError::kReservedNonZero: return "reserved field set";
  }
  return "unknown";
}

/// Writes exactly kHeaderSize bytes.
inline void encode_header(std::byte* dst, const MessageHeader& h) noexcept {
  store_le16(dst + 0, h.magic);
  dst[2] = static_cast<std::byte>(h.version);
  dst[3] = static_cast<std::byte>(h.type);
  store_le32(dst + 4, h.topic);
  store_le32(dst + 8, h.payload_size);
  store_le16(dst + 12, h.flags);
  store_le16(dst + 14, h.reserved);
  store_le64(dst + 16, h.sequence);
  store_le64(dst + 24, h.timestamp_ns);
}

/// Reads kHeaderSize bytes. Every field that can make the rest of the parse
/// unsafe is validated here, so callers never see a header they must not trust.
[[nodiscard]] inline DecodeError decode_header(const std::byte* src, MessageHeader& out,
                                               uint32_t max_payload) noexcept {
  const uint16_t magic = load_le16(src + 0);
  if (magic != kMagic) return DecodeError::kBadMagic;
  const auto version = static_cast<uint8_t>(src[2]);
  if (version != kProtocolVersion) return DecodeError::kBadVersion;
  const auto type = static_cast<uint8_t>(src[3]);
  if (!is_known_type(type)) return DecodeError::kBadType;
  const uint16_t reserved = load_le16(src + 14);
  if (reserved != 0) return DecodeError::kReservedNonZero;
  const uint32_t payload_size = load_le32(src + 8);
  if (payload_size > max_payload) return DecodeError::kPayloadTooLarge;

  out.magic = magic;
  out.version = version;
  out.type = static_cast<MessageType>(type);
  out.topic = load_le32(src + 4);
  out.payload_size = payload_size;
  out.flags = load_le16(src + 12);
  out.reserved = reserved;
  out.sequence = load_le64(src + 16);
  out.timestamp_ns = load_le64(src + 24);
  return DecodeError::kOk;
}

/// Reassembles frames from arbitrarily fragmented byte chunks.
///
/// Two paths: when nothing is held over from the previous chunk, frames are
/// parsed in place out of the caller's buffer and only the trailing partial
/// frame is copied; otherwise the chunk tops up the held partial frame first.
/// After construction it never allocates: the buffer is sized to hold one
/// maximum frame, which is the largest partial frame that can exist.
class StreamDecoder {
 public:
  explicit StreamDecoder(uint32_t max_payload = kMaxPayloadSize)
      : max_payload_(max_payload), buffer_(kHeaderSize + max_payload) {}

  /// Calls `fn(const MessageHeader&, const std::byte* payload, size_t size)`
  /// for every complete frame. Stops at the first malformed header and returns
  /// the error; the caller must close the connection, because FlashBus never
  /// tries to resynchronise a corrupt stream.
  template <typename Fn>
  DecodeError feed(const std::byte* data, size_t size, Fn&& fn) {
    while (size > 0) {
      if (buffered() == 0) {
        head_ = tail_ = 0;
        size_t consumed = 0;
        const DecodeError error = parse(data, size, consumed, fn);
        if (error != DecodeError::kOk) return error;
        data += consumed;
        size -= consumed;
        if (size == 0) return DecodeError::kOk;
        // What remains is one partial frame, so it fits by construction.
        assert(size <= buffer_.size());
        std::memcpy(buffer_.data(), data, size);
        tail_ = size;
        return DecodeError::kOk;
      }

      const size_t appended = append(data, size);
      assert(appended > 0);  // compaction always leaves room for a partial frame
      data += appended;
      size -= appended;

      size_t consumed = 0;
      const DecodeError error = parse(buffer_.data() + head_, buffered(), consumed, fn);
      head_ += consumed;
      if (error != DecodeError::kOk) return error;
      if (buffered() == 0) head_ = tail_ = 0;
    }
    return DecodeError::kOk;
  }

  [[nodiscard]] uint64_t frames_decoded() const noexcept { return frames_; }
  /// Bytes of an incomplete frame currently held over.
  [[nodiscard]] size_t pending() const noexcept { return buffered(); }

  void reset() noexcept {
    head_ = tail_ = 0;
    frames_ = 0;
  }

 private:
  [[nodiscard]] size_t buffered() const noexcept { return tail_ - head_; }

  /// Copies as much of `data` as fits after compacting the held bytes to the
  /// front. Returns the number of bytes taken, always at least one.
  size_t append(const std::byte* data, size_t size) noexcept {
    if (head_ > 0 && buffer_.size() - tail_ < size) {
      std::memmove(buffer_.data(), buffer_.data() + head_, buffered());
      tail_ = buffered();
      head_ = 0;
    }
    const size_t room = buffer_.size() - tail_;
    const size_t count = room < size ? room : size;
    std::memcpy(buffer_.data() + tail_, data, count);
    tail_ += count;
    return count;
  }

  template <typename Fn>
  DecodeError parse(const std::byte* p, size_t size, size_t& consumed, Fn& fn) {
    consumed = 0;
    for (;;) {
      const size_t available = size - consumed;
      if (available < kHeaderSize) return DecodeError::kOk;
      MessageHeader header;
      const DecodeError error = decode_header(p + consumed, header, max_payload_);
      if (error != DecodeError::kOk) return error;
      const size_t total = kHeaderSize + header.payload_size;
      if (available < total) return DecodeError::kOk;
      fn(header, p + consumed + kHeaderSize, static_cast<size_t>(header.payload_size));
      consumed += total;
      ++frames_;
    }
  }

  uint32_t max_payload_;
  std::vector<std::byte> buffer_;
  size_t head_ = 0;
  size_t tail_ = 0;
  uint64_t frames_ = 0;
};

}  // namespace flashbus
