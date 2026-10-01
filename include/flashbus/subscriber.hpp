#pragma once

// Subscriber client, with sequence-gap detection.
//
// The broker stamps every event with a per-topic broker sequence, so a
// subscriber can tell exactly how many events it missed and say so, rather
// than quietly receiving less than it thinks.

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <cstdint>
#include <string>
#include <vector>

#include "flashbus/protocol.hpp"

namespace flashbus {

struct SubscriberConfig {
  size_t read_buffer_bytes = 64 * 1024;
  uint32_t max_payload = static_cast<uint32_t>(kMaxInlinePayload);
  /// Blocking reads park the thread until bytes arrive, which is what a real
  /// consumer wants. Non-blocking suits a thread that has other work to do.
  bool blocking = true;
};

class Subscriber {
 public:
  Subscriber(const std::string& host, uint16_t port, SubscriberConfig config = {});
  ~Subscriber();
  Subscriber(const Subscriber&) = delete;
  Subscriber& operator=(const Subscriber&) = delete;

  void subscribe(uint32_t topic);

  /// Reads one chunk and calls `fn(const MessageHeader&, const std::byte*
  /// payload, size_t size)` for each complete frame it contains. Returns the
  /// number of frames handled; 0 means nothing was available (non-blocking
  /// mode) or the peer went away.
  template <typename Fn>
  size_t poll(Fn&& fn) {
    if (!connected_) return 0;
    boost::system::error_code error;
    const size_t bytes = socket_.read_some(
        boost::asio::buffer(read_buffer_.data(), read_buffer_.size()), error);
    if (error) {
      if (error != boost::asio::error::would_block && error != boost::asio::error::try_again) {
        connected_ = false;
      }
      return 0;
    }
    bytes_read_ += bytes;
    size_t frames = 0;
    const DecodeError decode_error = decoder_.feed(
        read_buffer_.data(), bytes,
        [&](const MessageHeader& header, const std::byte* payload, size_t size) {
          track_sequence(header);
          ++frames;
          fn(header, payload, size);
        });
    if (decode_error != DecodeError::kOk) {
      decode_error_ = decode_error;
      connected_ = false;
    }
    return frames;
  }

  void close();

  [[nodiscard]] bool connected() const { return connected_; }
  [[nodiscard]] uint64_t received() const { return received_; }
  [[nodiscard]] uint64_t bytes_read() const { return bytes_read_; }
  /// Number of discontinuities in the broker sequence.
  [[nodiscard]] uint64_t gaps() const { return gaps_; }
  /// Number of events those discontinuities account for.
  [[nodiscard]] uint64_t missing() const { return missing_; }
  [[nodiscard]] DecodeError decode_error() const { return decode_error_; }

 private:
  void track_sequence(const MessageHeader& header);

  boost::asio::io_context io_;
  boost::asio::ip::tcp::socket socket_;
  SubscriberConfig config_;
  StreamDecoder decoder_;
  std::vector<std::byte> read_buffer_;
  /// Last broker sequence seen per topic; 0 means nothing seen yet.
  std::vector<uint64_t> last_sequence_;
  uint64_t received_ = 0;
  uint64_t bytes_read_ = 0;
  uint64_t gaps_ = 0;
  uint64_t missing_ = 0;
  DecodeError decode_error_ = DecodeError::kOk;
  bool connected_ = false;
};

}  // namespace flashbus
