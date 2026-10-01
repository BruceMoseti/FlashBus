#pragma once

// One connected peer, from the network thread's point of view.
//
// A Session is touched by exactly one thread: the one running the io_context.
// Everything the dispatcher needs is behind the Channel, so there is no lock
// here and no shared mutable state beyond the two rings.

#include <boost/asio/ip/tcp.hpp>

#include <cstddef>
#include <memory>
#include <vector>

#include "flashbus/protocol.hpp"
#include "flashbus/transport.hpp"

namespace flashbus {

struct SessionConfig {
  size_t read_buffer_bytes = 64 * 1024;
  size_t egress_batch = 32;
  bool adaptive_batching = false;
  uint32_t max_payload = static_cast<uint32_t>(kMaxInlinePayload);
};

class Session : public std::enable_shared_from_this<Session> {
 public:
  Session(boost::asio::ip::tcp::socket socket, ChannelPtr channel, SessionConfig config);

  /// Arms the first read. Must be called on the io thread.
  void start();

  /// Moves frames from the egress ring to the socket and returns how many were
  /// written. A partially written batch is finished before a new one starts, so
  /// the write buffer never needs to grow.
  size_t pump_egress();

  /// True when a write is still owed to the kernel, which counts as work for
  /// the event loop's idle detection.
  [[nodiscard]] bool write_pending() const { return write_head_ < write_tail_; }

  void close();
  [[nodiscard]] bool closed() const { return channel_->closed.load(std::memory_order_relaxed); }
  [[nodiscard]] const ChannelPtr& channel() const { return channel_; }
  /// Why the connection was dropped, when it was dropped for a protocol error.
  [[nodiscard]] DecodeError decode_error() const { return decode_error_; }

 private:
  void arm_read();
  void on_frame(const MessageHeader& header, const std::byte* payload, size_t size);
  void flush();
  [[nodiscard]] size_t batch_size() const;

  boost::asio::ip::tcp::socket socket_;
  ChannelPtr channel_;
  SessionConfig config_;
  StreamDecoder decoder_;
  std::vector<std::byte> read_buffer_;
  std::vector<std::byte> write_buffer_;
  size_t write_head_ = 0;
  size_t write_tail_ = 0;
  bool read_armed_ = false;
  DecodeError decode_error_ = DecodeError::kOk;
};

}  // namespace flashbus
