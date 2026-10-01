#pragma once

// Publisher client.
//
// A blocking socket, deliberately: a publisher has one thing to do and
// blocking write() is both the simplest and the lowest-latency way to do it.
// The asynchronous machinery belongs in the broker, where there are many
// sockets to multiplex.
//
// The timestamp goes into the frame at publish() time, before any batching. If
// batching adds queueing delay, that delay appears in the measured latency
// rather than hiding in it.

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <cstdint>
#include <string>
#include <vector>

#include "flashbus/message.hpp"

namespace flashbus {

struct PublisherConfig {
  /// Frames coalesced into one write() call. 1 means send immediately.
  size_t batch_frames = 1;
  uint32_t max_payload = static_cast<uint32_t>(kMaxInlinePayload);
};

class Publisher {
 public:
  /// Throws boost::system::system_error if the connection fails.
  Publisher(const std::string& host, uint16_t port, PublisherConfig config = {});
  ~Publisher();
  Publisher(const Publisher&) = delete;
  Publisher& operator=(const Publisher&) = delete;

  /// Appends a frame to the batch buffer, flushing when the batch is full.
  /// Returns false if the payload is too large or the socket is gone.
  bool publish(uint32_t topic, const void* payload, size_t size);
  /// Writes any buffered frames. Must be called before measuring, or the last
  /// partial batch is still sitting in this process.
  void flush();
  void close();

  [[nodiscard]] uint64_t sent() const { return sent_; }
  [[nodiscard]] uint64_t bytes_written() const { return bytes_written_; }
  [[nodiscard]] bool connected() const { return connected_; }

 private:
  boost::asio::io_context io_;
  boost::asio::ip::tcp::socket socket_;
  PublisherConfig config_;
  std::vector<std::byte> buffer_;
  size_t buffered_bytes_ = 0;
  size_t buffered_frames_ = 0;
  uint64_t sequence_ = 0;
  uint64_t sent_ = 0;
  uint64_t bytes_written_ = 0;
  bool connected_ = false;
};

}  // namespace flashbus
