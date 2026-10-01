#include "flashbus/publisher.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/write.hpp>

#include <cstring>

#include "flashbus/clock.hpp"
#include "flashbus/protocol.hpp"

namespace flashbus {

Publisher::Publisher(const std::string& host, uint16_t port, PublisherConfig config)
    : socket_(io_),
      config_(config),
      buffer_(config.batch_frames * (kHeaderSize + config.max_payload)) {
  boost::asio::ip::tcp::resolver resolver(io_);
  boost::asio::connect(socket_, resolver.resolve(host, std::to_string(port)));
  socket_.set_option(boost::asio::ip::tcp::no_delay(true));
  connected_ = true;
}

Publisher::~Publisher() { close(); }

bool Publisher::publish(uint32_t topic, const void* payload, size_t size) {
  if (!connected_ || size > config_.max_payload) return false;

  MessageHeader header;
  header.type = MessageType::kData;
  header.topic = topic;
  header.payload_size = static_cast<uint32_t>(size);
  header.sequence = ++sequence_;
  header.timestamp_ns = now_ns();

  std::byte* out = buffer_.data() + buffered_bytes_;
  encode_header(out, header);
  // A zero-length payload is legal, and callers reach it with a null pointer
  // (an empty container's data()). memcpy from null is undefined even with a
  // length of zero.
  if (size != 0) std::memcpy(out + kHeaderSize, payload, size);
  buffered_bytes_ += kHeaderSize + size;
  ++buffered_frames_;
  ++sent_;

  if (buffered_frames_ >= config_.batch_frames) flush();
  return connected_;
}

void Publisher::flush() {
  if (buffered_bytes_ == 0 || !connected_) {
    buffered_bytes_ = buffered_frames_ = 0;
    return;
  }
  boost::system::error_code error;
  const size_t written = boost::asio::write(
      socket_, boost::asio::buffer(buffer_.data(), buffered_bytes_), error);
  bytes_written_ += written;
  buffered_bytes_ = 0;
  buffered_frames_ = 0;
  if (error) connected_ = false;
}

void Publisher::close() {
  if (!socket_.is_open()) return;
  flush();
  boost::system::error_code ignored;
  socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored);
  socket_.close(ignored);
  connected_ = false;
}

}  // namespace flashbus
