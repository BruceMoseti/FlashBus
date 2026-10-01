#include "flashbus/session.hpp"

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>

#include <algorithm>
#include <cstring>

namespace flashbus {
namespace {

/// Non-blocking sockets plus an explicit poll loop, rather than async_write
/// per frame: the write path then costs one syscall per batch and allocates
/// nothing per message.
void configure(boost::asio::ip::tcp::socket& socket) {
  boost::system::error_code ignored;
  socket.set_option(boost::asio::ip::tcp::no_delay(true), ignored);
  socket.non_blocking(true, ignored);
}

bool is_retryable(const boost::system::error_code& error) {
  return error == boost::asio::error::would_block || error == boost::asio::error::try_again;
}

}  // namespace

Session::Session(boost::asio::ip::tcp::socket socket, ChannelPtr channel, SessionConfig config)
    : socket_(std::move(socket)),
      channel_(std::move(channel)),
      config_(config),
      decoder_(config.max_payload),
      read_buffer_(config.read_buffer_bytes),
      // One batch of maximum-size frames, allocated once. Because a new batch
      // is only started once the previous one is fully written, this is an
      // exact bound rather than a guess.
      write_buffer_(config.egress_batch * (kHeaderSize + kMaxInlinePayload)) {
  configure(socket_);
}

void Session::start() { arm_read(); }

void Session::arm_read() {
  if (closed()) return;
  read_armed_ = true;
  auto self = shared_from_this();
  socket_.async_read_some(
      boost::asio::buffer(read_buffer_.data(), read_buffer_.size()),
      [this, self](const boost::system::error_code& error, std::size_t bytes) {
        read_armed_ = false;
        if (error) {
          close();
          return;
        }
        channel_->net.bytes_in.increment(bytes);
        const DecodeError decode_error = decoder_.feed(
            read_buffer_.data(), bytes,
            [this](const MessageHeader& header, const std::byte* payload, size_t size) {
              on_frame(header, payload, size);
            });
        if (decode_error != DecodeError::kOk) {
          // A corrupt stream is not resynchronised, by design: we cannot know
          // where the next real header starts.
          decode_error_ = decode_error;
          close();
          return;
        }
        arm_read();
      });
}

void Session::on_frame(const MessageHeader& header, const std::byte* payload, size_t size) {
  if (size > Frame::kPayloadCapacity) {
    channel_->net.frames_oversize.increment();
    return;
  }
  Frame* slot = channel_->ingress.claim();
  if (slot == nullptr) {
    // Ingress backpressure: the dispatcher is behind. Counted, not buffered.
    channel_->net.frames_rejected.increment();
    return;
  }
  slot->header = header;
  std::memcpy(slot->payload, payload, size);
  channel_->ingress.commit();
  channel_->net.frames_accepted.increment();
}

size_t Session::batch_size() const {
  if (!config_.adaptive_batching) return config_.egress_batch;
  // Send immediately when quiet, coalesce when the queue is building up: the
  // extra queueing delay of a batch is only worth paying when there is already
  // a queue. Thresholds are relative to the ring so they scale with capacity.
  const size_t depth = channel_->egress.size();
  const size_t capacity = channel_->egress.capacity();
  if (depth * 32 < capacity) return 1;
  if (depth * 8 < capacity) return std::min<size_t>(8, config_.egress_batch);
  return config_.egress_batch;
}

size_t Session::pump_egress() {
  if (closed()) {
    if (socket_.is_open()) close();
    return 0;
  }
  if (write_pending()) {
    flush();
    if (write_pending()) return 0;  // kernel buffer is full; retry next poll
  }

  size_t frames = 0;
  size_t bytes = 0;
  channel_->egress.consume(batch_size(), [&](Frame& frame) {
    const size_t payload_size = frame.header.payload_size;
    std::byte* out = write_buffer_.data() + write_tail_;
    encode_header(out, frame.header);
    std::memcpy(out + kHeaderSize, frame.payload, payload_size);
    write_tail_ += kHeaderSize + payload_size;
    ++frames;
    bytes += kHeaderSize + payload_size;
  });

  if (frames == 0) return 0;
  channel_->net.frames_delivered.increment(frames);
  (void)bytes;
  flush();
  return frames;
}

void Session::flush() {
  while (write_head_ < write_tail_) {
    boost::system::error_code error;
    const size_t written = socket_.write_some(
        boost::asio::buffer(write_buffer_.data() + write_head_, write_tail_ - write_head_), error);
    write_head_ += written;
    channel_->net.bytes_out.increment(written);
    if (is_retryable(error)) return;
    if (error) {
      close();
      return;
    }
  }
  write_head_ = write_tail_ = 0;
}

void Session::close() {
  channel_->closed.store(true, std::memory_order_release);
  if (socket_.is_open()) {
    boost::system::error_code ignored;
    socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored);
    socket_.close(ignored);
  }
}

}  // namespace flashbus
