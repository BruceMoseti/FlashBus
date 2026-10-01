#include "flashbus/subscriber.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/write.hpp>

namespace flashbus {

Subscriber::Subscriber(const std::string& host, uint16_t port, SubscriberConfig config)
    : socket_(io_),
      config_(config),
      decoder_(config.max_payload),
      read_buffer_(config.read_buffer_bytes),
      last_sequence_(kMaxTopicId + 1, 0) {
  boost::asio::ip::tcp::resolver resolver(io_);
  boost::asio::connect(socket_, resolver.resolve(host, std::to_string(port)));
  socket_.set_option(boost::asio::ip::tcp::no_delay(true));
  if (!config_.blocking) socket_.non_blocking(true);
  connected_ = true;
}

Subscriber::~Subscriber() { close(); }

void Subscriber::subscribe(uint32_t topic) {
  MessageHeader header;
  header.type = MessageType::kSubscribe;
  header.topic = topic;
  header.payload_size = 0;
  std::byte bytes[kHeaderSize];
  encode_header(bytes, header);

  boost::system::error_code error;
  boost::asio::write(socket_, boost::asio::buffer(bytes, kHeaderSize), error);
  if (error) connected_ = false;
}

void Subscriber::track_sequence(const MessageHeader& header) {
  ++received_;
  if (header.topic > kMaxTopicId) return;
  uint64_t& last = last_sequence_[header.topic];
  // The first event on a topic sets the baseline: a subscriber that connects
  // mid-stream has not missed anything it was entitled to.
  if (last != 0 && header.sequence > last + 1) {
    ++gaps_;
    missing_ += header.sequence - last - 1;
  }
  last = header.sequence;
}

void Subscriber::close() {
  if (!socket_.is_open()) return;
  boost::system::error_code ignored;
  socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored);
  socket_.close(ignored);
  connected_ = false;
}

}  // namespace flashbus
