#include "flashbus/transport.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <algorithm>
#include <chrono>
#include <mutex>
#include <thread>

#include "flashbus/dispatcher.hpp"
#include "flashbus/platform.hpp"
#include "flashbus/session.hpp"

namespace flashbus {

const char* to_string(OverflowPolicy policy) noexcept {
  switch (policy) {
    case OverflowPolicy::kDropNewest: return "drop-newest";
    case OverflowPolicy::kDisconnect: return "disconnect";
    case OverflowPolicy::kBlock: return "block";
  }
  return "unknown";
}

std::optional<OverflowPolicy> parse_policy(std::string_view name) {
  if (name == "drop-newest") return OverflowPolicy::kDropNewest;
  if (name == "disconnect") return OverflowPolicy::kDisconnect;
  if (name == "block") return OverflowPolicy::kBlock;
  return std::nullopt;
}

struct Server::Impl {
  explicit Impl(ServerConfig server_config)
      : config(server_config),
        acceptor(io),
        dispatcher(DispatcherConfig{64, server_config.dispatcher_cpu,
                                    server_config.spin_iterations,
                                    server_config.idle_sleep_us}) {}

  ServerConfig config;
  boost::asio::io_context io{1};
  boost::asio::ip::tcp::acceptor acceptor;
  Dispatcher dispatcher;
  std::thread dispatcher_thread;
  std::atomic<bool> running{false};

  // io-thread-owned.
  std::vector<std::shared_ptr<Session>> sessions;
  // Retained so that stats survive a disconnect; the whole point of the drop
  // counters is to be readable after the fact. Guarded because stats() is
  // called from whichever thread wants a report, not from the io thread.
  mutable std::mutex channels_mutex;
  std::vector<ChannelPtr> all_channels;
  Counter accepted;

  void arm_accept();
  size_t pump_sessions();
  void reap_sessions();
};

void Server::Impl::arm_accept() {
  acceptor.async_accept([this](const boost::system::error_code& error,
                               boost::asio::ip::tcp::socket socket) {
    if (error) {
      if (error != boost::asio::error::operation_aborted) arm_accept();
      return;
    }
    auto channel = std::make_shared<Channel>(config.ingress_capacity, config.egress_capacity,
                                             config.policy);
    const SessionConfig session_config{config.read_buffer_bytes, config.egress_batch,
                                       config.adaptive_batching, config.max_payload};
    auto session = std::make_shared<Session>(std::move(socket), channel, session_config);
    sessions.push_back(session);
    {
      const std::lock_guard<std::mutex> lock(channels_mutex);
      all_channels.push_back(channel);
    }
    dispatcher.add_channel(std::move(channel));
    session->start();
    accepted.increment();
    arm_accept();
  });
}

size_t Server::Impl::pump_sessions() {
  size_t work = 0;
  for (const std::shared_ptr<Session>& session : sessions) {
    work += session->pump_egress();
    // An unfinished write is work even though no frame moved: if the loop
    // called this idle it could sleep with bytes still owed to the kernel.
    if (session->write_pending()) ++work;
  }
  return work;
}

void Server::Impl::reap_sessions() {
  std::erase_if(sessions, [](const std::shared_ptr<Session>& session) {
    return session->closed() && !session->write_pending();
  });
}

Server::Server(ServerConfig config) : impl_(std::make_unique<Impl>(config)) {}

Server::~Server() {
  stop();
  if (impl_->dispatcher_thread.joinable()) impl_->dispatcher_thread.join();
}

void Server::start() {
  using boost::asio::ip::tcp;
  const tcp::endpoint endpoint(tcp::v4(), impl_->config.port);
  impl_->acceptor.open(endpoint.protocol());
  impl_->acceptor.set_option(tcp::acceptor::reuse_address(true));
  impl_->acceptor.bind(endpoint);
  impl_->acceptor.listen();

  impl_->running.store(true, std::memory_order_release);
  impl_->arm_accept();
  impl_->dispatcher_thread =
      std::thread([this] { impl_->dispatcher.run(impl_->running); });
}

void Server::run() {
  if (impl_->config.network_cpu >= 0) {
    pin_to_cpu(static_cast<unsigned>(impl_->config.network_cpu));
  }
  size_t idle = 0;
  while (impl_->running.load(std::memory_order_relaxed)) {
    if (impl_->io.stopped()) impl_->io.restart();
    size_t work = impl_->io.poll();
    work += impl_->pump_sessions();
    impl_->reap_sessions();
    if (work != 0) {
      idle = 0;
      continue;
    }
    if (++idle >= impl_->config.spin_iterations) {
      idle = impl_->config.spin_iterations;
      std::this_thread::sleep_for(std::chrono::microseconds(impl_->config.idle_sleep_us));
    }
  }

  boost::system::error_code ignored;
  impl_->acceptor.close(ignored);
  for (const std::shared_ptr<Session>& session : impl_->sessions) session->close();
  impl_->sessions.clear();
}

void Server::stop() { impl_->running.store(false, std::memory_order_release); }

uint16_t Server::port() const {
  boost::system::error_code error;
  const auto endpoint = impl_->acceptor.local_endpoint(error);
  return error ? 0 : endpoint.port();
}

ServerStats Server::stats() const {
  ServerStats stats;
  stats.connections_accepted = impl_->accepted.get();
  const std::lock_guard<std::mutex> lock(impl_->channels_mutex);
  for (const ChannelPtr& channel : impl_->all_channels) {
    if (!channel->closed.load(std::memory_order_relaxed)) ++stats.connections_open;
    stats.frames_in += channel->net.frames_accepted.get();
    stats.frames_rejected += channel->net.frames_rejected.get();
    stats.frames_delivered += channel->net.frames_delivered.get();
    stats.bytes_in += channel->net.bytes_in.get();
    stats.bytes_out += channel->net.bytes_out.get();
    stats.frames_enqueued += channel->dispatch.frames_enqueued.get();
    stats.frames_dropped += channel->dispatch.frames_dropped.get();
    stats.egress_high_water =
        std::max(stats.egress_high_water, channel->dispatch.egress_high_water.get());
  }
  const Dispatcher::Stats dispatcher_stats = impl_->dispatcher.stats();
  stats.frames_routed = dispatcher_stats.frames_routed;
  stats.frames_unroutable = dispatcher_stats.frames_unroutable;
  stats.block_spins = dispatcher_stats.block_spins;
  stats.subscriptions = dispatcher_stats.subscriptions;
  return stats;
}

}  // namespace flashbus
