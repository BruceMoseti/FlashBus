#include "flashbus/transport.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <algorithm>
#include <chrono>
#include <mutex>
#include <thread>

#include "flashbus/clock.hpp"
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
                                    server_config.idle_spin_us,
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
    session->ensure_read_armed();
    accepted.increment();
    arm_accept();
  });
}

size_t Server::Impl::pump_sessions() {
  size_t work = 0;
  for (const std::shared_ptr<Session>& session : sessions) {
    work += session->pump_egress();
    // An unfinished write is work even though no frame moved: if the loop
    // called this idle it could block with bytes still owed to the kernel.
    if (session->write_pending()) ++work;
    // Egress is drained first, then the next read is requested. That ordering
    // is what keeps ingress from running ahead of egress.
    session->ensure_read_armed();
    // A stalled read means the dispatcher is behind, so keep spinning rather
    // than blocking: the ring will drain and the read can go out immediately.
    if (session->read_stalled()) ++work;
  }
  return work;
}

void Server::Impl::reap_sessions() {
  std::erase_if(sessions, [](const std::shared_ptr<Session>& session) {
    if (!session->closed()) return false;
    // Close before dropping, always. The channel's closed flag can be set by
    // the dispatcher — that is what the disconnect policy does — and this is
    // the only thread allowed to touch the socket. Erasing the session first
    // and closing it later never happens: the in-flight read keeps the Session
    // object alive, so the socket would stay open with nobody left to poll it,
    // and the peer would never learn it had been disconnected.
    session->close();
    return true;
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
  const uint64_t spin_ns = uint64_t{impl_->config.idle_spin_us} * 1000;
  uint64_t idle_since_ns = now_ns();
  bool was_busy = false;

  while (impl_->running.load(std::memory_order_relaxed)) {
    if (impl_->io.stopped()) impl_->io.restart();
    size_t work = impl_->io.poll();
    work += impl_->pump_sessions();
    impl_->reap_sessions();
    if (work != 0) {
      was_busy = true;
      continue;
    }
    if (was_busy) {
      idle_since_ns = now_ns();
      was_busy = false;
      continue;
    }
    if (now_ns() - idle_since_ns < spin_ns) continue;

    // Park in epoll rather than sleep: a socket becoming readable wakes this
    // immediately, so an idle broker does not add its timeout to the first
    // event of a burst. The timeout only bounds how long a frame the dispatcher
    // queued while we were parked can sit in an egress ring.
    //
    // Treating the handler this runs as work matters. A read completing here
    // has produced no egress frame yet, because the dispatcher has not seen it;
    // if the iteration counted as idle the loop would park again immediately
    // and the timeout would land on the latency of every single message.
    if (impl_->io.run_one_for(std::chrono::microseconds(impl_->config.idle_sleep_us)) != 0) {
      was_busy = true;
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
