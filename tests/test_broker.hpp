#pragma once

// Starts a real broker in-process on an ephemeral port.
//
// The end-to-end tests go through actual sockets rather than a mock, because
// the bugs worth catching here live in the parts a mock would replace: partial
// reads, the egress ring filling up, a session closing mid-stream.

#include <atomic>
#include <chrono>
#include <thread>

#include "flashbus/clock.hpp"
#include "flashbus/transport.hpp"

namespace flashbus::testing {

class TestBroker {
 public:
  explicit TestBroker(ServerConfig config = {}) {
    config.port = 0;  // the kernel picks, so parallel test binaries never clash
    server_ = std::make_unique<Server>(config);
    server_->start();
    port_ = server_->port();
    thread_ = std::thread([this] { server_->run(); });
  }

  ~TestBroker() {
    server_->stop();
    if (thread_.joinable()) thread_.join();
  }

  TestBroker(const TestBroker&) = delete;
  TestBroker& operator=(const TestBroker&) = delete;

  [[nodiscard]] uint16_t port() const { return port_; }
  [[nodiscard]] ServerStats stats() const { return server_->stats(); }

  /// Publishing before the broker has the route would legitimately drop events,
  /// so every test waits for the subscription to land first.
  [[nodiscard]] bool wait_for_subscriptions(uint64_t expected, double timeout_s = 5.0) const {
    return wait_until([&] { return server_->stats().subscriptions >= expected; }, timeout_s);
  }

  template <typename Predicate>
  [[nodiscard]] static bool wait_until(Predicate predicate, double timeout_s = 5.0) {
    const uint64_t deadline = now_ns() + static_cast<uint64_t>(timeout_s * 1e9);
    while (now_ns() < deadline) {
      if (predicate()) return true;
      std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    return predicate();
  }

 private:
  std::unique_ptr<Server> server_;
  std::thread thread_;
  uint16_t port_ = 0;
};

}  // namespace flashbus::testing
