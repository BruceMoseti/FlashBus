#pragma once

// Starts a real broker in-process on an ephemeral port.
//
// The end-to-end tests go through actual sockets rather than a mock, because
// the bugs worth catching here live in the parts a mock would replace: partial
// reads, the egress ring filling up, a session closing mid-stream.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>

#include "flashbus/clock.hpp"
#include "flashbus/platform.hpp"
#include "flashbus/transport.hpp"

namespace flashbus::testing {

/// How much load a correctness test may generate on this machine.
///
/// Every FlashBus loop spins before it parks, and the broker alone needs two
/// threads. A test that hard-codes three publishers and three subscribers needs
/// eight runnable threads; on a two-core CI runner it will report dropped
/// events, which looks exactly like a backpressure bug and is not one. Load is
/// therefore derived from the core count, and the correctness assertions hold
/// at every size.
struct LoadBudget {
  unsigned publishers;
  unsigned subscribers;
  /// Paced, so that "nothing was dropped" is a property the test can require
  /// rather than a hope about how fast the machine is.
  uint64_t rate_per_publisher;
};

/// How long a test waits for something it expects to happen.
///
/// Deliberately generous. These deadlines are not the property under test --
/// they only bound patience -- so a tight one buys nothing when the test passes
/// and turns a slow or busy machine into a red build. Under ThreadSanitizer on
/// a shared CI runner, "slow" can mean an order of magnitude. CTest's own
/// per-test timeout is the real backstop against a genuine hang.
constexpr double kPatience = 120.0;

[[nodiscard]] inline LoadBudget load_budget() {
  // available_cpu_count(), not hardware_concurrency(): the latter reports the
  // machine's online CPUs and ignores the affinity mask, so under a cpuset or a
  // container CPU limit it over-reports and this budget oversubscribes.
  const unsigned cores = std::max(2u, available_cpu_count());
  const unsigned spare = cores > 2 ? cores - 2 : 1;  // minus the broker's two
  const unsigned publishers = std::clamp(spare / 2, 1u, 3u);
  const unsigned subscribers = std::clamp(spare - publishers, 1u, 3u);
  // The rate scales with the machine too, not just the thread counts. A paced
  // publisher busy-waits between events, so on a small box the load generators
  // are already competing with the broker's own two threads; offering 100k
  // events/s there turns a correctness test into a scheduling experiment.
  // Every property these tests assert holds at any rate — that is what pacing
  // is for — so the rate is free to be conservative.
  const uint64_t rate = cores >= 6 ? 100'000 : (cores >= 4 ? 50'000 : 25'000);
  return {publishers, subscribers, rate};
}

class TestBroker {
 public:
  explicit TestBroker(ServerConfig config = {}) {
    config.port = 0;  // the kernel picks, so parallel test binaries never clash
    // No spinning. Correctness tests do not measure latency, so the spin window
    // buys them nothing and costs two cores that the publishers and subscribers
    // need -- on a small CI runner that shows up as dropped events, which looks
    // like a bug in the thing under test.
    config.idle_spin_us = 0;
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
