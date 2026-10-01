#pragma once

// Transport-layer vocabulary shared by the network threads and the dispatcher,
// plus the server itself.
//
// The key type is Channel: the only state two FlashBus threads share. It holds
// no socket and no io_context, so either thread may hold the last reference to
// it and destroy it, which is what keeps session teardown free of
// cross-thread lifetime games.

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "flashbus/message.hpp"
#include "flashbus/metrics.hpp"
#include "flashbus/ring_buffer.hpp"

namespace flashbus {

/// What the dispatcher does when a subscriber's egress ring is full.
enum class OverflowPolicy : uint8_t {
  /// Discard the event for this subscriber only, and count it. The default,
  /// because a market-data subscriber that cannot keep up is better served by
  /// the next event than by an old one.
  kDropNewest,
  /// Close the slow subscriber. It must reconnect, and it knows it fell behind.
  kDisconnect,
  /// Wait for room. Correct, and it converts one slow subscriber into a
  /// system-wide stall; the slow-consumer benchmark exists to show that.
  kBlock,
};

[[nodiscard]] const char* to_string(OverflowPolicy policy) noexcept;
[[nodiscard]] std::optional<OverflowPolicy> parse_policy(std::string_view name);

/// Everything one connection shares between the network thread and the
/// dispatcher. Counters are single-writer; the comments say which thread owns
/// each one, and the cache-line groups keep the two threads' writes apart.
struct Channel {
  Channel(size_t ingress_capacity, size_t egress_capacity, OverflowPolicy overflow_policy)
      : ingress(ingress_capacity), egress(egress_capacity), policy(overflow_policy) {}

  /// Network thread produces, dispatcher consumes.
  SpscRing<Frame> ingress;
  /// Dispatcher produces, network thread consumes.
  SpscRing<Frame> egress;

  struct alignas(kCacheLineSize) NetworkOwned {
    Counter frames_accepted;   ///< decoded and queued for the dispatcher
    Counter frames_rejected;   ///< ingress ring was full
    Counter frames_oversize;   ///< payload larger than a ring slot
    Counter frames_delivered;  ///< written to the subscriber's socket
    Counter bytes_in;
    Counter bytes_out;
  };
  struct alignas(kCacheLineSize) DispatcherOwned {
    Counter frames_enqueued;
    Counter frames_dropped;     ///< overflow policy discarded them
    Counter egress_high_water;  ///< deepest the egress ring ever got
  };

  NetworkOwned net;
  DispatcherOwned dispatch;

  /// Set by whichever side notices first; never cleared. The network thread
  /// closes the socket, the dispatcher drops its routes.
  alignas(kCacheLineSize) std::atomic<bool> closed{false};
  OverflowPolicy policy;
};

using ChannelPtr = std::shared_ptr<Channel>;

struct ServerConfig {
  uint16_t port = 9000;
  size_t ingress_capacity = 4096;
  size_t egress_capacity = 4096;
  size_t read_buffer_bytes = 64 * 1024;
  /// Frames coalesced into one write() call. See docs/PERFORMANCE.md case
  /// study 3 for what this costs and buys.
  size_t egress_batch = 32;
  /// Scales the batch with queue depth: send immediately when quiet, coalesce
  /// when busy.
  bool adaptive_batching = false;
  OverflowPolicy policy = OverflowPolicy::kDropNewest;
  uint32_t max_payload = static_cast<uint32_t>(kMaxInlinePayload);
  /// -1 leaves placement to the scheduler, which is the honest default until
  /// the affinity experiment says otherwise on a given machine.
  int network_cpu = -1;
  int dispatcher_cpu = -1;
  /// How long a loop keeps spinning after its last piece of work before it
  /// parks. Measured in time, not iterations: the dispatcher's idle iteration
  /// costs about 8 ns and the network loop's costs hundreds, so an iteration
  /// count means something different on each loop and something different again
  /// on the next machine. Sized to cover the gap between events at the rates
  /// FlashBus is for; below that it parks and gives the core back.
  unsigned idle_spin_us = 500;
  /// How long a parked loop stays parked before looking again. The network loop
  /// parks in epoll, so a socket event wakes it early; the dispatcher has no
  /// descriptor to wait on, so for it this is a real sleep and a real addition
  /// to the latency of the event that ends an idle period.
  unsigned idle_sleep_us = 50;
};

struct ServerStats {
  uint64_t connections_accepted = 0;
  uint64_t connections_open = 0;
  uint64_t frames_in = 0;
  uint64_t frames_rejected = 0;
  uint64_t frames_routed = 0;
  uint64_t frames_enqueued = 0;
  uint64_t frames_dropped = 0;
  uint64_t frames_delivered = 0;
  uint64_t frames_unroutable = 0;
  uint64_t bytes_in = 0;
  uint64_t bytes_out = 0;
  uint64_t block_spins = 0;
  uint64_t egress_high_water = 0;
  uint64_t subscriptions = 0;
};

class Dispatcher;
class Session;

/// A FlashBus broker: one network thread, one dispatcher thread.
class Server {
 public:
  explicit Server(ServerConfig config);
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  /// Binds, listens, and starts the dispatcher thread. Throws on bind failure.
  void start();
  /// Runs the network loop on the calling thread until `stop()`.
  void run();
  /// Safe to call from any thread.
  void stop();

  /// The bound port, which matters when `port` was 0 and the kernel chose.
  [[nodiscard]] uint16_t port() const;
  [[nodiscard]] ServerStats stats() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace flashbus
