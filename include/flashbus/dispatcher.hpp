#pragma once

// Topic routing, sequencing, and the backpressure decision.
//
// One thread owns all of this. The routing table is never locked, because it is
// never read by anyone else: a SUBSCRIBE arrives as a frame on the subscriber's
// own ingress ring, so wiring up a route is just more dispatcher-thread work.

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "flashbus/transport.hpp"

namespace flashbus {

struct DispatcherConfig {
  /// Frames taken from one connection before moving to the next, so a single
  /// hot publisher cannot starve the others.
  size_t ingress_batch = 64;
  int cpu = -1;
  unsigned idle_spin_us = 500;
  unsigned idle_sleep_us = 50;
};

class Dispatcher {
 public:
  explicit Dispatcher(DispatcherConfig config);

  /// Called from the network thread when a connection is accepted. Takes a
  /// lock, which is fine: it happens once per connection, never per message.
  void add_channel(ChannelPtr channel);

  /// Runs until `running` goes false.
  void run(const std::atomic<bool>& running);

  struct Stats {
    uint64_t frames_routed = 0;
    uint64_t frames_unroutable = 0;  ///< no subscriber for the topic
    uint64_t frames_bad_topic = 0;   ///< topic id above kMaxTopicId
    uint64_t block_spins = 0;        ///< iterations spent waiting under kBlock
    uint64_t subscriptions = 0;
    uint64_t channels = 0;
  };
  [[nodiscard]] Stats stats() const;

 private:
  void absorb_new_channels();
  size_t poll_once();
  void handle(const ChannelPtr& channel, Frame& frame);
  void subscribe(const ChannelPtr& channel, uint32_t topic);
  void deliver(Channel& subscriber, const Frame& frame);
  void reap_closed();

  DispatcherConfig config_;
  std::vector<ChannelPtr> channels_;
  // Both are sized in the constructor with parentheses, never with braces.
  // `std::vector<uint64_t> v{n, 0}` is the initializer-list constructor and
  // builds a two-element vector; AddressSanitizer caught exactly that here.
  /// Flat by topic id: routing is an array index, not a hash lookup.
  std::vector<std::vector<ChannelPtr>> routes_;
  std::vector<uint64_t> next_sequence_;

  std::mutex pending_mutex_;
  std::vector<ChannelPtr> pending_;
  /// Checked once per poll so the hot loop touches a clean cache line instead
  /// of taking the lock.
  std::atomic<bool> has_pending_{false};

  Counter routed_, unroutable_, bad_topic_, block_spins_, subscriptions_;
  std::atomic<uint64_t> channel_count_{0};
};

}  // namespace flashbus
