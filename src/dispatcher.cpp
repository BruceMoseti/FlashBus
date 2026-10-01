#include "flashbus/dispatcher.hpp"

#include <chrono>
#include <cstring>
#include <thread>

#include "flashbus/clock.hpp"
#include "flashbus/platform.hpp"

namespace flashbus {

Dispatcher::Dispatcher(DispatcherConfig config) : config_(config) {}

void Dispatcher::add_channel(ChannelPtr channel) {
  const std::lock_guard<std::mutex> lock(pending_mutex_);
  pending_.push_back(std::move(channel));
  has_pending_.store(true, std::memory_order_release);
}

void Dispatcher::absorb_new_channels() {
  std::vector<ChannelPtr> taken;
  {
    const std::lock_guard<std::mutex> lock(pending_mutex_);
    taken.swap(pending_);
    has_pending_.store(false, std::memory_order_release);
  }
  for (auto& channel : taken) channels_.push_back(std::move(channel));
  channel_count_.store(channels_.size(), std::memory_order_relaxed);
}

void Dispatcher::run(const std::atomic<bool>& running) {
  if (config_.cpu >= 0) pin_to_cpu(static_cast<unsigned>(config_.cpu));

  const uint64_t spin_ns = uint64_t{config_.idle_spin_us} * 1000;
  uint64_t idle_since_ns = now_ns();
  bool was_busy = false;

  while (running.load(std::memory_order_relaxed)) {
    if (has_pending_.load(std::memory_order_acquire)) absorb_new_channels();
    if (poll_once() != 0) {
      was_busy = true;
      continue;
    }
    // The clock is read on the first idle iteration after work, never on the
    // busy path, so routing never pays for the backoff policy.
    if (was_busy) {
      idle_since_ns = now_ns();
      was_busy = false;
      continue;
    }
    // Spin before sleeping: this thread has nothing to wait on, so a sleep here
    // lands directly on the latency of whichever event ends the idle period.
    if (now_ns() - idle_since_ns < spin_ns) continue;
    std::this_thread::sleep_for(std::chrono::microseconds(config_.idle_sleep_us));
  }
}

size_t Dispatcher::poll_once() {
  size_t work = 0;
  bool any_closed = false;
  for (const ChannelPtr& channel : channels_) {
    work += channel->ingress.consume(config_.ingress_batch,
                                     [&](Frame& frame) { handle(channel, frame); });
    if (channel->closed.load(std::memory_order_acquire)) any_closed = true;
  }
  if (any_closed) reap_closed();
  return work;
}

void Dispatcher::handle(const ChannelPtr& channel, Frame& frame) {
  const uint32_t topic = frame.header.topic;
  if (topic > kMaxTopicId) {
    bad_topic_.increment();
    return;
  }
  switch (frame.header.type) {
    case MessageType::kSubscribe:
      subscribe(channel, topic);
      return;
    case MessageType::kHeartbeat:
      return;
    case MessageType::kData:
      break;
  }

  std::vector<ChannelPtr>& routes = routes_[topic];
  if (routes.empty()) {
    unroutable_.increment();
    return;
  }
  // Broker sequence, per topic. This is what lets a subscriber distinguish
  // "the publisher never sent it" from "FlashBus dropped it for me", with any
  // number of publishers on the topic.
  frame.header.sequence = ++next_sequence_[topic];
  for (const ChannelPtr& subscriber : routes) deliver(*subscriber, frame);
  routed_.increment();
}

void Dispatcher::subscribe(const ChannelPtr& channel, uint32_t topic) {
  std::vector<ChannelPtr>& routes = routes_[topic];
  for (const ChannelPtr& existing : routes) {
    if (existing == channel) return;
  }
  routes.push_back(channel);
  subscriptions_.increment();
}

void Dispatcher::deliver(Channel& subscriber, const Frame& frame) {
  Frame* slot = subscriber.egress.claim();
  if (slot == nullptr) {
    switch (subscriber.policy) {
      case OverflowPolicy::kDropNewest:
        subscriber.dispatch.frames_dropped.increment();
        return;
      case OverflowPolicy::kDisconnect:
        subscriber.dispatch.frames_dropped.increment();
        subscriber.closed.store(true, std::memory_order_release);
        return;
      case OverflowPolicy::kBlock:
        // Everyone waits for this one subscriber. That is the policy.
        while (slot == nullptr) {
          if (subscriber.closed.load(std::memory_order_relaxed)) return;
          block_spins_.increment();
          std::this_thread::yield();
          slot = subscriber.egress.claim();
        }
        break;
    }
  }
  slot->header = frame.header;
  std::memcpy(slot->payload, frame.payload, frame.header.payload_size);
  subscriber.egress.commit();
  subscriber.dispatch.frames_enqueued.increment();
  subscriber.dispatch.egress_high_water.set_max(subscriber.egress.size());
}

void Dispatcher::reap_closed() {
  // A closed subscriber stops receiving at once; there is no socket left to
  // write to.
  const auto is_closed = [](const ChannelPtr& channel) {
    return channel->closed.load(std::memory_order_relaxed);
  };
  for (std::vector<ChannelPtr>& routes : routes_) {
    if (routes.empty()) continue;
    std::erase_if(routes, is_closed);
  }
  // A closed publisher is kept until its ingress ring is drained, so frames
  // that arrived before the disconnect are still routed. Nothing can be pushed
  // to it any more, so this terminates on the next poll.
  std::erase_if(channels_, [](const ChannelPtr& channel) {
    return channel->closed.load(std::memory_order_relaxed) && channel->ingress.size() == 0;
  });
  channel_count_.store(channels_.size(), std::memory_order_relaxed);
}

Dispatcher::Stats Dispatcher::stats() const {
  Stats stats;
  stats.frames_routed = routed_.get();
  stats.frames_unroutable = unroutable_.get();
  stats.frames_bad_topic = bad_topic_.get();
  stats.block_spins = block_spins_.get();
  stats.subscriptions = subscriptions_.get();
  stats.channels = channel_count_.load(std::memory_order_relaxed);
  return stats;
}

}  // namespace flashbus
