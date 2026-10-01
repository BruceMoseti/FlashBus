// Bounded queues, overflow policies, and slow-consumer isolation.
//
// The claim being tested is the central one in DESIGN.md: a subscriber that
// cannot keep up loses events, says so, and does not take the others with it.

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <thread>
#include <vector>

#include "flashbus/publisher.hpp"
#include "flashbus/subscriber.hpp"
#include "test_broker.hpp"

namespace flashbus {
namespace {

using testing::TestBroker;

std::vector<std::byte> payload_of(size_t size) { return std::vector<std::byte>(size, std::byte{7}); }

/// A subscriber that connects, subscribes, and then deliberately never reads.
/// Its socket buffer fills, then the broker's egress ring fills, which is the
/// state the policies exist for.
class StalledSubscriber {
 public:
  StalledSubscriber(uint16_t port, uint32_t topic) {
    SubscriberConfig config;
    config.blocking = false;
    subscriber_ = std::make_unique<Subscriber>("127.0.0.1", port, config);
    subscriber_->subscribe(topic);
  }
  Subscriber& get() { return *subscriber_; }

 private:
  std::unique_ptr<Subscriber> subscriber_;
};

TEST(Backpressure, DropNewestLosesEventsAndCountsThem) {
  ServerConfig config;
  config.egress_capacity = 64;  // small, so overflow happens quickly
  config.policy = OverflowPolicy::kDropNewest;
  TestBroker broker(config);

  StalledSubscriber stalled(broker.port(), kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  Publisher publisher("127.0.0.1", broker.port());
  const auto payload = payload_of(1024);
  for (int i = 0; i < 200000; ++i) {
    publisher.publish(kTopicTrades, payload.data(), payload.size());
  }
  publisher.flush();

  ASSERT_TRUE(TestBroker::wait_until([&] { return broker.stats().frames_dropped > 0; }, 10.0))
      << "a stalled subscriber with a 64-frame queue should have caused drops";

  const ServerStats stats = broker.stats();
  EXPECT_GT(stats.frames_dropped, 0u);
  EXPECT_LE(stats.egress_high_water, config.egress_capacity)
      << "the egress ring exceeded its own capacity";
  // Connection stays up: dropping is the policy, not a failure.
  EXPECT_TRUE(stalled.get().connected());
}

TEST(Backpressure, DisconnectClosesTheSlowSubscriber) {
  ServerConfig config;
  config.egress_capacity = 64;
  config.policy = OverflowPolicy::kDisconnect;
  TestBroker broker(config);

  StalledSubscriber stalled(broker.port(), kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  Publisher publisher("127.0.0.1", broker.port());
  const auto payload = payload_of(1024);
  for (int i = 0; i < 200000; ++i) {
    publisher.publish(kTopicTrades, payload.data(), payload.size());
  }
  publisher.flush();

  // The subscriber should end up closed, and the broker should stop counting it
  // as an open connection.
  EXPECT_TRUE(TestBroker::wait_until(
      [&] {
        stalled.get().poll([](const MessageHeader&, const std::byte*, size_t) {});
        return !stalled.get().connected();
      },
      15.0))
      << "a stalled subscriber under the disconnect policy was never closed";
}

TEST(Backpressure, EgressRingNeverExceedsCapacity) {
  ServerConfig config;
  config.egress_capacity = 256;
  TestBroker broker(config);
  StalledSubscriber stalled(broker.port(), kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  Publisher publisher("127.0.0.1", broker.port());
  const auto payload = payload_of(512);
  for (int i = 0; i < 100000; ++i) {
    publisher.publish(kTopicTrades, payload.data(), payload.size());
  }
  publisher.flush();
  ASSERT_TRUE(TestBroker::wait_until([&] { return broker.stats().frames_dropped > 0; }, 10.0));

  // The capacity is rounded up to a power of two, so compare against that.
  EXPECT_LE(broker.stats().egress_high_water, 256u);
}

TEST(Backpressure, IngressRejectionIsCountedNotBuffered) {
  ServerConfig config;
  // A tiny ingress ring and a dispatcher that sleeps readily: the network
  // thread will decode faster than the dispatcher drains.
  config.ingress_capacity = 2;
  config.egress_capacity = 4096;
  TestBroker broker(config);

  SubscriberConfig subscriber_config;
  subscriber_config.blocking = false;
  Subscriber subscriber("127.0.0.1", broker.port(), subscriber_config);
  subscriber.subscribe(kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  Publisher publisher("127.0.0.1", broker.port());
  const auto payload = payload_of(64);
  for (int i = 0; i < 400000; ++i) {
    publisher.publish(kTopicTrades, payload.data(), payload.size());
  }
  publisher.flush();

  // Whether rejection actually triggers depends on the machine, so this asserts
  // the invariant rather than the outcome: nothing is ever silently lost.
  EXPECT_TRUE(TestBroker::wait_until(
      [&] {
        subscriber.poll([](const MessageHeader&, const std::byte*, size_t) {});
        const ServerStats stats = broker.stats();
        return stats.frames_in + stats.frames_rejected >= 400000;
      },
      20.0));
  const ServerStats stats = broker.stats();
  // frames_in also counts the subscriber's SUBSCRIBE frame, hence the offset.
  EXPECT_EQ(stats.frames_in + stats.frames_rejected - stats.subscriptions, 400000u)
      << "frames went missing without being counted as rejected";
}

// The property that matters most: one subscriber falling over must not move
// another subscriber's latency.
TEST(Backpressure, SlowConsumerDoesNotStallHealthyOnes) {
  ServerConfig config;
  config.egress_capacity = 512;
  config.policy = OverflowPolicy::kDropNewest;
  TestBroker broker(config);

  SubscriberConfig fast_config;
  fast_config.blocking = false;
  Subscriber fast("127.0.0.1", broker.port(), fast_config);
  fast.subscribe(kTopicTrades);
  StalledSubscriber stalled(broker.port(), kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(2));

  constexpr uint64_t kCount = 50000;
  std::atomic<bool> publishing{true};
  std::thread publisher_thread([&] {
    Publisher publisher("127.0.0.1", broker.port());
    const auto payload = payload_of(256);
    for (uint64_t i = 0; i < kCount; ++i) {
      publisher.publish(kTopicTrades, payload.data(), payload.size());
    }
    publisher.flush();
    publishing.store(false, std::memory_order_release);
  });

  uint64_t received = 0;
  const uint64_t deadline = now_ns() + 20'000'000'000;
  while (received < kCount && now_ns() < deadline && fast.connected()) {
    received += fast.poll([](const MessageHeader&, const std::byte*, size_t) {});
  }
  publisher_thread.join();

  EXPECT_EQ(received, kCount)
      << "the healthy subscriber lost events because another subscriber stalled";
  EXPECT_EQ(fast.gaps(), 0u) << "the healthy subscriber saw sequence gaps";
  EXPECT_GT(broker.stats().frames_dropped, 0u)
      << "the stalled subscriber should have been the one losing events";
}

TEST(Backpressure, BlockPolicyDoesNotLoseEvents) {
  ServerConfig config;
  config.egress_capacity = 64;
  config.policy = OverflowPolicy::kBlock;
  TestBroker broker(config);

  SubscriberConfig subscriber_config;
  subscriber_config.blocking = false;
  Subscriber subscriber("127.0.0.1", broker.port(), subscriber_config);
  subscriber.subscribe(kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  constexpr uint64_t kCount = 20000;
  std::thread publisher_thread([&] {
    Publisher publisher("127.0.0.1", broker.port());
    const auto payload = payload_of(512);
    for (uint64_t i = 0; i < kCount; ++i) {
      publisher.publish(kTopicTrades, payload.data(), payload.size());
    }
    publisher.flush();
  });

  uint64_t received = 0;
  uint64_t expected_sequence = 1;
  const uint64_t deadline = now_ns() + 30'000'000'000;
  while (received < kCount && now_ns() < deadline && subscriber.connected()) {
    received += subscriber.poll([&](const MessageHeader& header, const std::byte*, size_t) {
      EXPECT_EQ(header.sequence, expected_sequence++);
    });
  }
  publisher_thread.join();

  EXPECT_EQ(received, kCount) << "the block policy is supposed to trade latency for zero loss";
  EXPECT_EQ(broker.stats().frames_dropped, 0u);
  EXPECT_EQ(subscriber.gaps(), 0u);
}

TEST(Backpressure, GapsAreReportedWhenEventsAreDropped) {
  ServerConfig config;
  config.egress_capacity = 64;
  config.policy = OverflowPolicy::kDropNewest;
  TestBroker broker(config);

  SubscriberConfig subscriber_config;
  subscriber_config.blocking = false;
  Subscriber subscriber("127.0.0.1", broker.port(), subscriber_config);
  subscriber.subscribe(kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  // Publish hard without reading, so the ring overflows, then read what is
  // left. The sequence numbers must reveal the hole.
  Publisher publisher("127.0.0.1", broker.port());
  const auto payload = payload_of(1024);
  for (int i = 0; i < 100000; ++i) {
    publisher.publish(kTopicTrades, payload.data(), payload.size());
  }
  publisher.flush();
  ASSERT_TRUE(TestBroker::wait_until([&] { return broker.stats().frames_dropped > 100; }, 10.0));

  const uint64_t deadline = now_ns() + 5'000'000'000;
  while (now_ns() < deadline) {
    subscriber.poll([](const MessageHeader&, const std::byte*, size_t) {});
  }
  EXPECT_GT(subscriber.gaps(), 0u) << "events were dropped but the subscriber never noticed";
  EXPECT_GT(subscriber.missing(), 0u);
  // Every missing event must be one the broker admits to dropping; a gap the
  // broker cannot account for would mean events vanished somewhere else.
  EXPECT_LE(subscriber.missing(), broker.stats().frames_dropped);
}

}  // namespace
}  // namespace flashbus
