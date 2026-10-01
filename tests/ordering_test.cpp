// End-to-end ordering and delivery, through real sockets.

#include <gtest/gtest.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/write.hpp>

#include <cstring>
#include <thread>
#include <vector>

#include "flashbus/publisher.hpp"
#include "flashbus/subscriber.hpp"
#include "test_broker.hpp"

namespace flashbus {
namespace {

using testing::TestBroker;

/// Reads until `expected` frames have arrived or the deadline passes, handing
/// each one to `fn`.
template <typename Fn>
size_t drain(Subscriber& subscriber, size_t expected, Fn&& fn, double timeout_s = 10.0) {
  size_t seen = 0;
  const uint64_t deadline = now_ns() + static_cast<uint64_t>(timeout_s * 1e9);
  while (seen < expected && now_ns() < deadline && subscriber.connected()) {
    seen += subscriber.poll(fn);
  }
  return seen;
}

std::vector<std::byte> payload_of(uint64_t value, size_t size) {
  std::vector<std::byte> payload(size);
  std::memcpy(payload.data(), &value, std::min(size, sizeof(value)));
  return payload;
}

TEST(EndToEnd, DeliversEveryEventInOrder) {
  constexpr uint64_t kCount = 20000;
  TestBroker broker;

  SubscriberConfig subscriber_config;
  subscriber_config.blocking = false;
  Subscriber subscriber("127.0.0.1", broker.port(), subscriber_config);
  subscriber.subscribe(kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  Publisher publisher("127.0.0.1", broker.port());
  for (uint64_t i = 1; i <= kCount; ++i) {
    const auto payload = payload_of(i, 64);
    ASSERT_TRUE(publisher.publish(kTopicTrades, payload.data(), payload.size()));
  }
  publisher.flush();

  uint64_t expected_payload = 1;
  uint64_t expected_sequence = 1;
  const size_t seen = drain(subscriber, kCount, [&](const MessageHeader& header,
                                                    const std::byte* payload, size_t size) {
    ASSERT_EQ(size, 64u);
    uint64_t value = 0;
    std::memcpy(&value, payload, sizeof(value));
    EXPECT_EQ(value, expected_payload) << "payloads arrived out of order or were duplicated";
    EXPECT_EQ(header.sequence, expected_sequence) << "broker sequence is not contiguous";
    EXPECT_EQ(header.topic, kTopicTrades);
    ++expected_payload;
    ++expected_sequence;
  });

  EXPECT_EQ(seen, kCount);
  EXPECT_EQ(subscriber.gaps(), 0u);
  EXPECT_EQ(subscriber.missing(), 0u);
}

TEST(EndToEnd, PreservesOrderAcrossPayloadSizes) {
  TestBroker broker;
  SubscriberConfig config;
  config.blocking = false;
  Subscriber subscriber("127.0.0.1", broker.port(), config);
  subscriber.subscribe(kTopicQuotes);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  // Varying sizes make every frame boundary land somewhere different in the
  // TCP stream, which is where a decoder bug would show up.
  const std::vector<size_t> sizes = {0, 1, 7, 32, 63, 64, 65, 200, 512, 1024};
  Publisher publisher("127.0.0.1", broker.port());
  uint64_t value = 0;
  for (int round = 0; round < 200; ++round) {
    for (const size_t size : sizes) {
      const auto payload = payload_of(++value, size);
      ASSERT_TRUE(publisher.publish(kTopicQuotes, payload.data(), size));
    }
  }
  publisher.flush();
  const size_t total = sizes.size() * 200;

  size_t index = 0;
  const size_t seen =
      drain(subscriber, total, [&](const MessageHeader&, const std::byte* payload, size_t size) {
        const size_t want = sizes[index % sizes.size()];
        ASSERT_EQ(size, want);
        if (size >= sizeof(uint64_t)) {
          uint64_t received = 0;
          std::memcpy(&received, payload, sizeof(received));
          EXPECT_EQ(received, index + 1);
        }
        ++index;
      });
  EXPECT_EQ(seen, total);
  EXPECT_EQ(subscriber.gaps(), 0u);
}

TEST(EndToEnd, FansOutToEverySubscriber) {
  constexpr uint64_t kCount = 5000;
  TestBroker broker;
  SubscriberConfig config;
  config.blocking = false;

  std::vector<std::unique_ptr<Subscriber>> subscribers;
  for (int i = 0; i < 3; ++i) {
    subscribers.push_back(std::make_unique<Subscriber>("127.0.0.1", broker.port(), config));
    subscribers.back()->subscribe(kTopicTrades);
  }
  ASSERT_TRUE(broker.wait_for_subscriptions(3));

  Publisher publisher("127.0.0.1", broker.port());
  for (uint64_t i = 1; i <= kCount; ++i) {
    const auto payload = payload_of(i, 32);
    publisher.publish(kTopicTrades, payload.data(), payload.size());
  }
  publisher.flush();

  for (auto& subscriber : subscribers) {
    uint64_t expected = 1;
    const size_t seen = drain(*subscriber, kCount,
                              [&](const MessageHeader& header, const std::byte*, size_t) {
                                EXPECT_EQ(header.sequence, expected++);
                              });
    EXPECT_EQ(seen, kCount);
    EXPECT_EQ(subscriber->gaps(), 0u);
  }
}

TEST(EndToEnd, DoesNotDeliverOtherTopics) {
  TestBroker broker;
  SubscriberConfig config;
  config.blocking = false;
  Subscriber trades("127.0.0.1", broker.port(), config);
  trades.subscribe(kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  Publisher publisher("127.0.0.1", broker.port());
  const auto payload = payload_of(1, 16);
  for (int i = 0; i < 100; ++i) {
    publisher.publish(kTopicQuotes, payload.data(), payload.size());
    publisher.publish(kTopicOrders, payload.data(), payload.size());
  }
  for (int i = 0; i < 10; ++i) publisher.publish(kTopicTrades, payload.data(), payload.size());
  publisher.flush();

  uint64_t topics_seen = 0;
  const size_t seen = drain(trades, 10, [&](const MessageHeader& header, const std::byte*, size_t) {
    EXPECT_EQ(header.topic, kTopicTrades);
    ++topics_seen;
  });
  EXPECT_EQ(seen, 10u);
  EXPECT_EQ(topics_seen, 10u);

  // The quotes and orders had no subscriber, so the broker should say so rather
  // than quietly dropping them.
  EXPECT_TRUE(TestBroker::wait_until(
      [&] { return broker.stats().frames_unroutable >= 200; }, 2.0));
}

TEST(EndToEnd, SequencesEachTopicIndependently) {
  TestBroker broker;
  SubscriberConfig config;
  config.blocking = false;
  Subscriber trades("127.0.0.1", broker.port(), config);
  Subscriber quotes("127.0.0.1", broker.port(), config);
  trades.subscribe(kTopicTrades);
  quotes.subscribe(kTopicQuotes);
  ASSERT_TRUE(broker.wait_for_subscriptions(2));

  Publisher publisher("127.0.0.1", broker.port());
  const auto payload = payload_of(1, 8);
  for (int i = 0; i < 500; ++i) {
    publisher.publish(kTopicTrades, payload.data(), payload.size());
    publisher.publish(kTopicQuotes, payload.data(), payload.size());
  }
  publisher.flush();

  uint64_t trade_sequence = 1;
  drain(trades, 500, [&](const MessageHeader& header, const std::byte*, size_t) {
    EXPECT_EQ(header.sequence, trade_sequence++);
  });
  uint64_t quote_sequence = 1;
  drain(quotes, 500, [&](const MessageHeader& header, const std::byte*, size_t) {
    EXPECT_EQ(header.sequence, quote_sequence++);
  });
  EXPECT_EQ(trade_sequence, 501u);
  EXPECT_EQ(quote_sequence, 501u);
}

// Several publishers on one topic interleave, but the broker sequence stays
// contiguous, which is the property subscribers rely on for gap detection.
TEST(EndToEnd, MultiplePublishersShareAContiguousBrokerSequence) {
  constexpr uint64_t kPerPublisher = 3000;
  constexpr int kPublishers = 4;
  TestBroker broker;
  SubscriberConfig config;
  config.blocking = false;
  Subscriber subscriber("127.0.0.1", broker.port(), config);
  subscriber.subscribe(kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  std::vector<std::thread> publishers;
  for (int index = 0; index < kPublishers; ++index) {
    publishers.emplace_back([&, index] {
      Publisher publisher("127.0.0.1", broker.port());
      const auto payload = payload_of(static_cast<uint64_t>(index), 32);
      for (uint64_t i = 0; i < kPerPublisher; ++i) {
        publisher.publish(kTopicTrades, payload.data(), payload.size());
      }
      publisher.flush();
    });
  }

  constexpr size_t kTotal = kPerPublisher * kPublishers;
  uint64_t expected = 1;
  const size_t seen =
      drain(subscriber, kTotal, [&](const MessageHeader& header, const std::byte*, size_t) {
        EXPECT_EQ(header.sequence, expected++);
      });
  for (std::thread& thread : publishers) thread.join();

  EXPECT_EQ(seen, kTotal);
  EXPECT_EQ(subscriber.gaps(), 0u);
}

TEST(EndToEnd, SubscriberJoiningLateSeesNoFalseGap) {
  TestBroker broker;
  SubscriberConfig config;
  config.blocking = false;

  Subscriber early("127.0.0.1", broker.port(), config);
  early.subscribe(kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  Publisher publisher("127.0.0.1", broker.port());
  const auto payload = payload_of(1, 16);
  for (int i = 0; i < 200; ++i) publisher.publish(kTopicTrades, payload.data(), payload.size());
  publisher.flush();
  drain(early, 200, [](const MessageHeader&, const std::byte*, size_t) {});

  // This subscriber's first event has broker sequence 201, not 1. That is not a
  // gap: it never had a claim on the earlier events.
  Subscriber late("127.0.0.1", broker.port(), config);
  late.subscribe(kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(2));
  for (int i = 0; i < 50; ++i) publisher.publish(kTopicTrades, payload.data(), payload.size());
  publisher.flush();

  uint64_t first_sequence = 0;
  const size_t seen =
      drain(late, 50, [&](const MessageHeader& header, const std::byte*, size_t) {
        if (first_sequence == 0) first_sequence = header.sequence;
      });
  EXPECT_EQ(seen, 50u);
  EXPECT_GT(first_sequence, 200u);
  EXPECT_EQ(late.gaps(), 0u);
}

TEST(EndToEnd, PublisherDisconnectDoesNotLoseAlreadySentEvents) {
  constexpr uint64_t kCount = 4000;
  TestBroker broker;
  SubscriberConfig config;
  config.blocking = false;
  Subscriber subscriber("127.0.0.1", broker.port(), config);
  subscriber.subscribe(kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  {
    Publisher publisher("127.0.0.1", broker.port());
    const auto payload = payload_of(7, 48);
    for (uint64_t i = 0; i < kCount; ++i) {
      publisher.publish(kTopicTrades, payload.data(), payload.size());
    }
    publisher.flush();
  }  // closes the socket immediately after flushing

  const size_t seen =
      drain(subscriber, kCount, [](const MessageHeader&, const std::byte*, size_t) {});
  EXPECT_EQ(seen, kCount) << "events written before the disconnect were dropped";
  EXPECT_EQ(subscriber.gaps(), 0u);
}

TEST(EndToEnd, RejectsAGarbageStreamWithoutAffectingOtherConnections) {
  TestBroker broker;
  SubscriberConfig config;
  config.blocking = false;
  Subscriber subscriber("127.0.0.1", broker.port(), config);
  subscriber.subscribe(kTopicTrades);
  ASSERT_TRUE(broker.wait_for_subscriptions(1));

  {
    // A raw socket speaking nonsense. The broker must drop this connection and
    // nothing else.
    boost::asio::io_context io;
    boost::asio::ip::tcp::socket socket(io);
    socket.connect(boost::asio::ip::tcp::endpoint(
        boost::asio::ip::make_address("127.0.0.1"), broker.port()));
    const std::vector<std::byte> junk(4096, std::byte{0xAB});
    boost::system::error_code ignored;
    boost::asio::write(socket, boost::asio::buffer(junk.data(), junk.size()), ignored);
    socket.close(ignored);
  }

  Publisher publisher("127.0.0.1", broker.port());
  const auto payload = payload_of(1, 32);
  for (int i = 0; i < 500; ++i) publisher.publish(kTopicTrades, payload.data(), payload.size());
  publisher.flush();

  const size_t seen = drain(subscriber, 500, [](const MessageHeader&, const std::byte*, size_t) {});
  EXPECT_EQ(seen, 500u);
  EXPECT_TRUE(subscriber.connected());
}

}  // namespace
}  // namespace flashbus
