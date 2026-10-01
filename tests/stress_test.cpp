// Long, noisy runs. These are the tests that justify the sanitizer presets:
// under TSan they check the memory ordering, under ASan the buffer arithmetic.
//
// They are heavier than the rest of the suite on purpose. A concurrency bug
// that shows up once in ten million operations is still a bug.

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <random>
#include <thread>
#include <vector>

#include "flashbus/clock.hpp"
#include "flashbus/memory_pool.hpp"
#include "flashbus/protocol.hpp"
#include "flashbus/publisher.hpp"
#include "flashbus/ring_buffer.hpp"
#include "flashbus/subscriber.hpp"
#include "test_broker.hpp"

namespace flashbus {
namespace {

using testing::LoadBudget;
using testing::load_budget;
using testing::TestBroker;

/// Scaled down automatically under sanitizers, which slow everything by an
/// order of magnitude or more.
constexpr uint64_t scale(uint64_t full) {
#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
  return full / 20;
#else
  return full;
#endif
}

struct Payload {
  uint64_t sequence = 0;
  uint64_t checksum = 0;
  std::byte filler[32]{};
};

uint64_t checksum_of(uint64_t sequence) { return sequence * 0x9E3779B97F4A7C15ULL; }

TEST(Stress, RingSurvivesManyOperationsWithRandomStalls) {
  const uint64_t count = scale(20'000'000);
  SpscRing<Payload> ring(1024);
  std::atomic<uint64_t> produced{0};

  std::thread producer([&] {
    std::mt19937 rng(0xC0FFEE);
    for (uint64_t i = 0; i < count; ++i) {
      Payload* slot = nullptr;
      while ((slot = ring.claim()) == nullptr) {
      }
      slot->sequence = i;
      slot->checksum = checksum_of(i);
      ring.commit();
      if ((rng() & 0xFFFF) == 0) std::this_thread::yield();
    }
    produced.store(count, std::memory_order_release);
  });

  std::mt19937 rng(0xBEEF);
  uint64_t expected = 0;
  while (expected < count) {
    // Alternate between single pops and batch drains so both paths are
    // exercised against the same producer.
    if ((rng() & 1) == 0) {
      Payload payload;
      if (!ring.try_pop(payload)) continue;
      ASSERT_EQ(payload.sequence, expected);
      ASSERT_EQ(payload.checksum, checksum_of(expected));
      ++expected;
    } else {
      ring.consume(32, [&](Payload& payload) {
        ASSERT_EQ(payload.sequence, expected);
        ASSERT_EQ(payload.checksum, checksum_of(expected));
        ++expected;
      });
    }
    if ((rng() & 0xFFFF) == 0) std::this_thread::yield();
  }
  producer.join();
  EXPECT_EQ(expected, count);
  EXPECT_TRUE(ring.empty());
}

TEST(Stress, UnpaddedRingIsEquallyCorrectUnderLoad) {
  const uint64_t count = scale(10'000'000);
  SpscRingUnpadded<uint64_t> ring(256);
  std::thread producer([&] {
    for (uint64_t i = 0; i < count; ++i) {
      while (!ring.try_push(i)) {
      }
    }
  });
  uint64_t expected = 0;
  while (expected < count) {
    uint64_t value = 0;
    if (ring.try_pop(value)) {
      ASSERT_EQ(value, expected);
      ++expected;
    }
  }
  producer.join();
  EXPECT_EQ(expected, count);
}

// Two independent rings in opposite directions, which is the shape of a
// FlashBus channel: ingress one way, egress the other.
TEST(Stress, BidirectionalRingsDoNotInterfere) {
  const uint64_t count = scale(5'000'000);
  SpscRing<uint64_t> up(512);
  SpscRing<uint64_t> down(512);

  std::thread peer([&] {
    uint64_t echoed = 0;
    while (echoed < count) {
      uint64_t value = 0;
      if (!up.try_pop(value)) continue;
      if (value != echoed) {
        ADD_FAILURE() << "ingress order violated at " << echoed;
        return;
      }
      while (!down.try_push(value * 3)) {
      }
      ++echoed;
    }
  });

  uint64_t sent = 0;
  uint64_t received = 0;
  while (received < count) {
    if (sent < count && up.try_push(sent)) ++sent;
    uint64_t value = 0;
    if (down.try_pop(value)) {
      ASSERT_EQ(value, received * 3);
      ++received;
    }
  }
  peer.join();
  EXPECT_EQ(received, count);
}

TEST(Stress, DecoderHandlesLongRandomlyFragmentedStream) {
  const uint64_t frames = scale(200'000);
  std::mt19937 rng(1234);

  StreamDecoder decoder;
  std::vector<std::byte> stream;
  std::vector<uint32_t> sizes;
  stream.reserve(frames * 128);
  for (uint64_t i = 1; i <= frames; ++i) {
    const auto payload_size = static_cast<uint32_t>(rng() % 300);
    sizes.push_back(payload_size);
    MessageHeader header;
    header.topic = static_cast<uint32_t>(i % 8);
    header.sequence = i;
    header.payload_size = payload_size;
    const size_t offset = stream.size();
    stream.resize(offset + kHeaderSize + payload_size);
    encode_header(stream.data() + offset, header);
    for (uint32_t b = 0; b < payload_size; ++b) {
      stream[offset + kHeaderSize + b] = static_cast<std::byte>((i + b) & 0xFF);
    }
  }

  uint64_t decoded = 0;
  size_t offset = 0;
  while (offset < stream.size()) {
    // Chunk sizes from one byte to larger than a frame, so every boundary case
    // happens somewhere in the run.
    const size_t chunk = std::min<size_t>(1 + rng() % 700, stream.size() - offset);
    const DecodeError error = decoder.feed(
        stream.data() + offset, chunk,
        [&](const MessageHeader& header, const std::byte* payload, size_t size) {
          ++decoded;
          ASSERT_EQ(header.sequence, decoded);
          ASSERT_EQ(size, sizes[decoded - 1]);
          for (size_t b = 0; b < size; ++b) {
            ASSERT_EQ(static_cast<uint8_t>(payload[b]), static_cast<uint8_t>((decoded + b) & 0xFF));
          }
        });
    ASSERT_EQ(error, DecodeError::kOk);
    offset += chunk;
  }
  EXPECT_EQ(decoded, frames);
  EXPECT_EQ(decoder.pending(), 0u);
}

TEST(Stress, PoolChurnKeepsBlocksDistinct) {
  const uint64_t iterations = scale(2'000'000);
  constexpr size_t kCapacity = 256;
  MemoryPool<Payload> pool(kCapacity);
  std::vector<Payload*> live;
  std::mt19937 rng(77);
  uint64_t next = 1;

  for (uint64_t i = 0; i < iterations; ++i) {
    if (live.empty() || (live.size() < kCapacity && (rng() & 1) == 0)) {
      Payload* block = pool.acquire();
      ASSERT_NE(block, nullptr);
      block->sequence = next++;
      block->checksum = checksum_of(block->sequence);
      live.push_back(block);
    } else {
      const size_t index = rng() % live.size();
      ASSERT_EQ(live[index]->checksum, checksum_of(live[index]->sequence))
          << "a live block was handed out again and overwritten";
      std::swap(live[index], live.back());
      pool.release(live.back());
      live.pop_back();
    }
  }
  for (Payload* block : live) {
    EXPECT_EQ(block->checksum, checksum_of(block->sequence));
  }
}

// Full-path soak: real sockets, several publishers, several subscribers,
// everything checked at the end.
TEST(Stress, EndToEndSoak) {
  const LoadBudget budget = load_budget();
  const int kPublishers = static_cast<int>(budget.publishers);
  const int kSubscribers = static_cast<int>(budget.subscribers);
  const uint64_t per_publisher = scale(100'000);
  const uint64_t total = per_publisher * static_cast<uint64_t>(kPublishers);

  ServerConfig config;
  config.egress_capacity = 16384;
  config.ingress_capacity = 8192;
  TestBroker broker(config);

  std::vector<std::unique_ptr<Subscriber>> subscribers;
  SubscriberConfig subscriber_config;
  subscriber_config.blocking = false;
  for (int i = 0; i < kSubscribers; ++i) {
    subscribers.push_back(
        std::make_unique<Subscriber>("127.0.0.1", broker.port(), subscriber_config));
    subscribers.back()->subscribe(kTopicTrades);
  }
  ASSERT_TRUE(broker.wait_for_subscriptions(kSubscribers));

  // Atomic because the main thread watches progress while the readers run.
  struct ReaderState {
    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> gaps{0};
    std::atomic<bool> ordered{true};
  };
  std::vector<ReaderState> states(kSubscribers);

  std::atomic<bool> stop{false};
  std::vector<std::thread> readers;
  for (int i = 0; i < kSubscribers; ++i) {
    readers.emplace_back([&, i] {
      uint64_t expected = 0;
      uint64_t seen = 0;
      while (!stop.load(std::memory_order_relaxed) && subscribers[i]->connected()) {
        subscribers[i]->poll([&](const MessageHeader& header, const std::byte*, size_t) {
          if (expected != 0 && header.sequence != expected + 1) {
            states[i].ordered.store(false, std::memory_order_relaxed);
          }
          expected = header.sequence;
          ++seen;
        });
        states[i].count.store(seen, std::memory_order_relaxed);
      }
      states[i].gaps.store(subscribers[i]->gaps(), std::memory_order_relaxed);
    });
  }

  std::vector<std::thread> writers;
  for (int i = 0; i < kPublishers; ++i) {
    writers.emplace_back([&, i] {
      Publisher publisher("127.0.0.1", broker.port());
      std::vector<std::byte> payload(64, static_cast<std::byte>(i));
      // Paced, so that zero loss is a property this test can actually require.
      // How much load the pipeline survives before it has to drop is a
      // benchmark question, not a correctness one, and mixing the two gives a
      // test that fails whenever the machine is busy.
      const Pacer pacer(budget.rate_per_publisher);
      for (uint64_t n = 0; n < per_publisher; ++n) {
        pacer.wait_for(n);
        if (!publisher.publish(kTopicTrades, payload.data(), payload.size())) break;
      }
      publisher.flush();
    });
  }
  for (std::thread& thread : writers) thread.join();

  // Give the readers a chance to finish draining before declaring a shortfall.
  // The per-subscriber expectations below report what actually arrived, so the
  // timeout here only bounds how long we are willing to wait for it.
  (void)TestBroker::wait_until(
      [&] {
        for (int i = 0; i < kSubscribers; ++i) {
          if (states[i].count.load(std::memory_order_relaxed) < total) return false;
        }
        return true;
      },
      30.0);
  stop.store(true, std::memory_order_relaxed);
  for (std::thread& thread : readers) thread.join();

  for (int i = 0; i < kSubscribers; ++i) {
    EXPECT_EQ(states[i].count.load(), total) << "subscriber " << i << " was short";
    EXPECT_EQ(states[i].gaps.load(), 0u) << "subscriber " << i << " saw a sequence gap";
    EXPECT_TRUE(states[i].ordered.load()) << "subscriber " << i << " saw events out of order";
  }
  const ServerStats stats = broker.stats();
  EXPECT_EQ(stats.frames_dropped, 0u);
  EXPECT_EQ(stats.frames_rejected, 0u);
}

}  // namespace
}  // namespace flashbus
