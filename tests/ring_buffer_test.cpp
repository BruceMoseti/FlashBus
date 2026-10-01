#include "flashbus/ring_buffer.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <random>
#include <thread>
#include <vector>

#include "flashbus/platform.hpp"

namespace flashbus {
namespace {

TEST(SpscRing, RoundsCapacityUpToPowerOfTwo) {
  EXPECT_EQ(SpscRing<int>(1).capacity(), 1u);
  EXPECT_EQ(SpscRing<int>(2).capacity(), 2u);
  EXPECT_EQ(SpscRing<int>(3).capacity(), 4u);
  EXPECT_EQ(SpscRing<int>(1000).capacity(), 1024u);
  EXPECT_EQ(SpscRing<int>(1024).capacity(), 1024u);
}

TEST(SpscRing, CapacityOneHoldsExactlyOneItem) {
  SpscRing<int> ring(1);
  EXPECT_TRUE(ring.empty());
  EXPECT_TRUE(ring.try_push(7));
  EXPECT_TRUE(ring.full());
  EXPECT_FALSE(ring.try_push(8));

  int value = 0;
  ASSERT_TRUE(ring.try_pop(value));
  EXPECT_EQ(value, 7);
  EXPECT_TRUE(ring.empty());
  EXPECT_FALSE(ring.try_pop(value));
}

TEST(SpscRing, CapacityTwoAlternates) {
  SpscRing<int> ring(2);
  EXPECT_TRUE(ring.try_push(1));
  EXPECT_TRUE(ring.try_push(2));
  EXPECT_FALSE(ring.try_push(3));
  EXPECT_EQ(ring.size(), 2u);

  int value = 0;
  ASSERT_TRUE(ring.try_pop(value));
  EXPECT_EQ(value, 1);
  EXPECT_TRUE(ring.try_push(3));
  ASSERT_TRUE(ring.try_pop(value));
  EXPECT_EQ(value, 2);
  ASSERT_TRUE(ring.try_pop(value));
  EXPECT_EQ(value, 3);
  EXPECT_TRUE(ring.empty());
}

// Walks the write index far past the capacity so every index wraps many times,
// including the case where a batch spans the wrap point.
TEST(SpscRing, WrapsAroundRepeatedly) {
  constexpr size_t kCapacity = 8;
  SpscRing<uint64_t> ring(kCapacity);
  uint64_t expected = 0;
  for (uint64_t round = 0; round < 1000; ++round) {
    const size_t batch = static_cast<size_t>(round % kCapacity) + 1;
    for (size_t i = 0; i < batch; ++i) ASSERT_TRUE(ring.try_push(expected + i));
    uint64_t value = 0;
    for (size_t i = 0; i < batch; ++i) {
      ASSERT_TRUE(ring.try_pop(value));
      ASSERT_EQ(value, expected + i);
    }
    expected += batch;
  }
  EXPECT_TRUE(ring.empty());
}

TEST(SpscRing, ClaimAndCommitPublishInPlace) {
  SpscRing<uint64_t> ring(4);
  uint64_t* slot = ring.claim();
  ASSERT_NE(slot, nullptr);
  *slot = 42;
  // Not visible until committed.
  EXPECT_EQ(ring.size(), 0u);
  EXPECT_EQ(ring.front(), nullptr);
  ring.commit();
  ASSERT_NE(ring.front(), nullptr);
  EXPECT_EQ(*ring.front(), 42u);
}

TEST(SpscRing, ClaimReturnsNullWhenFull) {
  SpscRing<int> ring(2);
  ASSERT_TRUE(ring.try_push(1));
  ASSERT_TRUE(ring.try_push(2));
  EXPECT_EQ(ring.claim(), nullptr);
}

TEST(SpscRing, ConsumeDrainsAtMostRequested) {
  SpscRing<uint64_t> ring(16);
  for (uint64_t i = 0; i < 10; ++i) ASSERT_TRUE(ring.try_push(i));

  std::vector<uint64_t> seen;
  EXPECT_EQ(ring.consume(4, [&](uint64_t& v) { seen.push_back(v); }), 4u);
  EXPECT_EQ(ring.consume(100, [&](uint64_t& v) { seen.push_back(v); }), 6u);
  EXPECT_EQ(ring.consume(100, [&](uint64_t&) { FAIL(); }), 0u);

  ASSERT_EQ(seen.size(), 10u);
  for (uint64_t i = 0; i < 10; ++i) EXPECT_EQ(seen[i], i);
}

TEST(SpscRing, ConsumeSpansTheWrapPoint) {
  SpscRing<uint64_t> ring(8);
  for (uint64_t i = 0; i < 6; ++i) ASSERT_TRUE(ring.try_push(i));
  EXPECT_EQ(ring.consume(6, [](uint64_t&) {}), 6u);
  // Write index is now 6; pushing 8 more makes the batch straddle index 0.
  for (uint64_t i = 6; i < 14; ++i) ASSERT_TRUE(ring.try_push(i));
  std::vector<uint64_t> seen;
  EXPECT_EQ(ring.consume(8, [&](uint64_t& v) { seen.push_back(v); }), 8u);
  ASSERT_EQ(seen.size(), 8u);
  for (size_t i = 0; i < 8; ++i) EXPECT_EQ(seen[i], 6 + i);
}

TEST(SpscRing, SteadyStatePushPopDoesNotAllocate) {
  SpscRing<uint64_t> ring(64);
  reset_heap_allocation_count();
  const uint64_t before = heap_allocation_count();
  uint64_t value = 0;
  for (uint64_t i = 0; i < 100000; ++i) {
    ASSERT_TRUE(ring.try_push(i));
    ASSERT_TRUE(ring.try_pop(value));
  }
  EXPECT_EQ(heap_allocation_count(), before)
      << "the hot path must not allocate; the counter is linked in for this test";
}

// --- concurrent tests -------------------------------------------------------
// Every item pushed must be received exactly once, in order, with no
// duplicates and nothing invented. Run under TSan via the sanitizer presets.

template <typename Ring>
void run_handoff(size_t capacity, uint64_t count, int producer_pause_every,
                 int consumer_pause_every) {
  Ring ring(capacity);
  std::atomic<bool> producer_done{false};

  std::thread producer([&] {
    std::mt19937 rng(1234);
    for (uint64_t i = 0; i < count; ++i) {
      while (!ring.try_push(i)) {
      }
      if (producer_pause_every > 0 && rng() % static_cast<unsigned>(producer_pause_every) == 0) {
        std::this_thread::yield();
      }
    }
    producer_done.store(true, std::memory_order_release);
  });

  uint64_t expected = 0;
  std::mt19937 rng(5678);
  while (expected < count) {
    uint64_t value = 0;
    if (ring.try_pop(value)) {
      ASSERT_EQ(value, expected) << "ordering or duplication violated at " << expected;
      ++expected;
    } else {
      ASSERT_TRUE(!producer_done.load(std::memory_order_acquire) || ring.size() > 0 ||
                  expected == count)
          << "producer finished but the consumer is short at " << expected;
    }
    if (consumer_pause_every > 0 && rng() % static_cast<unsigned>(consumer_pause_every) == 0) {
      std::this_thread::yield();
    }
  }
  producer.join();
  EXPECT_EQ(expected, count);
  EXPECT_TRUE(ring.empty());
}

TEST(SpscRingConcurrent, CapacityOne) { run_handoff<SpscRing<uint64_t>>(1, 200000, 0, 0); }

TEST(SpscRingConcurrent, SmallCapacityKeepsOrder) {
  run_handoff<SpscRing<uint64_t>>(2, 200000, 0, 0);
}

TEST(SpscRingConcurrent, LargeCapacityKeepsOrder) {
  run_handoff<SpscRing<uint64_t>>(1024, 1000000, 0, 0);
}

TEST(SpscRingConcurrent, ProducerFasterThanConsumer) {
  run_handoff<SpscRing<uint64_t>>(64, 200000, 0, 16);
}

TEST(SpscRingConcurrent, ConsumerFasterThanProducer) {
  run_handoff<SpscRing<uint64_t>>(64, 200000, 16, 0);
}

TEST(SpscRingConcurrent, RandomPausesOnBothSides) {
  run_handoff<SpscRing<uint64_t>>(16, 200000, 32, 32);
}

TEST(SpscRingConcurrent, UnpaddedVariantIsEquallyCorrect) {
  run_handoff<SpscRingUnpadded<uint64_t>>(64, 500000, 0, 0);
}

// The payload is written through claim() and read through front(), so a
// successful run proves the release/acquire pair publishes the slot contents
// and not just the index.
TEST(SpscRingConcurrent, SlotContentsAreVisibleAfterCommit) {
  struct Payload {
    uint64_t sequence = 0;
    uint64_t checksum = 0;
    std::byte filler[48]{};
  };
  constexpr uint64_t kCount = 500000;
  SpscRing<Payload> ring(256);

  std::thread producer([&] {
    for (uint64_t i = 0; i < kCount; ++i) {
      Payload* slot = nullptr;
      while ((slot = ring.claim()) == nullptr) {
      }
      slot->sequence = i;
      slot->checksum = i * 2654435761u;
      ring.commit();
    }
  });

  for (uint64_t i = 0; i < kCount; ++i) {
    Payload* slot = nullptr;
    while ((slot = ring.front()) == nullptr) {
    }
    ASSERT_EQ(slot->sequence, i);
    ASSERT_EQ(slot->checksum, i * 2654435761u);
    ring.pop();
  }
  producer.join();
}

TEST(SpscRingConcurrent, BatchConsumeKeepsOrder) {
  constexpr uint64_t kCount = 1000000;
  SpscRing<uint64_t> ring(512);
  std::thread producer([&] {
    for (uint64_t i = 0; i < kCount; ++i) {
      while (!ring.try_push(i)) {
      }
    }
  });

  uint64_t expected = 0;
  while (expected < kCount) {
    ring.consume(64, [&](uint64_t& value) {
      ASSERT_EQ(value, expected);
      ++expected;
    });
  }
  producer.join();
  EXPECT_EQ(expected, kCount);
}

}  // namespace
}  // namespace flashbus
