#include "flashbus/memory_pool.hpp"

#include <gtest/gtest.h>

#include <random>
#include <set>
#include <vector>

#include "flashbus/platform.hpp"

namespace flashbus {
namespace {

struct Node {
  uint64_t id = 0;
  uint64_t price = 0;
  uint32_t quantity = 0;
};

TEST(MemoryPool, HandsOutDistinctBlocksUpToCapacity) {
  MemoryPool<Node> pool(4);
  EXPECT_EQ(pool.capacity(), 4u);
  EXPECT_EQ(pool.available(), 4u);

  std::set<Node*> seen;
  for (int i = 0; i < 4; ++i) {
    Node* node = pool.acquire();
    ASSERT_NE(node, nullptr);
    EXPECT_TRUE(seen.insert(node).second) << "a block was handed out twice";
  }
  EXPECT_EQ(pool.in_use(), 4u);
  EXPECT_EQ(pool.available(), 0u);
}

TEST(MemoryPool, ReturnsNullWhenExhaustedRatherThanGrowing) {
  MemoryPool<Node> pool(2);
  ASSERT_NE(pool.acquire(), nullptr);
  ASSERT_NE(pool.acquire(), nullptr);
  EXPECT_EQ(pool.acquire(), nullptr);
  EXPECT_EQ(pool.exhaustions(), 1u);
  EXPECT_EQ(pool.capacity(), 2u);
}

TEST(MemoryPool, ForwardsConstructorArguments) {
  MemoryPool<Node> pool(1);
  Node* node = pool.acquire(Node{7, 100, 5});
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->id, 7u);
  EXPECT_EQ(node->price, 100u);
  EXPECT_EQ(node->quantity, 5u);
}

TEST(MemoryPool, ReusesReleasedBlocks) {
  MemoryPool<Node> pool(2);
  Node* first = pool.acquire();
  pool.release(first);
  Node* second = pool.acquire();
  EXPECT_EQ(first, second) << "a freed block should come straight back";
  EXPECT_EQ(pool.in_use(), 1u);
}

TEST(MemoryPool, TracksHighWaterMark) {
  MemoryPool<Node> pool(8);
  std::vector<Node*> held;
  for (int i = 0; i < 5; ++i) held.push_back(pool.acquire());
  for (Node* node : held) pool.release(node);
  EXPECT_EQ(pool.high_water(), 5u);
  EXPECT_EQ(pool.in_use(), 0u);
}

TEST(MemoryPool, SurvivesRandomAcquireReleaseChurn) {
  constexpr size_t kCapacity = 64;
  MemoryPool<Node> pool(kCapacity);
  std::vector<Node*> live;
  std::mt19937 rng(4242);
  uint64_t next_id = 1;

  for (int iteration = 0; iteration < 200000; ++iteration) {
    const bool acquire = live.empty() || (live.size() < kCapacity && rng() % 2 == 0);
    if (acquire) {
      Node* node = pool.acquire();
      ASSERT_NE(node, nullptr) << "pool reported space but handed out nothing";
      node->id = next_id++;
      live.push_back(node);
    } else {
      const size_t index = rng() % live.size();
      std::swap(live[index], live.back());
      pool.release(live.back());
      live.pop_back();
    }
    ASSERT_EQ(pool.in_use(), live.size());
  }

  // Every live block must still hold the id it was given, i.e. no block was
  // handed out while it was already in use.
  std::set<uint64_t> ids;
  for (Node* node : live) EXPECT_TRUE(ids.insert(node->id).second);
}

TEST(MemoryPool, SteadyStateAcquireReleaseDoesNotAllocate) {
  MemoryPool<Node> pool(32);
  reset_heap_allocation_count();
  const uint64_t before = heap_allocation_count();
  for (int i = 0; i < 200000; ++i) {
    Node* node = pool.acquire();
    ASSERT_NE(node, nullptr);
    node->id = static_cast<uint64_t>(i);
    pool.release(node);
  }
  EXPECT_EQ(heap_allocation_count(), before);
}

int g_live_counted = 0;

struct Counted {
  Counted() { ++g_live_counted; }
  ~Counted() { --g_live_counted; }
  Counted(const Counted&) = delete;
  Counted& operator=(const Counted&) = delete;
  uint64_t padding[2]{};
};

// A non-trivial type must be constructed and destroyed exactly once per
// acquire/release, otherwise the pool is only safe for PODs.
TEST(MemoryPool, ConstructsAndDestroysExactlyOnce) {
  MemoryPool<Counted> pool(4);
  EXPECT_EQ(g_live_counted, 0);
  Counted* a = pool.acquire();
  Counted* b = pool.acquire();
  EXPECT_EQ(g_live_counted, 2);
  pool.release(a);
  EXPECT_EQ(g_live_counted, 1);
  pool.release(b);
  EXPECT_EQ(g_live_counted, 0);
}

}  // namespace
}  // namespace flashbus
