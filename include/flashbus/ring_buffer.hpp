#pragma once

// Single-producer / single-consumer ring buffer.
//
// Fixed power-of-two capacity, allocated once. Exactly one thread may push and
// exactly one thread may pop; that restriction is the whole point. FlashBus has
// no MPMC queue anywhere, because an ownership rule you can state in one
// sentence is worth more than a lock-free algorithm nobody can review.
//
// `PadIndices` exists so the false-sharing experiment can compare identical
// code with the two indices on separate cache lines versus sharing one. See
// benchmarks/false_sharing_bench.cpp.

#include <algorithm>
#include <atomic>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>

#include "flashbus/platform.hpp"

namespace flashbus {

template <typename T, bool PadIndices = true>
class SpscRing {
  static_assert(std::is_default_constructible_v<T>, "ring slots are constructed up front");

 public:
  /// Capacity is rounded up to the next power of two so indexing is a mask
  /// rather than a modulo.
  explicit SpscRing(size_t min_capacity) {
    assert(min_capacity >= 1);
    const size_t capacity = std::bit_ceil(min_capacity);
    // Value-initialised, so every page is touched here rather than on the
    // first publish. Page faults belong in startup, not in p99.9.
    storage_ = std::make_unique<T[]>(capacity);
    immutable_.slots = storage_.get();
    immutable_.capacity = capacity;
    immutable_.mask = capacity - 1;
  }

  [[nodiscard]] size_t capacity() const noexcept {
    return static_cast<size_t>(immutable_.capacity);
  }

  // ---- producer side -------------------------------------------------------

  /// Pointer to the slot that will be published by the next `commit()`, or
  /// nullptr if the ring is full. Writing through this pointer avoids the copy
  /// that `try_push` makes.
  [[nodiscard]] T* claim() noexcept {
    const uint64_t write = producer_.write.load(std::memory_order_relaxed);
    if (write - producer_.read_cache == immutable_.capacity) {
      // Only touch the consumer's cache line when the cached view says full.
      producer_.read_cache = consumer_.read.load(std::memory_order_acquire);
      if (write - producer_.read_cache == immutable_.capacity) return nullptr;
    }
    return &immutable_.slots[write & immutable_.mask];
  }

  /// Publishes the slot returned by the preceding `claim()`. The release store
  /// is what makes the slot's contents visible to the consumer's acquire load.
  void commit() noexcept {
    producer_.write.store(producer_.write.load(std::memory_order_relaxed) + 1,
                          std::memory_order_release);
  }

  bool try_push(const T& value) noexcept(std::is_nothrow_copy_assignable_v<T>) {
    T* slot = claim();
    if (slot == nullptr) return false;
    *slot = value;
    commit();
    return true;
  }

  // ---- consumer side -------------------------------------------------------

  /// Oldest unconsumed slot, or nullptr when empty. Valid until `pop()`.
  [[nodiscard]] T* front() noexcept {
    const uint64_t read = consumer_.read.load(std::memory_order_relaxed);
    if (read == consumer_.write_cache) {
      consumer_.write_cache = producer_.write.load(std::memory_order_acquire);
      if (read == consumer_.write_cache) return nullptr;
    }
    return &immutable_.slots[read & immutable_.mask];
  }

  /// Releases the slot returned by `front()` back to the producer.
  void pop() noexcept {
    consumer_.read.store(consumer_.read.load(std::memory_order_relaxed) + 1,
                         std::memory_order_release);
  }

  bool try_pop(T& out) noexcept(std::is_nothrow_copy_assignable_v<T>) {
    T* slot = front();
    if (slot == nullptr) return false;
    out = *slot;
    pop();
    return true;
  }

  /// Drains up to `max_items`, calling `fn(T&)` for each, with one acquire load
  /// and one release store for the whole batch instead of one per item. This is
  /// the primitive the egress batching experiment is built on.
  template <typename Fn>
  size_t consume(size_t max_items, Fn&& fn) {
    const uint64_t read = consumer_.read.load(std::memory_order_relaxed);
    if (read == consumer_.write_cache) {
      consumer_.write_cache = producer_.write.load(std::memory_order_acquire);
      if (read == consumer_.write_cache) return 0;
    }
    const size_t available = static_cast<size_t>(consumer_.write_cache - read);
    const size_t count = std::min(available, max_items);
    for (size_t i = 0; i < count; ++i) {
      fn(immutable_.slots[(read + i) & immutable_.mask]);
    }
    consumer_.read.store(read + count, std::memory_order_release);
    return count;
  }

  // ---- observers -----------------------------------------------------------
  // Approximate when called from a third thread: the two indices are read
  // separately. Exact when called from the producer or the consumer.

  [[nodiscard]] size_t size() const noexcept {
    const uint64_t write = producer_.write.load(std::memory_order_acquire);
    const uint64_t read = consumer_.read.load(std::memory_order_acquire);
    return static_cast<size_t>(write - read);
  }
  [[nodiscard]] bool empty() const noexcept { return size() == 0; }
  [[nodiscard]] bool full() const noexcept { return size() >= capacity(); }

 private:
  static constexpr size_t kIndexAlign = PadIndices ? kCacheLineSize : alignof(uint64_t);

  // Read-only after construction. Given its own line so that reading it never
  // touches a line another core is writing.
  struct alignas(kCacheLineSize) Immutable {
    T* slots = nullptr;
    uint64_t capacity = 0;
    uint64_t mask = 0;
  };

  // `write` is written only by the producer; `read_cache` is its private copy
  // of the consumer's index. Mirrored on the consumer side.
  struct alignas(kIndexAlign) ProducerState {
    std::atomic<uint64_t> write{0};
    uint64_t read_cache = 0;
  };
  struct alignas(kIndexAlign) ConsumerState {
    std::atomic<uint64_t> read{0};
    uint64_t write_cache = 0;
  };

  std::unique_ptr<T[]> storage_;
  Immutable immutable_;
  ProducerState producer_;
  ConsumerState consumer_;
};

/// Control variant for the false-sharing experiment: identical code, both
/// indices deliberately on one cache line.
template <typename T>
using SpscRingUnpadded = SpscRing<T, false>;

}  // namespace flashbus
