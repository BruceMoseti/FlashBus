#pragma once

// Preallocated fixed-size block pool.
//
// Not thread-safe, deliberately: one pool per owning thread costs nothing and
// keeps acquire/release down to a few instructions with no atomics. The free
// list is threaded through the blocks themselves, so there is no side table to
// walk and a freed block's own memory is the next pointer.
//
// This is the allocator for objects whose lifetime a ring slot cannot express.
// The ring path does not need it: ring slots are fixed-size and already
// preallocated. See docs/PERFORMANCE.md case study 2 for what it buys, and
// where it buys nothing.

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <new>
#include <utility>

namespace flashbus {

template <typename T>
class MemoryPool {
 public:
  explicit MemoryPool(size_t capacity) : nodes_(std::make_unique<Node[]>(capacity)),
                                         capacity_(capacity) {
    assert(capacity >= 1);
    // Building the free list writes every node, which faults the whole pool in
    // now rather than during the first burst.
    for (size_t i = 0; i + 1 < capacity; ++i) nodes_[i].next = &nodes_[i + 1];
    nodes_[capacity - 1].next = nullptr;
    free_ = &nodes_[0];
  }

  /// Constructs a T in a preallocated block. Returns nullptr when exhausted:
  /// a pool that grows is not a pool, it is a slower malloc.
  template <typename... Args>
  [[nodiscard]] T* acquire(Args&&... args) {
    if (free_ == nullptr) {
      ++exhaustions_;
      return nullptr;
    }
    Node* node = free_;
    free_ = node->next;
    ++in_use_;
    high_water_ = std::max(high_water_, in_use_);
    return std::construct_at(reinterpret_cast<T*>(node->storage), std::forward<Args>(args)...);
  }

  void release(T* object) noexcept {
    assert(object != nullptr);
    std::destroy_at(object);
    auto* node = reinterpret_cast<Node*>(object);
    node->next = free_;
    free_ = node;
    --in_use_;
  }

  [[nodiscard]] size_t capacity() const noexcept { return capacity_; }
  [[nodiscard]] size_t in_use() const noexcept { return in_use_; }
  [[nodiscard]] size_t available() const noexcept { return capacity_ - in_use_; }
  /// Peak concurrent blocks; the number that tells you how to size the pool.
  [[nodiscard]] size_t high_water() const noexcept { return high_water_; }
  /// Times `acquire` failed. Non-zero means the pool is too small for the load.
  [[nodiscard]] size_t exhaustions() const noexcept { return exhaustions_; }

 private:
  // A block is either a free-list link or a live T, never both.
  union Node {
    Node* next;
    alignas(T) std::byte storage[sizeof(T)];
  };

  std::unique_ptr<Node[]> nodes_;
  Node* free_ = nullptr;
  size_t capacity_;
  size_t in_use_ = 0;
  size_t high_water_ = 0;
  size_t exhaustions_ = 0;
};

}  // namespace flashbus
