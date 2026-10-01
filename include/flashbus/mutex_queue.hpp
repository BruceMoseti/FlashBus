#pragma once

// The intentionally boring baseline: std::queue behind a std::mutex.
//
// It exists to be beaten, and to be beaten honestly. The interface matches
// SpscRing so the same benchmark body runs against both, and there is no
// condition variable, because SpscRing has no blocking either and comparing a
// spinning queue against a sleeping one would measure the wrong thing.
//
// Its allocation behaviour is deliberately left alone: std::deque growth is
// part of what the baseline demonstrates.

#include <algorithm>
#include <cstddef>
#include <mutex>
#include <queue>

namespace flashbus {

template <typename T>
class MutexQueue {
 public:
  explicit MutexQueue(size_t capacity) : capacity_(capacity) {}

  [[nodiscard]] size_t capacity() const noexcept { return capacity_; }

  bool try_push(const T& value) {
    const std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.size() >= capacity_) return false;
    queue_.push(value);
    return true;
  }

  bool try_pop(T& out) {
    const std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.empty()) return false;
    out = queue_.front();
    queue_.pop();
    return true;
  }

  template <typename Fn>
  size_t consume(size_t max_items, Fn&& fn) {
    const std::lock_guard<std::mutex> lock(mutex_);
    const size_t count = std::min(max_items, queue_.size());
    for (size_t i = 0; i < count; ++i) {
      fn(queue_.front());
      queue_.pop();
    }
    return count;
  }

  [[nodiscard]] size_t size() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }
  [[nodiscard]] bool empty() const { return size() == 0; }
  [[nodiscard]] bool full() const { return size() >= capacity_; }

 private:
  mutable std::mutex mutex_;
  std::queue<T> queue_;
  size_t capacity_;
};

}  // namespace flashbus
