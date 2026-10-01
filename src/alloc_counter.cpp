// Replaces global operator new/delete with counting versions so that
// "no heap allocation on the steady-state hot path" is a checked property.
//
// Linked only by tests and benchmarks (as an OBJECT library, so the linker
// cannot leave it out), never by the shipped apps.

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <new>

#include "flashbus/platform.hpp"

namespace {
std::atomic<uint64_t> g_allocations{0};
}  // namespace

namespace flashbus {
uint64_t heap_allocation_count() { return g_allocations.load(std::memory_order_relaxed); }
void reset_heap_allocation_count() { g_allocations.store(0, std::memory_order_relaxed); }
}  // namespace flashbus

void* operator new(std::size_t size) {
  g_allocations.fetch_add(1, std::memory_order_relaxed);
  if (void* p = std::malloc(size ? size : 1)) return p;
  throw std::bad_alloc();
}

void* operator new[](std::size_t size) { return operator new(size); }

void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
  g_allocations.fetch_add(1, std::memory_order_relaxed);
  return std::malloc(size ? size : 1);
}

void* operator new[](std::size_t size, const std::nothrow_t& tag) noexcept {
  return operator new(size, tag);
}

void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }

// Aligned forms: std::vector of over-aligned ring slots goes through these.
void* operator new(std::size_t size, std::align_val_t align) {
  g_allocations.fetch_add(1, std::memory_order_relaxed);
  const std::size_t a = static_cast<std::size_t>(align);
  const std::size_t rounded = ((size ? size : 1) + a - 1) / a * a;  // aligned_alloc needs a multiple
  if (void* p = std::aligned_alloc(a, rounded)) return p;
  throw std::bad_alloc();
}

void* operator new[](std::size_t size, std::align_val_t align) {
  return operator new(size, align);
}

void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
