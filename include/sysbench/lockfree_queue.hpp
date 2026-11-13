#pragma once

// Vyukov-style bounded MPMC queue over preallocated cells.  No locks, no
// allocation, no blocking: producers can retry when full.

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace sb {

template <class T, std::size_t Capacity>
class LockFreeQueue {
    static_assert(Capacity >= 2, "queue capacity must be at least two");
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "queue capacity must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>);

public:
    LockFreeQueue() noexcept {
        for (std::size_t i = 0; i < Capacity; ++i) {
            m_cells[i].sequence.store(i, std::memory_order_relaxed);
        }
        m_enqueue_position.store(0, std::memory_order_relaxed);
        m_dequeue_position.store(0, std::memory_order_relaxed);
    }

    LockFreeQueue(const LockFreeQueue&) = delete;
    LockFreeQueue& operator=(const LockFreeQueue&) = delete;

    [[nodiscard]] bool try_push(const T& value) noexcept {
        std::size_t position = m_enqueue_position.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell = m_cells[position & (Capacity - 1)];
            const std::size_t sequence =
                cell.sequence.load(std::memory_order_acquire);
            const std::intptr_t difference =
                static_cast<std::intptr_t>(sequence) -
                static_cast<std::intptr_t>(position);
            if (difference == 0) {
                if (m_enqueue_position.compare_exchange_weak(
                        position, position + 1, std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    cell.value = value;
                    cell.sequence.store(position + 1, std::memory_order_release);
                    return true;
                }
            } else if (difference < 0) {
                return false;
            } else {
                position = m_enqueue_position.load(std::memory_order_relaxed);
            }
        }
    }

    [[nodiscard]] bool try_pop(T& value) noexcept {
        std::size_t position = m_dequeue_position.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell = m_cells[position & (Capacity - 1)];
            const std::size_t sequence =
                cell.sequence.load(std::memory_order_acquire);
            const std::intptr_t difference =
                static_cast<std::intptr_t>(sequence) -
                static_cast<std::intptr_t>(position + 1);
            if (difference == 0) {
                if (m_dequeue_position.compare_exchange_weak(
                        position, position + 1, std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    value = cell.value;
                    cell.sequence.store(position + Capacity,
                                        std::memory_order_release);
                    return true;
                }
            } else if (difference < 0) {
                return false;
            } else {
                position = m_dequeue_position.load(std::memory_order_relaxed);
            }
        }
    }

    [[nodiscard]] bool empty() const noexcept {
        return m_dequeue_position.load(std::memory_order_acquire) >=
               m_enqueue_position.load(std::memory_order_acquire);
    }

private:
    struct Cell {
        std::atomic<std::size_t> sequence{0};
        T value{};
    };

    std::array<Cell, Capacity> m_cells{};
    alignas(64) std::atomic<std::size_t> m_enqueue_position{0};
    alignas(64) std::atomic<std::size_t> m_dequeue_position{0};
};

}  // namespace sb
