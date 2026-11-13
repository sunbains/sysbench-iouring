#pragma once

// Cache-line-separated counters indexed by thread ID.  The hot update path is
// a relaxed atomic on the caller's own shard, so threads never bounce one
// shared cache line.

#include "sysbench/memory.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace sb {

template <class T, std::size_t ShardCount>
class ShardedCounter {
public:
    static constexpr std::size_t shard_count = ShardCount;

    void add(std::uint32_t thread_id, T delta = T{1}) noexcept {
        shard(thread_id).value.fetch_add(delta, std::memory_order_relaxed);
    }

    [[nodiscard]] T get(std::uint32_t thread_id) const noexcept {
        return shard(thread_id).value.load(std::memory_order_relaxed);
    }

    [[nodiscard]] T sum(std::uint32_t active_shards) const noexcept {
        const std::size_t count =
            active_shards < ShardCount ? active_shards : ShardCount;
        T total{};
        for (std::size_t i = 0; i < count; ++i) {
            total += m_shards[i].value.load(std::memory_order_relaxed);
        }
        return total;
    }

    void reset() noexcept {
        for (auto& shard : m_shards) {
            shard.value.store(T{}, std::memory_order_relaxed);
        }
    }

private:
    struct alignas(mem::kCacheLine) Shard {
        std::atomic<T> value{};
    };

    [[nodiscard]] Shard& shard(std::uint32_t thread_id) noexcept {
        return m_shards[thread_id % ShardCount];
    }
    [[nodiscard]] const Shard& shard(std::uint32_t thread_id) const noexcept {
        return m_shards[thread_id % ShardCount];
    }

    std::array<Shard, ShardCount> m_shards{};
};

}  // namespace sb
