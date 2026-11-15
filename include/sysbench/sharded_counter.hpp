// Copyright (C) 2026 Sunny Bains
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

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
        shard(thread_id).m_value.fetch_add(delta, std::memory_order_relaxed);
    }

    // The shard's only writer is the calling thread: a plain load and store,
    // no locked read-modify-write.  Readers still see whole values.
    void add_local(std::uint32_t thread_id, T delta = T{1}) noexcept {
        auto& value = shard(thread_id).m_value;
        value.store(value.load(std::memory_order_relaxed) + delta, std::memory_order_relaxed);
    }

    [[nodiscard]] T get(std::uint32_t thread_id) const noexcept {
        return shard(thread_id).m_value.load(std::memory_order_relaxed);
    }

    [[nodiscard]] T sum(std::uint32_t active_shards) const noexcept {
        const std::size_t count =
            active_shards < ShardCount ? active_shards : ShardCount;
        T total{};
        for (std::size_t i = 0; i < count; ++i) {
            total += m_shards[i].m_value.load(std::memory_order_relaxed);
        }
        return total;
    }

    void reset() noexcept {
        for (auto& shard : m_shards) {
            shard.m_value.store(T{}, std::memory_order_relaxed);
        }
    }

private:
    struct alignas(mem::kCacheLine) Shard {
        std::atomic<T> m_value{};
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
