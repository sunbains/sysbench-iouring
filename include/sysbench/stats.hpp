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

#include "sysbench/memory.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <span>
#include <string_view>

namespace sb {

enum class QueryKind : std::uint8_t { Other = 0, Read = 1, Write = 2, Transaction = 3 };

[[nodiscard]] QueryKind classify_query(std::string_view sql) noexcept;

class LatencyHistogram {
public:
    static constexpr std::size_t kBins = 224;
    static constexpr std::uint64_t kMinNs = 1000;           // 1 us
    static constexpr std::uint64_t kMaxNs = 60'000'000'000; // 60 s

    void record(std::uint64_t nanoseconds) noexcept;
    [[nodiscard]] double percentile(double fraction) const;
    [[nodiscard]] std::uint64_t observations() const noexcept;
    void reset() noexcept;
    void merge(const LatencyHistogram& other) noexcept;

    // Upper bound of a bin in seconds (useful for textual histograms).
    [[nodiscard]] static double bin_upper_bound_seconds(std::size_t bin) noexcept;

    struct Snapshot {
        std::array<std::uint64_t, kBins> counts{};
        std::uint64_t total = 0;
    };
    [[nodiscard]] Snapshot snapshot() const noexcept;

private:
    [[nodiscard]] static std::size_t bin_for(std::uint64_t nanoseconds) noexcept;
    [[nodiscard]] static double upper_bound(std::size_t bin) noexcept;
    std::array<std::atomic<std::uint64_t>, kBins> m_bins{};
};

// One instance per benchmark thread.  Counters are atomics because the reporter
// snapshots them while workers run.
class alignas(mem::kCacheLine) ThreadStats {
public:
    // Each instance is one shard of the statistics set, indexed by benchmark
    // thread ID.
    explicit ThreadStats(std::uint32_t thread_id = 0) noexcept;
    [[nodiscard]] std::uint32_t thread_id() const noexcept { return m_thread_id; }

    void begin_event(std::chrono::steady_clock::time_point start) noexcept;
    void end_event(std::chrono::steady_clock::time_point end, bool error) noexcept;
    void record_event_latency(std::uint64_t nanoseconds, bool error) noexcept;
    void record_query(QueryKind kind, std::uint64_t bytes_read = 0,
                      std::uint64_t bytes_written = 0) noexcept;
    void record_error() noexcept;
    void record_reconnect() noexcept;
    void set_active(bool active) noexcept { m_active.store(active, std::memory_order_relaxed); }

    [[nodiscard]] std::uint64_t events() const noexcept {
        return m_events.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t reads() const noexcept {
        return m_reads.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t writes() const noexcept {
        return m_writes.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t other() const noexcept {
        return m_other.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t errors() const noexcept {
        return m_errors.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t reconnects() const noexcept {
        return m_reconnects.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t bytes_read() const noexcept {
        return m_bytes_read.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t bytes_written() const noexcept {
        return m_bytes_written.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t latency_sum_ns() const noexcept {
        return m_latency_sum_ns.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t latency_min_ns() const noexcept {
        return m_latency_min_ns.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t latency_max_ns() const noexcept {
        return m_latency_max_ns.load(std::memory_order_relaxed);
    }
    [[nodiscard]] bool active() const noexcept {
        return m_active.load(std::memory_order_relaxed);
    }
    [[nodiscard]] LatencyHistogram::Snapshot histogram() const noexcept {
        return m_latency.snapshot();
    }

private:
    std::atomic<std::uint64_t> m_events{0};
    std::atomic<std::uint64_t> m_reads{0};
    std::atomic<std::uint64_t> m_writes{0};
    std::atomic<std::uint64_t> m_other{0};
    std::atomic<std::uint64_t> m_errors{0};
    std::atomic<std::uint64_t> m_reconnects{0};
    std::atomic<std::uint64_t> m_bytes_read{0};
    std::atomic<std::uint64_t> m_bytes_written{0};
    std::atomic<std::uint64_t> m_latency_sum_ns{0};
    std::atomic<std::uint64_t> m_latency_min_ns{UINT64_MAX};
    std::atomic<std::uint64_t> m_latency_max_ns{0};
    std::uint32_t m_thread_id = 0;
    std::atomic<bool> m_active{false};
    std::chrono::steady_clock::time_point m_event_start{};
    std::uint64_t m_min_local = UINT64_MAX;
    std::uint64_t m_max_local = 0;
    LatencyHistogram m_latency;
};

struct StatsSnapshot {
    std::uint64_t events = 0;
    std::uint64_t reads = 0;
    std::uint64_t writes = 0;
    std::uint64_t other = 0;
    std::uint64_t errors = 0;
    std::uint64_t reconnects = 0;
    std::uint64_t bytes_read = 0;
    std::uint64_t bytes_written = 0;
    std::uint64_t latency_sum_ns = 0;
    std::uint64_t latency_min_ns = 0;
    std::uint64_t latency_max_ns = 0;
    double latency_avg = 0.0;
    double latency_p50 = 0.0;
    double latency_p95 = 0.0;
    double latency_p99 = 0.0;
    double latency_percentile = 0.0;
    std::uint64_t threads_running = 0;
    std::uint64_t thread_events_min = 0;
    std::uint64_t thread_events_max = 0;
    double thread_events_avg = 0.0;
    double thread_events_stddev = 0.0;
    LatencyHistogram::Snapshot histogram{};

    [[nodiscard]] double events_per_second(double interval) const noexcept {
        return interval > 0.0 ? static_cast<double>(events) / interval : 0.0;
    }
    [[nodiscard]] double queries_per_second(double interval) const noexcept {
        return interval > 0.0
                   ? static_cast<double>(reads + writes + other) / interval
                   : 0.0;
    }

    [[nodiscard]] StatsSnapshot difference(const StatsSnapshot& older,
                                           double percentile) const;
};

[[nodiscard]] StatsSnapshot aggregate(std::span<ThreadStats* const> threads,
                                      double percentile);

}  // namespace sb
