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

#include <print>
#include "test_framework.hpp"

#include "sysbench/memory.hpp"
#include "sysbench/runtime.hpp"
#include "sysbench/sharded_counter.hpp"
#include "sysbench/stats.hpp"

#include <atomic>
#include <chrono>


using namespace sb;
using namespace std::chrono_literals;

TEST("query classification") {
    CHECK(classify_query("SELECT 1") == QueryKind::Read);
    CHECK(classify_query("  insert into t values(1)") == QueryKind::Write);
    CHECK(classify_query("/* c */ UPDATE t SET x=1") == QueryKind::Write);
    CHECK(classify_query("-- c\nDELETE FROM t") == QueryKind::Write);
    CHECK(classify_query("BEGIN") == QueryKind::Transaction);
    CHECK(classify_query("COMMIT") == QueryKind::Transaction);
    CHECK(classify_query("DROP TABLE t") == QueryKind::Write);
    CHECK(classify_query("CALL p()") == QueryKind::Other);
}


namespace {

// A coroutine function, not a capturing coroutine lambda: the closure of a
// coroutine lambda is a temporary and its captures would dangle while the task
// is queued.
Task<void> increment_shard(ShardedCounter<std::uint64_t, 64>& counter,
                           std::uint32_t id) {
    for (std::uint64_t i = 0; i < 100000; ++i) counter.add(id, 1);
    co_return;
}

}  // namespace

TEST("sharded counters are indexed by thread ID") {
    ShardedCounter<std::uint64_t, 64> counter;
    constexpr std::uint32_t kThreads = 8;
    constexpr std::uint64_t kPerShard = 100000;

    // Every increment runs as a Runtime task; the test owns no threads and
    // every scheduling decision belongs to the Runtime.
    Runtime runtime;
    runtime.start(Runtime::Config{.workers = kThreads,
                                  .worker_heap_bytes = 1024 * 1024,
                                  .pin_workers = false,
                                  .numa_aware = false});
    for (std::uint32_t id = 0; id < kThreads; ++id) {
        runtime.spawn(increment_shard(counter, id));
    }
    runtime.wait();
    CHECK_EQ(counter.sum(kThreads), kPerShard * kThreads);
    for (std::uint32_t id = 0; id < kThreads; ++id) {
        CHECK_EQ(counter.get(id), kPerShard);
    }
    counter.reset();
    CHECK_EQ(counter.sum(kThreads), 0u);
    runtime.shutdown();
}

TEST("latency histogram percentiles") {
    LatencyHistogram histogram;
    for (int i = 0; i < 1000; ++i) histogram.record(1000);  // 1us
    const double p50 = histogram.percentile(0.50);
    const double p99 = histogram.percentile(0.99);
    CHECK(p50 >= 0.0000009 && p50 < 0.000002);
    CHECK(p99 >= 0.0000009 && p99 < 0.000002);
    CHECK_EQ(histogram.observations(), 1000u);

    LatencyHistogram mixed;
    for (int i = 0; i < 900; ++i) mixed.record(1000);
    for (int i = 0; i < 100; ++i) mixed.record(1'000'000);
    const double mixed_p50 = mixed.percentile(0.50);
    const double mixed_p95 = mixed.percentile(0.95);
    CHECK(mixed_p50 < 0.0001);
    CHECK(mixed_p95 > 0.0001);
    mixed.reset();
    CHECK_EQ(mixed.observations(), 0u);
}

TEST("thread stats aggregate counters and latency") {
    ThreadStats thread;
    thread.set_active(true);
    for (int i = 0; i < 10; ++i) {
        if (i % 3 == 0) thread.record_query(QueryKind::Read);
        else if (i % 3 == 1) thread.record_query(QueryKind::Write);
        else thread.record_query(QueryKind::Transaction);
        thread.record_event_latency(static_cast<std::uint64_t>((i + 1) * 1000), false);
    }
    thread.record_error();
    thread.record_reconnect();
    std::vector<ThreadStats*> threads{&thread};
    const StatsSnapshot snapshot = aggregate(threads, 95);
    CHECK_EQ(snapshot.events, 10u);
    CHECK_EQ(snapshot.reads, 4u);
    CHECK_EQ(snapshot.writes, 3u);
    CHECK_EQ(snapshot.other, 3u);
    CHECK_EQ(snapshot.errors, 1u);
    CHECK_EQ(snapshot.reconnects, 1u);
    CHECK_EQ(snapshot.threads_running, 1u);
    CHECK(snapshot.latency_avg > 0.0);
    CHECK(snapshot.latency_p95 >= snapshot.latency_p50);
    CHECK(snapshot.thread_events_max == 10u);
    CHECK(snapshot.histogram.total == 10u);
}

TEST("aggregate spans multiple threads") {
    ThreadStats a;
    ThreadStats b;
    a.record_query(QueryKind::Read);
    b.record_query(QueryKind::Write);
    for (int i = 0; i < 5; ++i) a.record_event_latency(5000, false);
    for (int i = 0; i < 5; ++i) b.record_event_latency(5000, false);
    ThreadStats* threads[] = {&a, &b};
    const StatsSnapshot result = aggregate(threads, 99);
    CHECK_EQ(result.events, 10u);
    CHECK_EQ(result.reads, 1u);
    CHECK_EQ(result.writes, 1u);
    CHECK_EQ(result.histogram.total, 10u);
    CHECK_EQ(result.thread_events_min, 5u);
    CHECK_EQ(result.thread_events_max, 5u);
    CHECK(result.thread_events_avg == 5.0);
}

TEST("interval difference") {
    ThreadStats thread;
    for (int i = 0; i < 100; ++i) thread.record_event_latency(2000, false);
    ThreadStats* threads[] = {&thread};
    const auto first = aggregate(threads, 95);
    for (int i = 0; i < 50; ++i) thread.record_event_latency(4000, false);
    const auto second = aggregate(threads, 95);
    const auto delta = second.difference(first, 95);
    CHECK_EQ(delta.events, 50u);
    CHECK(delta.latency_p50 >= 0.000003);
    CHECK(delta.latency_p95 >= 0.000003);
}

TEST("timestamped event recording") {
    ThreadStats thread;
    const auto start = std::chrono::steady_clock::now();
    thread.begin_event(start);
    thread.end_event(start + 2ms, false);
    CHECK_EQ(thread.events(), 1u);
    CHECK(thread.latency_sum_ns() >= 1'000'000u);
    CHECK(thread.latency_max_ns() >= 1'000'000u);
}

TEST("stats recording throughput") {
    if (!::sb::test::perf_enabled()) return;
    ThreadStats thread;
    constexpr std::size_t kOps = 2'000'000;
    const double ops = ::sb::test::measure_ops_per_second(kOps, [&] {
        for (std::size_t i = 0; i < kOps; ++i) {
            thread.record_event_latency((i % 1000) + 1, false);
            if ((i & 15) == 0) thread.record_query(QueryKind::Read);
        }
    });
    std::println("  stats: {:.1f} Mevents/s", ops / 1e6);
    CHECK_EQ(thread.events(), kOps);
    CHECK(ops > 1'000'000.0);
}

int main(int argc, char** argv) {
    mem::GlobalAllocator::instance().initialize(256ull * 1024 * 1024);
    return ::sb::test::run_all(argc, argv);
}
