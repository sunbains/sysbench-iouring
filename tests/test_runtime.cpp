#include <print>
#include "test_framework.hpp"

#include "sysbench/memory.hpp"
#include "sysbench/runtime.hpp"
#include "sysbench/task.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace sb;
using namespace std::chrono_literals;

namespace {

Task<int> child_value(int input) {
    co_await yield_now();
    co_return input * 2;
}

Task<void> parent_of_child(std::atomic<int>& output) {
    int value = co_await child_value(21);
    output.store(value, std::memory_order_relaxed);
}

Task<void> yielding_counter(std::atomic<std::uint64_t>& count, int yields) {
    for (int i = 0; i < yields; ++i) {
        count.fetch_add(1, std::memory_order_relaxed);
        co_await yield_now();
    }
    count.fetch_add(1, std::memory_order_relaxed);
}

Task<void> swallow_error(std::atomic<bool>& caught) {
    try {
        throw std::runtime_error("expected");
    } catch (const std::runtime_error&) {
        caught.store(true, std::memory_order_relaxed);
    }
    co_return;
}

Task<void> sleep_then_mark(std::atomic<int>& done,
                           std::chrono::steady_clock::time_point started,
                           std::atomic<std::int64_t>& elapsed_us) {
    co_await sleep_for(2ms);
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    elapsed_us.fetch_add(elapsed, std::memory_order_relaxed);
    done.fetch_add(1, std::memory_order_relaxed);
}

Task<void> allocate_on_worker(std::atomic<int>& allocations,
                              std::atomic<std::uint32_t>& workers_seen,
                              std::uint32_t mask) {
    auto& allocator = Runtime::current_allocator();
    void* memory = allocator.allocate(128, 16);
    CHECK(allocator.owns(memory));
    CHECK(mem::global_root().owns(memory));
    workers_seen.fetch_or(mask, std::memory_order_relaxed);
    allocations.fetch_add(1, std::memory_order_relaxed);
    allocator.deallocate(memory, 128, 16);
    co_return;
}

}  // namespace

TEST("runtime executes and joins top-level tasks") {
    Runtime runtime;
    runtime.start(Runtime::Config{.workers = 2, .worker_heap_bytes = 2 * 1024 * 1024});
    std::atomic<std::uint64_t> count{0};
    for (int i = 0; i < 4000; ++i) {
        runtime.spawn(yielding_counter(count, 4));
    }
    runtime.wait();
    CHECK_EQ(count.load(), 4000u * 5u);
    CHECK(runtime.resumes() >= 4000u * 5u);
    runtime.shutdown();
}

TEST("task values and exceptions propagate") {
    Runtime runtime;
    runtime.start(Runtime::Config{.workers = 1, .worker_heap_bytes = 2 * 1024 * 1024});
    std::atomic<int> value{0};
    std::atomic<bool> caught{false};
    runtime.spawn(parent_of_child(value));
    runtime.spawn(swallow_error(caught));
    runtime.wait();
    CHECK_EQ(value.load(), 42);
    CHECK(caught.load());
    runtime.shutdown();
}

TEST("sleepers are scheduled by worker timers") {
    Runtime runtime;
    runtime.start(Runtime::Config{.workers = 4, .worker_heap_bytes = 2 * 1024 * 1024});
    const auto start = std::chrono::steady_clock::now();
    std::atomic<int> done{0};
    std::atomic<std::int64_t> elapsed_sum{0};
    for (int i = 0; i < 32; ++i) runtime.spawn(sleep_then_mark(done, start, elapsed_sum));
    runtime.wait();
    const auto total = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - start)
                           .count();
    CHECK_EQ(done.load(), 32);
    CHECK(total >= 1);
    CHECK(total < 1000);
    CHECK(elapsed_sum.load() / 32 >= 1000);
    runtime.shutdown();
}

TEST("worker allocators are hierarchical and worker-affine") {
    Runtime runtime;
    runtime.start(Runtime::Config{.workers = 4, .worker_heap_bytes = 2 * 1024 * 1024});
    std::atomic<int> allocations{0};
    std::atomic<std::uint32_t> seen{0};
    for (int i = 0; i < 100; ++i) {
        runtime.spawn(allocate_on_worker(allocations, seen, 1u << (i % 4)));
    }
    runtime.wait();
    CHECK_EQ(allocations.load(), 100);
    CHECK(seen.load() != 0);
    for (std::uint32_t worker = 0; worker < 4; ++worker) {
        const auto stats = runtime.worker_heap_stats(worker);
        CHECK(stats.allocations > 0);
        CHECK(stats.used < stats.capacity);
    }
    runtime.shutdown();
}

TEST("runtime task spawn throughput") {
    if (!::sb::test::perf_enabled()) return;
    Runtime runtime;
    runtime.start(Runtime::Config{.workers = 4, .worker_heap_bytes = 4 * 1024 * 1024});
    constexpr int kTasks = 50'000;
    std::atomic<std::uint64_t> count{0};
    const double ops = ::sb::test::measure_ops_per_second(kTasks, [&] {
        for (int i = 0; i < kTasks; ++i) {
            runtime.spawn(yielding_counter(count, 0));
        }
        runtime.wait();
    });
    std::println("  spawn+run: {:.0f} tasks/s", ops);
    CHECK_EQ(count.load(), static_cast<std::uint64_t>(kTasks));
    CHECK(ops > 50'000.0);
    runtime.shutdown();
}

TEST("runtime yield throughput") {
    if (!::sb::test::perf_enabled()) return;
    Runtime runtime;
    runtime.start(Runtime::Config{.workers = 1, .worker_heap_bytes = 2 * 1024 * 1024});
    constexpr std::uint64_t kYields = 1'000'000;
    std::atomic<std::uint64_t> count{0};
    auto task = yielding_counter(count, static_cast<int>(kYields));
    const double ops = ::sb::test::measure_ops_per_second(kYields, [&] {
        runtime.spawn(std::move(task));
        runtime.wait();
    });
    std::println("  yield: {:.1f} M switches/s", ops / 1e6);
    CHECK_EQ(count.load(), kYields + 1);
    CHECK(ops > 500'000.0);
    runtime.shutdown();
}

int main(int argc, char** argv) {
    mem::GlobalAllocator::instance().initialize(1024ull * 1024 * 1024);
    return ::sb::test::run_all(argc, argv);
}
