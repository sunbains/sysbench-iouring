#include <print>
#include "test_framework.hpp"

#include "sysbench/memory.hpp"

#include <cstdint>
#include <cstring>

using namespace sb::mem;

TEST("root reserves up front and honors alignment") {
    RootArena root(4 * 1024 * 1024);
    CHECK_EQ(root.capacity(), 4u * 1024 * 1024);
    void* a = root.allocate(17, 64);
    void* b = root.allocate(33, 4096);
    CHECK_EQ(reinterpret_cast<std::uintptr_t>(a) % 64, 0u);
    CHECK_EQ(reinterpret_cast<std::uintptr_t>(b) % 4096, 0u);
    CHECK(root.owns(a));
    CHECK(root.owns(b));
    CHECK(!root.owns(static_cast<void*>(&root)));
    const auto stats = root.stats();
    CHECK_EQ(stats.allocations, 2u);
    CHECK(stats.used >= 4096u);
    CHECK(root.base() != nullptr);
}

TEST("root exhaustion is deterministic") {
    RootArena root(64 * 1024);
    (void)root.allocate(48 * 1024);
    CHECK_THROWS(root.allocate(64 * 1024));
    CHECK_EQ(root.stats().failed, 1u);
}

TEST("heap allocate/free/coalesce loop returns to empty") {
    RootArena root(8 * 1024 * 1024);
    HeapArena heap(root, 2 * 1024 * 1024, "test-heap");
    void* blocks[256]{};
    for (int round = 0; round < 100; ++round) {
        for (int i = 0; i < 256; ++i) {
            const std::size_t size =
                static_cast<std::size_t>((i * 37 + round * 11) % 700) + 1;
            blocks[i] = heap.allocate(size, 16);
            CHECK(heap.owns(blocks[i]));
            std::memset(blocks[i], i & 0xff, size);
        }
        for (int i = 0; i < 256; i += 2) heap.deallocate(blocks[i], 0, 0);
        for (int i = 1; i < 256; i += 2) heap.deallocate(blocks[i], 0, 0);
    }
    const auto stats = heap.stats();
    CHECK_EQ(stats.used, 0u);
    CHECK(stats.allocations > 10000u);
    CHECK(stats.frees > 10000u);
}

TEST("heap handles random lifetimes and reuse") {
    RootArena root(8 * 1024 * 1024);
    HeapArena heap(root, 2 * 1024 * 1024);
    struct Slot { void* ptr; std::size_t size; };
    Slot slots[512]{};
    std::uint64_t state = 0x9e3779b97f4a7c15ULL;
    auto next = [&] {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    };
    for (int i = 0; i < 200000; ++i) {
        const std::size_t slot = next() % 512;
        if (slots[slot].ptr != nullptr) {
            heap.deallocate(slots[slot].ptr, slots[slot].size, 16);
            slots[slot] = {};
        }
        const std::size_t size = (next() % 4096) + 1;
        slots[slot].ptr = heap.allocate(size, 16);
        slots[slot].size = size;
        auto* bytes = static_cast<unsigned char*>(slots[slot].ptr);
        bytes[0] = 0x5a;
        bytes[size - 1] = 0xa5;
        CHECK(bytes[0] == (size == 1 ? 0xa5 : 0x5a));
        CHECK(bytes[size - 1] == 0xa5);
    }
    for (auto& slot : slots) {
        if (slot.ptr != nullptr) heap.deallocate(slot.ptr, slot.size, 16);
    }
    CHECK_EQ(heap.stats().used, 0u);
}

TEST("heap supports over-aligned objects and rejects absurd alignment") {
    RootArena root(1024 * 1024);
    HeapArena heap(root, 256 * 1024);
    void* memory = heap.allocate(64, 64);
    CHECK_EQ(reinterpret_cast<std::uintptr_t>(memory) % 64, 0u);
    std::memset(memory, 0x5a, 64);
    heap.deallocate(memory, 64, 64);
    CHECK_EQ(heap.stats().used, 0u);
    CHECK_THROWS(heap.allocate(64, 8192));
}

TEST("bump arena supports scopes, nesting, and block release") {
    RootArena root(4 * 1024 * 1024);
    HeapArena heap(root, 2 * 1024 * 1024);
    BumpArena arena(heap, 16 * 1024);
    const auto initial_used = heap.stats().used;

    {
        ScopedAllocator outer(arena);
        auto* a = static_cast<int*>(outer.allocate(sizeof(int) * 100, 16));
        a[0] = 42;
        const auto after_outer = heap.stats().used;
        CHECK_EQ(after_outer, initial_used);

        {
            ScopedAllocator inner(arena);
            auto* buffer = static_cast<char*>(inner.allocate(70 * 1024, 64));
            CHECK_EQ(reinterpret_cast<std::uintptr_t>(buffer) % 64, 0u);
            std::memset(buffer, 1, 70 * 1024);
            CHECK(arena.stats().capacity >= 70u * 1024u);
            CHECK(heap.stats().used > after_outer);
        }
        CHECK(heap.stats().used == after_outer);
        CHECK_EQ(a[0], 42);
    }
    CHECK_EQ(heap.stats().used, initial_used);
    CHECK_EQ(arena.stats().used, 0u);
}

TEST("many nested event scopes stay O(1)") {
    RootArena root(4 * 1024 * 1024);
    HeapArena heap(root, 2 * 1024 * 1024);
    BumpArena arena(heap, 32 * 1024);
    const auto baseline = heap.stats().used;
    for (int event = 0; event < 100000; ++event) {
        ScopedAllocator scope(arena);
        auto* p = static_cast<std::uint64_t*>(scope.allocate(256, 8));
        p[0] = static_cast<std::uint64_t>(event);
        {
            ScopedAllocator nested(arena);
            auto* q = static_cast<char*>(nested.allocate(1024, 16));
            q[3] = 7;
        }
        CHECK_EQ(p[0], static_cast<std::uint64_t>(event));
    }
    CHECK_EQ(arena.stats().used, 0u);
    CHECK_EQ(heap.stats().used, baseline);
}

TEST("std allocator aliases use the scoped resource") {
    RootArena root(4 * 1024 * 1024);
    HeapArena heap(root, 2 * 1024 * 1024);
    BumpArena arena(heap, 16 * 1024);
    {
        ScopedAllocator scope(arena);
        Vector<int> values{Allocator<int>(&scope)};
        for (int i = 0; i < 10000; ++i) values.push_back(i);
        CHECK_EQ(values.front(), 0);
        CHECK_EQ(values.back(), 9999);
        String text{Allocator<char>(&scope)};
        text.append("sysbench-cpp-uses-a-custom-scoped-allocator-not-the-system-heap");
        CHECK(text.size() > 32u);
        CHECK(scope.owns(values.data()));
        CHECK(scope.owns(text.data()));
    }
    CHECK_EQ(arena.stats().used, 0u);
}

TEST("global hierarchical allocator") {
    if (!GlobalAllocator::instance().initialized()) {
        GlobalAllocator::instance().initialize(512 * 1024 * 1024);
    }
    auto& root = GlobalAllocator::instance().root();
    CHECK(root.owns(root.base()));

    auto* heap = construct<HeapArena>(root, root, 8 * 1024 * 1024);
    auto* arena = construct<BumpArena>(*heap, *heap, 64 * 1024);
    {
        ScopedAllocator scope(*arena);
        void* value = scope.allocate(128, 32);
        CHECK(root.owns(value));
        CHECK(heap->owns(value));
        CHECK(arena->owns(value));
        CHECK(scope.owns(value));
    }
    destroy(*heap, arena);
    destroy(root, heap);
    CHECK(GlobalAllocator::instance().initialized());
}

TEST("bump allocation throughput") {
    if (!::sb::test::perf_enabled()) return;
    RootArena root(16 * 1024 * 1024);
    HeapArena heap(root, 8 * 1024 * 1024);
    BumpArena arena(heap, 256 * 1024);
    constexpr std::size_t kOps = 2'000'000;
    constexpr std::size_t kBatch = 2048;
    double ops = 0;
    for (int round = 0; round < 4; ++round) {
        ops = ::sb::test::measure_ops_per_second(kOps, [&] {
            std::size_t i = 0;
            while (i < kOps) {
                ScopedAllocator scope(arena);
                for (std::size_t n = 0; n < kBatch && i < kOps; ++n, ++i) {
                    auto* p = static_cast<std::uint64_t*>(scope.allocate(64, 16));
                    p[0] = i;
                }
            }
        });
    }
    std::println("  bump: {:.1f} Mops/s", ops / 1e6);
    CHECK(ops > 10'000'000.0);
}

TEST("heap allocate/free throughput") {
    if (!::sb::test::perf_enabled()) return;
    RootArena root(16 * 1024 * 1024);
    HeapArena heap(root, 8 * 1024 * 1024);
    constexpr std::size_t kOps = 500'000;
    double ops = 0;
    for (int round = 0; round < 3; ++round) {
        ops = ::sb::test::measure_ops_per_second(kOps * 2, [&] {
            for (std::size_t i = 0; i < kOps; ++i) {
                void* p = heap.allocate(96, 16);
                heap.deallocate(p, 96, 16);
            }
        });
    }
    std::println("  heap: {:.1f} Mops/s (alloc+free pairs)", ops / 1e6);
    CHECK(ops > 500'000.0);
}


TEST("coroutine frame pool reuses size classes") {
    void* frames[128]{};
    for (int i = 0; i < 128; ++i) frames[i] = allocate_task_frame(512);
    for (int i = 0; i < 128; ++i) {
        CHECK_EQ(reinterpret_cast<std::uintptr_t>(frames[i]) % 64, 0u);
        deallocate_task_frame(frames[i], 512);
    }
    void* reused = allocate_task_frame(512);
    CHECK(reused != nullptr);
    deallocate_task_frame(reused, 512);
}

TEST("coroutine frame pool throughput") {
    if (!::sb::test::perf_enabled()) return;
    constexpr std::size_t kOps = 2'000'000;
    const double ops = ::sb::test::measure_ops_per_second(kOps, [&] {
        for (std::size_t i = 0; i < kOps; ++i) {
            void* frame = allocate_task_frame(256 + (i % 7) * 64);
            deallocate_task_frame(frame, 0);
        }
    });
    std::println("  frame pool: {:.1f} Malloc+free/s", ops / 1e6);
    CHECK(ops > 500'000.0);
}

int main(int argc, char** argv) {
    GlobalAllocator::instance().initialize(512ull * 1024 * 1024);
    return ::sb::test::run_all(argc, argv);
}
