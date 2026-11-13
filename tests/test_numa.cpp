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
#include "sysbench/numa.hpp"
#include "sysbench/runtime.hpp"

#include <cstring>
#include <atomic>

using namespace sb;

namespace {

Task<void> verify_worker_locality(std::uint32_t index,
                                  std::atomic<int>* failures) {
    const int cpu = ::sched_getcpu();
    const int node = numa::Topology::instance().node_of_cpu(static_cast<unsigned>(cpu));
    Runtime* runtime = Runtime::current();
    if (runtime == nullptr || node != runtime->worker_numa_node(index)) {
        failures->fetch_add(1, std::memory_order_relaxed);
    }
    auto& allocator = Runtime::current_allocator();
    void* memory = allocator.allocate(4096, 16);
    std::memset(memory, 0x3c, 4096);
    if (numa::node_of_address(memory) != node) {
        failures->fetch_add(1, std::memory_order_relaxed);
    }
    allocator.deallocate(memory, 4096, 16);
    co_return;
}

}  // namespace

TEST("NUMA topology reports nodes CPUs and worker placement") {
    const numa::Topology& topology = numa::Topology::instance();
    if (!topology.available() || topology.node_count() < 2) {
        std::println("  NUMA topology unavailable; skipping placement assertions");
        return;
    }
    CHECK(topology.node_count() >= 2);
    for (std::uint32_t worker = 0; worker < 64; ++worker) {
        const int node = topology.node_for_worker(worker);
        const int cpu = topology.cpu_for_worker(worker);
        CHECK(node >= 0);
        CHECK(cpu >= 0);
        CHECK_EQ(topology.node_of_cpu(static_cast<unsigned>(cpu)), node);
        CHECK(node != topology.reserved_cpu());
    }
}

TEST("page ranges bind to their NUMA node before first touch") {
    const numa::Topology& topology = numa::Topology::instance();
    if (!topology.available() || topology.node_count() < 2) return;
    mem::RootArena root(32 * 1024 * 1024);
    for (std::uint32_t index = 0; index < topology.node_count(); ++index) {
        const int node = topology.node(index).m_id;
        void* memory = root.allocate(2 * 1024 * 1024, 4096);
        REQUIRE(memory != nullptr);
        CHECK(numa::bind_range(memory, 2 * 1024 * 1024, node));
        std::memset(memory, 0x5a, 2 * 1024 * 1024);
        CHECK_EQ(numa::node_of_address(memory), node);
        CHECK_EQ(numa::node_of_address(static_cast<std::byte*>(memory) +
                                      1024 * 1024),
                 node);
    }
}

TEST("heap and frame allocation are node-local") {
    const numa::Topology& topology = numa::Topology::instance();
    if (!topology.available() || topology.node_count() < 2) return;
    mem::RootArena root(32 * 1024 * 1024);
    for (std::uint32_t index = 0; index < topology.node_count(); ++index) {
        const int node = topology.node(index).m_id;
        mem::HeapArena heap(root, 4 * 1024 * 1024, "numa-test", node);
        if (!heap.numa_bound()) {
            std::println("  mbind unavailable; skipping node-local assertions");
            return;
        }
        auto* value = static_cast<int*>(heap.allocate(64, 16));
        *value = node;
        CHECK_EQ(numa::node_of_address(value), node);

        mem::set_thread_frame_node(node);
        void* frame = mem::allocate_task_frame(512);
        REQUIRE(frame != nullptr);
        CHECK_EQ(numa::node_of_address(frame), node);
        mem::deallocate_task_frame(frame, 512);
    }
    mem::set_thread_frame_node(0);
}

TEST("runtime caps workers at 64 and keeps each on one NUMA node") {
    if (!mem::GlobalAllocator::instance().initialized()) {
        mem::GlobalAllocator::instance().initialize(1024ull * 1024 * 1024);
    }
    Runtime runtime;
    runtime.start(Runtime::Config{.m_workers = 100,
                                  .m_worker_heap_bytes = 1024 * 1024,
                                  .m_pin_workers = true,
                                  .m_numa_aware = true,
                                  .m_first_cpu = 0});
    CHECK_EQ(runtime.worker_count(), Runtime::kMaxWorkers);
    const numa::Topology& topology = numa::Topology::instance();
    for (std::uint32_t worker = 0; worker < runtime.worker_count(); ++worker) {
        const int node = runtime.worker_numa_node(worker);
        const int cpu = runtime.worker_cpu(worker);
        if (topology.available()) {
            CHECK(node >= 0);
            CHECK(cpu >= 0);
            CHECK_EQ(topology.node_of_cpu(static_cast<unsigned>(cpu)), node);
        }
    }

    std::atomic<int> locality_failures{0};
    for (std::uint32_t worker = 0; worker < runtime.worker_count(); ++worker) {
        runtime.spawn_on(worker, [worker, &locality_failures] {
            return verify_worker_locality(worker, &locality_failures);
        });
    }
    runtime.wait();
    CHECK_EQ(locality_failures.load(), 0);
    runtime.shutdown();
}

int main(int argc, char** argv) {
    mem::GlobalAllocator::instance().initialize(1024ull * 1024 * 1024);
    return ::sb::test::run_all(argc, argv);
}
