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
#include "sysbench/sharded_counter.hpp"
#include "sysbench/task.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstddef>
#include <functional>
#include <thread>
#include <type_traits>

namespace sb {

struct IoCompletion;
class IoRing;

class Runtime;


// The Runtime owns every worker thread and every scheduling decision.  Tasks
// are pinned to a worker (no work stealing) because their sockets, Lua states
// and scoped arenas are thread-affine.
//
// Each worker owns one io_uring.  It runs every runnable task (each queues its
// SQEs in the ring), submits the batch and collects completed work in one
// non-blocking kernel entry, reaps the CQE batch and resumes those tasks.  It
// blocks in io_uring only when nothing is runnable and every connection it owns
// is in flight, and then only after a spin of 1.2x its tracked round trip.
class Runtime {
public:
    static constexpr std::uint32_t kMaxWorkers = 64;

    struct Config {
        std::uint32_t m_workers = 1;
        std::size_t m_worker_heap_bytes = 32 * 1024 * 1024;
        bool m_pin_workers = true;
        bool m_numa_aware = true;
        std::uint32_t m_first_cpu = 0;
        // How long wait() lets every task stay outstanding with no worker
        // running before it reports a stalled connection and returns.  Zero
        // waits forever.
        std::chrono::milliseconds m_stall_timeout{60000};
        // Submission queue entries of each worker's io_uring.
        std::uint32_t m_ring_entries = 4096;
        // Where a worker's wait-for-completion spin runs.  On: in the kernel,
        // as NAPI busy polling (the worker polls its sockets' NIC queues, so
        // each reply is processed on its CPU).  Off: in user space, polling
        // the completion queue.  Either way the spin is the scheduler's, and
        // its length is learned from the waits the worker observes.
        bool m_busy_poll = false;
    };

    Runtime() = default;
    ~Runtime();

    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;

    void start();
    void start(const Config& config);
    void shutdown();

    // Spawn a detached top-level task.  The runtime owns the coroutine frame
    // from this point on and destroys it when it completes.
    void spawn(Task<void>&& task);
    void spawn_on(std::uint32_t worker, Task<void>&& task);
    // The calling worker's io_uring (nullptr off a worker thread).
    [[nodiscard]] static IoRing* current_ring() noexcept;
    // The calling worker has queued an SQE whose CQE carries `completion`.
    static void io_issued(IoCompletion* completion) noexcept;
    // Completion records for sends nobody awaits, owned by the calling worker
    // (not by the connection), so a connection can close with sends in flight
    // without waiting: it orphans them and the reaper takes them back.
    [[nodiscard]] static IoCompletion* acquire_send_completion();
    static void release_send_completion(IoCompletion* completion) noexcept;

    // Factory form: the coroutine frame is allocated while the calling thread's
    // frame pool is temporarily switched to the target worker's NUMA node.
    template <class Factory>
        requires std::invocable<Factory&> &&
                 std::same_as<std::invoke_result_t<Factory&>, Task<void>>
    void spawn_on(std::uint32_t worker, Factory&& factory) {
        const int node = spawn_numa_node(worker);
        const int previous = mem::thread_frame_node();
        if (node >= 0) mem::set_thread_frame_node(node);
        Task<void> task;
        try {
            task = std::invoke(std::forward<Factory>(factory));
        } catch (...) {
            mem::set_thread_frame_node(previous);
            throw;
        }
        mem::set_thread_frame_node(previous);
        spawn_on(worker, std::move(task));
    }

    template <class Factory>
        requires std::invocable<Factory&> &&
                 std::same_as<std::invoke_result_t<Factory&>, Task<void>>
    void spawn_factory(Factory&& factory) {
        if (m_worker_count == 0) throw std::logic_error("runtime is not running");
        spawn_on(choose_worker(), std::forward<Factory>(factory));
    }

    // Wait until every spawned top-level task has completed.  Returns false,
    // after printing an error, when the tasks stay outstanding with no worker
    // running for longer than the configured stall timeout: the server has
    // stopped answering and there is nothing left to wait for.
    bool wait();
    [[nodiscard]] bool wait_for(std::chrono::nanoseconds timeout);

    // True when wait()/wait_for() stopped because no task made progress.
    [[nodiscard]] bool stall_detected() const noexcept {
        return m_stall_detected.load(std::memory_order_acquire);
    }

    // Cooperative stop request observed by benchmark loops (for SIGINT/time).
    void request_stop() noexcept { m_stop_requested.store(true, std::memory_order_release); }
    [[nodiscard]] bool stop_requested() const noexcept {
        return m_stop_requested.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool running() const noexcept { return m_running.load(std::memory_order_acquire); }
    [[nodiscard]] std::uint32_t worker_count() const noexcept { return m_worker_count; }
    // True while there are no more benchmark tasks than workers.  Under that
    // condition a short spin for an I/O completion is cheaper than parking;
    // when tasks outnumber workers the CPU is better spent running whatever
    // task is runnable (possibly on another socket), so waiters park instead.
    [[nodiscard]] bool undersubscribed() const noexcept {
        return m_undersubscribed.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t resumes() const noexcept;
    [[nodiscard]] std::uint32_t numa_node_count() const noexcept;
    [[nodiscard]] int worker_numa_node(std::uint32_t worker) const;
    // The worker's ID among its NUMA node's workers, assigned at creation.
    [[nodiscard]] std::uint32_t worker_node_index(std::uint32_t worker) const;
    [[nodiscard]] int worker_cpu(std::uint32_t worker) const;

    [[nodiscard]] mem::MemoryResource& worker_allocator(std::uint32_t worker);
    [[nodiscard]] mem::ResourceStats worker_heap_stats(std::uint32_t worker) const;

    static Runtime* current() noexcept;
    static std::uint32_t current_worker_id() noexcept;
    static mem::MemoryResource& current_allocator();
    static std::uint32_t hardware_workers() noexcept;

    // Opaque implementation type (public only so the runtime's free scheduling
    // hooks can name it; its definition is private to runtime.cpp).
    struct Worker;

private:
    friend struct Worker;

    void finish_task() noexcept;
    [[nodiscard]] int spawn_numa_node(std::uint32_t worker) const;
    [[nodiscard]] std::uint32_t choose_worker() noexcept;
    // @return true when the outstanding tasks have made no progress for the
    // stall timeout; prints the error once.
    // Called by a worker that has seen no progress for the stall timeout.
    void declare_stall(std::int64_t outstanding,
                       std::chrono::steady_clock::duration stalled) noexcept;

    Worker* m_workers = nullptr;
    void* m_worker_storage = nullptr;
    std::uint32_t m_worker_count = 0;
    Config m_config{};
    std::atomic<bool> m_running{false};
    std::atomic<bool> m_stop_requested{false};
    ShardedCounter<std::int64_t, kMaxWorkers> m_outstanding;
    ShardedCounter<std::uint64_t, kMaxWorkers> m_resumes;
    std::atomic<std::uint32_t> m_active_tasks{0};
    std::atomic<bool> m_undersubscribed{true};
    std::chrono::milliseconds m_stall_timeout{0};
    // Completions and finished tasks: what the workers' stall check watches.
    ShardedCounter<std::uint64_t, kMaxWorkers> m_progress;
    std::atomic<bool> m_stall_detected{false};
    // Futex word the waiting main thread sleeps on; bumped when all tasks finish.
    std::atomic<std::uint32_t> m_wait_word{0};
    // Per NUMA node, the one idle worker allowed to spin (-1: none); the
    // others go straight to their futex.
    struct alignas(64) IdleSpinner {
        std::atomic<std::int32_t> m_worker{-1};
    };
    std::array<IdleSpinner, 64> m_idle_spinners{};
};

}  // namespace sb
