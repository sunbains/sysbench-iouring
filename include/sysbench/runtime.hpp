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

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstddef>
#include <functional>
#include <thread>
#include <type_traits>

namespace sb {

// A source of I/O completions serviced by the Runtime.  Implementations only
// report readiness and block; every spin, yield and sleep decision is made by
// the Runtime's scheduler, which starts and runs the reaper thread.
class Runtime;

class IoReaper {
public:
    virtual ~IoReaper() = default;
    [[nodiscard]] virtual bool running() const noexcept = 0;
    // Drain all completions currently available.  Returns true if any.
    virtual bool reap() noexcept = 0;
    // Block until a completion or shutdown.  Returns false to stop reaping.
    virtual bool wait() noexcept = 0;
    // Called once on the reaper thread before the loop starts.
    virtual void pin() noexcept {}
    // Asked by the Runtime when it shuts down; must make wait() return and
    // running() report false.
    virtual void request_stop() noexcept = 0;
    [[nodiscard]] virtual Runtime& runtime() noexcept = 0;
};

// The Runtime owns every worker thread and every scheduling decision.  Tasks
// are pinned to a worker (no work stealing) because their registered io_uring
// buffers, Lua states and scoped arenas are thread-affine.  Incoming work is
// handed off through a lock-free power-of-two injection ring; once running, a
// task's continuations use intrusive queues and symmetric transfer only.
class Runtime {
public:
    static constexpr std::uint32_t kMaxWorkers = 64;

    struct Config {
        std::uint32_t m_workers = 1;
        std::size_t m_worker_heap_bytes = 32 * 1024 * 1024;
        bool m_pin_workers = true;
        bool m_numa_aware = true;
        std::uint32_t m_first_cpu = 0;
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
    void post_completion(std::uint32_t worker,
                         detail::PromiseBase* task) noexcept;
    static void note_io_submitted() noexcept;
    void note_io_completed(std::uint32_t worker) noexcept;

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

    // Wait until every spawned top-level task has completed.
    void wait();
    [[nodiscard]] bool wait_for(std::chrono::nanoseconds timeout);

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
    [[nodiscard]] int worker_cpu(std::uint32_t worker) const;

    [[nodiscard]] mem::MemoryResource& worker_allocator(std::uint32_t worker);
    [[nodiscard]] mem::ResourceStats worker_heap_stats(std::uint32_t worker) const;

    static Runtime* current() noexcept;
    static std::uint32_t current_worker_id() noexcept;
    static mem::MemoryResource& current_allocator();
    static std::uint32_t hardware_workers() noexcept;

    // Runtime-owned threads.  Every thread in the process is created, tracked
    // and joined by the Runtime; callers only supply a body and an optional
    // stop hook used during shutdown.
    struct ThreadSpec {
        const void* m_key = nullptr;
        void* (*m_body)(void*) = nullptr;
        void (*m_stop)(void*) = nullptr;
        void* m_argument = nullptr;
    };
    [[nodiscard]] bool start_thread(const ThreadSpec& spec);
    void join_thread(const void* key);

    // A reaper thread whose wait policy (spin while undersubscribed, block
    // otherwise) is owned by the Runtime scheduler.
    [[nodiscard]] bool start_reaper(IoReaper& reaper);
    void stop_reaper(IoReaper& reaper);

    // Opaque implementation type (public only so the runtime's free scheduling
    // hooks can name it; its definition is private to runtime.cpp).
    struct Worker;

private:
    friend struct Worker;
    void reaper_loop(IoReaper& reaper) noexcept;
    void finish_task() noexcept;
    [[nodiscard]] int spawn_numa_node(std::uint32_t worker) const;
    [[nodiscard]] std::uint32_t choose_worker() noexcept;

    Worker* m_workers = nullptr;
    void* m_worker_storage = nullptr;
    std::uint32_t m_worker_count = 0;
    Config m_config{};
    std::atomic<bool> m_running{false};
    std::atomic<bool> m_stop_requested{false};
    static constexpr std::uint32_t kMaxThreads = 256;
    struct ThreadSlot {
        std::atomic<std::uintptr_t> m_key{0};
        void* (*m_body)(void*) = nullptr;
        void (*m_stop)(void*) = nullptr;
        void* m_argument = nullptr;
        std::thread m_thread;
    };
    ThreadSlot m_thread_slots[kMaxThreads];

    ShardedCounter<std::int64_t, kMaxWorkers> m_outstanding;
    ShardedCounter<std::uint64_t, kMaxWorkers> m_resumes;
    std::atomic<std::uint32_t> m_active_tasks{0};
    std::atomic<bool> m_undersubscribed{true};
    int m_wait_event_fd = -1;
};

}  // namespace sb
