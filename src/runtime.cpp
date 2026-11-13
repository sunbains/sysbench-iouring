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
#include "sysbench/runtime.hpp"
#include "sysbench/lockfree_queue.hpp"
#include "sysbench/numa.hpp"

#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <stdexcept>
#include <utility>

namespace sb::detail {
thread_local PromiseBase* tls_current = nullptr;
thread_local void* tls_worker = nullptr;
}  // namespace sb::detail

namespace sb {

using detail::PromiseBase;

namespace {

using Clock = std::chrono::steady_clock;

void append_ready(PromiseBase*& head, PromiseBase*& tail, PromiseBase* task) noexcept {
    task->next_ready = nullptr;
    task->in_ready = true;
    if (tail != nullptr) {
        tail->next_ready = task;
    } else {
        head = task;
    }
    tail = task;
}

void set_thread_affinity(std::uint32_t cpu) {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    const long cpus = ::sysconf(_SC_NPROCESSORS_ONLN);
    if (cpus > 0) {
        CPU_SET(static_cast<int>(cpu % static_cast<std::uint32_t>(cpus)), &set);
        (void)::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set);
    }
#else
    (void)cpu;
#endif
}

inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#else
    std::this_thread::yield();
#endif
}

void set_thread_name(const char* name) {
#if defined(__linux__)
    (void)::pthread_setname_np(::pthread_self(), name);
#else
    (void)name;
#endif
}

// How long a worker keeps polling its wake sequence after it has submitted I/O
// before it parks on the eventfd.  A parked worker costs two context switches
// (and, on an idle core, a C-state exit) per completion, so a bounded spin is
// cheaper for a busy benchmark; the deadline still lets a slow peer release
// the CPU.
constexpr std::chrono::microseconds kIdleSpin{50};
}  // namespace

struct alignas(4096) Runtime::Worker {
    Runtime* runtime;
    std::uint32_t id;
    mem::HeapArena heap;
    std::thread thread;

    // Completion handoff: a multi-producer / single-consumer intrusive stack.
    // Producers are reapers and the spawning thread; the only popper is this
    // worker.  ABA cannot occur because only the consumer removes nodes, so a
    // pushed node can never return to the head behind the consumer's back.
    std::atomic<PromiseBase*> injected{nullptr};
    std::atomic<bool> waiting{false};
    int wake_fd = -1;
    PromiseBase* ready_head = nullptr;
    PromiseBase* ready_tail = nullptr;
    PromiseBase* completed_head = nullptr;
    PromiseBase* deferred_head = nullptr;
    PromiseBase* timer_head = nullptr;
    std::atomic<bool> exiting{false};
    std::atomic<std::uint32_t> pending_io{0};
    int numa_node = -1;
    int cpu = -1;

    Worker(Runtime* owner, std::uint32_t worker_id, const Config& config,
           int node, int pinned_cpu)
        : runtime(owner),
          id(worker_id),
          heap(mem::global_root(), config.worker_heap_bytes, "worker", node),
          numa_node(node),
          cpu(pinned_cpu) {
        wake_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wake_fd < 0) throw std::system_error(errno, std::generic_category(),
                                                 "eventfd");
    }

    ~Worker() {
        if (wake_fd >= 0) ::close(wake_fd);
    }

    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;

    void signal_wake() noexcept {
        if (wake_fd < 0) return;
        const std::uint64_t one = 1;
        const ssize_t ignored = ::write(wake_fd, &one, sizeof(one));
        (void)ignored;
    }

    void push_ready(PromiseBase* task) noexcept {
        append_ready(ready_head, ready_tail, task);
    }

    PromiseBase* pop_ready() noexcept {
        PromiseBase* task = ready_head;
        if (task != nullptr) {
            ready_head = task->next_ready;
            if (ready_head == nullptr) ready_tail = nullptr;
            task->next_ready = nullptr;
            task->in_ready = false;
        }
        return task;
    }

    void push_injected(PromiseBase* task) noexcept {
        PromiseBase* head = injected.load(std::memory_order_relaxed);
        do {
            if (exiting.load(std::memory_order_relaxed)) return;
            task->next_ready = head;
        } while (!injected.compare_exchange_weak(
            head, task, std::memory_order_seq_cst, std::memory_order_relaxed));
        // The stack publish and the `waiting` check must both be seq_cst: a
        // release/acquire pair still permits the store-buffer interleaving
        // where the worker stores waiting=true and loads the empty stack while
        // the producer pushes and loads waiting=false.  That loses the wakeup
        // and parks the worker forever with the task sitting in the stack.
        if (waiting.load(std::memory_order_seq_cst)) {
            signal_wake();
        }
    }

    void push_completed(PromiseBase* task) noexcept {
        task->next_completed = completed_head;
        completed_head = task;
    }

    void push_deferred(PromiseBase* task) noexcept {
        task->next_deferred = deferred_head;
        deferred_head = task;
    }

    void process_deferred() noexcept {
        PromiseBase* task = deferred_head;
        deferred_head = nullptr;
        while (task != nullptr) {
            PromiseBase* next = task->next_deferred;
            task->next_deferred = nullptr;
            task->self.destroy();
            task = next;
        }
    }


    void add_timer(PromiseBase* task) noexcept {
        task->timer_queued = true;
        PromiseBase** link = &timer_head;
        while (*link != nullptr && (*link)->wake_at <= task->wake_at) {
            link = &(*link)->next_timer;
        }
        task->next_timer = *link;
        *link = task;
    }

    void activate_due_timers() noexcept {
        const auto now = Clock::now();
        while (timer_head != nullptr && timer_head->wake_at <= now) {
            PromiseBase* task = timer_head;
            timer_head = task->next_timer;
            task->next_timer = nullptr;
            task->timer_queued = false;
            push_ready(task);
        }
    }

    void drain_injected() noexcept {
        PromiseBase* list = injected.exchange(nullptr, std::memory_order_acquire);
        while (list != nullptr) {
            PromiseBase* next = list->next_ready;
            push_ready(list);
            list = next;
        }
    }

    void process_completed() noexcept {
        PromiseBase* task = completed_head;
        completed_head = nullptr;
        while (task != nullptr) {
            PromiseBase* next = task->next_completed;
            task->next_completed = nullptr;
            task->self.destroy();
            runtime->finish_task();
            task = next;
        }
    }

    void resume(PromiseBase* task) noexcept {
        detail::tls_worker = this;
        detail::tls_current = task;
        task->self.resume();
        detail::tls_current = nullptr;
        runtime->m_resumes.add(id, 1);
    }

    void wait_for_work() {
        const auto now = Clock::now();
        if (timer_head != nullptr && timer_head->wake_at <= now) return;

        if (pending_io.load(std::memory_order_acquire) != 0 &&
            runtime->undersubscribed()) {
            const auto deadline = Clock::now() + kIdleSpin;
            std::uint32_t spins = 0;
            for (;;) {
                if (injected.load(std::memory_order_acquire) != nullptr ||
                    exiting.load(std::memory_order_acquire)) {
                    return;
                }
                // The wake sequence is checked on every pass so completion
                // handoff latency stays in the hundreds of nanoseconds; the
                // clock is sampled only periodically because the vDSO call is
                // far more expensive than the check itself.
                if ((++spins & 63) == 0 && Clock::now() >= deadline) break;
                cpu_relax();
            }
        }

        waiting.store(true, std::memory_order_seq_cst);
        if (injected.load(std::memory_order_seq_cst) != nullptr ||
            exiting.load(std::memory_order_acquire)) {
            waiting.store(false, std::memory_order_seq_cst);
            return;
        }

        int timeout_ms = -1;
        if (timer_head != nullptr) {
            const auto timer_now = Clock::now();
            if (timer_head->wake_at <= timer_now) {
                waiting.store(false, std::memory_order_seq_cst);
                return;
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                timer_head->wake_at - timer_now);
            timeout_ms = static_cast<int>(std::max<std::int64_t>(1, remaining.count()));
        }

        pollfd descriptor{.fd = wake_fd, .events = POLLIN, .revents = 0};
        const int result = ::poll(&descriptor, 1, timeout_ms);
        waiting.store(false, std::memory_order_release);
        if (result > 0 && (descriptor.revents & POLLIN) != 0) {
            std::uint64_t counter = 0;
            const ssize_t ignored = ::read(wake_fd, &counter, sizeof(counter));
            (void)ignored;
        }
    }

    void request_exit() noexcept {
        exiting.store(true, std::memory_order_release);
        signal_wake();
    }

    void run() noexcept {
        detail::tls_worker = this;
        mem::set_thread_frame_node(numa_node >= 0 ? numa_node : 0);
        if (numa_node >= 0) {
            (void)numa::set_preferred(numa_node);
        }
        set_thread_name(numa_node >= 0 ? "sb-worker-n" : "sb-worker");
        if (runtime->m_config.pin_workers) {
            if (numa_node >= 0) {
                const numa::Topology& topology = numa::Topology::instance();
                const numa::NodeInfo* info = topology.node_by_id(numa_node);
                if (info != nullptr) {
                    cpu_set_t set;
                    CPU_ZERO(&set);
                    for (std::uint32_t slot = 0; slot < info->cpu_count; ++slot) {
                        const int candidate = static_cast<int>(info->cpus[slot]);
                        if (candidate != topology.reserved_cpu()) {
                            CPU_SET(candidate, &set);
                        }
                    }
                    (void)::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set);
                }
            } else if (cpu >= 0) {
                set_thread_affinity(static_cast<std::uint32_t>(cpu));
            } else {
                set_thread_affinity(runtime->m_config.first_cpu + id);
            }
        }

        for (;;) {
            drain_injected();
            activate_due_timers();
            process_completed();
            process_deferred();

            if (PromiseBase* task = pop_ready()) {
                resume(task);
                continue;
            }

            if (exiting.load(std::memory_order_acquire)) break;
            wait_for_work();
        }

        drain_injected();
        process_completed();
        process_deferred();
        detail::tls_worker = nullptr;
    }
};

void Runtime::start() {
    Config defaults;
    start(defaults);
}

void Runtime::start(const Config& config_input) {
    Config config = config_input;
    if (m_workers != nullptr) throw std::logic_error("runtime already started");
    if (config.workers == 0) config.workers = hardware_workers();
    if (config.workers == 0) config.workers = 1;
    if (config.workers > kMaxWorkers) {
        std::println(stderr, "sysbench: worker count capped at {} (requested {})",
                     kMaxWorkers, config.workers);
        config.workers = kMaxWorkers;
    }
    if (config.worker_heap_bytes < 1024 * 1024) config.worker_heap_bytes = 1024 * 1024;
    m_config = config;
    m_worker_count = config.workers;
    m_wait_event_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (m_wait_event_fd < 0) {
        m_worker_count = 0;
        throw std::system_error(errno, std::generic_category(), "eventfd");
    }

    m_worker_storage =
        mem::global_root().allocate(sizeof(Worker) * m_worker_count, alignof(Worker));
    m_workers = static_cast<Worker*>(m_worker_storage);

    const numa::Topology& topology = numa::Topology::instance();
    const bool use_numa = m_config.numa_aware && topology.available();

    std::uint32_t constructed = 0;
    try {
        for (; constructed < m_worker_count; ++constructed) {
            const int node = use_numa ? topology.node_for_worker(constructed) : -1;
            const int cpu = use_numa ? topology.cpu_for_worker(constructed) : -1;
            if (node >= 0) {
                (void)numa::bind_range(m_workers + constructed, sizeof(Worker), node);
                (void)numa::set_preferred(node);
            }
            ::new (static_cast<void*>(m_workers + constructed))
                Worker(this, constructed, m_config, node, cpu);
            if (use_numa) (void)numa::set_preferred(topology.home_node());
        }
    } catch (...) {
        for (std::uint32_t i = constructed; i > 0; --i) m_workers[i - 1].~Worker();
        mem::global_root().deallocate(m_worker_storage,
                                      sizeof(Worker) * m_worker_count,
                                      alignof(Worker));
        m_workers = nullptr;
        m_worker_storage = nullptr;
        m_worker_count = 0;
        if (m_wait_event_fd >= 0) {
            ::close(m_wait_event_fd);
            m_wait_event_fd = -1;
        }
        throw;
    }

    std::uint32_t started = 0;
    try {
        for (; started < m_worker_count; ++started) {
            if (use_numa && m_workers[started].numa_node >= 0) {
                (void)numa::set_preferred(m_workers[started].numa_node);
            }
            m_workers[started].thread = std::thread([worker = &m_workers[started]] {
                worker->run();
            });
            if (use_numa) (void)numa::set_preferred(topology.home_node());
        }
    } catch (...) {
        for (std::uint32_t i = 0; i < started; ++i) m_workers[i].request_exit();
        for (std::uint32_t i = 0; i < started; ++i) {
            if (m_workers[i].thread.joinable()) m_workers[i].thread.join();
        }
        for (std::uint32_t i = m_worker_count; i > 0; --i) m_workers[i - 1].~Worker();
        mem::global_root().deallocate(m_worker_storage,
                                      sizeof(Worker) * m_worker_count,
                                      alignof(Worker));
        m_workers = nullptr;
        m_worker_storage = nullptr;
        m_worker_count = 0;
        if (m_wait_event_fd >= 0) {
            ::close(m_wait_event_fd);
            m_wait_event_fd = -1;
        }
        throw;
    }

    m_running.store(true, std::memory_order_release);
}

void Runtime::shutdown() {
    if (m_workers == nullptr) {
        if (m_wait_event_fd >= 0) {
            ::close(m_wait_event_fd);
            m_wait_event_fd = -1;
        }
        return;
    }
    m_running.store(false, std::memory_order_release);
    for (std::uint32_t i = 0; i < m_worker_count; ++i) m_workers[i].request_exit();
    for (std::uint32_t i = 0; i < m_worker_count; ++i) {
        if (m_workers[i].thread.joinable()) m_workers[i].thread.join();
    }
    for (std::uint32_t i = m_worker_count; i > 0; --i) m_workers[i - 1].~Worker();
    mem::global_root().deallocate(m_worker_storage,
                                  sizeof(Worker) * m_worker_count,
                                  alignof(Worker));
    m_workers = nullptr;
    m_worker_storage = nullptr;
    m_worker_count = 0;

    // Ask any remaining runtime-owned threads to stop, then join them.  Worker
    // threads are already joined, so no new threads can be registered now.
    for (ThreadSlot& slot : m_thread_slots) {
        if (slot.key.load(std::memory_order_acquire) == 0) continue;
        if (slot.stop != nullptr) slot.stop(slot.argument);
    }
    for (ThreadSlot& slot : m_thread_slots) {
        if (slot.key.load(std::memory_order_acquire) == 0) continue;
        if (slot.thread.joinable()) slot.thread.join();
        slot.key.store(0, std::memory_order_release);
    }

    if (m_wait_event_fd >= 0) {
        ::close(m_wait_event_fd);
        m_wait_event_fd = -1;
    }
}

Runtime::~Runtime() {
    shutdown();
}

namespace {

void complete_top_level(PromiseBase* task) noexcept {
    auto* worker = static_cast<Runtime::Worker*>(detail::tls_worker);
    if (worker != nullptr) worker->push_completed(task);
}

}  // namespace

void Runtime::spawn_on(std::uint32_t worker, Task<void>&& task) {
    if (!m_running.load(std::memory_order_acquire) || m_workers == nullptr) {
        throw std::logic_error("runtime is not running");
    }
    if (worker >= m_worker_count) throw std::out_of_range("worker id");
    auto handle = task.release();
    if (!handle) throw std::invalid_argument("spawning an empty task");

    PromiseBase* promise = &handle.promise();
    promise->on_complete = &complete_top_level;
    promise->started = true;
    promise->owner_worker = worker;
    m_outstanding.add(worker, 1);
    const std::uint32_t active = m_active_tasks.fetch_add(1, std::memory_order_relaxed) + 1;
    m_undersubscribed.store(active <= m_worker_count, std::memory_order_relaxed);
    m_workers[worker].push_injected(promise);
}

void Runtime::spawn(Task<void>&& task) {
    if (m_worker_count == 0) throw std::logic_error("runtime is not running");
    spawn_on(choose_worker(), std::move(task));
}

std::uint32_t Runtime::choose_worker() noexcept {
    static thread_local std::uint64_t cursor = 0;
    return static_cast<std::uint32_t>(cursor++ % m_worker_count);
}

int Runtime::spawn_numa_node(std::uint32_t worker) const {
    if (m_workers == nullptr || worker >= m_worker_count) return -1;
    return m_workers[worker].numa_node;
}

void Runtime::note_io_submitted() noexcept {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    if (worker != nullptr) worker->pending_io.fetch_add(1, std::memory_order_relaxed);
}

void Runtime::note_io_completed(std::uint32_t worker) noexcept {
    if (m_workers == nullptr || worker >= m_worker_count) return;
    // Release so a worker that reads zero pending operations is guaranteed to
    // observe the completion that was posted just before the decrement.
    m_workers[worker].pending_io.fetch_sub(1, std::memory_order_release);
}

void Runtime::post_completion(std::uint32_t worker,
                              detail::PromiseBase* task) noexcept {
    if (task == nullptr || m_workers == nullptr || worker >= m_worker_count ||
        !m_running.load(std::memory_order_acquire)) {
        return;
    }
    m_workers[worker].push_injected(task);
}

bool Runtime::start_thread(const ThreadSpec& spec) {
    if (spec.key == nullptr || spec.body == nullptr) return false;
    const std::uintptr_t key = reinterpret_cast<std::uintptr_t>(spec.key);
    for (ThreadSlot& slot : m_thread_slots) {
        std::uintptr_t expected = 0;
        if (!slot.key.compare_exchange_strong(expected, key,
                                              std::memory_order_acq_rel)) {
            continue;
        }
        slot.body = spec.body;
        slot.stop = spec.stop;
        slot.argument = spec.argument;
        slot.thread = std::thread([&slot] { (void)slot.body(slot.argument); });
        return true;
    }
    return false;
}

void Runtime::join_thread(const void* key) {
    const std::uintptr_t wanted = reinterpret_cast<std::uintptr_t>(key);
    for (ThreadSlot& slot : m_thread_slots) {
        if (slot.key.load(std::memory_order_acquire) != wanted) continue;
        if (slot.thread.joinable()) slot.thread.join();
        slot.key.store(0, std::memory_order_release);
        return;
    }
}

bool Runtime::start_reaper(IoReaper& reaper) {
    ThreadSpec spec;
    spec.key = &reaper;
    spec.body = [](void* context) -> void* {
        auto* target = static_cast<IoReaper*>(context);
        target->runtime().reaper_loop(*target);
        return nullptr;
    };
    spec.stop = [](void* context) {
        static_cast<IoReaper*>(context)->request_stop();
    };
    spec.argument = &reaper;
    return start_thread(spec);
}

void Runtime::stop_reaper(IoReaper& reaper) {
    join_thread(&reaper);
}

void Runtime::reaper_loop(IoReaper& reaper) noexcept {
    reaper.pin();
    while (reaper.running()) {
        if (reaper.reap()) continue;

        // Undersubscribed: a bounded spin is cheaper than a kernel sleep/wake
        // for a completion that is likely imminent.  Oversubscribed: every
        // cycle spent here is a cycle a runnable task (on any socket) does
        // not get, so block instead and let the scheduler run that task.
        if (undersubscribed()) {
            const auto deadline = Clock::now() + kIdleSpin;
            std::uint32_t spins = 0;
            bool ready = false;
            for (;;) {
                if (!reaper.running()) return;
                if ((++spins & 63) == 0 && Clock::now() >= deadline) break;
                if (reaper.reap()) {
                    ready = true;
                    break;
                }
                cpu_relax();
            }
            if (ready) continue;
        }

        if (!reaper.wait()) break;
    }
}

void Runtime::finish_task() noexcept {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    const std::uint32_t worker_id = worker != nullptr ? worker->id : 0;
    m_outstanding.add(worker_id, -1);
    const std::uint32_t active = m_active_tasks.fetch_sub(1, std::memory_order_relaxed) - 1;
    m_undersubscribed.store(active <= m_worker_count, std::memory_order_relaxed);
    if (m_wait_event_fd >= 0 && m_outstanding.sum(m_worker_count) == 0) {
        const std::uint64_t one = 1;
        const ssize_t ignored = ::write(m_wait_event_fd, &one, sizeof(one));
        (void)ignored;
    }
}

void Runtime::wait() {
    for (;;) {
        if (m_workers == nullptr || m_outstanding.sum(m_worker_count) == 0) return;
        pollfd descriptor{.fd = m_wait_event_fd, .events = POLLIN, .revents = 0};
        const int result = ::poll(&descriptor, 1, -1);
        if (result > 0 && (descriptor.revents & POLLIN) != 0) {
            std::uint64_t counter = 0;
            const ssize_t ignored = ::read(m_wait_event_fd, &counter, sizeof(counter));
            (void)ignored;
        }
    }
}

bool Runtime::wait_for(std::chrono::nanoseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    for (;;) {
        if (m_workers == nullptr || m_outstanding.sum(m_worker_count) == 0) return true;
        const auto now = Clock::now();
        if (now >= deadline) return false;
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        const int timeout_ms =
            static_cast<int>(std::max<std::int64_t>(1, remaining.count()));
        pollfd descriptor{.fd = m_wait_event_fd, .events = POLLIN, .revents = 0};
        const int result = ::poll(&descriptor, 1, timeout_ms);
        if (result > 0 && (descriptor.revents & POLLIN) != 0) {
            std::uint64_t counter = 0;
            const ssize_t ignored = ::read(m_wait_event_fd, &counter, sizeof(counter));
            (void)ignored;
        }
    }
}

mem::MemoryResource& Runtime::worker_allocator(std::uint32_t worker) {
    if (m_workers == nullptr || worker >= m_worker_count) {
        throw std::out_of_range("worker id");
    }
    return m_workers[worker].heap;
}

mem::ResourceStats Runtime::worker_heap_stats(std::uint32_t worker) const {
    if (m_workers == nullptr || worker >= m_worker_count) {
        throw std::out_of_range("worker id");
    }
    return m_workers[worker].heap.stats();
}

Runtime* Runtime::current() noexcept {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    return worker != nullptr ? worker->runtime : nullptr;
}

std::uint32_t Runtime::current_worker_id() noexcept {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    return worker != nullptr ? worker->id : 0;
}

mem::MemoryResource& Runtime::current_allocator() {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    if (worker == nullptr) throw std::logic_error("not running on a runtime worker");
    return worker->heap;
}



std::uint64_t Runtime::resumes() const noexcept {
    return m_resumes.sum(m_worker_count);
}

std::uint32_t Runtime::numa_node_count() const noexcept {
    return numa::Topology::instance().node_count();
}

int Runtime::worker_numa_node(std::uint32_t worker) const {
    if (m_workers == nullptr || worker >= m_worker_count) {
        throw std::out_of_range("worker id");
    }
    return m_workers[worker].numa_node;
}

int Runtime::worker_cpu(std::uint32_t worker) const {
    if (m_workers == nullptr || worker >= m_worker_count) {
        throw std::out_of_range("worker id");
    }
    return m_workers[worker].cpu;
}

std::uint32_t Runtime::hardware_workers() noexcept {
    const unsigned int count = std::thread::hardware_concurrency();
    return count == 0 ? 1 : count;
}

}  // namespace sb

namespace sb::detail {

void schedule_current(PromiseBase* task) noexcept {
    auto* worker = static_cast<Runtime::Worker*>(tls_worker);
    if (worker == nullptr) {
        no_runtime();
    }
    worker->push_ready(task);
}

void sleep_current(std::chrono::nanoseconds duration) noexcept {
    auto* worker = static_cast<Runtime::Worker*>(tls_worker);
    PromiseBase* task = tls_current;
    if (worker == nullptr || task == nullptr) {
        no_runtime();
    }
    if (duration <= std::chrono::nanoseconds::zero()) {
        worker->push_ready(task);
        return;
    }
    task->wake_at = std::chrono::steady_clock::now() + duration;
    worker->add_timer(task);
}

void defer_destroy(PromiseBase* task) noexcept {
    auto* worker = static_cast<Runtime::Worker*>(tls_worker);
    if (worker == nullptr) {
        task->self.destroy();
        return;
    }
    worker->push_deferred(task);
}

void no_runtime() {
    std::abort();
}

}  // namespace sb::detail
