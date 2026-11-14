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
#include "sysbench/layer_stats.hpp"
#include "sysbench/io_ring.hpp"
#include "sysbench/lockfree_queue.hpp"
#include "sysbench/numa.hpp"

#include <poll.h>
#include <pthread.h>
#include <ctime>
#include <sys/syscall.h>
#include <linux/futex.h>
#include <sched.h>
#include <unistd.h>

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <optional>
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
    task->m_next_ready = nullptr;
    task->m_in_ready = true;
    if (tail != nullptr) {
        tail->m_next_ready = task;
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

/* TSC timing for the scheduler's learned waits.  The rate is measured once
against the steady clock when the runtime starts. */
[[nodiscard]] inline std::uint64_t tsc_now() noexcept { return __builtin_ia32_rdtsc(); }
double g_cycles_per_ns = 1.0;

void calibrate_tsc() {
    const auto clock_start = std::chrono::steady_clock::now();
    const std::uint64_t tsc_start = tsc_now();
    while (std::chrono::steady_clock::now() - clock_start < std::chrono::milliseconds(2)) {
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - clock_start).count();
    const std::uint64_t cycles = tsc_now() - tsc_start;
    if (elapsed > 0 && cycles > 0) g_cycles_per_ns = static_cast<double>(cycles) / static_cast<double>(elapsed);
}

[[nodiscard]] inline std::uint64_t ns_to_cycles(std::uint64_t ns) noexcept {
    return static_cast<std::uint64_t>(static_cast<double>(ns) * g_cycles_per_ns);
}
[[nodiscard]] inline std::uint64_t cycles_to_ns(std::uint64_t cycles) noexcept {
    return static_cast<std::uint64_t>(static_cast<double>(cycles) / g_cycles_per_ns);
}

// The scheduler's spin-wait hint (only the scheduler spins).
inline void spin_pause() noexcept { __builtin_ia32_pause(); }

// Where every learned wait starts before the first sample (as db-runtime's
// initial read estimate).
constexpr std::uint64_t kInitialWaitNs = 50'000;

void set_thread_name(const char* name) {
#if defined(__linux__)
    (void)::pthread_setname_np(::pthread_self(), name);
#else
    (void)name;
#endif
}

}  // namespace

struct alignas(4096) Runtime::Worker {
    Runtime* m_runtime;
    std::uint32_t m_id;
    mem::HeapArena m_heap;
    std::thread m_thread;

    // New tasks: a multi-producer / single-consumer intrusive stack.  The
    // producer is the spawning thread; the only popper is this worker.  ABA cannot occur because only the consumer removes nodes, so a
    // pushed node can never return to the head behind the consumer's back.
    std::atomic<PromiseBase*> m_injected{nullptr};
    std::atomic<bool> m_waiting{false};
    // Futex word a sleeping worker waits on; a new task or exit bumps it.
    std::atomic<std::uint32_t> m_wake_word{0};
    PromiseBase* m_ready_head = nullptr;
    PromiseBase* m_ready_tail = nullptr;
    PromiseBase* m_completed_head = nullptr;
    PromiseBase* m_deferred_head = nullptr;
    PromiseBase* m_timer_head = nullptr;
    std::atomic<bool> m_exiting{false};
    int m_numa_node = -1;
    // The worker's ID among the workers of its NUMA node, given at creation.
    std::uint32_t m_node_index = 0;
    int m_cpu = -1;

    Worker(Runtime* owner, std::uint32_t worker_id, const Config& config,
           int node, int pinned_cpu)
        : m_runtime(owner),
          m_id(worker_id),
          m_heap(mem::global_root(), config.m_worker_heap_bytes, "worker", node),
          m_numa_node(node),
          m_cpu(pinned_cpu) {
    }

    ~Worker() {
    }

    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;

    // Stall detection, done by the workers themselves: no main thread polls.
    std::uint64_t m_seen_progress = 0;
    Clock::time_point m_idle_since{};

    void check_stall() noexcept {
        if (m_runtime->m_stall_timeout <= std::chrono::milliseconds(0)) return;
        const auto now = Clock::now();
        const std::int64_t outstanding = m_runtime->m_outstanding.sum(m_runtime->m_worker_count);
        const std::uint64_t progress = m_runtime->m_progress.sum(m_runtime->m_worker_count);
        if (outstanding <= 0 || progress != m_seen_progress ||
            m_idle_since == Clock::time_point{}) {
            m_seen_progress = progress;
            m_idle_since = now;
            return;
        }
        if (now - m_idle_since >= m_runtime->m_stall_timeout)
            m_runtime->declare_stall(outstanding, now - m_idle_since);
    }

    // This worker's io_uring, created on its own thread.  Only this worker
    // submits to it and reaps it.
    std::optional<IoRing> m_ring;
    // Busy-poll length currently registered for NAPI (us; 0: NAPI off).
    std::uint32_t m_napi_us = 0;
    // Requests issued and not yet reaped.
    std::uint32_t m_in_flight = 0;

    /* ---- Learned waits ---------------------------------------------------
    A task that suspends on I/O has declared what it waits for (the awaiter's
    WaitKind).  The scheduler times every awaited request from issue to
    completion and keeps, per kind, a decaying maximum of those times (as
    db-runtime's IoWaitLatency does): a longer sample is taken at once, a
    shorter one pulls the estimate down by 1/8 of the difference.  Its spins
    are sized by these estimates, never by a constant. */
    static constexpr unsigned kKinds = static_cast<unsigned>(WaitKind::Count);
    std::uint64_t m_wait_cycles[kKinds]{};
    std::uint32_t m_awaited[kKinds]{};

    void observe_wait(WaitKind kind, std::uint64_t sample) noexcept {
        std::uint64_t& estimate = m_wait_cycles[static_cast<unsigned>(kind)];
        if (sample > estimate) {
            estimate = sample;
        } else {
            estimate -= (estimate - sample) >> 3;
        }
    }

    // How long the soonest expected completion should take: the largest
    // estimate among the kinds this worker is waiting on.
    [[nodiscard]] std::uint64_t expected_wait_cycles() const noexcept {
        std::uint64_t expected = 0;
        for (unsigned kind = 0; kind < kKinds; ++kind) {
            if (m_awaited[kind] != 0) expected = std::max(expected, m_wait_cycles[kind]);
        }
        return expected != 0 ? expected
                             : m_wait_cycles[static_cast<unsigned>(WaitKind::Send)];
    }

    void issued(IoCompletion* completion) noexcept {
        ++m_in_flight;
        completion->m_done = false;
        if (completion->m_task != nullptr) {
            completion->m_issued = tsc_now();
            ++m_awaited[static_cast<unsigned>(completion->m_kind)];
        } else {
            completion->m_issued = 0;
        }
    }

    /* Completion records of sends nobody awaits.  The worker owns them, so a
    connection that goes away with sends in flight orphans its records instead
    of waiting for them; the reaper takes an orphan back when its CQE comes.
    Slots are carved from the global root a slab at a time and reused LIFO. */
    struct SendSlot {
        IoCompletion m_completion;
        SendSlot* m_next = nullptr;
    };
    static_assert(offsetof(SendSlot, m_completion) == 0);
    static constexpr std::size_t kSendSlab = 256;
    SendSlot* m_send_free = nullptr;

    IoCompletion* acquire_send() {
        if (m_send_free == nullptr) {
            auto* slab = static_cast<SendSlot*>(mem::global_root().allocate(
                sizeof(SendSlot) * kSendSlab, alignof(SendSlot)));
            for (std::size_t i = 0; i < kSendSlab; ++i) {
                auto* slot = ::new (static_cast<void*>(slab + i)) SendSlot{};
                slot->m_next = m_send_free;
                m_send_free = slot;
            }
        }
        SendSlot* slot = m_send_free;
        m_send_free = slot->m_next;
        slot->m_completion = IoCompletion{};
        slot->m_completion.m_kind = WaitKind::Send;
        return &slot->m_completion;
    }

    void release_send(IoCompletion* completion) noexcept {
        auto* slot = reinterpret_cast<SendSlot*>(completion);
        slot->m_next = m_send_free;
        m_send_free = slot;
    }

    /* Take every CQE that is ready (user space only, no syscall) and resume
    each waiting connection as its result is reaped, so it prepares its next
    request right away; the next submit sends the whole batch.  The CQ is
    advanced before any task runs. */
    bool reap() noexcept {
        SB_LAYER(Reap);
        constexpr unsigned kBatch = 256;
        io_uring_cqe* cqes[kBatch];
        PromiseBase* tasks[kBatch];
        io_uring& ring = m_ring->native_ring();
        bool any = false;
        for (;;) {
            const unsigned count = io_uring_peek_batch_cqe(&ring, cqes, kBatch);
            if (count == 0) break;
            unsigned runnable = 0;
            std::uint64_t now = 0;
            for (unsigned i = 0; i < count; ++i) {
                auto* completion = static_cast<IoCompletion*>(io_uring_cqe_get_data(cqes[i]));
                if (completion == nullptr) continue;
                completion->m_result = cqes[i]->res;
                completion->m_done = true;
                --m_in_flight;
                if (completion->m_issued != 0) {
                    if (now == 0) now = tsc_now();
                    observe_wait(completion->m_kind, now - completion->m_issued);
                    --m_awaited[static_cast<unsigned>(completion->m_kind)];
                    completion->m_issued = 0;
                }
                if (completion->m_orphan) {
                    release_send(completion);
                    continue;
                }
                if (completion->m_task != nullptr) tasks[runnable++] = completion->m_task;
            }
            io_uring_cq_advance(&ring, count);
            SB_COUNT(Cqes, count);
            m_runtime->m_progress.add_local(m_id, count);
            any = true;
            for (unsigned i = 0; i < runnable; ++i) resume(tasks[i]);
        }
        return any;
    }

    // One non-blocking kernel entry (only when there is something to submit or
    // collect), then reap.
    bool poll_io() noexcept {
        if (m_ring->queued() != 0 || m_ring->task_work_pending()) m_ring->submit_and_collect();
        return reap();
    }

    [[nodiscard]] bool have_work() const noexcept {
        return m_ready_head != nullptr ||
               m_injected.load(std::memory_order_acquire) != nullptr ||
               m_exiting.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool timer_due(Clock::time_point now) const noexcept {
        return m_timer_head != nullptr && m_timer_head->m_wake_at <= now;
    }

    void signal_wake() noexcept {
        m_wake_word.fetch_add(1, std::memory_order_seq_cst);
        (void)::syscall(SYS_futex, reinterpret_cast<std::uint32_t*>(&m_wake_word),
                        FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
    }

    void push_ready(PromiseBase* task) noexcept {
        append_ready(m_ready_head, m_ready_tail, task);
    }

    PromiseBase* pop_ready() noexcept {
        PromiseBase* task = m_ready_head;
        if (task != nullptr) {
            m_ready_head = task->m_next_ready;
            if (m_ready_head == nullptr) m_ready_tail = nullptr;
            task->m_next_ready = nullptr;
            task->m_in_ready = false;
        }
        return task;
    }

    void wake_if_waiting() noexcept {
        if (m_waiting.load(std::memory_order_seq_cst)) signal_wake();
    }

    void push_injected(PromiseBase* task, bool wake = true) noexcept {
        PromiseBase* head = m_injected.load(std::memory_order_relaxed);
        do {
            if (m_exiting.load(std::memory_order_relaxed)) return;
            task->m_next_ready = head;
        } while (!m_injected.compare_exchange_weak(
            head, task, std::memory_order_seq_cst, std::memory_order_relaxed));
        // The stack publish and the `waiting` check must both be seq_cst: a
        // release/acquire pair still permits the store-buffer interleaving
        // where the worker stores waiting=true and loads the empty stack while
        // the producer pushes and loads waiting=false.  That loses the wakeup
        // and parks the worker forever with the task sitting in the stack.
        if (wake) wake_if_waiting();
    }

    void push_completed(PromiseBase* task) noexcept {
        task->m_next_completed = m_completed_head;
        m_completed_head = task;
    }

    void push_deferred(PromiseBase* task) noexcept {
        task->m_next_deferred = m_deferred_head;
        m_deferred_head = task;
    }

    void process_deferred() noexcept {
        PromiseBase* task = m_deferred_head;
        m_deferred_head = nullptr;
        while (task != nullptr) {
            PromiseBase* next = task->m_next_deferred;
            task->m_next_deferred = nullptr;
            task->m_self.destroy();
            task = next;
        }
    }


    void add_timer(PromiseBase* task) noexcept {
        task->m_timer_queued = true;
        PromiseBase** link = &m_timer_head;
        while (*link != nullptr && (*link)->m_wake_at <= task->m_wake_at) {
            link = &(*link)->m_next_timer;
        }
        task->m_next_timer = *link;
        *link = task;
    }

    void activate_due_timers() noexcept {
        const auto now = Clock::now();
        while (m_timer_head != nullptr && m_timer_head->m_wake_at <= now) {
            PromiseBase* task = m_timer_head;
            m_timer_head = task->m_next_timer;
            task->m_next_timer = nullptr;
            task->m_timer_queued = false;
            push_ready(task);
        }
    }

    void drain_injected() noexcept {
        PromiseBase* list = m_injected.exchange(nullptr, std::memory_order_acquire);
        while (list != nullptr) {
            PromiseBase* next = list->m_next_ready;
            push_ready(list);
            list = next;
        }
    }

    void process_completed() noexcept {
        PromiseBase* task = m_completed_head;
        m_completed_head = nullptr;
        while (task != nullptr) {
            PromiseBase* next = task->m_next_completed;
            task->m_next_completed = nullptr;
            task->m_self.destroy();
            m_runtime->finish_task();
            task = next;
        }
    }

    void resume(PromiseBase* task) noexcept {
        SB_LAYER(Coroutine);
        SB_COUNT(Resumes, 1);
        detail::tls_worker = this;
        detail::tls_current = task;
        task->m_self.resume();
        detail::tls_current = nullptr;
        m_runtime->m_resumes.add_local(m_id, 1);
    }

    /* Nothing is runnable and every connection this worker owns is in
    flight.  The scheduler spins for as long as the waits it is in should
    take (expected_wait_cycles), then blocks in one kernel wait until a
    completion, the next timer or a stall-check interval.  With NAPI the spin
    is the kernel's busy poll of the sockets' NIC queues (so a reply is
    processed on this CPU, not in an interrupt elsewhere); its length is
    re-registered as the estimate moves.  Without it the spin polls the CQ
    and the task-work flag from user space. */
    void wait_io() noexcept {
        const std::uint64_t budget = expected_wait_cycles();
        if (m_ring->napi_enabled()) {
            const auto busy_us = static_cast<std::uint32_t>(
                std::clamp<std::uint64_t>(cycles_to_ns(budget) / 1000, 1, 1'000'000));
            /* Re-register only when the learned wait moved by over a quarter. */
            if (busy_us > m_napi_us + m_napi_us / 4 || busy_us + busy_us / 4 < m_napi_us) {
                if (m_ring->enable_napi(busy_us)) m_napi_us = busy_us;
            }
        } else {
            const std::uint64_t start = tsc_now();
            for (std::uint32_t spins = 1;; ++spins) {
                if (poll_io() || have_work()) return;
                if ((spins & 63) == 0) {
                    const std::uint64_t now = tsc_now();
                    if (now - start >= budget) break;
                    if (timer_due(Clock::now())) return;
                }
                spin_pause();
            }
        }
        auto limit = std::chrono::nanoseconds(std::chrono::milliseconds(50));
        if (m_timer_head != nullptr) {
            const auto now = Clock::now();
            if (timer_due(now)) return;
            limit = std::min(limit, std::chrono::duration_cast<std::chrono::nanoseconds>(
                                        m_timer_head->m_wake_at - now));
        }
        __kernel_timespec timeout{.tv_sec = static_cast<long long>(limit.count() / 1'000'000'000),
                                  .tv_nsec = static_cast<long long>(limit.count() % 1'000'000'000)};
        m_ring->submit_and_wait(&timeout);
        if (!poll_io()) check_stall();
    }

    /* Idle (nothing in flight, nothing ready): one worker per NUMA node may
    spin, for as long as a reply is expected to take, in case work arrives;
    the others go straight to their futex. */
    bool spin_idle() noexcept {
        auto& spinner = m_runtime->m_idle_spinners[static_cast<std::size_t>(
            std::max(0, m_numa_node)) % m_runtime->m_idle_spinners.size()].m_worker;
        std::int32_t none = -1;
        if (!spinner.compare_exchange_strong(none, static_cast<std::int32_t>(m_id),
                                             std::memory_order_acq_rel)) {
            return false;
        }
        const std::uint64_t budget = m_wait_cycles[static_cast<unsigned>(WaitKind::Reply)];
        const std::uint64_t start = tsc_now();
        bool woke = false;
        for (std::uint32_t spins = 1;; ++spins) {
            if (have_work()) {
                woke = true;
                break;
            }
            if ((spins & 63) == 0) {
                if (tsc_now() - start >= budget) break;
                if (timer_due(Clock::now())) {
                    woke = true;
                    break;
                }
            }
            spin_pause();
        }
        spinner.store(-1, std::memory_order_release);
        return woke;
    }

    /* Nothing in flight and nothing ready: only a new task, a timer or exit
    can give this worker work.  Futex wait, no spin. */
    void wait_idle() {
        if (spin_idle()) return;
        const std::uint32_t seen = m_wake_word.load(std::memory_order_seq_cst);
        m_waiting.store(true, std::memory_order_seq_cst);
        if (m_injected.load(std::memory_order_seq_cst) != nullptr ||
            m_exiting.load(std::memory_order_acquire)) {
            m_waiting.store(false, std::memory_order_seq_cst);
            return;
        }

        timespec timeout{};
        timespec* wait_for = nullptr;
        const bool watch = m_runtime->m_stall_timeout > std::chrono::milliseconds(0) &&
                           !m_runtime->stall_detected() &&
                           m_runtime->m_outstanding.sum(m_runtime->m_worker_count) > 0;
        if (watch) {
            const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                m_runtime->m_stall_timeout).count();
            timeout.tv_sec = static_cast<time_t>(ns / 1'000'000'000);
            timeout.tv_nsec = static_cast<long>(ns % 1'000'000'000);
            wait_for = &timeout;
        }
        if (m_timer_head != nullptr) {
            const auto now = Clock::now();
            if (timer_due(now)) {
                m_waiting.store(false, std::memory_order_seq_cst);
                return;
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(
                m_timer_head->m_wake_at - now);
            timeout.tv_sec = static_cast<time_t>(remaining.count() / 1'000'000'000);
            timeout.tv_nsec = static_cast<long>(remaining.count() % 1'000'000'000);
            wait_for = &timeout;
        }

        {
            SB_LAYER(Idle);
            (void)::syscall(SYS_futex, reinterpret_cast<std::uint32_t*>(&m_wake_word),
                            FUTEX_WAIT_PRIVATE, seen, wait_for, nullptr, 0);
        }
        m_waiting.store(false, std::memory_order_release);
        if (watch) check_stall();
    }

    void request_exit() noexcept {
        m_exiting.store(true, std::memory_order_release);
        signal_wake();
    }

    void run() noexcept {
        detail::tls_worker = this;
        mem::set_thread_frame_node(m_numa_node >= 0 ? m_numa_node : 0);
        if (m_numa_node >= 0) {
            (void)numa::set_preferred(m_numa_node);
        }
        set_thread_name(m_numa_node >= 0 ? "sb-worker-n" : "sb-worker");
        if (m_runtime->m_config.m_pin_workers) {
            if (m_numa_node >= 0) {
                const numa::Topology& topology = numa::Topology::instance();
                const numa::NodeInfo* info = topology.node_by_id(m_numa_node);
                if (info != nullptr) {
                    cpu_set_t set;
                    CPU_ZERO(&set);
                    for (std::uint32_t slot = 0; slot < info->m_cpu_count; ++slot) {
                        const int candidate = static_cast<int>(info->m_cpus[slot]);
                        if (candidate != topology.reserved_cpu()) {
                            CPU_SET(candidate, &set);
                        }
                    }
                    (void)::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set);
                }
            } else if (m_cpu >= 0) {
                set_thread_affinity(static_cast<std::uint32_t>(m_cpu));
            } else {
                set_thread_affinity(m_runtime->m_config.m_first_cpu + m_id);
            }
        }

        layer::register_thread();
        m_ring.emplace(m_runtime->m_config.m_ring_entries, IoRing::worker_flags());
        for (auto& estimate : m_wait_cycles) estimate = ns_to_cycles(kInitialWaitNs);
        if (m_runtime->m_config.m_busy_poll) {
            m_napi_us = static_cast<std::uint32_t>(kInitialWaitNs / 1000);
            if (!m_ring->enable_napi(m_napi_us)) m_napi_us = 0;
        }

        for (;;) {
            drain_injected();
            activate_due_timers();

            /* Run every runnable task; each queues its SQEs in the ring.  A
            long ready list is cut into batches so completions keep flowing. */
            for (std::uint32_t ran = 0; ran < 512; ++ran) {
                PromiseBase* task = pop_ready();
                if (task == nullptr) break;
                resume(task);
            }
            process_completed();
            process_deferred();

            /* Submit the batch and reap whatever has completed, resuming each
            connection as its result comes in: no wait. */
            (void)poll_io();
            /* Tasks resumed while reaping may have finished: retire them now,
            or a worker could go idle holding the last outstanding task. */
            process_completed();
            process_deferred();
            if (m_ready_head != nullptr) continue;
            if (m_injected.load(std::memory_order_acquire) != nullptr) continue;
            if (m_exiting.load(std::memory_order_acquire)) break;
            if (timer_due(Clock::now())) continue;

            if (m_in_flight != 0) {
                wait_io();
            } else {
                wait_idle();
            }
        }

        drain_injected();
        process_completed();
        process_deferred();
        m_ring.reset();
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
    if (config.m_workers == 0) config.m_workers = hardware_workers();
    if (config.m_workers == 0) config.m_workers = 1;
    if (config.m_workers > kMaxWorkers) {
        std::println(stderr, "sysbench: worker count capped at {} (requested {})",
                     kMaxWorkers, config.m_workers);
        config.m_workers = kMaxWorkers;
    }
    if (config.m_worker_heap_bytes < 1024 * 1024) config.m_worker_heap_bytes = 1024 * 1024;
    m_config = config;
    m_worker_count = config.m_workers;
    calibrate_tsc();
    m_stall_timeout = config.m_stall_timeout;
    m_stall_detected.store(false, std::memory_order_release);

    m_worker_storage =
        mem::global_root().allocate(sizeof(Worker) * m_worker_count, alignof(Worker));
    m_workers = static_cast<Worker*>(m_worker_storage);

    const numa::Topology& topology = numa::Topology::instance();
    const bool use_numa = m_config.m_numa_aware && topology.available();

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
            {
                std::uint32_t on_node = 0;
                for (std::uint32_t other = 0; other < constructed; ++other)
                    if (m_workers[other].m_numa_node == node) ++on_node;
                m_workers[constructed].m_node_index = on_node;
            }
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
        throw;
    }

    std::uint32_t started = 0;
    try {
        for (; started < m_worker_count; ++started) {
            if (use_numa && m_workers[started].m_numa_node >= 0) {
                (void)numa::set_preferred(m_workers[started].m_numa_node);
            }
            m_workers[started].m_thread = std::thread([worker = &m_workers[started]] {
                worker->run();
            });
            if (use_numa) (void)numa::set_preferred(topology.home_node());
        }
    } catch (...) {
        for (std::uint32_t i = 0; i < started; ++i) m_workers[i].request_exit();
        for (std::uint32_t i = 0; i < started; ++i) {
            if (m_workers[i].m_thread.joinable()) m_workers[i].m_thread.join();
        }
        for (std::uint32_t i = m_worker_count; i > 0; --i) m_workers[i - 1].~Worker();
        mem::global_root().deallocate(m_worker_storage,
                                      sizeof(Worker) * m_worker_count,
                                      alignof(Worker));
        m_workers = nullptr;
        m_worker_storage = nullptr;
        m_worker_count = 0;
        throw;
    }

    m_running.store(true, std::memory_order_release);
}


void Runtime::shutdown() {
    if (m_workers == nullptr) {
        return;
    }
    m_running.store(false, std::memory_order_release);
    for (std::uint32_t i = 0; i < m_worker_count; ++i) m_workers[i].request_exit();
    for (std::uint32_t i = 0; i < m_worker_count; ++i) {
        if (m_workers[i].m_thread.joinable()) m_workers[i].m_thread.join();
    }
    for (std::uint32_t i = m_worker_count; i > 0; --i) m_workers[i - 1].~Worker();
    mem::global_root().deallocate(m_worker_storage,
                                  sizeof(Worker) * m_worker_count,
                                  alignof(Worker));
    m_workers = nullptr;
    m_worker_storage = nullptr;
    m_worker_count = 0;

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
    promise->m_on_complete = &complete_top_level;
    promise->m_started = true;
    promise->m_owner_worker = worker;
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
    return m_workers[worker].m_numa_node;
}


IoRing* Runtime::current_ring() noexcept {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    return worker != nullptr && worker->m_ring ? &*worker->m_ring : nullptr;
}

void Runtime::io_issued(IoCompletion* completion) noexcept {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    if (worker == nullptr) detail::no_runtime();
    worker->issued(completion);
}

IoCompletion* Runtime::acquire_send_completion() {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    if (worker == nullptr) detail::no_runtime();
    return worker->acquire_send();
}

void Runtime::release_send_completion(IoCompletion* completion) noexcept {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    if (worker == nullptr) detail::no_runtime();
    worker->release_send(completion);
}

void Runtime::finish_task() noexcept {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    const std::uint32_t worker_id = worker != nullptr ? worker->m_id : 0;
    m_outstanding.add(worker_id, -1);
    m_progress.add(worker_id, 1);
    const std::uint32_t active = m_active_tasks.fetch_sub(1, std::memory_order_relaxed) - 1;
    m_undersubscribed.store(active <= m_worker_count, std::memory_order_relaxed);
    if (m_outstanding.sum(m_worker_count) == 0) {
        m_wait_word.fetch_add(1, std::memory_order_seq_cst);
        (void)::syscall(SYS_futex, reinterpret_cast<std::uint32_t*>(&m_wait_word),
                        FUTEX_WAKE_PRIVATE, INT32_MAX, nullptr, nullptr, 0);
    }
}

void Runtime::declare_stall(std::int64_t outstanding,
                            std::chrono::steady_clock::duration stalled) noexcept {
    if (m_stall_detected.exchange(true, std::memory_order_acq_rel)) return;
    const auto stalled_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(stalled).count();
    if (stalled_ms >= 1000) {
        std::println(stderr,
                     "sysbench: no progress for {}s with {} task(s) "
                     "outstanding; a server connection appears to have stalled",
                     stalled_ms / 1000, outstanding);
    } else {
        std::println(stderr,
                     "sysbench: no progress for {}ms with {} task(s) "
                     "outstanding; a server connection appears to have stalled",
                     stalled_ms, outstanding);
    }
    m_wait_word.fetch_add(1, std::memory_order_seq_cst);
    (void)::syscall(SYS_futex, reinterpret_cast<std::uint32_t*>(&m_wait_word),
                    FUTEX_WAKE_PRIVATE, INT32_MAX, nullptr, nullptr, 0);
}

/* The caller only blocks: the workers detect a stall and finish_task() wakes
it when the last task ends.  No polling and no coordination here. */
bool Runtime::wait() {
    for (;;) {
        if (m_workers == nullptr) return true;
        const std::uint32_t seen = m_wait_word.load(std::memory_order_seq_cst);
        if (m_outstanding.sum(m_worker_count) <= 0) return true;
        if (stall_detected()) return false;
        (void)::syscall(SYS_futex, reinterpret_cast<std::uint32_t*>(&m_wait_word),
                        FUTEX_WAIT_PRIVATE, seen, nullptr, nullptr, 0);
    }
}

bool Runtime::wait_for(std::chrono::nanoseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    for (;;) {
        if (m_workers == nullptr) return true;
        const std::uint32_t seen = m_wait_word.load(std::memory_order_seq_cst);
        if (m_outstanding.sum(m_worker_count) <= 0 || stall_detected()) return true;
        const auto now = Clock::now();
        if (now >= deadline) return false;
        const auto left = std::chrono::duration_cast<std::chrono::nanoseconds>(deadline - now);
        timespec slice{static_cast<time_t>(left.count() / 1'000'000'000),
                       static_cast<long>(left.count() % 1'000'000'000)};
        (void)::syscall(SYS_futex, reinterpret_cast<std::uint32_t*>(&m_wait_word),
                        FUTEX_WAIT_PRIVATE, seen, &slice, nullptr, 0);
    }
}

mem::MemoryResource& Runtime::worker_allocator(std::uint32_t worker) {
    if (m_workers == nullptr || worker >= m_worker_count) {
        throw std::out_of_range("worker id");
    }
    return m_workers[worker].m_heap;
}

mem::ResourceStats Runtime::worker_heap_stats(std::uint32_t worker) const {
    if (m_workers == nullptr || worker >= m_worker_count) {
        throw std::out_of_range("worker id");
    }
    return m_workers[worker].m_heap.stats();
}

Runtime* Runtime::current() noexcept {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    return worker != nullptr ? worker->m_runtime : nullptr;
}

std::uint32_t Runtime::current_worker_id() noexcept {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    return worker != nullptr ? worker->m_id : 0;
}

mem::MemoryResource& Runtime::current_allocator() {
    auto* worker = static_cast<Worker*>(detail::tls_worker);
    if (worker == nullptr) throw std::logic_error("not running on a runtime worker");
    return worker->m_heap;
}



std::uint64_t Runtime::resumes() const noexcept {
    return m_resumes.sum(m_worker_count);
}

std::uint32_t Runtime::numa_node_count() const noexcept {
    return numa::Topology::instance().node_count();
}

std::uint32_t Runtime::worker_node_index(std::uint32_t worker) const {
    if (m_workers == nullptr || worker >= m_worker_count) {
        throw std::out_of_range("worker id");
    }
    return m_workers[worker].m_node_index;
}

int Runtime::worker_numa_node(std::uint32_t worker) const {
    if (m_workers == nullptr || worker >= m_worker_count) {
        throw std::out_of_range("worker id");
    }
    return m_workers[worker].m_numa_node;
}

int Runtime::worker_cpu(std::uint32_t worker) const {
    if (m_workers == nullptr || worker >= m_worker_count) {
        throw std::out_of_range("worker id");
    }
    return m_workers[worker].m_cpu;
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
    task->m_wake_at = std::chrono::steady_clock::now() + duration;
    worker->add_timer(task);
}

void defer_destroy(PromiseBase* task) noexcept {
    auto* worker = static_cast<Runtime::Worker*>(tls_worker);
    if (worker == nullptr) {
        task->m_self.destroy();
        return;
    }
    worker->push_deferred(task);
}

void no_runtime() {
    std::abort();
}

}  // namespace sb::detail
