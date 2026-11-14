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

// io_uring transport primitives.  Each runtime worker owns one ring, created
// on the worker's thread (single issuer, deferred task work): only that worker
// submits to it and reaps it.  Socket buffers are thread-affine and pooled.

#include "sysbench/memory.hpp"
#include "sysbench/task.hpp"

#include <liburing.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>

namespace sb {

class IoRing;

// The user data of every SQE.  The owning worker reaps the CQE, stores the
// result, sets m_done and resumes m_task (if any).
// What a suspended task is waiting for.  An awaiter declares it (the task's
// "outcome": waiting on I/O of this kind); the scheduler times each awaited
// wait and learns, per kind, how long such waits take, which is what its
// spins are sized by.
enum class WaitKind : std::uint8_t { Reply, Send, Connect, Count };

struct IoCompletion {
    detail::PromiseBase* m_task = nullptr;
    int m_result = 0;
    bool m_done = false;
    WaitKind m_kind = WaitKind::Reply;
    // TSC when an awaited request was issued (0: not awaited at issue, so
    // not timed).
    std::uint64_t m_issued = 0;
    // A worker-owned send completion whose connection has gone: the reaper
    // returns it to the worker's pool when its CQE arrives.
    bool m_orphan = false;
};

class IoRing {
public:
    static constexpr unsigned kMaxRegisteredBuffers = 256;

    explicit IoRing(unsigned entries, unsigned setup_flags = 0);
    // The ring of a runtime worker: single issuer with deferred task work, so
    // completions are only processed when the owning thread enters the kernel,
    // and IORING_SQ_TASKRUN tells it when there is work to collect.  Falls back
    // to cooperative task work on kernels without deferred task work.
    [[nodiscard]] static unsigned worker_flags() noexcept {
        return IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN |
               IORING_SETUP_TASKRUN_FLAG;
    }
    // True when the kernel has completion work for the owning thread to run
    // (IORING_SETUP_TASKRUN_FLAG).  Read from user space: no syscall.
    [[nodiscard]] bool task_work_pending() const noexcept {
        return (__atomic_load_n(m_ring.sq.kflags, __ATOMIC_ACQUIRE) & IORING_SQ_TASKRUN) != 0;
    }
    // Submit the queued SQEs and run completion work, without waiting: one
    // kernel entry.  Afterwards the CQ holds every completion that is ready.
    void submit_and_collect() noexcept;
    // Submit, then block until at least one CQE is ready or `timeout` passes.
    // With NAPI busy polling enabled the kernel first polls the NIC queues of
    // this ring's sockets from the waiting thread.
    void submit_and_wait(__kernel_timespec* timeout) noexcept;
    // Busy-poll the NIC (NAPI) for up to `busy_poll_us` while waiting for
    // completions, as a blocking recv() does with net.core.busy_read: the
    // reply is then processed by the TCP stack on this thread's CPU instead of
    // in the interrupt handler on another one, so the socket and its buffers
    // stay in this CPU's cache.  False if the kernel lacks it.
    bool enable_napi(unsigned busy_poll_us) noexcept;
    [[nodiscard]] bool napi_enabled() const noexcept { return m_napi; }
    // SQEs prepared and not yet submitted.
    [[nodiscard]] unsigned queued() const noexcept { return m_queued; }
    // Number of submissions so far: an SQE prepared after the last one is
    // still in the SQ and can be changed (linked).
    [[nodiscard]] std::uint64_t submit_epoch() const noexcept { return m_submit_epoch; }
    [[nodiscard]] io_uring_sqe* last_sqe() const noexcept { return m_last_sqe; }
    ~IoRing();

    IoRing(const IoRing&) = delete;
    IoRing& operator=(const IoRing&) = delete;

    [[nodiscard]] bool valid() const noexcept { return m_initialized; }
    [[nodiscard]] unsigned entries() const noexcept { return m_entries; }
    [[nodiscard]] unsigned registered_buffers() const noexcept {
        return m_registered_buffers;
    }
    [[nodiscard]] unsigned pending() const noexcept {
        return m_inflight.load(std::memory_order_relaxed) + m_queued;
    }
    [[nodiscard]] unsigned inflight() const noexcept {
        return m_inflight.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t fixed_operations() const noexcept {
        return m_fixed_operations;
    }
    [[nodiscard]] std::uint64_t submissions() const noexcept { return m_submissions; }

    void register_buffers(std::span<const iovec> buffers);

    // All operations return true when an SQE was queued.  On false the caller
    // schedules its coroutine with the error stored in completion->result.
    bool submit_read_fixed(int fd, void* buffer, unsigned length, int slot,
                           IoCompletion* completion) noexcept;
    bool submit_write_fixed(int fd, const void* buffer, unsigned length, int slot,
                            IoCompletion* completion) noexcept;
    bool submit_connect(int fd, const sockaddr* address, socklen_t length,
                        IoCompletion* completion) noexcept;
    bool submit_accept(int fd, sockaddr* address, socklen_t* length,
                       IoCompletion* completion) noexcept;
    bool submit_recv(int fd, void* buffer, unsigned length, int flags,
                     IoCompletion* completion) noexcept;
    bool submit_send(int fd, const void* buffer, unsigned length, int flags,
                     IoCompletion* completion) noexcept;

    // Returns the number of SQEs handed to the kernel in this call.
    bool submit_nop(IoCompletion* completion) noexcept;
    // Chain the most recently prepared SQE to the next one (IOSQE_IO_LINK).
    void link_last() noexcept {
        if (m_last_sqe != nullptr) m_last_sqe->flags |= IOSQE_IO_LINK;
    }
    // A send that is not awaited: MSG_WAITALL sends all of it or fails.  Its
    // CQE only marks the completion done (the buffer is free again).
    bool submit_send_posted(int fd, const void* buffer, unsigned length,
                            IoCompletion* completion) noexcept;
    int flush_submit() noexcept;

    [[nodiscard]] io_uring_cqe* peek_completion() noexcept;
    void mark_seen(io_uring_cqe* completion) noexcept;

    // timeout==nullptr blocks until a completion.  Returns -ETIME on timeout.
    int wait_completion(__kernel_timespec* timeout) noexcept;

    [[nodiscard]] int last_error() const noexcept { return m_last_error; }
    [[nodiscard]] io_uring& native_ring() noexcept { return m_ring; }

private:
    io_uring_sqe* acquire_sqe(IoCompletion* completion) noexcept;
    void fail(IoCompletion* completion, int error) noexcept;

    io_uring m_ring{};
    unsigned m_entries = 0;
    unsigned m_registered_buffers = 0;
    unsigned m_queued = 0;
    unsigned m_features = 0;
    io_uring_sqe* m_last_sqe = nullptr;
    std::uint64_t m_submit_epoch = 0;
    std::atomic<unsigned> m_inflight{0};
    std::uint64_t m_fixed_operations = 0;
    std::uint64_t m_submissions = 0;
    int m_last_error = 0;
    bool m_initialized = false;
    bool m_napi = false;

    friend class RegisteredBufferPool;
};

class RegisteredBufferLease;

class RegisteredBufferPool {
public:
    RegisteredBufferPool(IoRing& ring, mem::MemoryResource& memory,
                         unsigned slot_size, unsigned slot_count,
                         int numa_node = -1);
    // Node-local slots that are not registered with any ring (the per-node
    // rings issue plain send/recv on them).
    RegisteredBufferPool(mem::MemoryResource& memory, unsigned slot_size,
                         unsigned slot_count, int numa_node = -1);
    ~RegisteredBufferPool();

    RegisteredBufferPool(const RegisteredBufferPool&) = delete;
    RegisteredBufferPool& operator=(const RegisteredBufferPool&) = delete;

    [[nodiscard]] unsigned slot_size() const noexcept { return m_slot_size; }
    [[nodiscard]] unsigned slot_count() const noexcept { return m_slot_count; }
    [[nodiscard]] unsigned available() const noexcept { return m_free_count; }
    [[nodiscard]] std::size_t bytes() const noexcept {
        return static_cast<std::size_t>(m_slot_size) * m_slot_count;
    }

    [[nodiscard]] RegisteredBufferLease acquire();
    [[nodiscard]] bool owns(const void* ptr) const noexcept;
    [[nodiscard]] int numa_node() const noexcept { return m_numa_node; }
    [[nodiscard]] bool numa_bound() const noexcept { return m_numa_bound; }

    void release(unsigned slot) noexcept;

private:
    friend class RegisteredBufferLease;

    struct Slot {
        void* m_data = nullptr;
        bool m_in_use = false;
    };

    IoRing* m_ring;
    mem::MemoryResource* m_memory;
    void* m_base = nullptr;
    unsigned m_slot_size = 0;
    unsigned m_slot_count = 0;
    unsigned m_free_count = 0;
    // Sized to the slot count, not to the ring's registration limit.
    Slot* m_slots = nullptr;
    unsigned* m_free_stack = nullptr;
    iovec* m_iovecs = nullptr;
    int m_numa_node = -1;
    bool m_numa_bound = false;
};

class RegisteredBufferLease {
public:
    RegisteredBufferLease() noexcept = default;
    RegisteredBufferLease(RegisteredBufferPool* pool, unsigned slot) noexcept
        : m_pool(pool), m_slot(slot) {}
    RegisteredBufferLease(RegisteredBufferLease&& other) noexcept
        : m_pool(std::exchange(other.m_pool, nullptr)), m_slot(other.m_slot) {}
    RegisteredBufferLease& operator=(RegisteredBufferLease&& other) noexcept {
        if (this != &other) {
            reset();
            m_pool = std::exchange(other.m_pool, nullptr);
            m_slot = other.m_slot;
        }
        return *this;
    }
    RegisteredBufferLease(const RegisteredBufferLease&) = delete;
    RegisteredBufferLease& operator=(const RegisteredBufferLease&) = delete;
    ~RegisteredBufferLease() { reset(); }

    [[nodiscard]] bool valid() const noexcept { return m_pool != nullptr; }
    [[nodiscard]] void* data() const noexcept {
        return m_pool != nullptr ? m_pool->m_slots[m_slot].m_data : nullptr;
    }
    [[nodiscard]] unsigned size() const noexcept {
        return m_pool != nullptr ? m_pool->m_slot_size : 0;
    }
    [[nodiscard]] int index() const noexcept { return static_cast<int>(m_slot); }

    void reset() noexcept {
        if (m_pool != nullptr) {
            m_pool->release(m_slot);
            m_pool = nullptr;
        }
    }

private:
    RegisteredBufferPool* m_pool = nullptr;
    unsigned m_slot = 0;
};

}  // namespace sb
