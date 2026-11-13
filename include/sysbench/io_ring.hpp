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

// io_uring transport primitives.  Every socket read and write uses a buffer
// registered with IORING_REGISTER_BUFFERS and m_IORING_OP{READ,WRITE}_FIXED.
// Each runtime worker owns one ring; buffers are thread-affine and pooled.

#include "sysbench/memory.hpp"
#include "sysbench/task.hpp"

#include <liburing.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>

namespace sb {

class IoRing;

struct IoCompletion {
    detail::PromiseBase* task = nullptr;
    int result = 0;
};

class IoRing {
public:
    static constexpr unsigned kMaxRegisteredBuffers = 256;

    explicit IoRing(unsigned entries);
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
    std::atomic<unsigned> m_inflight{0};
    std::uint64_t m_fixed_operations = 0;
    std::uint64_t m_submissions = 0;
    int m_last_error = 0;
    bool m_initialized = false;

    friend class RegisteredBufferPool;
};

class RegisteredBufferLease;

class RegisteredBufferPool {
public:
    RegisteredBufferPool(IoRing& ring, mem::MemoryResource& memory,
                         unsigned slot_size, unsigned slot_count,
                         int numa_node = -1);
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
        void* data = nullptr;
        bool in_use = false;
    };

    IoRing* m_ring;
    mem::MemoryResource* m_memory;
    void* m_base = nullptr;
    unsigned m_slot_size = 0;
    unsigned m_slot_count = 0;
    unsigned m_free_count = 0;
    std::array<Slot, IoRing::kMaxRegisteredBuffers> m_slots{};
    std::array<unsigned, IoRing::kMaxRegisteredBuffers> m_free_stack{};
    std::array<iovec, IoRing::kMaxRegisteredBuffers> m_iovecs{};
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
        return m_pool != nullptr ? m_pool->m_slots[m_slot].data : nullptr;
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
