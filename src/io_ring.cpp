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

#include "sysbench/io_ring.hpp"
#include "sysbench/numa.hpp"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace sb {
namespace {

[[nodiscard]] constexpr std::size_t round_up(std::size_t value,
                                             std::size_t alignment) noexcept {
    return (value + alignment - 1) & ~(alignment - 1);
}

constexpr unsigned kPageSize = 4096;
constexpr unsigned kMaxSlots = IoRing::kMaxRegisteredBuffers;

}  // namespace

IoRing::IoRing(unsigned entries) : m_entries(entries) {
    if (entries == 0) throw std::invalid_argument("io_uring entries must be non-zero");

    // Deliberately no IORING_SETUP_COOP_TASKRUN.  That mode defers completion
    // notification to task work run in the submitting task's context.  Here the
    // submitter is a runtime worker that may be spinning in user space and the
    // waiter is a different thread polling the ring's CQ eventfd, so the
    // notification would stall until the submitter next entered the kernel --
    // turning every wait into a spin-budget-length delay.
    io_uring_params params{};
    const int result = io_uring_queue_init_params(entries, &m_ring, &params);
    if (result < 0) {
        throw std::system_error(-result, std::generic_category(),
                                "io_uring_queue_init_params");
    }
    m_initialized = true;
}

IoRing::~IoRing() {
    if (m_initialized) {
        io_uring_queue_exit(&m_ring);
        m_initialized = false;
    }
}

void IoRing::register_buffers(std::span<const iovec> buffers) {
    if (!m_initialized) throw std::logic_error("ring is not initialized");
    if (buffers.empty() || buffers.size() > kMaxSlots) {
        throw std::invalid_argument("invalid registered buffer count");
    }
    const int result = io_uring_register_buffers(&m_ring, buffers.data(),
                                                 static_cast<unsigned>(buffers.size()));
    if (result < 0) {
        throw std::system_error(-result, std::generic_category(),
                                "io_uring_register_buffers");
    }
    m_registered_buffers = static_cast<unsigned>(buffers.size());
}

io_uring_sqe* IoRing::acquire_sqe(IoCompletion* completion) noexcept {
    io_uring_sqe* sqe = io_uring_get_sqe(&m_ring);
    if (sqe == nullptr) {
        (void)flush_submit();
        sqe = io_uring_get_sqe(&m_ring);
    }
    if (sqe == nullptr) {
        completion->m_result = -ENOBUFS;
        m_last_error = ENOBUFS;
        return nullptr;
    }
    io_uring_sqe_set_data(sqe, completion);
    ++m_queued;
    return sqe;
}

bool IoRing::submit_read_fixed(int fd, void* buffer, unsigned length, int slot,
                               IoCompletion* completion) noexcept {
    if (slot < 0 || static_cast<unsigned>(slot) >= m_registered_buffers) {
        completion->m_result = -EINVAL;
        return false;
    }
    io_uring_sqe* sqe = acquire_sqe(completion);
    if (sqe == nullptr) return false;
    io_uring_prep_read_fixed(sqe, fd, buffer, length, 0, slot);
    ++m_fixed_operations;
    return true;
}

bool IoRing::submit_write_fixed(int fd, const void* buffer, unsigned length,
                                int slot, IoCompletion* completion) noexcept {
    if (slot < 0 || static_cast<unsigned>(slot) >= m_registered_buffers) {
        completion->m_result = -EINVAL;
        return false;
    }
    io_uring_sqe* sqe = acquire_sqe(completion);
    if (sqe == nullptr) return false;
    io_uring_prep_write_fixed(sqe, fd, buffer, length, 0, slot);
    ++m_fixed_operations;
    return true;
}

bool IoRing::submit_connect(int fd, const sockaddr* address, socklen_t length,
                            IoCompletion* completion) noexcept {
    io_uring_sqe* sqe = acquire_sqe(completion);
    if (sqe == nullptr) return false;
    io_uring_prep_connect(sqe, fd, address, length);
    return true;
}

bool IoRing::submit_accept(int fd, sockaddr* address, socklen_t* length,
                           IoCompletion* completion) noexcept {
    io_uring_sqe* sqe = acquire_sqe(completion);
    if (sqe == nullptr) return false;
    io_uring_prep_accept(sqe, fd, address, length, SOCK_NONBLOCK | SOCK_CLOEXEC);
    return true;
}

bool IoRing::submit_recv(int fd, void* buffer, unsigned length, int flags,
                         IoCompletion* completion) noexcept {
    io_uring_sqe* sqe = acquire_sqe(completion);
    if (sqe == nullptr) return false;
    io_uring_prep_recv(sqe, fd, buffer, length, flags);
    return true;
}

bool IoRing::submit_send(int fd, const void* buffer, unsigned length, int flags,
                         IoCompletion* completion) noexcept {
    io_uring_sqe* sqe = acquire_sqe(completion);
    if (sqe == nullptr) return false;
    io_uring_prep_send(sqe, fd, buffer, length, flags);
    return true;
}

int IoRing::flush_submit() noexcept {
    if (m_queued == 0) return 0;
    int total = 0;
    while (m_queued > 0) {
        const int submitted = io_uring_submit(&m_ring);
        if (submitted < 0) {
            m_last_error = -submitted;
            return total;
        }
        if (submitted == 0) break;
        const unsigned count = static_cast<unsigned>(submitted);
        m_queued = count >= m_queued ? 0 : m_queued - count;
        m_inflight.fetch_add(count, std::memory_order_relaxed);
        total += submitted;
        m_submissions += count;
    }
    return total;
}

io_uring_cqe* IoRing::peek_completion() noexcept {
    io_uring_cqe* completion = nullptr;
    if (io_uring_peek_cqe(&m_ring, &completion) != 0) return nullptr;
    return completion;
}

void IoRing::mark_seen(io_uring_cqe* completion) noexcept {
    io_uring_cqe_seen(&m_ring, completion);
    const unsigned previous = m_inflight.load(std::memory_order_relaxed);
    if (previous > 0) m_inflight.store(previous - 1, std::memory_order_relaxed);
}

int IoRing::wait_completion(__kernel_timespec* timeout) noexcept {
    io_uring_cqe* completion = nullptr;
    const int result = io_uring_wait_cqe_timeout(&m_ring, &completion, timeout);
    if (result < 0) m_last_error = -result;
    return result;
}

// ---------------------------------------------------------------------------
// RegisteredBufferPool
// ---------------------------------------------------------------------------

RegisteredBufferPool::RegisteredBufferPool(IoRing& ring, mem::MemoryResource& memory,
                                           unsigned slot_size, unsigned slot_count,
                                           int numa_node)
    : m_ring(&ring), m_memory(&memory), m_slot_size(slot_size), m_slot_count(slot_count),
      m_numa_node(numa_node) {
    if (m_slot_count == 0 || m_slot_count > kMaxSlots) {
        throw std::invalid_argument("invalid registered buffer slot count");
    }
    if (m_slot_size == 0) throw std::invalid_argument("invalid registered buffer size");

    const std::size_t stride = round_up(m_slot_size, kPageSize);
    const std::size_t total = stride * m_slot_count;
    m_base = m_memory->allocate(total, kPageSize);
    if (m_numa_node >= 0) {
        m_numa_bound = numa::bind_range(m_base, total, m_numa_node);
    }

    for (unsigned i = 0; i < m_slot_count; ++i) {
        auto* data = static_cast<std::byte*>(m_base) + (stride * i);
        m_slots[i].m_data = data;
        m_slots[i].m_in_use = false;
        m_iovecs[i].iov_base = data;
        m_iovecs[i].iov_len = m_slot_size;
        m_free_stack[i] = i;
    }
    m_free_count = m_slot_count;
    m_ring->register_buffers(std::span<const iovec>(m_iovecs.data(), m_slot_count));
}

RegisteredBufferPool::~RegisteredBufferPool() {
    // The ring unregisters buffers in io_uring_queue_exit().  The backing memory
    // belongs to the root arena and remains valid until process exit.
}

RegisteredBufferLease RegisteredBufferPool::acquire() {
    if (m_free_count == 0) return {};
    const unsigned slot = m_free_stack[--m_free_count];
    m_slots[slot].m_in_use = true;
    return RegisteredBufferLease(this, slot);
}

void RegisteredBufferPool::release(unsigned slot) noexcept {
    if (slot >= m_slot_count || !m_slots[slot].m_in_use) return;
    m_slots[slot].m_in_use = false;
    m_free_stack[m_free_count++] = slot;
}

bool RegisteredBufferPool::owns(const void* ptr) const noexcept {
    const auto address = reinterpret_cast<std::uintptr_t>(ptr);
    const auto begin = reinterpret_cast<std::uintptr_t>(m_base);
    return address >= begin && address < begin + bytes();
}

// ---------------------------------------------------------------------------
// Awaiters
// ---------------------------------------------------------------------------

}  // namespace sb
