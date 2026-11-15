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

#include "sysbench/socket_io.hpp"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <algorithm>
#include <system_error>

#include "sysbench/layer_stats.hpp"

namespace sb {
namespace {

bool enqueue_awaiter(SocketIo& io, IoRequest& request) noexcept {
    request.m_completion.m_task = detail::tls_current;
    if (request.m_completion.m_task == nullptr) detail::no_runtime();
    if (!io.enqueue(request)) {
        request.m_completion.m_result = -ENOBUFS;
        detail::schedule_current(request.m_completion.m_task);
        return false;
    }
    return true;
}

[[nodiscard]] WaitKind wait_kind(IoOperation operation) noexcept {
    switch (operation) {
        case IoOperation::ReadFixed:
        case IoOperation::Recv: return WaitKind::Reply;
        case IoOperation::Connect:
        case IoOperation::Accept: return WaitKind::Connect;
        default: return WaitKind::Send;
    }
}

[[nodiscard]] bool prepare(IoRing& ring, IoRequest& item) noexcept {
    IoCompletion* completion = &item.m_completion;
    completion->m_kind = wait_kind(item.m_operation);
    switch (item.m_operation) {
        case IoOperation::ReadFixed:
        case IoOperation::Recv:
            return ring.submit_recv(item.m_fd, item.m_buffer, item.m_length,
                                    item.m_operation == IoOperation::Recv ? item.m_flags : 0,
                                    completion);
        case IoOperation::WriteFixed:
        case IoOperation::Send:
            return ring.submit_send(item.m_fd, item.m_const_buffer, item.m_length,
                                    (item.m_operation == IoOperation::Send ? item.m_flags : 0) |
                                        MSG_NOSIGNAL,
                                    completion);
        case IoOperation::SendPosted:
            return ring.submit_send_posted(item.m_fd, item.m_const_buffer, item.m_length,
                                           completion);
        case IoOperation::Connect:
            return ring.submit_connect(item.m_fd, item.m_address, item.m_address_length,
                                       completion);
        case IoOperation::Accept:
            return ring.submit_accept(item.m_fd, const_cast<sockaddr*>(item.m_address),
                                      item.m_result_length, completion);
        case IoOperation::Nop:
            return ring.submit_nop(completion);
    }
    return false;
}

}  // namespace

SocketIo::SocketIo(Runtime& runtime, std::uint32_t worker_id,
                   unsigned buffer_slots, unsigned buffer_size)
    : m_numa_node(runtime.worker_numa_node(worker_id)),
      m_buffers(mem::global_root(), buffer_size, buffer_slots, m_numa_node),
      m_worker_id(worker_id),
      m_ring(Runtime::current_ring()) {
    if (m_ring == nullptr) detail::no_runtime();
}

SocketIo::~SocketIo() {
    /* Sends in flight complete into worker-owned records: orphan them, no
    waiting.  (Once the worker has gone, its ring and records have gone too.) */
    if (Runtime::current_ring() == m_ring) abandon_posted();
    m_running = false;
}

void SocketIo::set_send_buffer(std::byte* base, std::size_t size) noexcept {
    m_send_base = base;
    m_send_size = static_cast<std::uint32_t>(size);
    m_send_head = 0;
}

void SocketIo::retire_posted() noexcept {
    while (m_posted_count != 0) {
        Posted& oldest = m_posted[m_posted_first];
        if (!oldest.m_completion->m_done) break;
        const int result = oldest.m_completion->m_result;
        if (m_post_failure == 0) {
            if (result < 0) {
                m_post_failure = result;
            } else if (static_cast<std::uint32_t>(result) != oldest.m_length) {
                m_post_failure = -EIO;
            }
        }
        Runtime::release_send_completion(oldest.m_completion);
        oldest.m_completion = nullptr;
        m_posted_first = (m_posted_first + 1) % kMaxPosted;
        --m_posted_count;
    }
    if (m_posted_count == 0) {
        m_send_head = 0;
        m_post_fd = -1;
        m_post_sqe = nullptr;
    }
}

std::byte* SocketIo::stage_send(std::size_t length, IoCompletion*& blocker) {
    SB_LAYER(IoPrep);
    blocker = nullptr;
    retire_posted();
    if (m_posted_count == 0) {
        return length <= m_send_size ? m_send_base : nullptr;
    }
    Posted& newest = m_posted[(m_posted_first + m_posted_count - 1) % kMaxPosted];
    Posted& oldest = m_posted[m_posted_first];
    /* The previous send went out in an earlier submission and is still in
    flight (the socket was full, so it is waiting for room): a new one could
    overtake it. */
    if (!newest.m_completion->m_done && newest.m_epoch != m_ring->submit_epoch()) {
        blocker = newest.m_completion;
        return nullptr;
    }
    if (m_posted_count == kMaxPosted || length > m_send_size) {
        blocker = oldest.m_completion;
        return nullptr;
    }
    /* Live bytes run from the oldest region to the head, possibly wrapped. */
    const std::uint32_t tail = oldest.m_offset;
    const auto need = static_cast<std::uint32_t>(length);
    if (m_send_head >= tail) {
        if (m_send_head + need <= m_send_size) return m_send_base + m_send_head;
        if (need <= tail) return m_send_base;  // wrap
    } else if (m_send_head + need <= tail) {
        return m_send_base + m_send_head;
    }
    blocker = oldest.m_completion;
    return nullptr;
}

bool SocketIo::post_send(int fd, std::byte* buffer, unsigned length) {
    SB_LAYER(IoPrep);
    if (!m_running) return false;
    /* Direct send: with nothing of this socket's in flight, hand the bytes
    to the kernel now.  A small send completes inline either way; doing it as
    a plain send() skips io_uring's request around tcp_sendmsg, and the
    buffer is free again at once.  What does not go out (a full socket) goes
    through the ring, behind nothing, so order is kept. */
    if (m_ring->direct_send() && m_posted_count == 0) {
        const ssize_t sent = ::send(fd, buffer, length, MSG_DONTWAIT | MSG_NOSIGNAL);
        if (sent == static_cast<ssize_t>(length)) return true;
        if (sent > 0) {
            buffer += sent;
            length -= static_cast<unsigned>(sent);
        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
            m_post_failure = -errno;
            return true;  // reported by take_post_failure() with the reply
        }
    }
    IoCompletion* completion = Runtime::acquire_send_completion();
    /* Keep a socket's sends in order: chain this one behind its previous
    send when that is still the last SQE of this submission. */
    if (m_post_fd == fd && m_post_sqe != nullptr && m_ring->last_sqe() == m_post_sqe &&
        m_posted_count != 0 &&
        m_posted[(m_posted_first + m_posted_count - 1) % kMaxPosted].m_epoch ==
            m_ring->submit_epoch()) {
        m_ring->link_last();
    }
    if (!m_ring->submit_send_posted(fd, buffer, length, completion)) {
        Runtime::release_send_completion(completion);
        return false;
    }
    Runtime::io_issued(completion);
    const auto offset = static_cast<std::uint32_t>(buffer - m_send_base);
    m_posted[(m_posted_first + m_posted_count) % kMaxPosted] =
        Posted{completion, offset, length, m_ring->submit_epoch()};
    ++m_posted_count;
    m_send_head = offset + length;
    m_post_fd = fd;
    m_post_sqe = m_ring->last_sqe();
    return true;
}

IoCompletion* SocketIo::oldest_posted() noexcept {
    retire_posted();
    return m_posted_count == 0 ? nullptr : m_posted[m_posted_first].m_completion;
}

int SocketIo::take_post_failure() noexcept {
    retire_posted();
    const int failure = m_post_failure;
    m_post_failure = 0;
    return failure;
}

void SocketIo::abandon_posted() noexcept {
    while (m_posted_count != 0) {
        Posted& oldest = m_posted[m_posted_first];
        if (oldest.m_completion->m_done) {
            Runtime::release_send_completion(oldest.m_completion);
        } else {
            oldest.m_completion->m_task = nullptr;
            oldest.m_completion->m_orphan = true;
        }
        oldest.m_completion = nullptr;
        m_posted_first = (m_posted_first + 1) % kMaxPosted;
        --m_posted_count;
    }
    m_posted_first = 0;
    m_send_head = 0;
    m_post_failure = 0;
    m_post_fd = -1;
    m_post_sqe = nullptr;
}

bool SocketIo::enqueue(IoRequest& request) noexcept {
    SB_LAYER(IoPrep);
    if (!m_running) return false;
    /* A connection never leaves its worker: its SQEs go to that worker's ring. */
    if (Runtime::current_ring() != m_ring) detail::no_runtime();
    const bool receive = request.m_operation == IoOperation::ReadFixed ||
                         request.m_operation == IoOperation::Recv;
    /* The receive for a reply right behind its command's send.  It is not
    linked to the send: a linked request is issued from task work after the
    send completes (a second pass through the ring, ~0.5 us), while unlinked
    it is issued in the same submission.  Order does not matter: the receive
    only waits for data.  A failed send breaks the socket, so the receive
    fails too, and the send's own CQE reports the error. */
    const bool follows_send = receive && m_posted_count != 0 && m_post_fd == request.m_fd &&
                              m_posted[(m_posted_first + m_posted_count - 1) % kMaxPosted]
                                      .m_epoch == m_ring->submit_epoch();
    if (!prepare(*m_ring, request)) return false;
    /* The reply cannot be there the moment the query has been sent: wait for
    data instead of trying the receive first.  The speculative attempt would
    take the socket lock (contending with the softirq handling the send's
    ACK), busy-poll the NIC once and fail, and the receive would run again. */
    if (follows_send) m_ring->last_sqe()->ioprio |= IORING_RECVSEND_POLL_FIRST;
    /* A later send must not be chained behind this receive. */
    if (follows_send) m_post_sqe = nullptr;
    Runtime::io_issued(&request.m_completion);
    return true;
}

namespace {
thread_local std::uint16_t tls_next_buffer_group = 1;
// The worker's free receivers (connections never leave their worker).
thread_local MultishotReceiver* tls_free_receivers = nullptr;
}  // namespace

MultishotReceiver::MultishotReceiver() {
    IoRing* ring = Runtime::current_ring();
    if (ring == nullptr) detail::no_runtime();
    m_group = tls_next_buffer_group++;
    int error = 0;
    m_ring = io_uring_setup_buf_ring(&ring->native_ring(), kBuffers, m_group, 0, &error);
    if (m_ring == nullptr) throw std::system_error(-error, std::generic_category(), "io_uring_setup_buf_ring");
    m_memory = static_cast<std::byte*>(mem::global_root().allocate(
        static_cast<std::size_t>(kBuffers) * kBufferSize, 4096));
    const int mask = io_uring_buf_ring_mask(kBuffers);
    for (unsigned id = 0; id < kBuffers; ++id) {
        io_uring_buf_ring_add(m_ring, m_memory + static_cast<std::size_t>(id) * kBufferSize,
                              kBufferSize, static_cast<unsigned short>(id), mask, static_cast<int>(id));
    }
    io_uring_buf_ring_advance(m_ring, static_cast<int>(kBuffers));
    m_completion.m_on_cqe = &MultishotReceiver::on_cqe;
    m_completion.m_kind = WaitKind::Reply;
}

MultishotReceiver* MultishotReceiver::acquire(int fd) {
    MultishotReceiver* receiver = tls_free_receivers;
    if (receiver != nullptr) {
        tls_free_receivers = receiver->m_next_free;
        receiver->m_next_free = nullptr;
    } else {
        void* memory = mem::global_root().allocate(sizeof(MultishotReceiver), alignof(MultishotReceiver));
        receiver = ::new (memory) MultishotReceiver();
    }
    receiver->m_fd = fd;
    receiver->m_abandoned = false;
    receiver->m_ended = false;
    receiver->m_end_result = 0;
    receiver->arm();
    return receiver;
}

void MultishotReceiver::recycle() noexcept {
    m_fd = -1;
    m_first = 0;
    m_count = 0;
    m_partial_length = 0;
    m_completion.m_task = nullptr;
    m_completion.m_issued = 0;
    m_next_free = tls_free_receivers;
    tls_free_receivers = this;
}

void MultishotReceiver::abandon() noexcept {
    /* Give back the buffers of arrivals nobody will read. */
    if (m_partial_length != 0) {
        release(m_partial_buffer);
        m_partial_length = 0;
    }
    while (m_count != 0) {
        const Raw raw = m_events[m_first];
        m_first = (m_first + 1) % kEvents;
        --m_count;
        if ((raw.m_flags & IORING_CQE_F_BUFFER) != 0) release(raw.m_flags >> IORING_CQE_BUFFER_SHIFT);
    }
    m_completion.m_task = nullptr;
    m_abandoned = true;
    if (!m_armed) recycle();
}

void MultishotReceiver::arm() noexcept {
    IoRing* ring = Runtime::current_ring();
    if (!ring->submit_recv_multishot(m_fd, m_group, &m_completion)) return;
    Runtime::io_issued(&m_completion);
    m_armed = true;
}

detail::PromiseBase* MultishotReceiver::on_cqe(IoCompletion* completion, int result, unsigned flags) {
    auto* self = reinterpret_cast<MultishotReceiver*>(
        reinterpret_cast<std::byte*>(completion) - offsetof(MultishotReceiver, m_completion));
    const bool more = (flags & IORING_CQE_F_MORE) != 0;
    if (!more) self->m_armed = false;
    if (self->m_abandoned) {
        if ((flags & IORING_CQE_F_BUFFER) != 0) self->release(flags >> IORING_CQE_BUFFER_SHIFT);
        if (!more) self->recycle();
        return nullptr;
    }
    if (result == -ENOBUFS && (flags & IORING_CQE_F_BUFFER) == 0) {
        /* The ring ran dry: nothing arrived; the reader re-arms. */
    } else if (self->m_count < kEvents) {
        self->m_events[(self->m_first + self->m_count) % kEvents] = Raw{result, flags};
        ++self->m_count;
    }
    detail::PromiseBase* task = self->m_completion.m_task;
    if (task == nullptr) return nullptr;
    if (self->m_count == 0) {
        if (!self->m_armed) self->arm();  // keep waiting on the re-armed receive
        return nullptr;
    }
    self->m_completion.m_task = nullptr;
    return task;
}

bool MultishotReceiver::ready() noexcept {
    if (m_partial_length != 0 || m_count != 0 || m_ended) return true;
    if (!m_armed) arm();
    return false;
}

void MultishotReceiver::wait() noexcept {
    m_completion.m_task = detail::tls_current;
    Runtime::wait_started(&m_completion);
}

int MultishotReceiver::take(std::byte* destination, unsigned capacity) noexcept {
    Runtime::wait_finished(&m_completion);
    if (m_partial_length == 0) {
        if (m_count == 0) return m_ended ? m_end_result : -EIO;
        const Raw raw = m_events[m_first];
        m_first = (m_first + 1) % kEvents;
        --m_count;
        if (raw.m_result <= 0 || (raw.m_flags & IORING_CQE_F_BUFFER) == 0) {
            m_ended = true;
            m_end_result = raw.m_result <= 0 ? raw.m_result : -EIO;
            return m_end_result;
        }
        m_partial_buffer = raw.m_flags >> IORING_CQE_BUFFER_SHIFT;
        m_partial_offset = 0;
        m_partial_length = static_cast<unsigned>(raw.m_result);
    }
    const unsigned count = std::min(capacity, m_partial_length);
    std::memcpy(destination,
                m_memory + static_cast<std::size_t>(m_partial_buffer) * kBufferSize + m_partial_offset,
                count);
    m_partial_offset += count;
    m_partial_length -= count;
    if (m_partial_length == 0) release(m_partial_buffer);
    return static_cast<int>(count);
}

void MultishotReceiver::release(unsigned buffer) noexcept {
    io_uring_buf_ring_add(m_ring, m_memory + static_cast<std::size_t>(buffer) * kBufferSize,
                          kBufferSize, static_cast<unsigned short>(buffer),
                          io_uring_buf_ring_mask(kBuffers), 0);
    io_uring_buf_ring_advance(m_ring, 1);
}

void SocketReadAwaiter::await_suspend(std::coroutine_handle<>) noexcept {
    (void)enqueue_awaiter(*m_io, m_request);
}
void SocketWriteAwaiter::await_suspend(std::coroutine_handle<>) noexcept {
    (void)enqueue_awaiter(*m_io, m_request);
}
void SocketConnectAwaiter::await_suspend(std::coroutine_handle<>) noexcept {
    (void)enqueue_awaiter(*m_io, m_request);
}
void SocketAcceptAwaiter::await_suspend(std::coroutine_handle<>) noexcept {
    (void)enqueue_awaiter(*m_io, m_request);
}
void SocketRecvAwaiter::await_suspend(std::coroutine_handle<>) noexcept {
    (void)enqueue_awaiter(*m_io, m_request);
}
void SocketSendAwaiter::await_suspend(std::coroutine_handle<>) noexcept {
    (void)enqueue_awaiter(*m_io, m_request);
}

}  // namespace sb
