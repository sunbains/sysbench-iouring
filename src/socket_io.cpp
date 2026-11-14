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
