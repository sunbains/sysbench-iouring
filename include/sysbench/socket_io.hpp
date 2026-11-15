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

// Socket I/O on the owning worker's io_uring.  A connection lives on one
// worker; its awaiters prepare SQEs directly in that worker's ring, and the
// worker submits them in a batch, reaps the CQEs and resumes the coroutines.
// No queue, no other thread, no wakeup.

#include "sysbench/io_ring.hpp"
#include "sysbench/runtime.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <sys/socket.h>

namespace sb {

enum class IoOperation : std::uint8_t {
    ReadFixed,
    WriteFixed,
    Connect,
    Accept,
    Recv,
    Send,
    SendPosted,
    Nop
};

class SocketIo;

struct IoRequest {
    IoOperation m_operation = IoOperation::ReadFixed;
    int m_fd = -1;
    void* m_buffer = nullptr;
    const void* m_const_buffer = nullptr;
    unsigned m_length = 0;
    int m_flags = 0;
    int m_slot = 0;
    const sockaddr* m_address = nullptr;
    socklen_t m_address_length = 0;
    socklen_t* m_result_length = nullptr;
    IoCompletion m_completion{};
};

class SocketIo {
public:
    SocketIo(Runtime& runtime, std::uint32_t worker_id,
             unsigned buffer_slots = 2, unsigned buffer_size = 16 * 1024);
    ~SocketIo();

    SocketIo(const SocketIo&) = delete;
    SocketIo& operator=(const SocketIo&) = delete;

    // Owning worker only.  Prepare the request's SQE in the worker's ring; the
    // request lives in the awaiting coroutine frame until it is reaped.  A
    // receive that directly follows a posted send on the same socket is linked
    // to it, so it is issued only once the send is done.
    [[nodiscard]] bool enqueue(IoRequest& request) noexcept;

    // ---- Posted sends ------------------------------------------------------
    // Commands are framed into the send buffer, used as a ring, and sent
    // without being awaited; several can be in flight (pipelining).  A region
    // is reused once its send's CQE has been reaped.  Consecutive sends on
    // one socket in one submission are linked, so they go out in order; the
    // receive for the reply is linked behind the last one.  Nothing here
    // waits: when the next send cannot be staged yet, the caller awaits the
    // blocking send's completion (PostedSendAwaiter), which hands control to
    // the worker's scheduler.

    // The buffer posted sends are framed in (the connection's write buffer).
    void set_send_buffer(std::byte* base, std::size_t size) noexcept;
    // Space for an `length`-byte send, or nullptr and the send that must
    // complete first (the ring is full, or the socket's previous send was
    // submitted in an earlier batch and is still in flight: issuing another
    // now could reorder the two on the wire).
    [[nodiscard]] std::byte* stage_send(std::size_t length, IoCompletion*& blocker);
    // Post the staged bytes [buffer, buffer + length) on `fd`.  False if the
    // ring has no SQE (the caller falls back to an awaited send).
    [[nodiscard]] bool post_send(int fd, std::byte* buffer, unsigned length);
    // The oldest send still in flight (nullptr if none): await it to drain.
    [[nodiscard]] IoCompletion* oldest_posted() noexcept;
    // A completed posted send's failure (negative errno), consumed; 0 if none.
    [[nodiscard]] int take_post_failure() noexcept;
    // Forget every posted send without waiting (connection closed): sends in
    // flight are orphaned to the worker, which takes them back when reaped.
    void abandon_posted() noexcept;

    [[nodiscard]] RegisteredBufferPool& buffers() noexcept { return m_buffers; }
    [[nodiscard]] int numa_node() const noexcept { return m_numa_node; }
    [[nodiscard]] std::uint32_t worker_id() const noexcept { return m_worker_id; }

private:
    static constexpr unsigned kMaxPosted = 64;
    struct Posted {
        IoCompletion* m_completion = nullptr;
        std::uint32_t m_offset = 0;
        std::uint32_t m_length = 0;
        std::uint64_t m_epoch = 0;
    };
    // Release the regions of reaped sends, oldest first.
    void retire_posted() noexcept;

    int m_numa_node = -1;
    RegisteredBufferPool m_buffers;
    std::uint32_t m_worker_id = 0;
    IoRing* m_ring = nullptr;
    bool m_running = true;
    std::byte* m_send_base = nullptr;
    std::uint32_t m_send_size = 0;
    // Next free byte of the send ring.
    std::uint32_t m_send_head = 0;
    std::array<Posted, kMaxPosted> m_posted{};
    unsigned m_posted_first = 0;
    unsigned m_posted_count = 0;
    int m_post_failure = 0;
    // The most recent posted send's socket and SQE (for linking).
    int m_post_fd = -1;
    io_uring_sqe* m_post_sqe = nullptr;
};

// A connection's receive armed once (multishot) into a ring of provided
// buffers: every arrival posts a CQE naming the buffer it filled, and the
// request stays armed.  No receive SQE, no poll arming per reply.
//
// Receivers belong to the worker: a connection acquire()s one armed on its
// socket and abandon()s it when it closes, without waiting; the receiver
// goes back to the worker's pool when the kernel ends the multishot (the
// socket's shutdown does), so a late CQE never reaches freed memory.
class MultishotReceiver {
public:
    static constexpr unsigned kBuffers = 16;
    static constexpr unsigned kBufferSize = 16 * 1024;

    [[nodiscard]] static MultishotReceiver* acquire(int fd);
    void abandon() noexcept;

    // Copy the next received bytes (at most `capacity`) to `destination`:
    // the count, 0 at end of stream, or -errno.  Bytes beyond `capacity`
    // stay for the next call.
    class ReceiveAwaiter {
    public:
        ReceiveAwaiter(MultishotReceiver& receiver, std::byte* destination, unsigned capacity) noexcept
            : m_receiver(&receiver), m_destination(destination), m_capacity(capacity) {}
        [[nodiscard]] bool await_ready() noexcept { return m_receiver->ready(); }
        void await_suspend(std::coroutine_handle<>) noexcept { m_receiver->wait(); }
        int await_resume() noexcept { return m_receiver->take(m_destination, m_capacity); }

    private:
        MultishotReceiver* m_receiver;
        std::byte* m_destination;
        unsigned m_capacity;
    };
    [[nodiscard]] ReceiveAwaiter receive_into(std::byte* destination, unsigned capacity) noexcept {
        return ReceiveAwaiter(*this, destination, capacity);
    }

    // The awaiter's steps, for awaiters that wrap this one.
    [[nodiscard]] bool ready() noexcept;
    void wait() noexcept;
    int take(std::byte* destination, unsigned capacity) noexcept;

private:
    MultishotReceiver();
    static detail::PromiseBase* on_cqe(IoCompletion* completion, int result, unsigned flags);
    void arm() noexcept;
    void release(unsigned buffer) noexcept;
    void recycle() noexcept;

    IoCompletion m_completion{};
    int m_fd = -1;
    std::uint16_t m_group = 0;
    io_uring_buf_ring* m_ring = nullptr;
    std::byte* m_memory = nullptr;
    bool m_armed = false;
    bool m_abandoned = false;
    bool m_ended = false;   // the stream ended (0) or failed: keep reporting it
    int m_end_result = 0;
    MultishotReceiver* m_next_free = nullptr;
    // Unconsumed tail of the buffer last taken from.
    unsigned m_partial_buffer = 0;
    unsigned m_partial_offset = 0;
    unsigned m_partial_length = 0;
    static constexpr unsigned kEvents = 64;
    struct Raw {
        int m_result;
        unsigned m_flags;
    };
    Raw m_events[kEvents]{};
    unsigned m_first = 0;
    unsigned m_count = 0;
};

// Await a posted send's completion: the task is parked on the CQE and the
// worker runs other tasks meanwhile.
class PostedSendAwaiter {
public:
    explicit PostedSendAwaiter(IoCompletion* completion) noexcept : m_completion(completion) {}
    [[nodiscard]] bool await_ready() const noexcept {
        return m_completion == nullptr || m_completion->m_done;
    }
    void await_suspend(std::coroutine_handle<>) noexcept {
        m_completion->m_task = detail::tls_current;
    }
    void await_resume() const noexcept {
        if (m_completion != nullptr) m_completion->m_task = nullptr;
    }

private:
    IoCompletion* m_completion;
};

// Awaiters.  They only prepare an SQE; the request body is embedded in the
// coroutine frame and only the owning worker touches it.
class SocketReadAwaiter {
public:
    SocketReadAwaiter(SocketIo& io, int fd, void* buffer, unsigned length,
                      int slot)
        : m_io(&io) {
        m_request.m_operation = IoOperation::ReadFixed;
        m_request.m_fd = fd;
        m_request.m_buffer = buffer;
        m_request.m_length = length;
        m_request.m_slot = slot;
    }
    [[nodiscard]] bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<>) noexcept;
    [[nodiscard]] int await_resume() const noexcept { return m_request.m_completion.m_result; }

private:
    SocketIo* m_io;
    IoRequest m_request{};
};

class SocketWriteAwaiter {
public:
    SocketWriteAwaiter(SocketIo& io, int fd, const void* buffer, unsigned length,
                       int slot)
        : m_io(&io) {
        m_request.m_operation = IoOperation::WriteFixed;
        m_request.m_fd = fd;
        m_request.m_const_buffer = buffer;
        m_request.m_length = length;
        m_request.m_slot = slot;
    }
    [[nodiscard]] bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<>) noexcept;
    [[nodiscard]] int await_resume() const noexcept { return m_request.m_completion.m_result; }

private:
    SocketIo* m_io;
    IoRequest m_request{};
};

class SocketConnectAwaiter {
public:
    SocketConnectAwaiter(SocketIo& io, int fd, const sockaddr* address,
                         socklen_t length)
        : m_io(&io) {
        m_request.m_operation = IoOperation::Connect;
        m_request.m_fd = fd;
        m_request.m_address = address;
        m_request.m_address_length = length;
    }
    [[nodiscard]] bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<>) noexcept;
    [[nodiscard]] int await_resume() const noexcept { return m_request.m_completion.m_result; }

private:
    SocketIo* m_io;
    IoRequest m_request{};
};

class SocketAcceptAwaiter {
public:
    SocketAcceptAwaiter(SocketIo& io, int fd, sockaddr* address,
                        socklen_t* length)
        : m_io(&io) {
        m_request.m_operation = IoOperation::Accept;
        m_request.m_fd = fd;
        m_request.m_address = address;
        m_request.m_result_length = length;
    }
    [[nodiscard]] bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<>) noexcept;
    [[nodiscard]] int await_resume() const noexcept { return m_request.m_completion.m_result; }

private:
    SocketIo* m_io;
    IoRequest m_request{};
};

class SocketRecvAwaiter {
public:
    SocketRecvAwaiter(SocketIo& io, int fd, void* buffer, unsigned length,
                      int flags)
        : m_io(&io) {
        m_request.m_operation = IoOperation::Recv;
        m_request.m_fd = fd;
        m_request.m_buffer = buffer;
        m_request.m_length = length;
        m_request.m_flags = flags;
    }
    [[nodiscard]] bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<>) noexcept;
    [[nodiscard]] int await_resume() const noexcept { return m_request.m_completion.m_result; }

private:
    SocketIo* m_io;
    IoRequest m_request{};
};

class SocketSendAwaiter {
public:
    SocketSendAwaiter(SocketIo& io, int fd, const void* buffer, unsigned length,
                      int flags)
        : m_io(&io) {
        m_request.m_operation = IoOperation::Send;
        m_request.m_fd = fd;
        m_request.m_const_buffer = buffer;
        m_request.m_length = length;
        m_request.m_flags = flags;
    }
    [[nodiscard]] bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<>) noexcept;
    [[nodiscard]] int await_resume() const noexcept { return m_request.m_completion.m_result; }

private:
    SocketIo* m_io;
    IoRequest m_request{};
};

[[nodiscard]] inline SocketReadAwaiter read_fixed(SocketIo& io, int fd,
                                                  void* buffer, unsigned length,
                                                  int slot) {
    return SocketReadAwaiter(io, fd, buffer, length, slot);
}
[[nodiscard]] inline SocketWriteAwaiter write_fixed(SocketIo& io, int fd,
                                                    const void* buffer,
                                                    unsigned length, int slot) {
    return SocketWriteAwaiter(io, fd, buffer, length, slot);
}
[[nodiscard]] inline SocketConnectAwaiter ring_connect(SocketIo& io, int fd,
                                                       const sockaddr* address,
                                                       socklen_t length) {
    return SocketConnectAwaiter(io, fd, address, length);
}
[[nodiscard]] inline SocketAcceptAwaiter ring_accept(SocketIo& io, int fd,
                                                     sockaddr* address,
                                                     socklen_t* length) {
    return SocketAcceptAwaiter(io, fd, address, length);
}
[[nodiscard]] inline SocketRecvAwaiter ring_recv(SocketIo& io, int fd,
                                                 void* buffer, unsigned length,
                                                 int flags) {
    return SocketRecvAwaiter(io, fd, buffer, length, flags);
}
[[nodiscard]] inline SocketSendAwaiter ring_send(SocketIo& io, int fd,
                                                 const void* buffer,
                                                 unsigned length, int flags) {
    return SocketSendAwaiter(io, fd, buffer, length, flags);
}

}  // namespace sb
