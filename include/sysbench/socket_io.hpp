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

// One io_uring per socket with three roles:
//   * the owning runtime worker is the sole SQ submitter and drains a lock-free
//     power-of-two request ring (io_uring does not permit shared SQ submit);
//   * a dedicated reaper thread waits for CQEs and posts suspended coroutines
//     back to the runtime worker.
// No mutex, rwlock or condition variable guards the ring.

#include "sysbench/io_ring.hpp"
#include "sysbench/lockfree_queue.hpp"
#include "sysbench/runtime.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <sys/socket.h>

namespace sb {

enum class IoOperation : std::uint8_t {
    ReadFixed,
    WriteFixed,
    Connect,
    Accept,
    Recv,
    Send
};

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

class SocketIo : public IoReaper {
public:
    SocketIo(Runtime& runtime, std::uint32_t worker_id,
             unsigned entries = 256, unsigned buffer_slots = 2,
             unsigned buffer_size = 16 * 1024);
    ~SocketIo();

    SocketIo(const SocketIo&) = delete;
    SocketIo& operator=(const SocketIo&) = delete;

    // Enqueue from the owning runtime worker and submit everything queued.
    // The request lives in the awaiting coroutine frame until completion.
    [[nodiscard]] bool enqueue(IoRequest& request) noexcept;

    // IoReaper: the Runtime scheduler drives reaping and waiting.
    [[nodiscard]] bool running() const noexcept override {
        return m_running.load(std::memory_order_acquire);
    }
    bool reap() noexcept override;
    bool wait() noexcept override;
    void pin() noexcept override;
    void request_stop() noexcept override;
    [[nodiscard]] Runtime& runtime() noexcept override { return *m_runtime; }

    [[nodiscard]] IoRing& ring() noexcept { return m_ring; }
    [[nodiscard]] RegisteredBufferPool& buffers() noexcept { return m_buffers; }
    [[nodiscard]] int numa_node() const noexcept { return m_numa_node; }
    [[nodiscard]] std::uint32_t worker_id() const noexcept { return m_worker_id; }

private:
    static constexpr std::size_t kSubmitRingCapacity = 512;

    void submit_pending() noexcept;

    int m_numa_node = -1;
    IoRing m_ring;
    RegisteredBufferPool m_buffers;
    Runtime* m_runtime = nullptr;
    std::uint32_t m_worker_id = 0;
    std::atomic<bool> m_running{true};
    LockFreeQueue<IoRequest*, kSubmitRingCapacity> m_submit_ring;
    // The reaper sleeps in poll() on the ring's CQ eventfd instead of blocking
    // in io_uring_enter(), so shutdown can wake it immediately instead of
    // waiting out a timeout.
    int m_cq_fd = -1;
    int m_shutdown_fd = -1;
    bool m_cq_eventfd_registered = false;
};

// Awaiters.  They only enqueue; the request body is embedded in the coroutine
// frame and is not touched by another thread until the submitter picks it up.
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
