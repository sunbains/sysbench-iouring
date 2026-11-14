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
#include "test_framework.hpp"

#include "sysbench/io_ring.hpp"
#include "sysbench/memory.hpp"
#include "sysbench/runtime.hpp"
#include "sysbench/socket_io.hpp"
#include "sysbench/task.hpp"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstddef>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

using namespace sb;

namespace {

Task<void> socketpair_roundtrip(std::atomic<bool>& ok,
                                std::atomic<std::uint64_t>& ops) {
    SocketIo io(*Runtime::current(), Runtime::current_worker_id(), 8, 4096);
    RegisteredBufferPool& pool = io.buffers();
    auto read_buffer = pool.acquire();
    auto write_buffer = pool.acquire();
    if (!read_buffer.valid() || !write_buffer.valid()) {
        co_return;
    }

    int fds[2]{};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        co_return;
    }

    const char message[] = "sysbench-cpp";
    constexpr unsigned length = sizeof(message) - 1;
    std::memcpy(write_buffer.data(), message, length);

    const int written = co_await write_fixed(io, fds[0], write_buffer.data(),
                                             length, write_buffer.index());
    const int read = co_await read_fixed(io, fds[1], read_buffer.data(),
                                         read_buffer.size(), read_buffer.index());
    ok.store(written == static_cast<int>(length) && read == static_cast<int>(length) &&
                 std::memcmp(read_buffer.data(), message, length) == 0,
             std::memory_order_relaxed);
    ops.fetch_add(2, std::memory_order_relaxed);
    ::close(fds[0]);
    ::close(fds[1]);
}

Task<void> fixed_roundtrip_loop(int iterations, std::atomic<std::uint64_t>& ops,
                                std::atomic<bool>& ok) {
    SocketIo io(*Runtime::current(), Runtime::current_worker_id(), 8, 4096);
    RegisteredBufferPool& pool = io.buffers();
    auto read_buffer = pool.acquire();
    auto write_buffer = pool.acquire();
    int fds[2]{};
    if (!read_buffer.valid() || !write_buffer.valid() ||
        ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        co_return;
    }

    std::uint32_t value = 0;
    bool good = true;
    for (int i = 0; i < iterations; ++i) {
        value += 1;
        std::memcpy(write_buffer.data(), &value, sizeof(value));
        const int written = co_await write_fixed(io, fds[0], write_buffer.data(),
                                                 sizeof(value), write_buffer.index());
        const int read = co_await read_fixed(io, fds[1], read_buffer.data(),
                                             sizeof(value), read_buffer.index());
        std::uint32_t echoed = 0;
        std::memcpy(&echoed, read_buffer.data(), sizeof(echoed));
        good = good && written == static_cast<int>(sizeof(value)) &&
               read == static_cast<int>(sizeof(value)) && echoed == value;
        ops.fetch_add(2, std::memory_order_relaxed);
    }
    ok.store(good, std::memory_order_relaxed);
    ::close(fds[0]);
    ::close(fds[1]);
}

Task<void> inspect_pool(std::atomic<bool>& registered_ok,
                        std::atomic<bool>& exhaustion_ok) {
    SocketIo io(*Runtime::current(), Runtime::current_worker_id(), 8, 4096);
    RegisteredBufferPool& pool = io.buffers();
    registered_ok.store(pool.slot_count() > 0 && pool.available() == pool.slot_count(),
                        std::memory_order_relaxed);

    const unsigned capacity = pool.available();
    bool valid = true;
    RegisteredBufferLease leases[IoRing::kMaxRegisteredBuffers];
    for (unsigned i = 0; i < capacity; ++i) {
        leases[i] = pool.acquire();
        valid = valid && leases[i].valid() && pool.owns(leases[i].data());
    }
    RegisteredBufferLease overflow = pool.acquire();
    exhaustion_ok.store(valid && !overflow.valid() && pool.available() == 0,
                        std::memory_order_relaxed);
    for (unsigned i = 0; i < capacity; ++i) leases[i].reset();
    exhaustion_ok.store(exhaustion_ok.load() && pool.available() == capacity,
                        std::memory_order_relaxed);
    co_return;
}

Task<void> connect_accept(std::atomic<bool>& ok) {
    SocketIo io(*Runtime::current(), Runtime::current_worker_id(), 2, 4096);
    const int listener = ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    const int client = ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listener < 0 || client < 0) {
        if (listener >= 0) ::close(listener);
        if (client >= 0) ::close(client);
        co_return;
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    const std::string name =
        "sysbench-cpp-" + std::to_string(::getpid()) + "-" +
        std::to_string(reinterpret_cast<std::uintptr_t>(&address));
    std::memcpy(address.sun_path + 1, name.data(), name.size());
    const socklen_t address_length = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) + 1 + name.size());

    if (::bind(listener, reinterpret_cast<sockaddr*>(&address), address_length) != 0 ||
        ::listen(listener, 8) != 0) {
        ::close(listener);
        ::close(client);
        co_return;
    }

    const int connected = co_await ring_connect(
        io, client, reinterpret_cast<sockaddr*>(&address), address_length);
    sockaddr_un peer{};
    socklen_t peer_length = sizeof(peer);
    const int accepted_fd = co_await ring_accept(
        io, listener, reinterpret_cast<sockaddr*>(&peer), &peer_length);
    ok.store(connected == 0 && accepted_fd >= 0, std::memory_order_relaxed);
    if (accepted_fd >= 0) ::close(accepted_fd);
    ::close(listener);
    ::close(client);
}

}  // namespace

TEST("registered buffers are page-backed and exhaustible") {
    Runtime runtime;
    runtime.start(Runtime::Config{.m_workers = 1,
                                  .m_worker_heap_bytes = 2 * 1024 * 1024});
    std::atomic<bool> registered{false};
    std::atomic<bool> exhaustion{false};
    runtime.spawn(inspect_pool(registered, exhaustion));
    runtime.wait();
    CHECK(registered.load());
    CHECK(exhaustion.load());
    runtime.shutdown();
}

TEST("read_fixed/write_fixed roundtrip on a socketpair") {
    Runtime runtime;
    runtime.start(Runtime::Config{.m_workers = 1,
                                  .m_worker_heap_bytes = 2 * 1024 * 1024});
    std::atomic<bool> ok{false};
    std::atomic<std::uint64_t> ops{0};
    runtime.spawn(socketpair_roundtrip(ok, ops));
    runtime.wait();
    CHECK(ok.load());
    CHECK_EQ(ops.load(), 2u);
    runtime.shutdown();
}

TEST("fixed-buffer I/O throughput") {
    if (!::sb::test::perf_enabled()) return;
    Runtime runtime;
    runtime.start(Runtime::Config{.m_workers = 1,
                                  .m_worker_heap_bytes = 4 * 1024 * 1024});
    constexpr int kIterations = 10'000;
    std::atomic<bool> ok{false};
    std::atomic<std::uint64_t> ops{0};
    const double throughput = ::sb::test::measure_ops_per_second(
        2 * kIterations, [&] {
            runtime.spawn(fixed_roundtrip_loop(kIterations, ops, ok));
            runtime.wait();
        });
    std::println("  io_uring socketpair: {:.0f} fixed ops/s", throughput);
    CHECK(ok.load());
    CHECK_EQ(ops.load(), 2u * kIterations);
    CHECK(throughput > 20'000.0);
    runtime.shutdown();
}

TEST("ring connect and accept") {
    Runtime runtime;
    runtime.start(Runtime::Config{.m_workers = 1,
                                  .m_worker_heap_bytes = 2 * 1024 * 1024});
    std::atomic<bool> ok{false};
    runtime.spawn(connect_accept(ok));
    runtime.wait();
    CHECK(ok.load());
    runtime.shutdown();
}

/*
  The benchmark engine starts one connection per benchmark thread at once:
  on a run with thousands of threads, a few hundred workers (or fewer) run
  thousands of connect coroutines, each with its own SocketIo and reaper
  thread.  A lost completion or a lost park wakeup leaves some of those
  coroutines suspended forever, and Runtime::wait() never returns -- the
  whole benchmark then sits in poll with no output.  This test drives exactly
  that shape: many connect coroutines on a handful of workers, all connecting
  to one accepting socket.
*/
Task<void> mass_connect(const char* path, std::size_t path_length,
                        std::atomic<std::uint64_t>& connected,
                        std::atomic<std::uint64_t>& failed) {
    SocketIo io(*Runtime::current(), Runtime::current_worker_id(), 2, 4096);
    const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        failed.fetch_add(1, std::memory_order_relaxed);
        co_return;
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path + 1, path, path_length);
    const socklen_t address_length = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) + 1 + path_length);

    const int connected_result = co_await ring_connect(
        io, fd, reinterpret_cast<sockaddr*>(&address), address_length);
    if (connected_result == 0) {
        connected.fetch_add(1, std::memory_order_relaxed);
    } else {
        failed.fetch_add(1, std::memory_order_relaxed);
    }
    ::close(fd);
}

Task<void> mass_accept(int listener, std::size_t count,
                       std::atomic<std::uint64_t>& accepted,
                       std::atomic<std::uint64_t>& failed) {
    SocketIo io(*Runtime::current(), Runtime::current_worker_id(), 2, 4096);
    for (std::size_t i = 0; i < count; ++i) {
        sockaddr_un peer{};
        socklen_t peer_length = sizeof(peer);
        const int fd = co_await ring_accept(io, listener,
                                           reinterpret_cast<sockaddr*>(&peer),
                                           &peer_length);
        if (fd < 0) {
            failed.fetch_add(1, std::memory_order_relaxed);
            co_return;
        }
        accepted.fetch_add(1, std::memory_order_relaxed);
        ::close(fd);
    }
}

void mass_connect_test(Runtime& runtime, std::size_t connections) {
    char name[64]{};
    std::snprintf(name, sizeof(name), "sysbench-cpp-mass-%d", static_cast<int>(::getpid()));
    const std::size_t name_length = std::strlen(name) + 1;

    const int listener =
        ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    CHECK(listener >= 0);

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path + 1, name, name_length);
    const socklen_t address_length = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) + 1 + name_length);

    CHECK_EQ(::bind(listener, reinterpret_cast<sockaddr*>(&address), address_length), 0);
    CHECK_EQ(::listen(listener, static_cast<int>(connections)), 0);

    std::atomic<std::uint64_t> connected{0};
    std::atomic<std::uint64_t> accepted{0};
    std::atomic<std::uint64_t> connect_failed{0};
    std::atomic<std::uint64_t> accept_failed{0};
    std::atomic<bool> finished{false};

    std::thread watchdog([&finished] {
        for (int i = 0; i < 120 && !finished.load(std::memory_order_acquire); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        if (!finished.load(std::memory_order_acquire)) {
            std::println(stderr, "HANG: mass connect never completed");
            std::_Exit(2);
        }
    });

    runtime.spawn(mass_accept(listener, connections, accepted, accept_failed));
    for (std::size_t i = 0; i < connections; ++i) {
        runtime.spawn(mass_connect(name, name_length, connected, connect_failed));
    }
    runtime.wait();
    finished.store(true, std::memory_order_release);
    watchdog.join();

    std::println("  mass connect: connections={} connected={} accepted={} "
                 "connect_failed={} accept_failed={}",
                 connections, connected.load(), accepted.load(),
                 connect_failed.load(), accept_failed.load());
    CHECK_EQ(connected.load(), connections);
    CHECK_EQ(accepted.load(), connections);
    CHECK_EQ(connect_failed.load(), 0u);
    ::close(listener);
}

TEST("many concurrent connect coroutines") {
    const char* requested = std::getenv("SYSBENCH_MASS_CONNECTIONS");
    const std::size_t connections =
        requested != nullptr ? std::strtoull(requested, nullptr, 10) : 4096;

    Runtime runtime;
    runtime.start(Runtime::Config{.m_workers = 8,
                                  .m_worker_heap_bytes = 512 * 1024 * 1024});
    mass_connect_test(runtime, connections);
    runtime.shutdown();
}

int main(int argc, char** argv) {
    // The mass-connect test keeps thousands of SocketIo objects (rings and
    // registered buffers) alive at once, like the benchmark engine does for
    // thousands of connections.
    mem::GlobalAllocator::instance().initialize(8ull * 1024 * 1024 * 1024);
    return ::sb::test::run_all(argc, argv);
}
