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

// netbench: prepared SELECT 1 on one connection, one layer at a time.
//
//   --mode=raw-blocking  the exact COM_STMT_EXECUTE bytes with blocking
//                        send()/recv() on the worker thread (network + kernel
//                        only: what sysbench's client does underneath)
//   --mode=raw-uring     the same bytes through the Runtime and SocketIo (posted
//                        send, POLL_FIRST receive, the scheduler's wait)
//   --mode=driver        MysqlConnection prepare/execute (adds the MySQL
//                        protocol layer and coroutine glue; no Lua)
//
// Login and prepare always go through the driver; the raw modes then take
// over its socket.  Each run prints qps and microseconds per query.

#include <liburing.h>

#include "sysbench/db.hpp"
#include "sysbench/memory.hpp"
#include "sysbench/mysql.hpp"
#include "sysbench/runtime.hpp"
#include "sysbench/socket_io.hpp"

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>

using namespace sb;

namespace {

struct Args {
    std::string m_host = "127.0.0.1";
    std::string m_port = "3306";
    std::string m_user = "root";
    std::string m_password;
    std::string m_database;
    std::string m_mode = "driver";
    double m_seconds = 8.0;
    bool m_text = false;  // COM_QUERY "SELECT 1" instead of a prepared execute
};

struct Outcome {
    std::atomic<std::uint64_t> m_queries{0};
    std::atomic<std::int64_t> m_elapsed_ns{0};
    std::atomic<bool> m_ok{false};
    std::atomic<std::uint32_t> m_reply_bytes{0};
};

using Clock = std::chrono::steady_clock;

// COM_STMT_EXECUTE for a statement without parameters, framed.
std::size_t execute_packet(std::uint32_t statement_id, std::byte* out) {
    const std::uint8_t body[] = {0x17,
                                 static_cast<std::uint8_t>(statement_id),
                                 static_cast<std::uint8_t>(statement_id >> 8),
                                 static_cast<std::uint8_t>(statement_id >> 16),
                                 static_cast<std::uint8_t>(statement_id >> 24),
                                 0x00,  // flags: no cursor
                                 0x01, 0x00, 0x00, 0x00};  // iteration count
    out[0] = static_cast<std::byte>(sizeof(body));
    out[1] = std::byte{0};
    out[2] = std::byte{0};
    out[3] = std::byte{0};  // sequence
    std::memcpy(out + 4, body, sizeof(body));
    return 4 + sizeof(body);
}

// COM_QUERY "SELECT 1", framed.
std::size_t query_packet(std::byte* out) {
    const char body[] = "\x03SELECT 1";
    const std::size_t length = sizeof(body) - 1;
    out[0] = static_cast<std::byte>(length);
    out[1] = std::byte{0};
    out[2] = std::byte{0};
    out[3] = std::byte{0};
    std::memcpy(out + 4, body, length);
    return 4 + length;
}

// Send one execute with blocking calls and collect the whole reply (it comes
// in one segment): its length is what every later reply must add up to.
std::uint32_t learn_reply_length(int fd, const std::byte* packet, std::size_t length) {
    if (::send(fd, packet, length, MSG_NOSIGNAL) != static_cast<ssize_t>(length)) return 0;
    pollfd waiter{.fd = fd, .events = POLLIN, .revents = 0};
    if (::poll(&waiter, 1, 2000) != 1) return 0;
    ::usleep(20'000);
    std::byte buffer[4096];
    const ssize_t got = ::recv(fd, buffer, sizeof(buffer), MSG_DONTWAIT);
    return got > 0 ? static_cast<std::uint32_t>(got) : 0;
}

Task<void> bench(const Args& args, Outcome& outcome) {
    auto& allocator = Runtime::current_allocator();
    db::Driver* driver = db::DriverRegistry::instance().create("mysql", allocator);
    if (driver == nullptr) co_return;
    db::ConnectionOptions options;
    options.m_host = args.m_host;
    options.m_port = args.m_port;
    options.m_user = args.m_user;
    options.m_password = args.m_password;
    options.m_database = args.m_database;
    options.m_steer_rx = std::getenv("NETBENCH_STEER") != nullptr;
    auto connected = co_await driver->connect(options, allocator);
    if (connected.is_err()) {
        std::fprintf(stderr, "connect: %s\n", connected.error().message());
        co_return;
    }
    db::Connection* connection = connected.value();
    auto prepared = co_await connection->prepare("SELECT 1");
    if (prepared.is_err()) {
        std::fprintf(stderr, "prepare: %s\n", prepared.error().message());
        co_return;
    }
    db::PreparedStatement* statement = prepared.value();
    mem::BumpArena arena(allocator, 64 * 1024);

    const auto warmup = std::chrono::milliseconds(500);
    const auto duration = std::chrono::nanoseconds(
        static_cast<std::int64_t>(args.m_seconds * 1e9));
    std::uint64_t queries = 0;
    Clock::time_point start{};
    Clock::time_point end{};

    /* NETBENCH_DELAY_NS: busy work after each reply, to see how extra client
    time between reply and next request shows up in the round trip. */
    const std::uint64_t delay_cycles =
        std::getenv("NETBENCH_DELAY_NS") != nullptr
            ? static_cast<std::uint64_t>(std::atof(std::getenv("NETBENCH_DELAY_NS")) * 2.6)
            : 0;
    const bool use_sync = std::getenv("NETBENCH_SYNC") != nullptr;
    if (args.m_mode == "driver") {
        const auto warm_until = Clock::now() + warmup;
        bool measuring = false;
        for (std::uint64_t i = 0;; ++i) {
            {
                mem::ScopedAllocator scope(arena);
                /* NETBENCH_SYNC: the synchronous calls (blocking architecture). */
                /* Plain if/else: GCC mis-evaluates co_await inside the arms
                of a conditional expression (it ran every arm). */
                const bool sync = use_sync && connection->supports_sync();
                db::QueryResult result = db::QueryResult::err(db::Error("not run"));
                if (sync && args.m_text) {
                    result = connection->query_sync("SELECT 1", scope);
                } else if (sync) {
                    result = statement->execute_sync({}, scope);
                } else if (args.m_text) {
                    result = co_await connection->query("SELECT 1", scope);
                } else {
                    result = co_await statement->execute({}, scope);
                }
                if (result.is_err()) {
                    std::fprintf(stderr, "execute: %s\n", result.error().message());
                    co_return;
                }
            }
            if (delay_cycles != 0) {
                const std::uint64_t until = __builtin_ia32_rdtsc() + delay_cycles;
                while (__builtin_ia32_rdtsc() < until) {
                }
            }
            if (measuring) ++queries;
            if ((i & 255) == 0) {
                const auto now = Clock::now();
                if (!measuring && now >= warm_until) {
                    measuring = true;
                    start = now;
                } else if (measuring && now - start >= duration) {
                    end = now;
                    break;
                }
            }
        }
    } else {
        const int fd = connection->native_socket();
        const std::uint32_t id = statement->native_id();
        if (fd < 0 || id == 0) {
            std::fprintf(stderr, "no socket or statement id\n");
            co_return;
        }
        std::byte packet[32];
        const std::size_t packet_length = args.m_text ? query_packet(packet) : execute_packet(id, packet);
        const std::uint32_t reply = learn_reply_length(fd, packet, packet_length);
        if (reply == 0) {
            std::fprintf(stderr, "could not learn the reply length\n");
            co_return;
        }
        outcome.m_reply_bytes.store(reply);
        if (std::getenv("NETBENCH_NOBUSY") != nullptr && args.m_mode != "raw-blocking") {
            int zero = 0;
            (void)::setsockopt(fd, SOL_SOCKET, SO_BUSY_POLL, &zero, sizeof(zero));
        }
        if (std::getenv("NETBENCH_FIXED") != nullptr && args.m_mode != "raw-blocking") {
            if (Runtime::current_ring()->register_socket(fd) < 0) std::fprintf(stderr, "register_socket failed\n");
        }
        const auto warm_until = Clock::now() + warmup;
        bool measuring = false;

        if (args.m_mode == "paper" || args.m_mode == "paper-pollfirst" || args.m_mode == "paper-nolink") {
            /* The VLDB'26 io_uring ping-pong client, as published: its own
            ring (SINGLE_ISSUER | DEFER_TASKRUN), NAPI busy_poll_to=50 with
            prefer_busy_poll, a linked send -> recv (MSG_WAITALL), then
            io_uring_submit_and_wait(ring, 1) and for_each_cqe.  It bypasses
            the Runtime: the worker thread blocks in the ring directly. */
            io_uring ring{};
            io_uring_params params{};
            params.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_CLAMP |
                           IORING_SETUP_CQSIZE;
            params.cq_entries = 4096;
            if (io_uring_queue_init_params(256, &ring, &params) < 0) {
                std::fprintf(stderr, "ring init failed\n");
                co_return;
            }
            if (std::getenv("NETBENCH_NAPI") != nullptr) {
                io_uring_napi napi{};
                napi.prefer_busy_poll = 1;
                napi.busy_poll_to = 50;
                if (io_uring_register_napi(&ring, &napi) != 0) std::fprintf(stderr, "napi failed\n");
            }
            std::byte buffer[4096];
            const bool link = args.m_mode != "paper-nolink";
            const bool poll_first = args.m_mode == "paper-pollfirst";
            for (std::uint64_t i = 0;; ++i) {
                io_uring_sqe* send = io_uring_get_sqe(&ring);
                io_uring_prep_send(send, fd, packet, packet_length, MSG_WAITALL);
                io_uring_sqe_set_data64(send, 1);
                if (link) send->flags |= IOSQE_IO_LINK;
                io_uring_sqe* recv = io_uring_get_sqe(&ring);
                io_uring_prep_recv(recv, fd, buffer, reply, MSG_WAITALL);
                io_uring_sqe_set_data64(recv, 2);
                if (poll_first) recv->ioprio |= IORING_RECVSEND_POLL_FIRST;
                bool replied = false;
                bool ok = true;
                while (!replied) {
                    if (io_uring_submit_and_wait(&ring, 1) < 0) continue;
                    io_uring_cqe* cqe;
                    unsigned head;
                    unsigned seen = 0;
                    io_uring_for_each_cqe(&ring, head, cqe) {
                        ++seen;
                        if (cqe->user_data == 2) {
                            replied = true;
                            if (cqe->res != static_cast<int>(reply)) ok = false;
                        } else if (cqe->res < 0) {
                            ok = false;
                        }
                    }
                    io_uring_cq_advance(&ring, seen);
                }
                if (!ok) {
                    std::fprintf(stderr, "paper round trip failed\n");
                    co_return;
                }
                if (measuring) ++queries;
                if ((i & 255) == 0) {
                    const auto now = Clock::now();
                    if (!measuring && now >= warm_until) {
                        measuring = true;
                        start = now;
                    } else if (measuring && now - start >= duration) {
                        end = now;
                        break;
                    }
                }
            }
            io_uring_queue_exit(&ring);
        } else if (args.m_mode == "raw-spinrecv") {
            /* The scheduler's spin does the busy polling: non-blocking
            recv() calls (each one NIC poll pass via the socket's busy-poll
            setting) until the reply is in, after a direct send(). */
            std::byte buffer[4096];
            for (std::uint64_t i = 0;; ++i) {
                if (::send(fd, packet, packet_length, MSG_NOSIGNAL | MSG_DONTWAIT) !=
                    static_cast<ssize_t>(packet_length)) {
                    std::fprintf(stderr, "send failed\n");
                    co_return;
                }
                std::uint32_t got = 0;
                while (got < reply) {
                    const ssize_t n = ::recv(fd, buffer + got, sizeof(buffer) - got, MSG_DONTWAIT);
                    if (n > 0) {
                        got += static_cast<std::uint32_t>(n);
                    } else if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
                        std::fprintf(stderr, "recv failed\n");
                        co_return;
                    }
                }
                if (measuring) ++queries;
                if ((i & 255) == 0) {
                    const auto now = Clock::now();
                    if (!measuring && now >= warm_until) {
                        measuring = true;
                        start = now;
                    } else if (measuring && now - start >= duration) {
                        end = now;
                        break;
                    }
                }
            }
        } else if (args.m_mode == "raw-blocking" || args.m_mode == "raw-blocking-nobusy") {
            /* raw-blocking-nobusy: SO_BUSY_POLL=0 turns off what the host's
            net.core.busy_read gives every blocking recv() (it may always be
            lowered without privileges). */
            if (args.m_mode == "raw-blocking-nobusy") {
                int zero = 0;
                if (::setsockopt(fd, SOL_SOCKET, SO_BUSY_POLL, &zero, sizeof(zero)) != 0) {
                    std::perror("SO_BUSY_POLL");
                }
            }
            const int flags = ::fcntl(fd, F_GETFL);
            (void)::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
            std::byte buffer[4096];
            for (std::uint64_t i = 0;; ++i) {
                if (::send(fd, packet, packet_length, MSG_NOSIGNAL) !=
                    static_cast<ssize_t>(packet_length)) {
                    std::fprintf(stderr, "send failed\n");
                    co_return;
                }
                std::uint32_t got = 0;
                while (got < reply) {
                    const ssize_t n = ::recv(fd, buffer + got, sizeof(buffer) - got, 0);
                    if (n <= 0) {
                        std::fprintf(stderr, "recv failed\n");
                        co_return;
                    }
                    got += static_cast<std::uint32_t>(n);
                }
                if (measuring) ++queries;
                if ((i & 255) == 0) {
                    const auto now = Clock::now();
                    if (!measuring && now >= warm_until) {
                        measuring = true;
                        start = now;
                    } else if (measuring && now - start >= duration) {
                        end = now;
                        break;
                    }
                }
            }
            (void)::fcntl(fd, F_SETFL, flags);
        } else if (args.m_mode == "raw-uring" || args.m_mode == "send-uring" ||
                   args.m_mode == "recv-uring") {
            /* raw-uring: both directions through the ring.  send-uring: posted
            send, blocking recv().  recv-uring: blocking send(), ring receive. */
            const bool ring_send = args.m_mode != "recv-uring";
            const bool ring_recv = args.m_mode != "send-uring";
            std::byte blocking_buffer[4096];
            SocketIo io(*Runtime::current(), Runtime::current_worker_id(), 2, 16 * 1024);
            auto read_buffer = io.buffers().acquire();
            auto write_buffer = io.buffers().acquire();
            io.set_send_buffer(static_cast<std::byte*>(write_buffer.data()), write_buffer.size());
            auto* receive = static_cast<std::byte*>(read_buffer.data());
            for (std::uint64_t i = 0;; ++i) {
                if (ring_send) {
                    IoCompletion* blocker = nullptr;
                    std::byte* target = io.stage_send(packet_length, blocker);
                    while (target == nullptr) {
                        co_await PostedSendAwaiter(blocker);
                        target = io.stage_send(packet_length, blocker);
                    }
                    std::memcpy(target, packet, packet_length);
                    if (!io.post_send(fd, target, static_cast<unsigned>(packet_length))) {
                        std::fprintf(stderr, "post_send failed\n");
                        co_return;
                    }
                } else if (::send(fd, packet, packet_length, MSG_NOSIGNAL) !=
                           static_cast<ssize_t>(packet_length)) {
                    std::fprintf(stderr, "send failed\n");
                    co_return;
                }
                /* A posted send goes out with the worker's next submission:
                hand control to the scheduler once so it is submitted. */
                if (ring_send && !ring_recv) co_await yield_now();
                std::uint32_t got = 0;
                if (!ring_recv) {
                    /* Blocking recv on the non-blocking socket: wait in poll,
                    as libmysqlclient does (it polls, then reads). */
                    while (got < reply) {
                        const ssize_t n = ::recv(fd, blocking_buffer + got,
                                                 sizeof(blocking_buffer) - got, 0);
                        if (n > 0) {
                            got += static_cast<std::uint32_t>(n);
                            continue;
                        }
                        if (n < 0 && errno == EAGAIN) {
                            pollfd waiter{.fd = fd, .events = POLLIN, .revents = 0};
                            (void)::poll(&waiter, 1, -1);
                            continue;
                        }
                        std::fprintf(stderr, "recv failed\n");
                        co_return;
                    }
                    (void)io.take_post_failure();
                }
                while (got < reply) {
                    const int n = co_await read_fixed(io, fd, receive + got,
                                                      static_cast<unsigned>(read_buffer.size() - got),
                                                      read_buffer.index());
                    if (n <= 0 || io.take_post_failure() < 0) {
                        std::fprintf(stderr, "receive failed: %d\n", n);
                        co_return;
                    }
                    got += static_cast<std::uint32_t>(n);
                }
                if (measuring) ++queries;
                if ((i & 255) == 0) {
                    const auto now = Clock::now();
                    if (!measuring && now >= warm_until) {
                        measuring = true;
                        start = now;
                    } else if (measuring && now - start >= duration) {
                        end = now;
                        break;
                    }
                }
            }
            while (IoCompletion* oldest = io.oldest_posted()) co_await PostedSendAwaiter(oldest);
        } else if (args.m_mode == "raw-uring-ms" || args.m_mode == "recv-uring-ms") {
            /* A multishot receive armed once; the send posted to the ring
            (raw-uring-ms) or a direct non-blocking send() (recv-uring-ms). */
            const bool direct_send = args.m_mode == "recv-uring-ms";
            SocketIo io(*Runtime::current(), Runtime::current_worker_id(), 2, 16 * 1024);
            auto write_buffer = io.buffers().acquire();
            io.set_send_buffer(static_cast<std::byte*>(write_buffer.data()), write_buffer.size());
            MultishotReceiver* receiver = MultishotReceiver::acquire(fd);
            std::byte scratch[4096];
            for (std::uint64_t i = 0;; ++i) {
                if (direct_send) {
                    if (::send(fd, packet, packet_length, MSG_NOSIGNAL | MSG_DONTWAIT) !=
                        static_cast<ssize_t>(packet_length)) {
                        std::fprintf(stderr, "send failed\n");
                        co_return;
                    }
                } else {
                    IoCompletion* blocker = nullptr;
                    std::byte* target = io.stage_send(packet_length, blocker);
                    while (target == nullptr) {
                        co_await PostedSendAwaiter(blocker);
                        target = io.stage_send(packet_length, blocker);
                    }
                    std::memcpy(target, packet, packet_length);
                    if (!io.post_send(fd, target, static_cast<unsigned>(packet_length))) {
                        std::fprintf(stderr, "post_send failed\n");
                        co_return;
                    }
                }
                std::uint32_t got = 0;
                while (got < reply) {
                    const int n = co_await receiver->receive_into(scratch, sizeof(scratch));
                    if (n <= 0) {
                        std::fprintf(stderr, "receive failed: %d\n", n);
                        co_return;
                    }
                    got += static_cast<std::uint32_t>(n);
                }
                (void)io.take_post_failure();
                if (measuring) ++queries;
                if ((i & 255) == 0) {
                    const auto now = Clock::now();
                    if (!measuring && now >= warm_until) {
                        measuring = true;
                        start = now;
                    } else if (measuring && now - start >= duration) {
                        end = now;
                        break;
                    }
                }
            }
            while (IoCompletion* oldest = io.oldest_posted()) co_await PostedSendAwaiter(oldest);
        } else {
            std::fprintf(stderr, "unknown --mode=%s\n", args.m_mode.c_str());
            co_return;
        }
    }
    outcome.m_queries.store(queries);
    outcome.m_elapsed_ns.store(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    outcome.m_ok.store(true);
    connection->close();
}

bool option(std::string_view arg, std::string_view name, std::string& out) {
    if (arg.size() > name.size() + 3 && arg.substr(0, 2) == "--" &&
        arg.substr(2, name.size()) == name && arg[2 + name.size()] == '=') {
        out.assign(arg.substr(3 + name.size()));
        return true;
    }
    return false;
}

}  // namespace

int main(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg(argv[i]);
        std::string seconds;
        if (option(arg, "mysql-host", args.m_host) || option(arg, "mysql-port", args.m_port) ||
            option(arg, "mysql-user", args.m_user) ||
            option(arg, "mysql-password", args.m_password) ||
            option(arg, "mysql-db", args.m_database) || option(arg, "mode", args.m_mode)) {
            continue;
        }
        if (arg == "--text") {
            args.m_text = true;
            continue;
        }
        if (option(arg, "time", seconds)) {
            args.m_seconds = std::stod(seconds);
            continue;
        }
        std::fprintf(stderr, "unknown argument %s\n", argv[i]);
        return 2;
    }
    mem::GlobalAllocator::instance().initialize(std::size_t{4} << 30, false);
    mysql::register_driver();
    Runtime runtime;
    /* Raw modes read the driver's socket themselves: the driver must not
    hold a multishot receive on it as well. */
    const bool raw = args.m_mode.rfind("raw", 0) == 0 || args.m_mode.find("uring") != std::string::npos;
    const bool blocking = std::getenv("NETBENCH_BLOCKING") != nullptr;
    runtime.start(Runtime::Config{.m_workers = 1,
                                  .m_busy_poll = std::getenv("NETBENCH_NAPI") != nullptr,
                                  .m_multishot_receive = !raw && !blocking && std::getenv("NETBENCH_NOMS") == nullptr,
                                  .m_direct_send = std::getenv("NETBENCH_DIRECT") != nullptr,
                                  .m_blocking_io = blocking});
    Outcome outcome;
    runtime.spawn(bench(args, outcome));
    runtime.wait();
    runtime.shutdown();
    if (!outcome.m_ok.load()) return 1;
    const double seconds = static_cast<double>(outcome.m_elapsed_ns.load()) * 1e-9;
    const double qps = static_cast<double>(outcome.m_queries.load()) / seconds;
    std::printf("%-13s qps %10.0f  %7.2f us/query", args.m_mode.c_str(), qps, 1e6 / qps);
    if (outcome.m_reply_bytes.load() != 0) std::printf("  (reply %u bytes)", outcome.m_reply_bytes.load());
    std::printf("\n");
    return 0;
}
