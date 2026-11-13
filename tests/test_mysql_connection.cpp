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

#include "sysbench/db.hpp"
#include "sysbench/memory.hpp"
#include "sysbench/mysql.hpp"
#include "sysbench/runtime.hpp"
#include "sysbench/socket_io.hpp"
#include "sysbench/task.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>
#include <cstdlib>
#include <utility>
#include <vector>

using namespace sb;

namespace {

enum class MockAuth { Native, CachingSha2Fast };

// Scripted MySQL server.  It is a plain Runtime coroutine: every socket
// operation is an io_uring awaiter, so the fixture owns no threads and never
// blocks a worker.
class MockMysqlServer {
public:
    explicit MockMysqlServer(MockAuth auth = MockAuth::Native) : m_auth(auth) {
        m_listen_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (m_listen_fd < 0) return;
        int one = 1;
        ::setsockopt(m_listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (::bind(m_listen_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
            ::listen(m_listen_fd, 4) != 0) {
            ::close(m_listen_fd);
            m_listen_fd = -1;
            return;
        }
        socklen_t length = sizeof(address);
        ::getsockname(m_listen_fd, reinterpret_cast<sockaddr*>(&address), &length);
        m_port = ntohs(address.sin_port);
        build_responses();
    }

    ~MockMysqlServer() {
        if (m_fd >= 0) ::close(m_fd);
        if (m_listen_fd >= 0) ::close(m_listen_fd);
    }

    [[nodiscard]] std::uint16_t port() const { return m_port; }
    [[nodiscard]] bool started() const { return m_listen_fd >= 0; }

    Task<void> run() {
        SocketIo io(*Runtime::current(), Runtime::current_worker_id(), 64, 4, 4096);
        m_io = &io;

        sockaddr_in peer{};
        socklen_t peer_length = sizeof(peer);
        m_fd = co_await ring_accept(io, m_listen_fd,
                                   reinterpret_cast<sockaddr*>(&peer), &peer_length);
        if (m_fd < 0) co_return;
        {
            int one = 1;
            ::setsockopt(m_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        }
        if (!co_await send_handshake()) co_return;

        std::vector<std::uint8_t> payload;
        if (!co_await read_packet(payload)) co_return;
        if (m_auth == MockAuth::CachingSha2Fast) {
            const std::vector<std::uint8_t> more{0x01, 0x03};
            if (!co_await send_packet(more, 2)) co_return;
            if (!co_await send_packet(ok_packet(), 3)) co_return;
        } else {
            if (!co_await send_packet(ok_packet(), 2)) co_return;
        }

        while (co_await read_packet(payload)) {
            if (payload.empty()) co_return;
            switch (payload[0]) {
                case 0x03:
                    if (!co_await handle_query(payload)) co_return;
                    break;
                case 0x16:
                    if (!co_await handle_prepare()) co_return;
                    break;
                case 0x17:
                    if (!co_await handle_execute()) co_return;
                    break;
                case 0x19:
                    break;  // COM_STMT_CLOSE has no response
                case 0x01:
                    co_return;  // COM_QUIT
                default: {
                    std::vector<std::uint8_t> error{0xff};
                    put_u16(error, 1047);
                    error.push_back('#');
                    const char state[] = "08S01";
                    error.insert(error.end(), state, state + 5);
                    const char message[] = "unsupported command";
                    error.insert(error.end(), message, message + sizeof(message) - 1);
                    (void)co_await send_packet(error, 1);
                    co_return;
                }
            }
        }
    }

private:
    Task<bool> recv_all(void* data, std::size_t size) {
        auto* bytes = static_cast<std::uint8_t*>(data);
        while (size > 0) {
            const int got = co_await ring_recv(*m_io, m_fd, bytes,
                                               static_cast<unsigned>(size), 0);
            if (got <= 0) co_return false;
            bytes += got;
            size -= static_cast<std::size_t>(got);
        }
        co_return true;
    }

    Task<bool> send_all(const void* data, std::size_t size) {
        const auto* bytes = static_cast<const std::uint8_t*>(data);
        while (size > 0) {
            const int sent = co_await ring_send(*m_io, m_fd, bytes,
                                                static_cast<unsigned>(size),
                                                MSG_NOSIGNAL);
            if (sent <= 0) co_return false;
            bytes += sent;
            size -= static_cast<std::size_t>(sent);
        }
        co_return true;
    }

    Task<bool> read_packet(std::vector<std::uint8_t>& payload) {
        std::uint8_t header[4]{};
        if (!co_await recv_all(header, 4)) co_return false;
        const std::uint32_t length = header[0] | (header[1] << 8) | (header[2] << 16);
        payload.resize(length);
        co_return length == 0 || co_await recv_all(payload.data(), length);
    }

    Task<bool> send_packet(const std::vector<std::uint8_t>& payload,
                           std::uint8_t sequence) {
        const std::uint32_t length = static_cast<std::uint32_t>(payload.size());
        std::uint8_t packet[4 + 64];
        std::vector<std::uint8_t> large;
        std::uint8_t* out = packet;
        if (payload.size() > 64) {
            large.resize(4 + payload.size());
            out = large.data();
        }
        out[0] = static_cast<std::uint8_t>(length & 0xff);
        out[1] = static_cast<std::uint8_t>((length >> 8) & 0xff);
        out[2] = static_cast<std::uint8_t>((length >> 16) & 0xff);
        out[3] = sequence;
        if (!payload.empty()) std::memcpy(out + 4, payload.data(), payload.size());
        co_return co_await send_all(out, 4 + payload.size());
    }

    // Real servers build a response in the connection net buffer and flush it
    // once, so the client sees the whole result set in a single read.
    static void append_packet(std::vector<std::uint8_t>& out,
                              const std::vector<std::uint8_t>& payload,
                              std::uint8_t sequence) {
        const std::uint32_t length = static_cast<std::uint32_t>(payload.size());
        out.push_back(static_cast<std::uint8_t>(length & 0xff));
        out.push_back(static_cast<std::uint8_t>((length >> 8) & 0xff));
        out.push_back(static_cast<std::uint8_t>((length >> 16) & 0xff));
        out.push_back(sequence);
        out.insert(out.end(), payload.begin(), payload.end());
    }

    Task<bool> send_response(const std::vector<std::vector<std::uint8_t>>& packets,
                             std::uint8_t first_sequence = 1) {
        std::vector<std::uint8_t> out;
        std::uint8_t sequence = first_sequence;
        for (const auto& packet : packets) append_packet(out, packet, sequence++);
        co_return out.empty() || co_await send_all(out.data(), out.size());
    }

    static void put_u16(std::vector<std::uint8_t>& out, std::uint16_t value) {
        out.push_back(static_cast<std::uint8_t>(value));
        out.push_back(static_cast<std::uint8_t>(value >> 8));
    }
    static void put_u32(std::vector<std::uint8_t>& out, std::uint32_t value) {
        for (int i = 0; i < 4; ++i) out.push_back(static_cast<std::uint8_t>(value >> (i * 8)));
    }
    static void put_lenenc(std::vector<std::uint8_t>& out, std::uint64_t value) {
        if (value < 251) out.push_back(static_cast<std::uint8_t>(value));
        else if (value <= 0xffff) {
            out.push_back(0xfc);
            put_u16(out, static_cast<std::uint16_t>(value));
        } else {
            out.push_back(0xfd);
            out.push_back(static_cast<std::uint8_t>(value));
            out.push_back(static_cast<std::uint8_t>(value >> 8));
            out.push_back(static_cast<std::uint8_t>(value >> 16));
        }
    }
    static void put_str(std::vector<std::uint8_t>& out, std::string_view value) {
        put_lenenc(out, value.size());
        out.insert(out.end(), value.begin(), value.end());
    }

    std::vector<std::uint8_t> ok_packet(std::uint64_t affected = 0) {
        std::vector<std::uint8_t> packet{0x00};
        put_lenenc(packet, affected);
        put_lenenc(packet, 0);
        put_u16(packet, 2);
        put_u16(packet, 0);
        return packet;
    }

    std::vector<std::uint8_t> eof_packet() {
        std::vector<std::uint8_t> packet{0xfe};
        put_u16(packet, 0);
        put_u16(packet, 2);
        return packet;
    }

    std::vector<std::uint8_t> column_packet(std::string_view name, std::uint8_t type,
                                            std::uint16_t flags = 0) {
        std::vector<std::uint8_t> packet;
        put_str(packet, "def");
        put_str(packet, "test");
        put_str(packet, "t");
        put_str(packet, "t");
        put_str(packet, name);
        put_str(packet, name);
        put_lenenc(packet, 12);
        put_u16(packet, 45);
        put_u32(packet, 255);
        packet.push_back(type);
        put_u16(packet, flags);
        packet.push_back(0);
        put_u16(packet, 0);
        return packet;
    }

    Task<bool> send_text_result(std::string_view column, std::string_view value) {
        std::vector<std::uint8_t> count{1};
        std::vector<std::uint8_t> row;
        put_lenenc(row, value.size());
        row.insert(row.end(), value.begin(), value.end());
        co_return co_await send_response({count, column_packet(column, 253),
                                          eof_packet(), row, eof_packet()});
    }

    Task<bool> handle_query(const std::vector<std::uint8_t>& payload) {
        const std::string_view sql(
            reinterpret_cast<const char*>(payload.data() + 1), payload.size() - 1);
        if (sql.find("INSERT") != std::string_view::npos ||
            sql.find("CREATE") != std::string_view::npos ||
            sql.find("UPDATE") != std::string_view::npos ||
            sql.find("DELETE") != std::string_view::npos ||
            sql.find("BEGIN") != std::string_view::npos ||
            sql.find("COMMIT") != std::string_view::npos) {
            co_return co_await send_all(m_ok_response.data(), m_ok_response.size());
        }
        if (sql.find("SELECT 1") != std::string_view::npos) {
            co_return co_await send_all(m_select_response.data(),
                                        m_select_response.size());
        }
        co_return co_await send_all(m_select_response.data(),
                                    m_select_response.size());
    }

    Task<bool> handle_prepare() {
        co_return co_await send_all(m_prepare_response.data(),
                                    m_prepare_response.size());
    }

    Task<bool> handle_execute() {
        co_return co_await send_all(m_execute_response.data(),
                                    m_execute_response.size());
    }

    Task<bool> send_handshake() {
        std::vector<std::uint8_t> payload;
        payload.reserve(128);
        payload.push_back(10);
        const std::string_view version = "8.0.36-mock";
        payload.insert(payload.end(), version.begin(), version.end());
        payload.push_back(0);
        put_u32(payload, 1234);
        const char auth1[] = "abcdefgh";
        payload.insert(payload.end(), auth1, auth1 + 8);
        payload.push_back(0);
        const std::uint32_t capabilities =
            0x00000200 | 0x00008000 | 0x00080000 | 0x00200000 | 0x00020000;
        put_u16(payload, static_cast<std::uint16_t>(capabilities & 0xffff));
        payload.push_back(45);
        put_u16(payload, 2);
        put_u16(payload, static_cast<std::uint16_t>(capabilities >> 16));
        payload.push_back(21);
        for (int i = 0; i < 10; ++i) payload.push_back(0);
        for (char character : std::string_view("ijklmnopqrst")) {
            payload.push_back(static_cast<std::uint8_t>(character));
        }
        payload.push_back(0);
        const std::string_view plugin = m_auth == MockAuth::CachingSha2Fast
                                            ? "caching_sha2_password"
                                            : "mysql_native_password";
        payload.insert(payload.end(), plugin.begin(), plugin.end());
        payload.push_back(0);
        co_return co_await send_packet(payload, 0);
    }

    void build_responses() {
        auto build = [](std::vector<std::uint8_t>& out,
                        std::initializer_list<std::vector<std::uint8_t>> packets) {
            std::uint8_t sequence = 1;
            for (const auto& packet : packets) append_packet(out, packet, sequence++);
        };
        std::vector<std::uint8_t> count{1};
        std::vector<std::uint8_t> row;
        put_lenenc(row, 1);
        row.push_back(static_cast<std::uint8_t>('1'));
        build(m_select_response, {count, column_packet("1", 253), eof_packet(),
                                  row, eof_packet()});
        build(m_ok_response, {ok_packet(1)});
        std::vector<std::uint8_t> prepare_ok{0x00};
        put_u32(prepare_ok, 1);
        put_u16(prepare_ok, 1);
        put_u16(prepare_ok, 1);
        prepare_ok.push_back(0);
        put_u16(prepare_ok, 0);
        build(m_prepare_response, {prepare_ok, column_packet("id", 3), eof_packet(),
                                   column_packet("v", 8), eof_packet()});
        std::vector<std::uint8_t> row8{0x00, 0x00};
        const std::uint64_t value = 42;
        for (int i = 0; i < 8; ++i) {
            row8.push_back(static_cast<std::uint8_t>(value >> (i * 8)));
        }
        build(m_execute_response, {{1}, column_packet("v", 8), eof_packet(),
                                   row8, eof_packet()});
    }

    MockAuth m_auth;
    std::vector<std::uint8_t> m_select_response;
    std::vector<std::uint8_t> m_ok_response;
    std::vector<std::uint8_t> m_prepare_response;
    std::vector<std::uint8_t> m_execute_response;
    int m_listen_fd = -1;
    int m_fd = -1;
    std::uint16_t m_port = 0;
    SocketIo* m_io = nullptr;
};

Task<void> client_flow(std::uint16_t port, std::atomic<int>& stages,
                       std::atomic<bool>& ok) {
    auto& allocator = Runtime::current_allocator();
    db::Driver* driver = db::DriverRegistry::instance().create("mysql", allocator);
    if (driver == nullptr) co_return;

    const std::string port_string = std::to_string(port);
    db::ConnectionOptions options;
    options.host = "127.0.0.1";
    options.port = port_string;
    options.user = "bench";
    options.password = "password";

    auto connect_result = co_await driver->connect(options, allocator);
    if (connect_result.is_err()) {
        ok.store(false, std::memory_order_relaxed);
        driver->destroy(allocator);
        co_return;
    }
    db::Connection* connection = connect_result.value();
    stages.fetch_add(1);

    mem::BumpArena arena(allocator, 64 * 1024);
    {
        mem::ScopedAllocator scope(arena);
        auto result = co_await connection->query("SELECT 1", scope);
        if (result.is_err()) {
            ok.store(false, std::memory_order_relaxed);
        } else {
            const db::ResultSet& rows = result.value();
            ok.store(rows.has_rows && rows.columns.size() == 1 &&
                         rows.rows.size() == 1 &&
                         rows.rows[0].fields.size() == 1 &&
                         !rows.rows[0].fields[0].is_null &&
                         std::string_view(rows.rows[0].fields[0].data,
                                          rows.rows[0].fields[0].length) == "1",
                     std::memory_order_relaxed);
        }
        stages.fetch_add(1);
    }
    {
        mem::ScopedAllocator scope(arena);
        auto result = co_await connection->query("INSERT INTO t VALUES (1)", scope);
        if (result.is_err()) {
            ok.store(false, std::memory_order_relaxed);
        } else {
            const db::ResultSet& rows = result.value();
            ok.store(ok.load() && !rows.has_rows && rows.affected_rows == 1,
                     std::memory_order_relaxed);
        }
        stages.fetch_add(1);
    }
    {
        auto prepared =
            co_await connection->prepare("SELECT c FROM t WHERE id = ?");
        if (prepared.is_err()) {
            ok.store(false, std::memory_order_relaxed);
            stages.fetch_add(1);
        } else {
            db::PreparedStatement* statement = prepared.value();
            mem::ScopedAllocator scope(arena);
            const db::BindValue values[] = {db::BindValue::from_int(7)};
            auto result = co_await statement->execute(values, scope);
            if (result.is_err()) {
                ok.store(false, std::memory_order_relaxed);
            } else {
                const db::ResultSet& rows = result.value();
                ok.store(ok.load() && rows.has_rows && rows.rows.size() == 1 &&
                             rows.rows[0].fields.size() == 1 &&
                             std::string_view(rows.rows[0].fields[0].data,
                                              rows.rows[0].fields[0].length) == "42",
                         std::memory_order_relaxed);
            }
            stages.fetch_add(1);
            (void)co_await statement->close();
            statement->destroy(allocator);
        }
    }

    connection->close();
    connection->destroy(allocator);
    driver->destroy(allocator);
    stages.fetch_add(1);
    co_return;
}

}  // namespace

TEST("MySQL client handshake, query and prepared statement") {
    MockMysqlServer server(MockAuth::Native);
    REQUIRE(server.started());
    mysql::register_driver();

    Runtime runtime;
    runtime.start(Runtime::Config{.workers = 1,
                                  .worker_heap_bytes = 8 * 1024 * 1024});
    std::atomic<int> stages{0};
    std::atomic<bool> ok{true};
    runtime.spawn(server.run());
    runtime.spawn(client_flow(server.port(), stages, ok));
    runtime.wait();
    CHECK_EQ(stages.load(), 5);
    CHECK(ok.load());
    runtime.shutdown();
}

TEST("MySQL caching_sha2 fast auth") {
    MockMysqlServer server(MockAuth::CachingSha2Fast);
    REQUIRE(server.started());
    mysql::register_driver();

    Runtime runtime;
    runtime.start(Runtime::Config{.workers = 1,
                                  .worker_heap_bytes = 8 * 1024 * 1024});
    std::atomic<int> stages{0};
    std::atomic<bool> ok{true};
    runtime.spawn(server.run());
    runtime.spawn(client_flow(server.port(), stages, ok));
    runtime.wait();
    CHECK_EQ(stages.load(), 5);
    CHECK(ok.load());
    runtime.shutdown();
}

TEST("MySQL query throughput over a local mock server") {
    if (!::sb::test::perf_enabled()) return;
    MockMysqlServer server(MockAuth::Native);
    REQUIRE(server.started());
    mysql::register_driver();

    Runtime runtime;
    runtime.start(Runtime::Config{.workers = 2,
                                  .worker_heap_bytes = 8 * 1024 * 1024});

    constexpr int kQueries = 2000;
    std::atomic<int> completed{0};
    std::atomic<bool> ok{true};
    auto throughput_task = [&]() -> Task<void> {
        auto& allocator = Runtime::current_allocator();
        db::Driver* driver = db::DriverRegistry::instance().create("mysql", allocator);
        const std::string port_string = std::to_string(server.port());
        db::ConnectionOptions options;
        options.host = "127.0.0.1";
        options.port = port_string;
        options.user = "bench";
        auto connect_result = co_await driver->connect(options, allocator);
        if (connect_result.is_err()) {
            ok.store(false);
            co_return;
        }
        db::Connection* connection = connect_result.value();
        mem::BumpArena arena(allocator, 64 * 1024);
        for (int i = 0; i < kQueries; ++i) {
            mem::ScopedAllocator scope(arena);
            auto result = co_await connection->query("SELECT 1", scope);
            if (result.is_err() || !result.value().has_rows ||
                result.value().rows.size() != 1) {
                ok.store(false);
                break;
            }
            completed.fetch_add(1, std::memory_order_relaxed);
        }
        connection->close();
        connection->destroy(allocator);
        driver->destroy(allocator);
        co_return;
    };

    const double qps = ::sb::test::measure_ops_per_second(kQueries, [&] {
        runtime.spawn(server.run());
        runtime.spawn(throughput_task());
        runtime.wait();
    });
    std::println("  MySQL mock query: {:.0f} qps ({:.0f} ns/query)", qps,
                qps > 0.0 ? 1e9 / qps : 0.0);
    CHECK_EQ(completed.load(), kQueries);
    CHECK(ok.load());
    CHECK(qps > 500.0);
    runtime.shutdown();
}

TEST("MySQL concurrent point-select throughput") {
    if (!::sb::test::perf_enabled()) return;
    std::uint32_t kPairs = 16;
    if (const char* text = std::getenv("SYSBENCH_MOCK_PAIRS")) {
        kPairs = static_cast<std::uint32_t>(std::strtoul(text, nullptr, 10));
    }
    if (kPairs == 0) kPairs = 1;
    if (kPairs > 32) kPairs = 32;
    constexpr int kQueriesPerPair = 1000;

    std::vector<MockMysqlServer> servers;
    servers.reserve(kPairs);
    for (std::uint32_t i = 0; i < kPairs; ++i) servers.emplace_back(MockAuth::Native);
    for (auto& server : servers) REQUIRE(server.started());
    mysql::register_driver();

    Runtime runtime;
    const bool unpinned = std::getenv("SYSBENCH_UNPINNED") != nullptr;
    runtime.start(Runtime::Config{.workers = 2 * kPairs,
                                  .worker_heap_bytes = 8 * 1024 * 1024,
                                  .pin_workers = !unpinned,
                                  .numa_aware = !unpinned});

    std::atomic<int> completed{0};
    std::atomic<int> failures{0};
    auto client = [&](std::uint16_t port) -> Task<void> {
        auto& allocator = Runtime::current_allocator();
        db::Driver* driver = db::DriverRegistry::instance().create("mysql", allocator);
        const std::string port_string = std::to_string(port);
        db::ConnectionOptions options;
        options.host = "127.0.0.1";
        options.port = port_string;
        options.user = "bench";
        auto connect_result = co_await driver->connect(options, allocator);
        if (connect_result.is_err()) {
            failures.fetch_add(1, std::memory_order_relaxed);
            co_return;
        }
        db::Connection* connection = connect_result.value();
        mem::BumpArena arena(allocator, 64 * 1024);
        for (int i = 0; i < kQueriesPerPair; ++i) {
            mem::ScopedAllocator scope(arena);
            auto result = co_await connection->query("SELECT 1", scope);
            if (result.is_err() || !result.value().has_rows ||
                result.value().rows.size() != 1) {
                failures.fetch_add(1, std::memory_order_relaxed);
                break;
            }
            completed.fetch_add(1, std::memory_order_relaxed);
        }
        connection->close();
        connection->destroy(allocator);
        driver->destroy(allocator);
        co_return;
    };

    const std::uint64_t total = static_cast<std::uint64_t>(kPairs) * kQueriesPerPair;
    const double qps = ::sb::test::measure_ops_per_second(total, [&] {
        for (auto& server : servers) runtime.spawn(server.run());
        for (auto& server : servers) runtime.spawn(client(server.port()));
        runtime.wait();
    });
    std::println("  MySQL concurrent point-select: {:.0f} qps ({} connections, "
                 "{:.0f} ns/query aggregate)", qps, kPairs,
                 qps > 0.0 ? 1e9 / qps : 0.0);
    CHECK_EQ(completed.load(), static_cast<int>(total));
    CHECK_EQ(failures.load(), 0);
    CHECK(qps > 100000.0);
    runtime.shutdown();
}

int main(int argc, char** argv) {
    mem::GlobalAllocator::instance().initialize(1024ull * 1024 * 1024);
    return ::sb::test::run_all(argc, argv);
}
