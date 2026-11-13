#include <print>
#include "test_framework.hpp"

#include "sysbench/lua.hpp"
#include "sysbench/memory.hpp"
#include "sysbench/mysql.hpp"
#include "sysbench/options.hpp"
#include "sysbench/runtime.hpp"
#include "sysbench/socket_io.hpp"

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
#include <vector>

using namespace sb;

namespace {

class MockServer {
public:
    MockServer() {
        m_listen_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (m_listen_fd < 0) return;
        int one = 1;
        ::setsockopt(m_listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::bind(m_listen_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
            ::listen(m_listen_fd, 4) != 0) {
            ::close(m_listen_fd);
            m_listen_fd = -1;
            return;
        }
        socklen_t length = sizeof(address);
        ::getsockname(m_listen_fd, reinterpret_cast<sockaddr*>(&address), &length);
        m_port = ntohs(address.sin_port);
    }
    ~MockServer() {
        if (m_fd >= 0) ::close(m_fd);
        if (m_listen_fd >= 0) ::close(m_listen_fd);
    }
    std::uint16_t port() const { return m_port; }
    bool started() const { return m_listen_fd >= 0; }
    int queries() const { return m_queries.load(); }
    int prepares() const { return m_prepares.load(); }
    int executes() const { return m_executes.load(); }

    Task<void> run() {
        SocketIo io(*Runtime::current(), Runtime::current_worker_id(), 64, 4, 4096);
        m_io = &io;
        sockaddr_in peer{};
        socklen_t length = sizeof(peer);
        m_fd = co_await ring_accept(io, m_listen_fd,
                                   reinterpret_cast<sockaddr*>(&peer), &length);
        if (m_fd < 0) co_return;
        int one = 1;
        ::setsockopt(m_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        if (!co_await send_handshake()) co_return;
        std::vector<std::uint8_t> packet;
        if (!co_await read_packet(packet)) co_return;
        if (!co_await send_packet(ok(), 2)) co_return;
        while (co_await read_packet(packet)) {
            if (packet.empty()) co_return;
            switch (packet[0]) {
                case 0x03: {
                    m_queries.fetch_add(1);
                    const std::string_view sql(reinterpret_cast<const char*>(packet.data() + 1),
                                               packet.size() - 1);
                    if (sql.find("INSERT") != std::string_view::npos) {
                        if (!co_await send_packet(ok(1), 1)) co_return;
                    } else if (!co_await send_text_result("1")) {
                        co_return;
                    }
                    break;
                }
                case 0x16:
                    m_prepares.fetch_add(1);
                    if (!co_await handle_prepare()) co_return;
                    break;
                case 0x17:
                    m_executes.fetch_add(1);
                    if (!co_await handle_execute()) co_return;
                    break;
                case 0x19: break;
                case 0x01: co_return;
                default: co_return;
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
        payload.resize(header[0] | (header[1] << 8) | (header[2] << 16));
        co_return payload.empty() || co_await recv_all(payload.data(), payload.size());
    }
    Task<bool> send_packet(const std::vector<std::uint8_t>& payload,
                           std::uint8_t sequence) {
        const std::uint32_t length = static_cast<std::uint32_t>(payload.size());
        std::vector<std::uint8_t> packet;
        packet.reserve(4 + payload.size());
        packet.push_back(static_cast<std::uint8_t>(length & 0xff));
        packet.push_back(static_cast<std::uint8_t>((length >> 8) & 0xff));
        packet.push_back(static_cast<std::uint8_t>((length >> 16) & 0xff));
        packet.push_back(sequence);
        packet.insert(packet.end(), payload.begin(), payload.end());
        co_return co_await send_all(packet.data(), packet.size());
    }
    static void u16(std::vector<std::uint8_t>& out, std::uint16_t value) {
        out.push_back(static_cast<std::uint8_t>(value));
        out.push_back(static_cast<std::uint8_t>(value >> 8));
    }
    static void u32(std::vector<std::uint8_t>& out, std::uint32_t value) {
        for (int i = 0; i < 4; ++i) out.push_back(static_cast<std::uint8_t>(value >> (i * 8)));
    }
    static void lenenc(std::vector<std::uint8_t>& out, std::uint64_t value) {
        if (value < 251) out.push_back(static_cast<std::uint8_t>(value));
        else if (value <= 0xffff) {
            out.push_back(0xfc);
            u16(out, static_cast<std::uint16_t>(value));
        } else {
            out.push_back(0xfd);
            out.push_back(static_cast<std::uint8_t>(value));
            out.push_back(static_cast<std::uint8_t>(value >> 8));
            out.push_back(static_cast<std::uint8_t>(value >> 16));
        }
    }
    static void str(std::vector<std::uint8_t>& out, std::string_view value) {
        lenenc(out, value.size());
        out.insert(out.end(), value.begin(), value.end());
    }
    std::vector<std::uint8_t> ok(std::uint64_t affected = 0) {
        std::vector<std::uint8_t> packet{0x00};
        lenenc(packet, affected);
        lenenc(packet, 0);
        u16(packet, 2);
        u16(packet, 0);
        return packet;
    }
    std::vector<std::uint8_t> eof() {
        std::vector<std::uint8_t> packet{0xfe};
        u16(packet, 0);
        u16(packet, 2);
        return packet;
    }
    std::vector<std::uint8_t> column(std::string_view name, std::uint8_t type) {
        std::vector<std::uint8_t> packet;
        str(packet, "def"); str(packet, "test"); str(packet, "t"); str(packet, "t");
        str(packet, name); str(packet, name);
        lenenc(packet, 12); u16(packet, 45); u32(packet, 255);
        packet.push_back(type); u16(packet, 0); packet.push_back(0); u16(packet, 0);
        return packet;
    }
    // Batch one response into a single write, matching a real server's net
    // buffer flush.
    static void append(std::vector<std::uint8_t>& out,
                       const std::vector<std::uint8_t>& payload, std::uint8_t sequence) {
        const std::uint32_t length = static_cast<std::uint32_t>(payload.size());
        out.push_back(static_cast<std::uint8_t>(length & 0xff));
        out.push_back(static_cast<std::uint8_t>((length >> 8) & 0xff));
        out.push_back(static_cast<std::uint8_t>((length >> 16) & 0xff));
        out.push_back(sequence);
        out.insert(out.end(), payload.begin(), payload.end());
    }
    Task<bool> send_batch(const std::vector<std::vector<std::uint8_t>>& packets) {
        std::vector<std::uint8_t> out;
        std::uint8_t sequence = 1;
        for (const auto& packet : packets) append(out, packet, sequence++);
        co_return co_await send_all(out.data(), out.size());
    }
    Task<bool> send_text_result(std::string_view value) {
        std::vector<std::uint8_t> row;
        str(row, value);
        co_return co_await send_batch({{1}, column("1", 253), eof(), row, eof()});
    }
    Task<bool> send_handshake() {
        std::vector<std::uint8_t> packet;
        packet.reserve(128);
        packet.push_back(10);
        const std::string_view version = "8.0.36-mock";
        packet.insert(packet.end(), version.begin(), version.end());
        packet.push_back(0);
        u32(packet, 7);
        const char auth1[] = "abcdefgh";
        packet.insert(packet.end(), auth1, auth1 + 8);
        packet.push_back(0);
        const std::uint32_t caps = 0x00000200 | 0x00008000 | 0x00080000 | 0x00200000 | 0x00020000;
        u16(packet, static_cast<std::uint16_t>(caps & 0xffff));
        packet.push_back(45); u16(packet, 2);
        u16(packet, static_cast<std::uint16_t>(caps >> 16));
        packet.push_back(21);
        for (int i = 0; i < 10; ++i) packet.push_back(0);
        for (char character : std::string_view("ijklmnopqrst")) {
            packet.push_back(static_cast<std::uint8_t>(character));
        }
        packet.push_back(0);
        const std::string_view plugin = "mysql_native_password";
        packet.insert(packet.end(), plugin.begin(), plugin.end());
        packet.push_back(0);
        co_return co_await send_packet(packet, 0);
    }
    Task<bool> handle_prepare() {
        std::vector<std::uint8_t> prepare{0x00};
        u32(prepare, 1); u16(prepare, 1); u16(prepare, 1);
        prepare.push_back(0); u16(prepare, 0);
        co_return co_await send_batch({prepare, column("p", 3), eof(),
                                       column("v", 8), eof()});
    }
    Task<bool> handle_execute() {
        std::vector<std::uint8_t> row{0x00, 0x00};
        const std::uint64_t value = 42;
        for (int i = 0; i < 8; ++i) row.push_back(static_cast<std::uint8_t>(value >> (i * 8)));
        co_return co_await send_batch({{1}, column("v", 8), eof(), row, eof()});
    }

    int m_listen_fd = -1;
    int m_fd = -1;
    std::uint16_t m_port = 0;
    SocketIo* m_io = nullptr;
    std::atomic<int> m_queries{0};
    std::atomic<int> m_prepares{0};
    std::atomic<int> m_executes{0};
};

LuaRunOptions make_run_options(std::uint16_t port, std::uint64_t threads,
                               std::uint64_t events, std::uint64_t seconds) {
    LuaRunOptions result;
    result.threads = static_cast<std::uint32_t>(threads);
    result.time_seconds = seconds;
    result.events = events;
    result.report_interval = 0;
    result.percentile = 95;
    result.database.host = "127.0.0.1";
    static thread_local std::string port_string;
    port_string = std::to_string(port);
    result.database.port = port_string;
    result.database.user = "root";
    result.database.password = "password";
    return result;
}

Runtime::Config runtime_config() {
    return Runtime::Config{.workers = 1,
                           .worker_heap_bytes = 8 * 1024 * 1024};
}

}  // namespace

TEST("Lua SQL API exercises text, prepared and bulk paths") {
    MockServer server;
    REQUIRE(server.started());
    mysql::register_driver();

    mem::RootArena root(512 * 1024 * 1024);
    mem::HeapArena main_heap(root, 64 * 1024 * 1024);
    Options options(main_heap);
    mem::String error{mem::Allocator<char>(&main_heap)};
    LuaEngine engine(main_heap, options, "tests/fixtures/lua_sql_test.lua", "run",
                     0, nullptr);
    CHECK(engine.load(error));
    CHECK(engine.collect_options(error));
    CHECK_EQ(options.as_int("test_option"), 3);
    CHECK(options.as_bool("flag_option"));
    CHECK_EQ(options.as_string("string_option"), "abc");
    CHECK(engine.install_options(error));

    Runtime runtime;
    runtime.start(runtime_config());
    runtime.spawn(server.run());
    LuaRunOptions run_options = make_run_options(server.port(), 1, 10, 5);
    const int result = engine.run(runtime, run_options);
    runtime.shutdown();
    CHECK_EQ(result, 0);
    CHECK_EQ(server.queries(), 20);   // 10 SELECT + 10 bulk INSERT
    CHECK_EQ(server.prepares(), 1);
    CHECK_EQ(server.executes(), 10);
}


TEST("Lua SQL allocations stay bounded across events") {
    MockServer server;
    REQUIRE(server.started());
    mysql::register_driver();

    mem::RootArena root(256 * 1024 * 1024);
    mem::HeapArena main_heap(root, 64 * 1024 * 1024);
    Options options(main_heap);
    mem::String error{mem::Allocator<char>(&main_heap)};
    LuaEngine engine(main_heap, options, "tests/fixtures/lua_sql_test.lua", "run",
                     0, nullptr);
    CHECK(engine.load(error));
    CHECK(engine.collect_options(error));
    CHECK(engine.install_options(error));

    Runtime runtime;
    runtime.start(Runtime::Config{.workers = 1,
                                  .worker_heap_bytes = 8 * 1024 * 1024});
    runtime.spawn(server.run());
    LuaRunOptions run_options = make_run_options(server.port(), 1, 2000, 20);
    CHECK_EQ(engine.run(runtime, run_options), 0);
    const auto stats = runtime.worker_heap_stats(0);
    CHECK(stats.used < 4 * 1024 * 1024);
    CHECK_EQ(server.executes(), 2000);
    runtime.shutdown();
}

TEST("Lua CPU benchmark and random API") {
    mem::RootArena root(256 * 1024 * 1024);
    mem::HeapArena main_heap(root, 64 * 1024 * 1024);
    Options options(main_heap);
    mem::String error{mem::Allocator<char>(&main_heap)};
    LuaEngine engine(main_heap, options, "tests/fixtures/lua_cpu_test.lua", "run",
                     0, nullptr);
    CHECK(engine.load(error));
    CHECK(engine.collect_options(error));
    CHECK(engine.install_options(error));

    Runtime runtime;
    runtime.start(runtime_config());
    LuaRunOptions run_options;
    run_options.threads = 2;
    run_options.events = 1000;
    run_options.time_seconds = 5;
    run_options.report_interval = 0;
    const int result = engine.run(runtime, run_options);
    runtime.shutdown();
    CHECK_EQ(result, 0);
}

TEST("Lua run_command dispatches prepare and cleanup") {
    MockServer server;
    REQUIRE(server.started());
    mysql::register_driver();

    mem::RootArena root(256 * 1024 * 1024);
    mem::HeapArena main_heap(root, 64 * 1024 * 1024);
    Options options(main_heap);
    mem::String error{mem::Allocator<char>(&main_heap)};
    LuaEngine engine(main_heap, options, "tests/fixtures/lua_sql_test.lua", "prepare",
                     0, nullptr);
    CHECK(engine.load(error));
    CHECK(engine.collect_options(error));
    CHECK(engine.install_options(error));

    Runtime runtime;
    runtime.start(runtime_config());
    runtime.spawn(server.run());
    LuaRunOptions run_options = make_run_options(server.port(), 1, 0, 1);
    const int result = engine.run_command(runtime, run_options, "prepare", false);
    runtime.shutdown();
    CHECK_EQ(result, 0);
}


TEST("Lua event throughput") {
    if (!::sb::test::perf_enabled()) return;
    mem::RootArena root(256 * 1024 * 1024);
    mem::HeapArena main_heap(root, 64 * 1024 * 1024);
    Options options(main_heap);
    mem::String error{mem::Allocator<char>(&main_heap)};
    LuaEngine engine(main_heap, options, "tests/fixtures/lua_cpu_test.lua", "run",
                     0, nullptr);
    CHECK(engine.load(error));
    CHECK(engine.collect_options(error));
    CHECK(options.set("local_iters", "1", error));
    CHECK(engine.install_options(error));

    Runtime runtime;
    runtime.start(Runtime::Config{.workers = 4,
                                  .worker_heap_bytes = 8 * 1024 * 1024});
    LuaRunOptions run_options;
    run_options.threads = 4;
    run_options.events = 200'000;
    run_options.time_seconds = 20;
    run_options.report_interval = 0;
    const double throughput = ::sb::test::measure_ops_per_second(
        run_options.events, [&] {
            const int result = engine.run(runtime, run_options);
            CHECK_EQ(result, 0);
        });
    std::println("  Lua events: {:.1f} Mevents/s", throughput / 1e6);
    runtime.shutdown();
    CHECK(throughput > 200'000.0);
}

int main(int argc, char** argv) {
    mem::GlobalAllocator::instance().initialize(1024ull * 1024 * 1024);
    return ::sb::test::run_all(argc, argv);
}
