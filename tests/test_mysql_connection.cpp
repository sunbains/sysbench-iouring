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
#include "sysbench/lua.hpp"
#include "sysbench/options.hpp"
#include "sysbench/runtime.hpp"
#include "sysbench/socket_io.hpp"
#include "sysbench/task.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#include <openssl/ssl.h>

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

enum class MockAuth { Native, CachingSha2Fast, CachingSha2Full };

// Scripted MySQL server.  It is a plain Runtime coroutine: every socket
// operation is an io_uring awaiter, so the fixture owns no threads and never
// blocks a worker.
class MockMysqlServer {
public:
    explicit MockMysqlServer(MockAuth auth = MockAuth::Native, bool v2 = false,
                             bool deprecate_eof = true, unsigned sessions = 1,
                             bool tls = false, bool mutual_tls = false)
        : m_auth(auth), m_v2(v2), m_deprecate_eof(v2 && deprecate_eof),
          m_sessions(sessions), m_tls(tls), m_mutual_tls(mutual_tls) {
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
        close_tls();
        if (m_fd >= 0) ::close(m_fd);
        if (m_listen_fd >= 0) ::close(m_listen_fd);
    }

    [[nodiscard]] std::uint16_t port() const { return m_port; }
    [[nodiscard]] bool started() const { return m_listen_fd >= 0; }
    [[nodiscard]] unsigned v2_sessions() const { return m_v2_sessions; }
    [[nodiscard]] unsigned tls_sessions() const { return m_tls_sessions; }

    Task<void> run() {
        for (unsigned i = 0; i < m_sessions; ++i) {
            co_await run_session();
            close_tls();
            if (m_fd >= 0) ::close(m_fd);
            m_fd = -1;
        }
    }

    Task<void> run_session() {
        SocketIo io(*Runtime::current(), Runtime::current_worker_id(), 4, 4096);
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
        if (payload.size() == 32 && m_tls) {
            if (!co_await accept_tls()) co_return;
            ++m_tls_sessions;
            if (!co_await read_packet(payload)) co_return;
        }
        if (m_v2) {
            const std::string_view response(reinterpret_cast<const char*>(payload.data()), payload.size());
            const std::string_view attribute("\x10sqlrust_protocol\x01" "2", 19);
            CHECK(response.find(attribute) != std::string_view::npos);
            if (response.find(attribute) != std::string_view::npos) ++m_v2_sessions;
        }
        if (m_auth == MockAuth::CachingSha2Full) {
            if (!co_await send_packet({1, 4}, 3)) co_return;
            if (!co_await read_packet(payload)) co_return;
            CHECK(m_ssl != nullptr);
            CHECK_EQ(std::string_view(reinterpret_cast<const char*>(payload.data()), payload.size()),
                     std::string_view("password\0", 9));
            if (!co_await send_packet(ok_packet(), 5)) co_return;
        } else if (m_auth == MockAuth::CachingSha2Fast) {
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
                    if (!co_await handle_prepare(payload)) co_return;
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
    void close_tls() {
        SSL_free(std::exchange(m_ssl, nullptr));
        SSL_CTX_free(std::exchange(m_ssl_ctx, nullptr));
        m_rbio = nullptr;
        m_wbio = nullptr;
    }
    Task<bool> tls_flush() {
        std::uint8_t buffer[4096];
        while (BIO_ctrl_pending(m_wbio) > 0) {
            const int size = BIO_read(m_wbio, buffer, sizeof(buffer));
            if (size <= 0 || !co_await raw_send_all(buffer, static_cast<std::size_t>(size))) co_return false;
        }
        co_return true;
    }
    Task<bool> tls_feed() {
        std::uint8_t buffer[4096];
        const int size = co_await ring_recv(*m_io, m_fd, buffer, sizeof(buffer), 0);
        co_return size > 0 && BIO_write(m_rbio, buffer, size) == size;
    }
    Task<bool> accept_tls() {
#ifdef SYSBENCH_HAVE_MYSQL_V2
        m_ssl_ctx = SSL_CTX_new(TLS_server_method());
        if (m_ssl_ctx == nullptr) co_return false;
        SSL_CTX_set_min_proto_version(m_ssl_ctx, TLS1_2_VERSION);
        if (SSL_CTX_use_certificate_chain_file(m_ssl_ctx,
                SYSBENCH_MYSQL_CLIENT_LIB_DIR "/tests/fixtures/server.pem") != 1 ||
            SSL_CTX_use_PrivateKey_file(m_ssl_ctx,
                SYSBENCH_MYSQL_CLIENT_LIB_DIR "/tests/fixtures/server-key.pem", SSL_FILETYPE_PEM) != 1) {
            co_return false;
        }
        if (m_mutual_tls) {
            if (SSL_CTX_load_verify_locations(m_ssl_ctx,
                SYSBENCH_MYSQL_CLIENT_LIB_DIR "/tests/fixtures/ca.pem", nullptr) != 1) co_return false;
            SSL_CTX_set_verify(m_ssl_ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
        }
        m_ssl = SSL_new(m_ssl_ctx);
        if (m_ssl == nullptr) co_return false;
        m_rbio = BIO_new(BIO_s_mem());
        m_wbio = BIO_new(BIO_s_mem());
        SSL_set_bio(m_ssl, m_rbio, m_wbio);
        SSL_set_accept_state(m_ssl);
        for (;;) {
            const int result = SSL_do_handshake(m_ssl);
            const int error = result == 1 ? SSL_ERROR_NONE : SSL_get_error(m_ssl, result);
            if (!co_await tls_flush()) co_return false;
            if (result == 1) co_return true;
            if (error == SSL_ERROR_WANT_READ) {
                if (!co_await tls_feed()) co_return false;
            } else if (error != SSL_ERROR_WANT_WRITE) {
                co_return false;
            }
        }
#else
        co_return false;
#endif
    }
    Task<bool> recv_all(void* data, std::size_t size) {
        if (m_ssl == nullptr) co_return co_await raw_recv_all(data, size);
        auto* bytes = static_cast<std::uint8_t*>(data);
        while (size != 0) {
            const int got = SSL_read(m_ssl, bytes, static_cast<int>(size));
            if (got > 0) {
                bytes += got;
                size -= static_cast<std::size_t>(got);
            } else {
                const int error = SSL_get_error(m_ssl, got);
                if (!co_await tls_flush()) co_return false;
                if (error == SSL_ERROR_WANT_READ) {
                    if (!co_await tls_feed()) co_return false;
                } else if (error != SSL_ERROR_WANT_WRITE) {
                    co_return false;
                }
            }
        }
        co_return true;
    }
    Task<bool> send_all(const void* data, std::size_t size) {
        if (m_ssl == nullptr) co_return co_await raw_send_all(data, size);
        const auto* bytes = static_cast<const std::uint8_t*>(data);
        while (size != 0) {
            const int sent = SSL_write(m_ssl, bytes, static_cast<int>(size));
            if (sent > 0) {
                bytes += sent;
                size -= static_cast<std::size_t>(sent);
                if (!co_await tls_flush()) co_return false;
            } else {
                const int error = SSL_get_error(m_ssl, sent);
                if (!co_await tls_flush()) co_return false;
                if (error == SSL_ERROR_WANT_READ) {
                    if (!co_await tls_feed()) co_return false;
                } else if (error != SSL_ERROR_WANT_WRITE) {
                    co_return false;
                }
            }
        }
        co_return true;
    }
    Task<bool> raw_recv_all(void* data, std::size_t size) {
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

    Task<bool> raw_send_all(const void* data, std::size_t size) {
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
        if (m_v2 && sql.find("RETURNING") != std::string_view::npos) {
            auto final_ok = ok_packet(2);
            final_ok[2] = 41; // last_insert_id
            final_ok[5] = 3;  // warnings
            std::vector<std::vector<std::uint8_t>> response{
                {3}, column_packet("c", 253), column_packet("id", 8),
                column_packet("nullable", 253)};
            if (!m_deprecate_eof) response.push_back(eof_packet());
            response.push_back({0, 2, '4', '1', 0xfb}); // empty string and NULL
            response.push_back({2, '4', '1', 2, '4', '2', 1, 'x'});
            if (m_deprecate_eof) {
                final_ok[0] = 0xfe;
            } else {
                auto more = eof_packet();
                more[3] = 0x0a; // SERVER_MORE_RESULTS_EXISTS
                response.push_back(more);
            }
            response.push_back(final_ok);
            co_return co_await send_response(response);
        }
        if (m_v2 && sql == "FAIL") {
            co_return co_await send_packet({0xff, 0xbd, 0x04, '#', '4', '0', '0', '0', '1',
                                           'd', 'e', 'a', 'd', 'l', 'o', 'c', 'k'}, 1);
        }
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

    Task<bool> handle_prepare(const std::vector<std::uint8_t>& payload) {
        const std::string_view sql(reinterpret_cast<const char*>(payload.data() + 1), payload.size() - 1);
        m_returning_prepared = m_v2 && sql.find("RETURNING") != std::string_view::npos;
        if (m_v2 && sql == "BEGIN") {
            co_return co_await send_packet({0xff, 0x0f, 0x05, '#', 'H', 'Y', '0', '0', '0',
                                           'u', 'n', 's', 'u', 'p', 'p', 'o', 'r', 't', 'e', 'd'}, 1);
        }
        co_return co_await send_all(m_prepare_response.data(),
                                    m_prepare_response.size());
    }

    Task<bool> handle_execute() {
        if (m_returning_prepared) {
            std::vector<std::vector<std::uint8_t>> response{{1}, column_packet("id", 8)};
            if (!m_deprecate_eof) response.push_back(eof_packet());
            response.push_back({0, 0, 42, 0, 0, 0, 0, 0, 0, 0});
            auto final_ok = ok_packet(1);
            final_ok[2] = 42;
            if (m_deprecate_eof) {
                final_ok[0] = 0xfe;
            } else {
                auto more = eof_packet();
                more[3] = 0x0a;
                response.push_back(more);
            }
            response.push_back(final_ok);
            co_return co_await send_response(response);
        }
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
            0x00000200 | 0x00008000 | 0x00080000 | 0x00200000 | 0x00020000 |
            (m_v2 ? 0x00100000 | 0x00040000 : 0) |
            (m_tls ? 0x00000800 : 0) |
            (m_deprecate_eof ? 0x01000000 : 0);
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
        const std::string_view plugin = m_auth != MockAuth::Native
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
        auto terminator = eof_packet();
        if (m_deprecate_eof) {
            terminator = ok_packet();
            terminator[0] = 0xfe;
            build(m_select_response, {count, column_packet("1", 253), row, terminator});
        } else {
            build(m_select_response, {count, column_packet("1", 253), eof_packet(), row, terminator});
        }
        build(m_ok_response, {ok_packet(1)});
        std::vector<std::uint8_t> prepare_ok{0x00};
        put_u32(prepare_ok, 1);
        put_u16(prepare_ok, 1);
        put_u16(prepare_ok, 1);
        prepare_ok.push_back(0);
        put_u16(prepare_ok, 0);
        if (m_deprecate_eof) {
            build(m_prepare_response, {prepare_ok, column_packet("id", 3), column_packet("v", 8)});
        } else {
            build(m_prepare_response, {prepare_ok, column_packet("id", 3), eof_packet(),
                                       column_packet("v", 8), eof_packet()});
        }
        std::vector<std::uint8_t> row8{0x00, 0x00};
        const std::uint64_t value = 42;
        for (int i = 0; i < 8; ++i) {
            row8.push_back(static_cast<std::uint8_t>(value >> (i * 8)));
        }
        if (m_deprecate_eof) {
            terminator = ok_packet(1);
            terminator[0] = 0xfe;
            build(m_execute_response, {{1}, column_packet("v", 8), row8, terminator});
        } else {
            build(m_execute_response, {{1}, column_packet("v", 8), eof_packet(), row8, eof_packet()});
        }
    }

    MockAuth m_auth;
    bool m_v2, m_deprecate_eof;
    unsigned m_sessions;
    unsigned m_v2_sessions = 0;
    bool m_tls, m_mutual_tls;
    unsigned m_tls_sessions = 0;
    SSL_CTX* m_ssl_ctx = nullptr;
    SSL* m_ssl = nullptr;
    BIO* m_rbio = nullptr;
    BIO* m_wbio = nullptr;
    bool m_returning_prepared = false;
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
                       std::atomic<bool>& ok, std::uint32_t protocol_version = 1,
                       bool tls = false, bool client_certificate = false) {
    auto& allocator = Runtime::current_allocator();
    db::Driver* driver = db::DriverRegistry::instance().create("mysql", allocator);
    if (driver == nullptr) co_return;

    const std::string port_string = std::to_string(port);
    db::ConnectionOptions options;
    options.m_host = "127.0.0.1";
    options.m_port = port_string;
    options.m_user = "bench";
    options.m_password = "password";
    options.m_protocol_version = protocol_version;
#ifdef SYSBENCH_HAVE_MYSQL_V2
    if (tls) {
        options.m_tls_mode = db::TlsMode::Required;
        options.m_tls_verify_server_cert = true;
        options.m_tls_ca = SYSBENCH_MYSQL_CLIENT_LIB_DIR "/tests/fixtures/ca.pem";
        if (client_certificate) {
            options.m_tls_cert = SYSBENCH_MYSQL_CLIENT_LIB_DIR "/tests/fixtures/client.pem";
            options.m_tls_key = SYSBENCH_MYSQL_CLIENT_LIB_DIR "/tests/fixtures/client-key.pem";
        }
    }
#else
    (void)tls;
    (void)client_certificate;
#endif

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
            ok.store(rows.m_has_rows && rows.m_columns.size() == 1 &&
                         rows.m_rows.size() == 1 &&
                         rows.m_rows[0].m_fields.size() == 1 &&
                         !rows.m_rows[0].m_fields[0].m_is_null &&
                         std::string_view(rows.m_rows[0].m_fields[0].m_data,
                                          rows.m_rows[0].m_fields[0].m_length) == "1",
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
            ok.store(ok.load() && !rows.m_has_rows && rows.m_affected_rows == 1,
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
                ok.store(ok.load() && rows.m_has_rows && rows.m_rows.size() == 1 &&
                             rows.m_rows[0].m_fields.size() == 1 &&
                             std::string_view(rows.m_rows[0].m_fields[0].m_data,
                                              rows.m_rows[0].m_fields[0].m_length) == "42",
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
    runtime.start(Runtime::Config{.m_workers = 1,
                                  .m_worker_heap_bytes = 8 * 1024 * 1024});
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
    runtime.start(Runtime::Config{.m_workers = 1,
                                  .m_worker_heap_bytes = 8 * 1024 * 1024});
    std::atomic<int> stages{0};
    std::atomic<bool> ok{true};
    runtime.spawn(server.run());
    runtime.spawn(client_flow(server.port(), stages, ok));
    runtime.wait();
    CHECK_EQ(stages.load(), 5);
    CHECK(ok.load());
    runtime.shutdown();
}

#ifdef SYSBENCH_HAVE_MYSQL_V2
namespace {

std::string_view field_text(const db::Field& field) {
    return {field.m_data, field.m_length};
}

Task<void> returning_flow(std::uint16_t port, bool reconnect, std::atomic<bool>& complete,
                          bool tls = false) {
    auto& allocator = Runtime::current_allocator();
    auto* driver = db::DriverRegistry::instance().create("mysql", allocator);
    const auto port_string = std::to_string(port);
    db::ConnectionOptions options;
    options.m_port = port_string;
    options.m_protocol_version = 2;
    if (tls) {
        options.m_tls_mode = db::TlsMode::Required;
        options.m_tls_verify_server_cert = true;
        options.m_tls_ca = SYSBENCH_MYSQL_CLIENT_LIB_DIR "/tests/fixtures/ca.pem";
    }
    auto connected = co_await driver->connect(options, allocator);
    CHECK(connected.is_ok());
    if (connected.is_err()) {
        std::println(stderr, "v2 connect failed: {}", connected.error().message());
        driver->destroy(allocator);
        co_return;
    }
    auto* connection = connected.value();
    CHECK_EQ(connection->driver_name(), "mysql");
    mem::BumpArena arena(allocator, 128 * 1024);
    {
        auto query = co_await connection->query(
            "INSERT INTO t(c) VALUES (''), ('41') RETURNING c, id, nullable", arena);
        CHECK(query.is_ok());
        if (query.is_ok()) {
            const auto& result = query.value();
            CHECK(result.m_has_rows);
            CHECK(!result.m_more_results);
            CHECK_EQ(result.m_columns.size(), 3u);
            CHECK_EQ(result.m_rows.size(), 2u);
            CHECK_EQ(result.m_affected_rows, 2u);
            CHECK_EQ(result.m_last_insert_id, 41u);
            CHECK_EQ(result.m_warnings, 3);
            CHECK_EQ(result.m_status, 2);
            if (result.m_rows.size() == 2 && result.m_columns.size() == 3) {
                CHECK_EQ(result.m_columns[0].m_name, "c");
                CHECK_EQ(field_text(result.m_rows[0].m_fields[0]), "");
                CHECK(!result.m_rows[0].m_fields[0].m_is_null);
                CHECK(result.m_rows[0].m_fields[2].m_is_null);
                CHECK_EQ(field_text(result.m_rows[1].m_fields[0]), "41");
                CHECK_EQ(field_text(result.m_rows[1].m_fields[1]), "42");
            }
        } else {
            std::println(stderr, "v2 RETURNING failed: {}", query.error().message());
        }
    }
    // The next command must see its own response after all RETURNING results
    // have been drained, including the trailing OK used by EOF-style servers.
    {
        auto query = co_await connection->query("SELECT 1", arena);
        CHECK(query.is_ok());
        if (query.is_ok()) CHECK_EQ(query.value().m_rows.size(), 1u);
    }
    {
        auto query = co_await connection->query("FAIL", arena);
        CHECK(query.is_err());
        if (query.is_err()) {
            CHECK_EQ(query.error().code(), 1213);
            CHECK_EQ(std::string_view(query.error().state()), "40001");
            CHECK_EQ(std::string_view(query.error().message()), "deadlock");
            CHECK(!query.error().fatal());
        }
        CHECK(connection->healthy());
    }
    auto prepared = co_await connection->prepare("INSERT INTO t(c) VALUES (?) RETURNING id");
    CHECK(prepared.is_ok());
    db::PreparedStatement* statement = prepared.is_ok() ? prepared.value() : nullptr;
    if (statement != nullptr) {
        const db::BindValue parameters[] = {db::BindValue::from_bytes("value")};
        auto query = co_await statement->execute(parameters, arena);
        CHECK(query.is_ok());
        if (query.is_ok()) {
            CHECK_EQ(query.value().m_rows.size(), 1u);
            CHECK_EQ(query.value().m_affected_rows, 1u);
            CHECK_EQ(query.value().m_last_insert_id, 42u);
            if (!query.value().m_rows.empty()) {
                CHECK_EQ(field_text(query.value().m_rows[0].m_fields[0]), "42");
            }
        }
    }
    {
        auto begin = co_await connection->prepare("BEGIN");
        CHECK(begin.is_ok());
        if (begin.is_ok()) {
            auto query = co_await begin.value()->execute({}, arena);
            CHECK(query.is_ok());
            CHECK((co_await begin.value()->close()).is_ok());
            begin.value()->destroy(allocator);
        }
    }
    if (reconnect) {
        CHECK((co_await connection->reconnect()).is_ok());
        CHECK(connection->healthy());
        if (statement != nullptr) {
            auto stale = co_await statement->execute({}, arena);
            CHECK(stale.is_err());
            if (stale.is_err()) CHECK_EQ(stale.error().code(), 1243);
        }
        auto query = co_await connection->query("SELECT 1", arena);
        CHECK(query.is_ok());
    }
    if (statement != nullptr) {
        CHECK((co_await statement->close()).is_ok());
        CHECK((co_await statement->close()).is_ok());
        CHECK(statement->closed());
        statement->destroy(allocator);
    }
    connection->close();
    connection->destroy(allocator);
    driver->destroy(allocator);
    complete = true;
}

Task<void> rejected_v2_flow(std::uint16_t port, db::TlsMode tls, int expected_error,
                            std::atomic<bool>& complete, bool verify_certificate = false) {
    auto& allocator = Runtime::current_allocator();
    auto* driver = db::DriverRegistry::instance().create("mysql", allocator);
    const auto port_string = std::to_string(port);
    db::ConnectionOptions options;
    options.m_port = port_string;
    options.m_protocol_version = 2;
    options.m_tls_mode = tls;
    options.m_tls_verify_server_cert = verify_certificate;
    auto connected = co_await driver->connect(options, allocator);
    CHECK(connected.is_err());
    if (connected.is_err()) CHECK_EQ(connected.error().code(), expected_error);
    if (connected.is_ok()) connected.value()->destroy(allocator);
    driver->destroy(allocator);
    complete = true;
}

}  // namespace

TEST("MySQL v2 Rust client handshake, text and prepared protocols") {
    mysql::register_driver();
    for (const auto auth : {MockAuth::Native, MockAuth::CachingSha2Fast}) {
        MockMysqlServer server(auth, true);
        REQUIRE(server.started());
        Runtime runtime;
        runtime.start(Runtime::Config{.m_workers = 1, .m_worker_heap_bytes = 8 * 1024 * 1024});
        std::atomic<int> stages{0};
        std::atomic<bool> ok{true};
        runtime.spawn(server.run());
        runtime.spawn(client_flow(server.port(), stages, ok, 2));
        runtime.wait();
        CHECK_EQ(stages.load(), 5);
        CHECK_EQ(server.v2_sessions(), 1u);
        CHECK(ok.load());
        runtime.shutdown();
    }
}

TEST("MySQL v2 RETURNING rows, counts and result draining") {
    mysql::register_driver();
    for (bool deprecate_eof : {true, false}) {
        MockMysqlServer server(MockAuth::Native, true, deprecate_eof);
        REQUIRE(server.started());
        Runtime runtime;
        runtime.start(Runtime::Config{.m_workers = 1, .m_worker_heap_bytes = 8 * 1024 * 1024});
        std::atomic<bool> complete{false};
        runtime.spawn(server.run());
        runtime.spawn(returning_flow(server.port(), false, complete));
        runtime.wait();
        CHECK(complete.load());
        CHECK_EQ(server.v2_sessions(), 1u);
        runtime.shutdown();
    }
}

TEST("MySQL v2 interleaves sessions on one worker and renegotiates on reconnect") {
    mysql::register_driver();
    MockMysqlServer first(MockAuth::Native, true, true, 2);
    MockMysqlServer second(MockAuth::CachingSha2Fast, true, true, 2);
    REQUIRE(first.started() && second.started());
    Runtime runtime;
    runtime.start(Runtime::Config{.m_workers = 1, .m_worker_heap_bytes = 16 * 1024 * 1024});
    std::atomic<bool> first_done{false}, second_done{false};
    runtime.spawn(first.run());
    runtime.spawn(second.run());
    runtime.spawn(returning_flow(first.port(), true, first_done));
    runtime.spawn(returning_flow(second.port(), true, second_done));
    runtime.wait();
    CHECK(first_done.load() && second_done.load());
    CHECK_EQ(first.v2_sessions(), 2u);
    CHECK_EQ(second.v2_sessions(), 2u);
    runtime.shutdown();
}

TEST("MySQL v2 rejects required TLS on plaintext servers and missing connection attributes") {
    mysql::register_driver();
    MockMysqlServer server;
    MockMysqlServer tls_server;
    REQUIRE(server.started() && tls_server.started());
    Runtime runtime;
    runtime.start(Runtime::Config{.m_workers = 1, .m_worker_heap_bytes = 8 * 1024 * 1024});
    std::atomic<bool> tls_done{false}, attrs_done{false};
    runtime.spawn(server.run());
    runtime.spawn(tls_server.run());
    runtime.spawn(rejected_v2_flow(tls_server.port(), db::TlsMode::Required, 2026, tls_done));
    runtime.spawn(rejected_v2_flow(server.port(), db::TlsMode::Disabled, 2027, attrs_done));
    runtime.wait();
    CHECK(tls_done.load() && attrs_done.load());
    runtime.shutdown();
}

TEST("MySQL v2 Rust TLS verifies certificates and supports mutual and full authentication") {
    mysql::register_driver();
    for (const auto auth : {MockAuth::Native, MockAuth::CachingSha2Full}) {
        const bool mutual = auth == MockAuth::Native;
        MockMysqlServer server(auth, true, true, 1, true, mutual);
        REQUIRE(server.started());
        Runtime runtime;
        runtime.start(Runtime::Config{.m_workers = 1, .m_worker_heap_bytes = 8 * 1024 * 1024});
        std::atomic<int> stages{0};
        std::atomic<bool> ok{true};
        runtime.spawn(server.run());
        runtime.spawn(client_flow(server.port(), stages, ok, 2, true, mutual));
        runtime.wait();
        CHECK_EQ(stages.load(), 5);
        CHECK(ok.load());
        CHECK_EQ(server.v2_sessions(), 1u);
        CHECK_EQ(server.tls_sessions(), 1u);
        runtime.shutdown();
    }
}

TEST("MySQL v2 TLS retains RETURNING results and reconnect settings") {
    mysql::register_driver();
    MockMysqlServer server(MockAuth::Native, true, true, 2, true);
    REQUIRE(server.started());
    Runtime runtime;
    runtime.start(Runtime::Config{.m_workers = 1, .m_worker_heap_bytes = 8 * 1024 * 1024});
    std::atomic<bool> complete{false};
    runtime.spawn(server.run());
    runtime.spawn(returning_flow(server.port(), true, complete, true));
    runtime.wait();
    CHECK(complete.load());
    CHECK_EQ(server.v2_sessions(), 2u);
    CHECK_EQ(server.tls_sessions(), 2u);
    runtime.shutdown();
}

TEST("MySQL v2 refuses untrusted TLS certificates without authenticating") {
    mysql::register_driver();
    MockMysqlServer server(MockAuth::Native, true, true, 1, true);
    REQUIRE(server.started());
    Runtime runtime;
    runtime.start(Runtime::Config{.m_workers = 1, .m_worker_heap_bytes = 8 * 1024 * 1024});
    std::atomic<bool> complete{false};
    runtime.spawn(server.run());
    runtime.spawn(rejected_v2_flow(server.port(), db::TlsMode::Required, 2026, complete, true));
    runtime.wait();
    CHECK(complete.load());
    CHECK_EQ(server.v2_sessions(), 0u);
    runtime.shutdown();
}

TEST("MySQL v2 Lua RETURNING preserves rows and statement metadata with bounded allocations") {
    mysql::register_driver();
    for (bool tls : {false, true}) {
        MockMysqlServer server(MockAuth::Native, true, true, 1, tls);
        REQUIRE(server.started());
        mem::RootArena root(256 * 1024 * 1024);
        mem::HeapArena main_heap(root, 64 * 1024 * 1024);
        Options options(main_heap);
        mem::String error{mem::Allocator<char>(&main_heap)};
        LuaEngine engine(main_heap, options, SYSBENCH_LUA_DIR "/../tests/fixtures/lua_mysql_v2_test.lua",
                         "run", 0, nullptr);
        REQUIRE(engine.load(error));
        REQUIRE(engine.collect_options(error));
        REQUIRE(options.set("mysql-protocol", "2", error));
        REQUIRE(engine.install_options(error));
        Runtime runtime;
        runtime.start(Runtime::Config{.m_workers = 1, .m_worker_heap_bytes = 8 * 1024 * 1024});
        runtime.spawn(server.run());
        const auto port_string = std::to_string(server.port());
        LuaRunOptions run_options;
        run_options.m_database.m_port = port_string;
        run_options.m_database.m_protocol_version = 2;
        if (tls) {
            run_options.m_database.m_tls_mode = db::TlsMode::Required;
            run_options.m_database.m_tls_verify_server_cert = true;
            run_options.m_database.m_tls_ca = SYSBENCH_MYSQL_CLIENT_LIB_DIR "/tests/fixtures/ca.pem";
        }
        run_options.m_events = 200;
        run_options.m_time_seconds = 5;
        run_options.m_report_interval = 0;
        CHECK_EQ(engine.run(runtime, run_options), 0);
        CHECK(runtime.worker_heap_stats(0).m_used < 4 * 1024 * 1024);
        CHECK_EQ(server.v2_sessions(), 1u);
        runtime.shutdown();
    }
}
#endif

TEST("MySQL query throughput over a local mock server") {
    if (!::sb::test::perf_enabled()) return;
    MockMysqlServer server(MockAuth::Native);
    REQUIRE(server.started());
    mysql::register_driver();

    Runtime runtime;
    runtime.start(Runtime::Config{.m_workers = 2,
                                  .m_worker_heap_bytes = 8 * 1024 * 1024});

    constexpr int kQueries = 2000;
    std::atomic<int> completed{0};
    std::atomic<bool> ok{true};
    auto throughput_task = [&]() -> Task<void> {
        auto& allocator = Runtime::current_allocator();
        db::Driver* driver = db::DriverRegistry::instance().create("mysql", allocator);
        const std::string port_string = std::to_string(server.port());
        db::ConnectionOptions options;
        options.m_host = "127.0.0.1";
        options.m_port = port_string;
        options.m_user = "bench";
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
            if (result.is_err() || !result.value().m_has_rows ||
                result.value().m_rows.size() != 1) {
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
    runtime.start(Runtime::Config{.m_workers = 2 * kPairs,
                                  .m_worker_heap_bytes = 8 * 1024 * 1024,
                                  .m_pin_workers = !unpinned,
                                  .m_numa_aware = !unpinned});

    std::atomic<int> completed{0};
    std::atomic<int> failures{0};
    auto client = [&](std::uint16_t port) -> Task<void> {
        auto& allocator = Runtime::current_allocator();
        db::Driver* driver = db::DriverRegistry::instance().create("mysql", allocator);
        const std::string port_string = std::to_string(port);
        db::ConnectionOptions options;
        options.m_host = "127.0.0.1";
        options.m_port = port_string;
        options.m_user = "bench";
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
            if (result.is_err() || !result.value().m_has_rows ||
                result.value().m_rows.size() != 1) {
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
