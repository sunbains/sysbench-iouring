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

#include "sysbench/mysql.hpp"

#include "sysbench/io_ring.hpp"
#include "sysbench/memory.hpp"
#include "sysbench/mysql_wire.hpp"
#include "sysbench/runtime.hpp"
#include "sysbench/socket_io.hpp"
#include "sysbench/layer_stats.hpp"
#include "sysbench/nic_queues.hpp"
#include "sysbench/task.hpp"
#include "mysql_transport.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <new>
#include <vector>
#include <span>
#include <string_view>

namespace sb::mysql {
namespace {

constexpr std::size_t kMaxColumns = 4096;
// Per-connection heap for requests assembled before framing (statement
// executes with their bound values).
constexpr std::size_t kRequestHeapBytes = 64 * 1024;

class PacketChannel final : public ByteStream {
public:
    explicit PacketChannel(mem::MemoryResource& persistent)
        : m_io(*Runtime::current(), Runtime::current_worker_id()) {
        
        auto& pool = m_io.buffers();
        m_read_buffer = pool.acquire();
        m_write_buffer = pool.acquire();
        if (!m_read_buffer.valid() || !m_write_buffer.valid()) {
            throw db::Error("no registered MySQL socket buffers available", 2002);
        }
        m_io.set_send_buffer(static_cast<std::byte*>(m_write_buffer.data()),
                             m_write_buffer.size());
        (void)persistent;
    }

    ~PacketChannel() override { close(); }

    PacketChannel(const PacketChannel&) = delete;
    PacketChannel& operator=(const PacketChannel&) = delete;

    Task<void> connect(const db::ConnectionOptions& options) override {
        close();

        if (!options.m_socket_path.empty() ||
            (!options.m_host.empty() && options.m_host.front() == '/')) {
            const std::string_view path =
                !options.m_socket_path.empty() ? options.m_socket_path : options.m_host;
            if (path.size() >= sizeof(sockaddr_un::sun_path)) {
                throw db::Error("MySQL socket path too long", 2002);
            }
            const int fd =
                ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
            if (fd < 0) throw_network("socket", 2002);

            sockaddr_un address{};
            address.sun_family = AF_UNIX;
            std::memcpy(address.sun_path, path.data(), path.size());
            const socklen_t length = static_cast<socklen_t>(
                offsetof(sockaddr_un, sun_path) + path.size() + 1);
            const int result = co_await ring_connect(
                m_io, fd, reinterpret_cast<sockaddr*>(&address), length);
            if (result < 0) {
                ::close(fd);
                throw_network("connect(unix)", 2003, -result);
            }
            m_fd = fd;
            m_healthy = true;
            start_receiving();
            co_return;
        }

        char host[256]{};
        char port[32]{};
        const std::size_t host_length = std::min(options.m_host.size(), sizeof(host) - 1);
        const std::size_t port_length = std::min(options.m_port.size(), sizeof(port) - 1);
        std::memcpy(host, options.m_host.data(), host_length);
        std::memcpy(port, options.m_port.data(), port_length);
        if (host_length == 0) std::memcpy(host, "127.0.0.1", 10);

        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;
        addrinfo* addresses = nullptr;
        const int lookup = ::getaddrinfo(host, port, &hints, &addresses);
        if (lookup != 0 || addresses == nullptr) {
            throw db::Error("MySQL host resolution failed", 2005);
        }

        int last_error = ECONNREFUSED;
        bool connected = false;
        for (addrinfo* address = addresses; address != nullptr;
             address = address->ai_next) {
            const int fd =
                ::socket(address->ai_family,
                         address->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                         address->ai_protocol);
            if (fd < 0) {
                last_error = errno;
                continue;
            }
            const int result = co_await ring_connect(
                m_io, fd, address->ai_addr, address->ai_addrlen);
            if (result < 0) {
                last_error = -result;
                ::close(fd);
                continue;
            }
            int one = 1;
            (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            m_fd = fd;
            connected = true;
            break;
        }
        ::freeaddrinfo(addresses);
        if (!connected) throw_network("connect", 2003, last_error);
        m_healthy = true;
        start_receiving();
    }

    /* Every read of the socket goes through one multishot receive armed here
    (when enabled): a second, ordinary receive on the same socket would split
    the stream unpredictably. */
    void start_receiving() {
        Runtime* runtime = Runtime::current();
        /* Blocking architecture: the socket becomes blocking; sends are plain
        send() calls (SocketIo's direct send) and receives plain recv()s that
        busy-poll in the kernel, as libmysqlclient's do. */
        if (runtime != nullptr && runtime->blocking_io()) {
            const int flags = ::fcntl(m_fd, F_GETFL);
            if (flags >= 0) (void)::fcntl(m_fd, F_SETFL, flags & ~O_NONBLOCK);
            m_blocking = true;
            return;
        }
        /* With io_uring NAPI busy polling the worker's wait polls the NIC;
        the socket's own busy-poll (inherited from net.core.busy_read) would
        add a NIC poll pass to every receive attempt io_uring makes. */
        if (runtime != nullptr && runtime->busy_poll()) {
            int zero = 0;
            (void)::setsockopt(m_fd, SOL_SOCKET, SO_BUSY_POLL, &zero, sizeof(zero));
        }
        if (runtime != nullptr && runtime->multishot_receive()) {
            m_receiver = MultishotReceiver::acquire(m_fd);
        }
    }

    // Receive into `destination`: through the multishot receiver, or one
    // fixed-buffer receive request.  An awaiter: no coroutine frame.
    class ReceiveAwaiter {
    public:
        ReceiveAwaiter(PacketChannel& channel, std::byte* destination, unsigned capacity)
            : m_receiver(channel.m_receiver),
              m_destination(destination),
              m_capacity(capacity),
              m_blocking_fd(channel.m_blocking ? channel.m_fd : -1),
              m_read(channel.m_io, channel.m_fd, destination, capacity, channel.m_read_buffer.index()) {}
        [[nodiscard]] bool await_ready() noexcept {
            if (m_blocking_fd >= 0) {
                /* Blocking recv() right here: the worker waits in the kernel
                (busy-polling the NIC) and the task never suspends. */
                ssize_t received;
                do {
                    received = ::recv(m_blocking_fd, m_destination, m_capacity, 0);
                } while (received < 0 && errno == EINTR);
                m_blocking_result = received < 0 ? -errno : static_cast<int>(received);
                return true;
            }
            return m_receiver != nullptr && m_receiver->ready();
        }
        void await_suspend(std::coroutine_handle<> handle) noexcept {
            if (m_receiver != nullptr) {
                m_receiver->wait();
            } else {
                m_read.await_suspend(handle);
            }
        }
        int await_resume() noexcept {
            if (m_blocking_fd >= 0) return m_blocking_result;
            return m_receiver != nullptr ? m_receiver->take(m_destination, m_capacity)
                                         : m_read.await_resume();
        }

    private:
        MultishotReceiver* m_receiver;
        std::byte* m_destination;
        unsigned m_capacity;
        int m_blocking_fd;
        int m_blocking_result = 0;
        SocketReadAwaiter m_read;
    };
    [[nodiscard]] ReceiveAwaiter receive(void* destination, std::size_t capacity) {
        return ReceiveAwaiter(*this, static_cast<std::byte*>(destination),
                              static_cast<unsigned>(capacity));
    }

    [[nodiscard]] bool tls_active() const noexcept { return m_tls_active; }

    Task<void> start_tls(const db::ConnectionOptions& options) {
        close_tls();
        SSL_CTX* context = SSL_CTX_new(TLS_client_method());
        if (context == nullptr) throw tls_failure("SSL_CTX_new");
        SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION);
        SSL_CTX_set_options(context, SSL_OP_NO_COMPRESSION);
        if (options.m_tls_verify_server_cert) {
            SSL_CTX_set_verify(context, SSL_VERIFY_PEER, nullptr);
            if (!options.m_tls_ca.empty()) {
                char ca[4096]{};
                const std::size_t length =
                    std::min(options.m_tls_ca.size(), sizeof(ca) - 1);
                std::memcpy(ca, options.m_tls_ca.data(), length);
                if (SSL_CTX_load_verify_locations(context, ca, nullptr) != 1) {
                    SSL_CTX_free(context);
                    throw tls_failure("SSL_CTX_load_verify_locations");
                }
            } else if (SSL_CTX_set_default_verify_paths(context) != 1) {
                SSL_CTX_free(context);
                throw tls_failure("SSL_CTX_set_default_verify_paths");
            }
        } else {
            SSL_CTX_set_verify(context, SSL_VERIFY_NONE, nullptr);
        }
        auto load_identity = [&](std::string_view path, bool certificate) {
            if (path.empty()) return;
            char name[4096]{};
            const std::size_t length = std::min(path.size(), sizeof(name) - 1);
            std::memcpy(name, path.data(), length);
            const int result = certificate
                                   ? SSL_CTX_use_certificate_chain_file(context, name)
                                   : SSL_CTX_use_PrivateKey_file(context, name,
                                                                 SSL_FILETYPE_PEM);
            if (result != 1) {
                SSL_CTX_free(context);
                throw db::Error("invalid MySQL TLS client identity", 2026);
            }
        };
        load_identity(options.m_tls_cert, true);
        load_identity(options.m_tls_key, false);
        if (!options.m_tls_cert.empty() && !options.m_tls_key.empty() &&
            SSL_CTX_check_private_key(context) != 1) {
            SSL_CTX_free(context);
            throw db::Error("MySQL TLS client key does not match certificate", 2026);
        }

        SSL* ssl = SSL_new(context);
        if (ssl == nullptr) {
            SSL_CTX_free(context);
            throw tls_failure("SSL_new");
        }
        BIO* rbio = BIO_new(BIO_s_mem());
        BIO* wbio = BIO_new(BIO_s_mem());
        if (rbio == nullptr || wbio == nullptr) {
            if (rbio != nullptr) BIO_free(rbio);
            if (wbio != nullptr) BIO_free(wbio);
            SSL_free(ssl);
            SSL_CTX_free(context);
            throw tls_failure("BIO_new");
        }
        BIO_set_mem_eof_return(rbio, -1);
        BIO_set_mem_eof_return(wbio, -1);
        SSL_set_bio(ssl, rbio, wbio);
        SSL_set_connect_state(ssl);

        char host[256]{};
        const std::size_t host_length = std::min(options.m_host.size(), sizeof(host) - 1);
        std::memcpy(host, options.m_host.data(), host_length);
        const bool is_path = host[0] == '/';
        in_addr ipv4{};
        in6_addr ipv6{};
        const bool is_ip = ::inet_pton(AF_INET, host, &ipv4) == 1 ||
                           ::inet_pton(AF_INET6, host, &ipv6) == 1;
        const char* verify_host = host_length == 0 ? "localhost" : host;
        if (!is_path && !is_ip && host_length != 0) {
            (void)SSL_set_tlsext_host_name(ssl, host);
        }
        if (options.m_tls_verify_server_cert) {
            if (is_ip) {
                if (X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), verify_host) != 1) {
                    SSL_free(ssl);
                    SSL_CTX_free(context);
                    throw db::Error("cannot configure MySQL TLS IP verification", 2026);
                }
            } else if (SSL_set1_host(ssl, verify_host) != 1) {
                SSL_free(ssl);
                SSL_CTX_free(context);
                throw db::Error("cannot configure MySQL TLS hostname verification", 2026);
            }
        }

        m_ssl_ctx = context;
        m_ssl = ssl;
        m_rbio = rbio;
        m_wbio = wbio;
        for (;;) {
            ERR_clear_error();
            const int result = SSL_connect(m_ssl);
            if (result == 1) break;
            const int error = SSL_get_error(m_ssl, result);
            if (error == SSL_ERROR_WANT_READ) {
                co_await flush_tls_wbio();
                co_await feed_tls_rbio();
                continue;
            }
            if (error == SSL_ERROR_WANT_WRITE) {
                co_await flush_tls_wbio();
                continue;
            }
            throw tls_failure("SSL_connect");
        }
        co_await flush_tls_wbio();
        if (m_tls_plain.empty()) {
            constexpr std::size_t kTlsPlainBytes = 16384;
            m_tls_plain = std::span<std::byte>(
                static_cast<std::byte*>(mem::global_root().allocate(kTlsPlainBytes, 64)),
                kTlsPlainBytes);
        }
        m_tls_active = true;
    }

    // The next packet read starts a command's reply (sequence 1).
    void expect_reply() noexcept { m_sequence = 1; }

    Task<void> write_command(std::span<const std::byte> payload) {
        m_sequence = 0;
        co_await write_packet(payload);
    }

    /* A one-byte command and its argument (COM_QUERY, COM_STMT_PREPARE),
    framed straight into the registered write buffer: the SQL is never copied
    into an intermediate request buffer. */
    Task<void> write_command(std::uint8_t command, std::span<const std::byte> body) {
        if (!m_healthy) throw db::Error("MySQL connection is closed", 2013);
        m_sequence = 0;
        const std::size_t total = 1 + body.size();
        if (total >= 0x00ffffff) {
            /* Multi-frame packet: rare, assemble it once and frame it. */
            std::vector<std::byte> payload(total);
            payload[0] = static_cast<std::byte>(command);
            std::memcpy(payload.data() + 1, body.data(), body.size());
            co_await write_packet(payload);
            co_return;
        }
        std::array<std::byte, 5> head{
            static_cast<std::byte>(total & 0xff),
            static_cast<std::byte>((total >> 8) & 0xff),
            static_cast<std::byte>((total >> 16) & 0xff),
            static_cast<std::byte>(m_sequence++),
            static_cast<std::byte>(command)};
        if (m_tls_active) {
            co_await write_plain(head.data(), head.size());
            if (!body.empty()) co_await write_plain(body.data(), body.size());
        } else if (total + 4 <= m_write_buffer.size()) {
            std::byte* target = co_await stage(total + 4);
            std::memcpy(target, head.data(), head.size());
            if (!body.empty()) std::memcpy(target + head.size(), body.data(), body.size());
            co_await send_staged(target, total + 4);
        } else {
            co_await write_exact(head.data(), head.size());
            co_await write_exact(body.data(), body.size());
        }
    }

    /* Space for `length` bytes in the send ring.  While the ring cannot take
    them (full, or an earlier send must go first) the task waits on the
    blocking send's completion: control goes back to the scheduler. */
    Task<std::byte*> stage(std::size_t length) {
        for (;;) {
            IoCompletion* blocker = nullptr;
            if (std::byte* target = m_io.stage_send(length, blocker)) co_return target;
            co_await PostedSendAwaiter(blocker);
        }
    }

    [[nodiscard]] int fd() const noexcept { return m_fd; }
    // Wait (via the scheduler) until every send of this connection is done.
    Task<void> flush_sends() { co_await drain_posted(); }

    // Every posted send of this connection is done.
    Task<void> drain_posted() {
        while (IoCompletion* oldest = m_io.oldest_posted()) {
            co_await PostedSendAwaiter(oldest);
        }
    }

    /* Send `length` staged bytes.  The send is posted, not waited for: it goes
    out with the next submission, together with the receive for the reply.
    Without an SQE the send is awaited, after the earlier ones. */
    Task<void> send_staged(std::byte* target, std::size_t length) {
        if (m_io.post_send(m_fd, target, static_cast<unsigned>(length))) co_return;
        co_await drain_posted();
        const int written = co_await write_fixed(m_io, m_fd, target,
                                                 static_cast<unsigned>(length),
                                                 m_write_buffer.index());
        if (written < 0) {
            close();
            throw write_failure(written);
        }
        const std::size_t sent = static_cast<std::size_t>(written);
        if (sent < length) co_await write_exact(target + sent, length - sent);
    }

    Task<void> write_packet(std::span<const std::byte> payload) {
        if (!m_healthy) throw db::Error("MySQL connection is closed", 2013);
        std::size_t offset = 0;
        bool sent_zero_tail = false;
        do {
            const std::size_t chunk =
                std::min<std::size_t>(payload.size() - offset, 0x00ffffff);
            std::array<std::byte, 4> header{
                static_cast<std::byte>(chunk & 0xff),
                static_cast<std::byte>((chunk >> 8) & 0xff),
                static_cast<std::byte>((chunk >> 16) & 0xff),
                static_cast<std::byte>(m_sequence++)};

            if (m_tls_active) {
                co_await write_plain(header.data(), header.size());
                if (chunk != 0) {
                    co_await write_plain(payload.data() + offset, chunk);
                }
            } else if (chunk + 4 <= m_write_buffer.size()) {
                std::byte* target = co_await stage(chunk + 4);
                std::memcpy(target, header.data(), header.size());
                if (chunk != 0) {
                    std::memcpy(target + 4, payload.data() + offset, chunk);
                }
                co_await send_staged(target, chunk + 4);
            } else {
                co_await write_exact(header.data(), header.size());
                if (chunk != 0) co_await write_exact(payload.data() + offset, chunk);
            }
            offset += chunk;
            if (offset == payload.size() && payload.size() > 0 &&
                payload.size() % 0x00ffffff == 0) {
                sent_zero_tail = true;
                offset = payload.size();
                // MySQL requires an empty terminator frame after a 16 MiB frame.
                std::array<std::byte, 4> tail{std::byte{0}, std::byte{0}, std::byte{0},
                                              static_cast<std::byte>(m_sequence++)};
                if (m_tls_active) {
                    co_await write_plain(tail.data(), tail.size());
                } else {
                    co_await write_exact(tail.data(), tail.size());
                }
                break;
            }
        } while (offset < payload.size() || sent_zero_tail);
    }

    // -----------------------------------------------------------------------
    // Hot path without coroutine frames.  A command is framed into the write
    // buffer and posted by a plain call; a reply packet is taken straight from
    // the read buffer when it is all there, and the only await is the receive
    // that refills the buffer (an awaiter, not a coroutine).  TLS and packets
    // larger than the read buffer use the coroutine paths below.
    // -----------------------------------------------------------------------

    // Frame and post a one-byte command and its argument.  False (nothing
    // sent) when it needs the coroutine path.
    [[nodiscard]] bool try_send_command(std::uint8_t command, std::span<const std::byte> body) {
        const std::size_t total = 1 + body.size();
        SB_LAYER(Protocol);
        if (!m_healthy || m_tls_active || total + 4 > m_write_buffer.size()) return false;
        SB_COUNT(CopyOut, total + 4);
        IoCompletion* blocker = nullptr;
        std::byte* target = m_io.stage_send(total + 4, blocker);
        if (target == nullptr) return false;
        target[0] = static_cast<std::byte>(total & 0xff);
        target[1] = static_cast<std::byte>((total >> 8) & 0xff);
        target[2] = static_cast<std::byte>((total >> 16) & 0xff);
        target[3] = std::byte{0};
        target[4] = static_cast<std::byte>(command);
        if (!body.empty()) std::memcpy(target + 5, body.data(), body.size());
        if (!m_io.post_send(m_fd, target, static_cast<unsigned>(total + 4))) return false;
        m_sequence = 1;
        return true;
    }

    // Frame and post a whole command payload (its first byte is the command).
    [[nodiscard]] bool try_send_command(std::span<const std::byte> payload) {
        if (payload.empty()) return false;
        return try_send_command(std::to_integer<std::uint8_t>(payload[0]), payload.subspan(1));
    }

    // Take the next packet from the read buffer if it is all there: the
    // payload is copied into `resource`.  False when more bytes are needed.
    [[nodiscard]] bool try_read_packet(mem::MemoryResource& resource,
                                       std::span<const std::byte>& packet) {
        SB_LAYER(Protocol);
        if (m_tls_active || m_read_available < 4) return false;
        const auto* at = static_cast<const std::byte*>(m_read_buffer.data()) + m_read_position;
        const std::size_t length = std::to_integer<std::size_t>(at[0]) |
                                   (std::to_integer<std::size_t>(at[1]) << 8) |
                                   (std::to_integer<std::size_t>(at[2]) << 16);
        if (m_read_available < 4 + length) return false;
        if (std::to_integer<std::uint8_t>(at[3]) != m_sequence) {
            throw db::Error("unexpected MySQL packet sequence", 2027);
        }
        if (length == 0x00ffffff) {
            throw db::Error("MySQL packets larger than 16 MiB are unsupported", 2027);
        }
        ++m_sequence;
        if (length == 0) {
            packet = {};
        } else {
            auto* memory = static_cast<std::byte*>(resource.allocate(length, 1));
            std::memcpy(memory, at + 4, length);
            SB_COUNT(CopyIn, length);
            packet = std::span<const std::byte>(memory, length);
        }
        m_read_position += 4 + length;
        m_read_available -= 4 + length;
        return true;
    }

    // ---- Synchronous I/O (blocking architecture) ---------------------------
    // Requests complete in the call, as libmysqlclient's do: no coroutine.

    [[nodiscard]] bool synchronous() const noexcept {
        return m_blocking && !m_tls_active && m_healthy;
    }

    void send_command_sync(std::uint8_t command, std::span<const std::byte> body) {
        if (try_send_command(command, body)) return;  // framed and sent (direct send)
        const std::size_t total = 1 + body.size();
        if (total >= 0x00ffffff) {
            throw db::Error("MySQL commands of 16 MiB or more are unsupported here", 2020);
        }
        const std::array<std::byte, 5> head{
            static_cast<std::byte>(total & 0xff), static_cast<std::byte>((total >> 8) & 0xff),
            static_cast<std::byte>((total >> 16) & 0xff), std::byte{0},
            static_cast<std::byte>(command)};
        send_all(head.data(), head.size());
        send_all(body.data(), body.size());
        m_sequence = 1;
    }

    // The next reply packet: a view into the read buffer (valid until the next
    // read), or, for a packet larger than the buffer, a copy in `resource`.
    std::span<const std::byte> next_packet_sync(mem::MemoryResource& resource) {
        for (;;) {
            std::span<const std::byte> packet;
            if (try_read_packet_view(packet)) return packet;
            if (packet_needs_coroutine()) return read_large_packet_sync(resource);
            refill_sync();
        }
    }

    // Like try_read_packet(), but the packet stays in the read buffer: no
    // copy, no allocation.  The view is valid until the next read (only a
    // refill moves the buffer's bytes), so it is for packets parsed at once.
    [[nodiscard]] bool try_read_packet_view(std::span<const std::byte>& packet) {
        SB_LAYER(Protocol);
        if (m_tls_active || m_read_available < 4) return false;
        const auto* at = static_cast<const std::byte*>(m_read_buffer.data()) + m_read_position;
        const std::size_t length = std::to_integer<std::size_t>(at[0]) |
                                   (std::to_integer<std::size_t>(at[1]) << 8) |
                                   (std::to_integer<std::size_t>(at[2]) << 16);
        if (m_read_available < 4 + length) return false;
        if (std::to_integer<std::uint8_t>(at[3]) != m_sequence) {
            throw db::Error("unexpected MySQL packet sequence", 2027);
        }
        if (length == 0x00ffffff) {
            throw db::Error("MySQL packets larger than 16 MiB are unsupported", 2027);
        }
        ++m_sequence;
        packet = std::span<const std::byte>(at + 4, length);
        m_read_position += 4 + length;
        m_read_available -= 4 + length;
        return true;
    }

    // The next packet cannot come through try_read_packet()/refill(): TLS, or
    // a packet larger than the read buffer.
    [[nodiscard]] bool packet_needs_coroutine() const noexcept {
        if (m_tls_active) return true;
        if (m_read_available < 4) return false;
        const auto* at = static_cast<const std::byte*>(m_read_buffer.data()) + m_read_position;
        const std::size_t length = std::to_integer<std::size_t>(at[0]) |
                                   (std::to_integer<std::size_t>(at[1]) << 8) |
                                   (std::to_integer<std::size_t>(at[2]) << 16);
        return 4 + length > m_read_buffer.size();
    }

    /* Receive more bytes behind the unread ones (moved to the front of the
    buffer first).  An awaiter, so the wait costs no coroutine frame. */
    class RefillAwaiter {
    public:
        explicit RefillAwaiter(PacketChannel& channel)
            : m_channel(&channel),
              m_read(channel, channel.refill_target(),
                     static_cast<unsigned>(channel.m_read_buffer.size() -
                                           channel.m_read_available)) {}
        [[nodiscard]] bool await_ready() noexcept { return m_read.await_ready(); }
        void await_suspend(std::coroutine_handle<> handle) noexcept { m_read.await_suspend(handle); }
        void await_resume() { m_channel->refilled(m_read.await_resume()); }

    private:
        PacketChannel* m_channel;
        ReceiveAwaiter m_read;
    };
    [[nodiscard]] RefillAwaiter refill() { return RefillAwaiter(*this); }

    Task<std::span<const std::byte>> read_packet(mem::MemoryResource& resource) {
        std::array<std::byte, 4> header{};
        co_await read_exact(header.data(), header.size());
        PacketHeader parsed{};
        if (!parse_packet_header(header, parsed)) {
            throw db::Error("malformed MySQL packet header", 2027);
        }
        if (parsed.m_sequence != m_sequence) {
            throw db::Error("unexpected MySQL packet sequence", 2027);
        }
        ++m_sequence;
        if (parsed.m_length == 0x00ffffff) {
            throw db::Error("MySQL packets larger than 16 MiB are unsupported", 2027);
        }
        if (parsed.m_length == 0) {
            co_return std::span<const std::byte>();
        }
        auto* memory = static_cast<std::byte*>(resource.allocate(parsed.m_length, 1));
        co_await read_exact(memory, parsed.m_length);
        co_return std::span<const std::byte>(memory, parsed.m_length);
    }

    Task<std::size_t> read(std::span<std::byte> destination) override {
        if (destination.empty()) co_return 0;
        if (!healthy()) throw db::Error("MySQL connection is closed", 2013);
        if (m_tls_active) {
            if (m_tls_plain_offset == m_tls_plain_size) co_await fill_tls_plaintext();
            const auto count = std::min(destination.size(), m_tls_plain_size - m_tls_plain_offset);
            std::memcpy(destination.data(), m_tls_plain.data() + m_tls_plain_offset, count);
            m_tls_plain_offset += count;
            co_return count;
        }
        if (m_read_available == 0) {
            const int received = co_await receive(m_read_buffer.data(), m_read_buffer.size());
            if (received <= 0) {
                close();
                if (received < 0) throw_network("read", 2013, -received);
                throw db::Error("MySQL server closed the connection", 2013);
            }
            m_read_available = static_cast<std::size_t>(received);
            m_read_position = 0;
        }
        const auto count = std::min(destination.size(), m_read_available);
        std::memcpy(destination.data(),
                    static_cast<std::byte*>(m_read_buffer.data()) + m_read_position, count);
        m_read_position += count;
        m_read_available -= count;
        co_return count;
    }

    Task<void> write(std::span<const std::byte> source) override {
        if (!healthy()) throw db::Error("MySQL connection is closed", 2013);
        co_await write_plain(source.data(), source.size());
    }

    void destroy(mem::MemoryResource& resource) noexcept override {
        mem::destroy(resource, this);
    }

    void close() noexcept override {
        /* No waiting: sends still in flight are orphaned to the worker, and
        the receiver goes back to it once the shutdown below ends its
        multishot receive. */
        m_io.abandon_posted();
        if (m_receiver != nullptr) {
            m_receiver->abandon();
            m_receiver = nullptr;
        }
        close_tls();
        m_read_available = 0;
        m_read_position = 0;
        m_tls_plain_offset = 0;
        m_tls_plain_size = 0;
        if (m_fd >= 0) {
            ::shutdown(m_fd, SHUT_RDWR);
            ::close(m_fd);
            m_fd = -1;
        }
        m_healthy = false;
        m_sequence = 0;
    }

    [[nodiscard]] bool healthy() const noexcept override { return m_healthy && m_fd >= 0; }

private:
    void send_all(const std::byte* data, std::size_t count) {
        while (count > 0) {
            const ssize_t sent = ::send(m_fd, data, count, MSG_NOSIGNAL);
            if (sent < 0) {
                if (errno == EINTR) continue;
                const int error = errno;
                close();
                throw_network("write", 2013, error);
            }
            data += sent;
            count -= static_cast<std::size_t>(sent);
        }
    }

    void refill_sync() {
        std::byte* target = refill_target();
        const std::size_t room = m_read_buffer.size() - m_read_available;
        ssize_t received;
        do {
            received = ::recv(m_fd, target, room, 0);
        } while (received < 0 && errno == EINTR);
        refilled(received < 0 ? -errno : static_cast<int>(received));
    }

    std::span<const std::byte> read_large_packet_sync(mem::MemoryResource& resource) {
        const auto* at = static_cast<const std::byte*>(m_read_buffer.data()) + m_read_position;
        const std::size_t length = std::to_integer<std::size_t>(at[0]) |
                                   (std::to_integer<std::size_t>(at[1]) << 8) |
                                   (std::to_integer<std::size_t>(at[2]) << 16);
        if (std::to_integer<std::uint8_t>(at[3]) != m_sequence) {
            throw db::Error("unexpected MySQL packet sequence", 2027);
        }
        if (length == 0x00ffffff) {
            throw db::Error("MySQL packets larger than 16 MiB are unsupported", 2027);
        }
        ++m_sequence;
        auto* memory = static_cast<std::byte*>(resource.allocate(length, 1));
        std::size_t got = m_read_available - 4;
        std::memcpy(memory, at + 4, got);
        m_read_position = 0;
        m_read_available = 0;
        while (got < length) {
            const ssize_t received = ::recv(m_fd, memory + got, length - got, 0);
            if (received > 0) {
                got += static_cast<std::size_t>(received);
                continue;
            }
            if (received < 0 && errno == EINTR) continue;
            const int error = received < 0 ? errno : 0;
            close();
            if (error != 0) throw_network("read", 2013, error);
            throw db::Error("MySQL server closed the connection", 2013);
        }
        return std::span<const std::byte>(memory, length);
    }

    // Where a refill receives into: after the unread bytes, moved to the front.
    std::byte* refill_target() noexcept {
        auto* base = static_cast<std::byte*>(m_read_buffer.data());
        if (m_read_available != 0 && m_read_position != 0) {
            std::memmove(base, base + m_read_position, m_read_available);
            SB_COUNT(CompactBytes, m_read_available);
        }
        m_read_position = 0;
        return base + m_read_available;
    }

    void refilled(int received) {
        if (const int failed = m_io.take_post_failure(); failed < 0) {
            close();
            throw write_failure(failed);
        }
        if (received <= 0) {
            close();
            if (received < 0) throw_network("read", 2013, -received);
            throw db::Error("MySQL server closed the connection", 2013);
        }
        m_read_available += static_cast<std::size_t>(received);
    }

    Task<void> read_exact(std::byte* destination, std::size_t count) {
        if (m_tls_active) {
            while (count > 0) {
                if (m_tls_plain_offset == m_tls_plain_size) {
                    co_await fill_tls_plaintext();
                }
                const std::size_t take =
                    std::min(count, m_tls_plain_size - m_tls_plain_offset);
                std::memcpy(destination,
                            m_tls_plain.data() + m_tls_plain_offset, take);
                destination += take;
                m_tls_plain_offset += take;
                count -= take;
            }
            co_return;
        }
        while (count > 0) {
            if (m_read_available == 0) {
                const int received = co_await receive(m_read_buffer.data(), m_read_buffer.size());
                if (const int failed = m_io.take_post_failure(); failed < 0) {
                    close();
                    throw write_failure(failed);
                }
                if (received <= 0) {
                    close();
                    if (received < 0) throw_network("read", 2013, -received);
                    throw db::Error("MySQL server closed the connection", 2013);
                }
                m_read_available = static_cast<std::size_t>(received);
                m_read_position = 0;
            }
            const std::size_t take = std::min(count, m_read_available);
            std::memcpy(destination,
                        static_cast<std::byte*>(m_read_buffer.data()) + m_read_position,
                        take);
            destination += take;
            m_read_position += take;
            m_read_available -= take;
            count -= take;
        }
    }

    Task<void> write_exact(const std::byte* source, std::size_t count) {
        /* The whole write buffer is used below: earlier sends must be done. */
        co_await drain_posted();
        while (count > 0) {
            const std::size_t chunk = std::min<std::size_t>(count, m_write_buffer.size());
            std::memcpy(m_write_buffer.data(), source, chunk);
            const int written =
                co_await write_fixed(m_io, m_fd, m_write_buffer.data(),
                                     static_cast<unsigned>(chunk),
                                     m_write_buffer.index());
            if (written <= 0) {
                close();
                throw write_failure(written);
            }
            source += written;
            count -= static_cast<std::size_t>(written);
        }
    }

    Task<void> write_plain(const std::byte* data, std::size_t count) {
        if (!m_tls_active) {
            co_await write_exact(data, count);
            co_return;
        }
        std::size_t offset = 0;
        while (offset < count) {
            ERR_clear_error();
            const std::size_t chunk =
                std::min<std::size_t>(count - offset, 1024 * 1024);
            const int written =
                SSL_write(m_ssl, data + offset, static_cast<int>(chunk));
            if (written > 0) {
                offset += static_cast<std::size_t>(written);
                co_await flush_tls_wbio();
                continue;
            }
            const int error = SSL_get_error(m_ssl, written);
            if (error == SSL_ERROR_WANT_READ) {
                co_await flush_tls_wbio();
                co_await feed_tls_rbio();
                continue;
            }
            if (error == SSL_ERROR_WANT_WRITE) {
                co_await flush_tls_wbio();
                continue;
            }
            close();
            throw tls_failure("SSL_write");
        }
    }

    Task<void> fill_tls_plaintext() {
        for (;;) {
            ERR_clear_error();
            const int received =
                SSL_read(m_ssl, m_tls_plain.data(), static_cast<int>(m_tls_plain.size()));
            if (received > 0) {
                m_tls_plain_offset = 0;
                m_tls_plain_size = static_cast<std::size_t>(received);
                co_await flush_tls_wbio();
                co_return;
            }
            const int error = SSL_get_error(m_ssl, received);
            if (error == SSL_ERROR_WANT_READ) {
                co_await flush_tls_wbio();
                co_await feed_tls_rbio();
                continue;
            }
            if (error == SSL_ERROR_WANT_WRITE) {
                co_await flush_tls_wbio();
                continue;
            }
            close();
            if (error == SSL_ERROR_ZERO_RETURN) {
                throw db::Error("MySQL TLS connection closed", 2013);
            }
            throw tls_failure("SSL_read");
        }
    }

    Task<void> flush_tls_wbio() {
        co_await drain_posted();
        while (m_wbio != nullptr && BIO_ctrl_pending(m_wbio) > 0) {
            const int pending = static_cast<int>(BIO_ctrl_pending(m_wbio));
            const int chunk = std::min(pending, static_cast<int>(m_write_buffer.size()));
            const int count = BIO_read(m_wbio, m_write_buffer.data(),
                                       static_cast<int>(chunk));
            if (count <= 0) {
                close();
                throw tls_failure("BIO_read");
            }
            co_await write_exact(static_cast<const std::byte*>(m_write_buffer.data()),
                                 static_cast<std::size_t>(count));
        }
    }

    Task<void> feed_tls_rbio() {
        const int received =
            co_await receive(m_read_buffer.data(), m_read_buffer.size());
        if (received <= 0) {
            close();
            if (received < 0) throw_network("read(tls)", 2013, -received);
            throw db::Error("MySQL server closed the TLS connection", 2013);
        }
        const int written =
            BIO_write(m_rbio, m_read_buffer.data(), received);
        if (written != received) {
            close();
            throw tls_failure("BIO_write");
        }
    }

    void close_tls() noexcept {
        m_tls_active = false;
        if (m_ssl != nullptr) {
            SSL_free(m_ssl);  // frees m_rbio/m_wbio
            m_ssl = nullptr;
        }
        m_rbio = nullptr;
        m_wbio = nullptr;
        if (m_ssl_ctx != nullptr) {
            SSL_CTX_free(m_ssl_ctx);
            m_ssl_ctx = nullptr;
        }
    }

    static db::Error tls_failure(const char* operation) {
        char detail[256] = "unknown TLS error";
        const unsigned long code = ERR_get_error();
        if (code != 0) {
            ERR_error_string_n(code, detail, sizeof(detail));
        }
        char message[512];
        std::snprintf(message, sizeof(message), "MySQL TLS %s failed: %s",
                      operation, detail);
        return db::Error(message, 2026);
    }

    static db::Error write_failure(int result) {
        if (result < 0) {
            char message[256];
            std::snprintf(message, sizeof(message), "MySQL write failed: %s",
                          std::strerror(-result));
            return db::Error(message, 2013);
        }
        return db::Error("MySQL write returned zero bytes", 2013);
    }

    [[noreturn]] static void throw_network(const char* operation, int code,
                                           int error = 0) {
        char message[256];
        if (error == 0) error = errno;
        std::snprintf(message, sizeof(message), "MySQL %s failed: %s", operation,
                      std::strerror(error));
        throw db::Error(message, code);
    }

    SocketIo m_io;
    MultishotReceiver* m_receiver = nullptr;
    bool m_blocking = false;
    RegisteredBufferLease m_read_buffer;
    RegisteredBufferLease m_write_buffer;
    int m_fd = -1;
    std::size_t m_read_available = 0;
    std::size_t m_read_position = 0;
    std::uint8_t m_sequence = 0;
    bool m_healthy = false;
    bool m_tls_active = false;
    SSL_CTX* m_ssl_ctx = nullptr;
    SSL* m_ssl = nullptr;
    BIO* m_rbio = nullptr;
    BIO* m_wbio = nullptr;
    // Only a TLS connection needs it: allocated when TLS is negotiated.
    std::span<std::byte> m_tls_plain{};
    std::size_t m_tls_plain_offset = 0;
    std::size_t m_tls_plain_size = 0;
};

/* Read the next reply packet into `out` without a coroutine frame when its
bytes are in (or arrive into) the read buffer; see PacketChannel's hot path. */
#define SB_NEXT_PACKET(channel, resource, out)                   \
    for (;;) {                                                    \
        if ((channel).try_read_packet((resource), (out))) break;  \
        if ((channel).packet_needs_coroutine()) {                 \
            (out) = co_await (channel).read_packet(resource);     \
            break;                                                \
        }                                                         \
        co_await (channel).refill();                              \
    }

// The next packet as a view into the read buffer (see try_read_packet_view):
// for packets parsed before the next read.  Packets too big for the buffer,
// or TLS, still come through the copying coroutine path.
#define SB_NEXT_PACKET_VIEW(channel, resource, out)              \
    for (;;) {                                                    \
        if ((channel).try_read_packet_view(out)) break;           \
        if ((channel).packet_needs_coroutine()) {                 \
            (out) = co_await (channel).read_packet(resource);     \
            break;                                                \
        }                                                         \
        co_await (channel).refill();                              \
    }

class MysqlConnection;

struct OwnedOptions {
    mem::String m_host;
    mem::String m_port;
    mem::String m_user;
    mem::String m_password;
    mem::String m_database;
    mem::String m_socket_path;
    mem::String m_tls_ca;
    mem::String m_tls_cert;
    mem::String m_tls_key;

    OwnedOptions(const db::ConnectionOptions& source, mem::MemoryResource& resource)
        : m_host(mem::Allocator<char>(&resource)),
          m_port(mem::Allocator<char>(&resource)),
          m_user(mem::Allocator<char>(&resource)),
          m_password(mem::Allocator<char>(&resource)),
          m_database(mem::Allocator<char>(&resource)),
          m_socket_path(mem::Allocator<char>(&resource)),
          m_tls_ca(mem::Allocator<char>(&resource)),
          m_tls_cert(mem::Allocator<char>(&resource)),
          m_tls_key(mem::Allocator<char>(&resource)),
          m_connect_timeout_ms(source.m_connect_timeout_ms),
          m_max_packet_size(source.m_max_packet_size),
          m_tls_mode(source.m_tls_mode),
          m_tls_verify_server_cert(source.m_tls_verify_server_cert),
          m_steer_rx(source.m_steer_rx) {
        m_host.assign(source.m_host);
        m_port.assign(source.m_port);
        m_user.assign(source.m_user);
        m_password.assign(source.m_password);
        m_database.assign(source.m_database);
        m_socket_path.assign(source.m_socket_path);
        m_tls_ca.assign(source.m_tls_ca);
        m_tls_cert.assign(source.m_tls_cert);
        m_tls_key.assign(source.m_tls_key);
    }

    [[nodiscard]] db::ConnectionOptions view() const noexcept {
        return db::ConnectionOptions{
            .m_host = m_host,
            .m_port = m_port,
            .m_user = m_user,
            .m_password = m_password,
            .m_database = m_database,
            .m_socket_path = m_socket_path,
            .m_connect_timeout_ms = m_connect_timeout_ms,
            .m_max_packet_size = m_max_packet_size,
            .m_tls_mode = m_tls_mode,
            .m_tls_verify_server_cert = m_tls_verify_server_cert,
            .m_tls_ca = m_tls_ca,
            .m_tls_cert = m_tls_cert,
            .m_tls_key = m_tls_key,
            .m_steer_rx = m_steer_rx};
    }

    std::uint32_t m_connect_timeout_ms;
    std::uint32_t m_max_packet_size;
    db::TlsMode m_tls_mode;
    bool m_tls_verify_server_cert;
    bool m_steer_rx;
};

class MysqlStatement final : public db::PreparedStatement {
public:
    MysqlStatement(MysqlConnection& connection, mem::MemoryResource& persistent,
                   std::uint32_t id, std::uint16_t parameters, std::string_view sql);
    ~MysqlStatement() override = default;

    Task<db::QueryResult> execute(std::span<const db::BindValue> parameters,
                                  mem::MemoryResource& result_resource) override;
    Task<db::Status> send_execute(std::span<const db::BindValue> parameters,
                                  mem::MemoryResource& scratch) override;
    bool execute_into(std::span<const db::BindValue> parameters, db::ResultSet& out,
                      mem::MemoryResource& resource, db::Error& error) override;
    Task<db::Status> close() override;
    Task<db::ResultSet> execute_impl(std::span<const db::BindValue> parameters,
                                     mem::MemoryResource& result_resource);
    Task<void> close_impl();
    [[nodiscard]] bool closed() const noexcept override { return m_closed; }
    [[nodiscard]] std::string_view sql() const noexcept override { return m_sql; }
    [[nodiscard]] std::uint32_t id() const noexcept { return m_id; }
    // Whether this execute must send the parameter types: on the first
    // execute and whenever a type differs from the ones the server holds.
    [[nodiscard]] bool types_changed(std::span<const db::BindValue> parameters) noexcept {
        const std::size_t count = std::min(parameters.size(), m_bound_types.size());
        bool same = m_types_bound && parameters.size() == m_bound_count;
        for (std::size_t i = 0; i < count; ++i) {
            const auto kind = static_cast<std::uint8_t>(parameters[i].m_kind);
            if (same && m_bound_types[i] != kind) same = false;
            m_bound_types[i] = kind;
        }
        m_bound_count = parameters.size();
        m_types_bound = true;
        return !same;
    }
    [[nodiscard]] std::uint32_t native_id() const noexcept override {
        return m_text_mode ? 0 : m_id;
    }
    [[nodiscard]] std::uint16_t parameter_count() const noexcept { return m_parameters; }

    void destroy(mem::MemoryResource& resource) noexcept override {
        std::destroy_at(this);
        resource.deallocate(this, sizeof(*this), alignof(MysqlStatement));
    }

private:
    friend class MysqlConnection;
    MysqlConnection* m_connection;
    mem::String m_sql;
    std::uint32_t m_id = 0;
    std::uint16_t m_parameters = 0;
    bool m_closed = false;
    bool m_text_mode = false;
    // Parameter types the server holds for this statement.
    std::array<std::uint8_t, 64> m_bound_types{};
    std::size_t m_bound_count = 0;
    bool m_types_bound = false;
};

class MysqlConnection final : public db::Connection {
public:
    MysqlConnection(mem::MemoryResource& persistent, const db::ConnectionOptions& options)
        : m_persistent(&persistent),
          m_owned_options(options, persistent),
          m_channel(persistent),
          m_options(m_owned_options.view()),
          m_request_heap(mem::global_root(), kRequestHeapBytes, "request",
                         Runtime::current()->worker_numa_node(
                             Runtime::current_worker_id())),
          m_request_buffer(mem::Allocator<std::byte>(&m_request_heap)) {}

    ~MysqlConnection() override { close(); }

    /* With m_steer_rx, the connection is re-made from another local port
    until RSS delivers its replies to the channel this worker's CPU transmits
    on: send, TX completion, receive and the read then all stay on the
    worker's CPU.  The port decides the channel, so each attempt is a full
    login and a COM_QUIT (a clean disconnect for the server, never a
    connection error).  RSS spreads ports evenly, so it takes about as many
    attempts as the NIC has channels; after a few times that, keep what we
    have. */
    Task<void> connect_async() {
        co_await m_channel.connect(m_options);
        co_await login();
        if (!m_options.m_steer_rx || m_channel.fd() < 0) co_return;
        Runtime* runtime = Runtime::current();
        if (runtime == nullptr) co_return;
        const int cpu = runtime->worker_cpu(Runtime::current_worker_id());
        const std::uint32_t target = nic::napi_for_cpu(m_channel.fd(), cpu);
        if (target == 0) co_return;
        const std::uint32_t attempts = 8 * std::max<std::uint32_t>(1, nic::channel_count(m_channel.fd()));
        for (std::uint32_t attempt = 0; attempt < attempts; ++attempt) {
            if (nic::incoming_napi(m_channel.fd()) == target) co_return;
            co_await m_channel.write_command(0x01, std::span<const std::byte>());  // COM_QUIT
            co_await m_channel.flush_sends();
            m_channel.close();
            m_connected = false;
            co_await m_channel.connect(m_options);
            co_await login();
        }
    }

    Task<db::QueryResult> query(std::string_view sql,
                                mem::MemoryResource& result_resource) override {
        /* The body of query_impl(), inline: one coroutine frame per query, not two. */
        try {
            const auto body = std::as_bytes(std::span(sql));
            if (!m_channel.try_send_command(0x03, body)) {
                co_await m_channel.write_command(0x03, body);
            }
            std::span<const std::byte> first;
            SB_NEXT_PACKET_VIEW(m_channel, result_resource, first);
            co_return db::QueryResult::ok(co_await read_result(result_resource, first, false));
        } catch (const db::Error& error) {
            co_return db::QueryResult::err(error);
        } catch (const std::bad_alloc&) {
            co_return db::QueryResult::err(db::Error("MySQL client out of memory (heap exhausted)", 2008));
        } catch (...) {
            co_return db::QueryResult::err(db::Error("unexpected MySQL query error"));
        }
    }

    Task<db::ResultSet> query_impl(std::string_view sql,
                                   mem::MemoryResource& result_resource) {
        if (!m_channel.try_send_command(0x03, std::as_bytes(std::span(sql)))) {
            co_await m_channel.write_command(0x03, std::as_bytes(std::span(sql)));
        }
        std::span<const std::byte> first;
        SB_NEXT_PACKET_VIEW(m_channel, result_resource, first);
        db::ResultSet result = co_await read_result(result_resource, first, false);
        co_return result;
    }

    /* A steady stream of one command.  MySQL answers a connection's commands
    in order and every reply starts at packet sequence 1, so writing the next
    command between complete results keeps the sequence state consistent. */
    Task<db::Status> query_stream(std::string_view sql, std::uint32_t depth,
                                  std::uint64_t total,
                                  mem::MemoryResource& scratch) override {
        try {
            const auto body = std::as_bytes(std::span(sql));
            if (depth == 0) depth = 1;

            std::uint64_t sent = 0;
            std::uint64_t received = 0;
            while (sent < total && sent - received < depth) {
                if (!m_channel.try_send_command(0x03, body)) {
                    co_await m_channel.write_command(0x03, body);
                }
                ++sent;
            }
            mem::BumpArena results(scratch, 16 * 1024, "stream-results");
            while (received < total) {
                {
                    mem::ScopedAllocator scope(results);
                    m_channel.expect_reply();
                    std::span<const std::byte> first;
                    SB_NEXT_PACKET_VIEW(m_channel, scope, first);
                    (void)co_await read_result(scope, first, false);
                }
                ++received;
                if (sent < total) {
                    if (!m_channel.try_send_command(0x03, body)) {
                        co_await m_channel.write_command(0x03, body);
                    }
                    ++sent;
                }
            }
            co_return db::Status::ok();
        } catch (const db::Error& error) {
            co_return db::Status::err(error);
        } catch (const std::bad_alloc&) {
            co_return db::Status::err(db::Error("MySQL client out of memory (heap exhausted)", 2008));
        } catch (...) {
            co_return db::Status::err(db::Error("unexpected MySQL stream error"));
        }
    }

    Task<db::PrepareResult> prepare(std::string_view sql) override {
        try {
            co_return db::PrepareResult::ok(co_await prepare_impl(sql));
        } catch (const db::Error& error) {
            co_return db::PrepareResult::err(error);
        } catch (const std::bad_alloc&) {
            co_return db::PrepareResult::err(db::Error("MySQL client out of memory (heap exhausted)", 2008));
        } catch (...) {
            co_return db::PrepareResult::err(
                db::Error("unexpected MySQL prepare error"));
        }
    }

    Task<db::PreparedStatement*> prepare_impl(std::string_view sql) {
        if (!m_channel.try_send_command(0x16, std::as_bytes(std::span(sql)))) {
            co_await m_channel.write_command(0x16, std::as_bytes(std::span(sql)));
        }

        /* Transient: the response is parsed and dropped; it does not belong
        in the connection's heap. */
        mem::BumpArena scratch(mem::global_root(), 16 * 1024, "prepare-response");
        std::span<const std::byte> first;
        SB_NEXT_PACKET(m_channel, scratch, first);
        if (!first.empty() && std::to_integer<std::uint8_t>(first[0]) == 0xff) {
            ErrorPacket error;
            if (!parse_error(first, error)) {
                throw db::Error("malformed MySQL error packet", 2027);
            }
            // MySQL 8 refuses a handful of commands in the prepared protocol
            // (notably BEGIN/COMMIT).  Keep the Lua statement API intact by
            // executing those as text commands.
            if (error.m_code == 1295 && sql.find('?') == std::string_view::npos) {
                auto* fallback = mem::construct<MysqlStatement>(
                    *m_persistent, *this, *m_persistent, std::uint32_t{0}, std::uint16_t{0}, sql);
                fallback->m_text_mode = true;
                co_return fallback;
            }
            throw db::Error(error.m_code, error.m_state, error.m_message);
        }
        PrepareOk prepared;
        if (!parse_prepare_ok(first, prepared)) {
            throw db::Error("malformed COM_STMT_PREPARE_OK", 2027);
        }

        for (std::uint16_t i = 0; i < prepared.m_parameters; ++i) {
            (void)co_await m_channel.read_packet(scratch);
        }
        if (prepared.m_parameters > 0) {
            std::span<const std::byte> eof;
            SB_NEXT_PACKET(m_channel, scratch, eof);
            EofPacket ignored{};
            if (!parse_eof(eof, ignored)) {
                throw db::Error("missing EOF after prepared parameters", 2027);
            }
        }
        for (std::uint16_t i = 0; i < prepared.m_columns; ++i) {
            (void)co_await m_channel.read_packet(scratch);
        }
        if (prepared.m_columns > 0) {
            std::span<const std::byte> eof;
            SB_NEXT_PACKET(m_channel, scratch, eof);
            EofPacket ignored{};
            if (!parse_eof(eof, ignored)) {
                throw db::Error("missing EOF after prepared columns", 2027);
            }
        }

        auto* statement = mem::construct<MysqlStatement>(*m_persistent, *this, *m_persistent,
                                                         prepared.m_statement_id,
                                                         prepared.m_parameters, sql);
        co_return statement;
    }

    Task<db::Status> reconnect() override {
        try {
            co_await reconnect_impl();
            co_return db::Status::ok();
        } catch (const db::Error& error) {
            co_return db::Status::err(error);
        } catch (const std::bad_alloc&) {
            co_return db::Status::err(db::Error("MySQL client out of memory (heap exhausted)", 2008));
        } catch (...) {
            co_return db::Status::err(db::Error("unexpected MySQL reconnect error"));
        }
    }

    Task<void> reconnect_impl() {
        m_channel.close();
        m_pending_count = 0;
        m_connected = false;
        co_await m_channel.connect(m_options);
        co_await login();
    }

    void close() noexcept override {
        m_connected = false;
        m_pending_count = 0;
        m_channel.close();
    }

    // ---- Pipelining --------------------------------------------------------
    // Commands written ahead of their replies.  MySQL answers a connection's
    // commands in order and each reply starts at packet sequence 1, so a
    // command may be written whenever no reply is half read.  The ring records
    // which protocol (text or binary rows) each outstanding reply uses.

    Task<db::Status> send_query(std::string_view sql, mem::MemoryResource& scratch) override {
        try {
            if (m_pending_count == kMaxPipeline) {
                auto status = co_await read_reply(scratch);
                if (status.is_err()) co_return status;
            }
            const auto body = std::as_bytes(std::span(sql));
            if (!m_channel.try_send_command(0x03, body)) {
                co_await m_channel.write_command(0x03, body);
            }
            note_pending(false);
            co_return db::Status::ok();
        } catch (const db::Error& error) {
            co_return db::Status::err(error);
        } catch (const std::bad_alloc&) {
            co_return db::Status::err(db::Error("MySQL client out of memory (heap exhausted)", 2008));
        } catch (...) {
            co_return db::Status::err(db::Error("unexpected MySQL pipeline error"));
        }
    }

    // Read and discard the oldest outstanding reply.  A server error is
    // returned (the connection stays usable); a network error closes it.
    Task<db::Status> read_reply(mem::MemoryResource& scratch) override {
        if (m_pending_count == 0) co_return db::Status::ok();
        const bool binary = m_pending_binary[m_pending_first];
        m_pending_first = (m_pending_first + 1) % kMaxPipeline;
        --m_pending_count;
        try {
            m_channel.expect_reply();
            std::span<const std::byte> first;
            SB_NEXT_PACKET_VIEW(m_channel, scratch, first);
            (void)co_await read_result(scratch, first, binary);
            co_return db::Status::ok();
        } catch (const db::Error& error) {
            if (!m_channel.healthy()) m_pending_count = 0;
            co_return db::Status::err(error);
        } catch (const std::bad_alloc&) {
            co_return db::Status::err(db::Error("MySQL client out of memory (heap exhausted)", 2008));
        } catch (...) {
            co_return db::Status::err(db::Error("unexpected MySQL pipeline error"));
        }
    }

    [[nodiscard]] std::uint32_t pending_replies() const noexcept override {
        return m_pending_count;
    }
    [[nodiscard]] int native_socket() const noexcept override { return m_channel.fd(); }

    [[nodiscard]] bool supports_sync() const noexcept override {
        return m_connected && m_pending_count == 0 && m_channel.synchronous();
    }

    bool query_into(std::string_view sql, db::ResultSet& out, mem::MemoryResource& resource,
                    db::Error& error) override {
        try {
            m_channel.send_command_sync(0x03, std::as_bytes(std::span(sql)));
            read_result_sync(out, resource, m_channel.next_packet_sync(resource), false);
            return true;
        } catch (const db::Error& failure) {
            error = failure;
        } catch (const std::bad_alloc&) {
            error = db::Error("MySQL client out of memory (heap exhausted)", 2008);
        } catch (...) {
            error = db::Error("unexpected MySQL query error");
        }
        return false;
    }

    static constexpr std::uint32_t kMaxPipeline = 256;
    [[nodiscard]] bool pipeline_full() const noexcept { return m_pending_count == kMaxPipeline; }
    void note_pending(bool binary) noexcept {
        m_pending_binary[(m_pending_first + m_pending_count) % kMaxPipeline] = binary;
        ++m_pending_count;
    }

    [[nodiscard]] bool healthy() const noexcept override {
        return m_connected && m_channel.healthy();
    }

    [[nodiscard]] std::string_view driver_name() const noexcept override {
        return "mysql";
    }

    void destroy(mem::MemoryResource& resource) noexcept override {
        std::destroy_at(this);
        resource.deallocate(this, sizeof(*this), alignof(MysqlConnection));
    }

    /* A reply's packets, parsed one at a time into a ResultSet.  The parser
    only consumes packets; the reader (awaiting or blocking) fetches them.
    Packets may be views into the read buffer: only row packets are kept
    (copied), and column definitions keep just their names.  Further result
    sets (SERVER_MORE_RESULTS_EXISTS) are parsed and dropped, adding their
    warnings and taking their status, as before. */
    class ResultParser {
    public:
        // Fills `out`, which the caller has reset.
        ResultParser(db::ResultSet& out, mem::MemoryResource& resource, bool binary)
            : m_result(out), m_resource(&resource), m_binary(binary) {}

        // True when the reply is complete.
        bool feed(std::span<const std::byte> packet) {
            SB_LAYER(Protocol);
            switch (m_state) {
                case State::First: return first(packet);
                case State::Columns: return column(packet);
                case State::Terminator: return terminator(packet);
                case State::Rows: return row(packet);
            }
            return true;
        }
    private:
        enum class State : std::uint8_t { First, Columns, Terminator, Rows };

        bool first(std::span<const std::byte> packet) {
            if (packet.empty()) throw db::Error("empty MySQL response", 2027);
            const auto header = std::to_integer<std::uint8_t>(packet[0]);
            if (header == 0xff) throw_server_error(packet);
            if (header == 0x00) {
                OkPacket ok;
                if (!parse_ok(packet, ok)) throw db::Error("malformed OK packet", 2027);
                if (m_extra) return finish_extra(ok.m_status, ok.m_warnings);
                m_result.m_has_rows = false;
                m_result.m_affected_rows = ok.m_affected_rows;
                m_result.m_last_insert_id = ok.m_last_insert_id;
                m_result.m_status = ok.m_status;
                m_result.m_warnings = ok.m_warnings;
                return next_result(ok.m_status);
            }
            Reader count_reader(packet);
            if (!count_reader.lenenc(m_column_count) || m_column_count > kMaxColumns) {
                throw db::Error("invalid MySQL column count", 2027);
            }
            m_columns_seen = 0;
            if (!m_extra) m_result.m_columns.reserve(static_cast<std::size_t>(m_column_count));
            m_state = m_column_count == 0 ? State::Terminator : State::Columns;
            return false;
        }

        bool column(std::span<const std::byte> packet) {
            db::Column column;
            if (!parse_column(packet, column)) {
                throw db::Error("malformed MySQL column definition", 2027);
            }
            if (!m_extra) {
                if (!column.m_name.empty()) {
                    auto* name = static_cast<char*>(m_resource->allocate(column.m_name.size(), 1));
                    std::memcpy(name, column.m_name.data(), column.m_name.size());
                    column.m_name = std::string_view(name, column.m_name.size());
                }
                m_result.m_columns.push_back(column);
            }
            if (++m_columns_seen == m_column_count) m_state = State::Terminator;
            return false;
        }

        bool terminator(std::span<const std::byte> packet) {
            EofPacket eof{};
            if (!parse_eof(packet, eof)) {
                // Tolerate servers that answer with OK despite no DEPRECATE_EOF.
                OkPacket ok{};
                if (!parse_ok(packet, ok)) throw db::Error("missing metadata terminator", 2027);
                eof.m_status = ok.m_status;
                eof.m_warnings = ok.m_warnings;
            }
            if (!m_extra) {
                m_result.m_status = eof.m_status;
                m_result.m_warnings = eof.m_warnings;
                m_result.m_has_rows = true;
            }
            m_state = State::Rows;
            return false;
        }

        bool row(std::span<const std::byte> packet) {
            if (!packet.empty() && std::to_integer<std::uint8_t>(packet[0]) == 0xff) {
                throw_server_error(packet);
            }
            if (!packet.empty() && std::to_integer<std::uint8_t>(packet[0]) == 0xfe &&
                packet.size() < 9) {
                EofPacket row_eof{};
                if (!parse_eof(packet, row_eof)) throw db::Error("malformed EOF packet", 2027);
                if (m_extra) return finish_extra(row_eof.m_status, row_eof.m_warnings);
                m_result.m_status = row_eof.m_status;
                m_result.m_warnings = row_eof.m_warnings;
                return next_result(row_eof.m_status);
            }
            if (m_extra) return false;
            /* A row's fields point into its packet, which outlives this read:
            copy the packet (only rows are kept; terminators are not). */
            if (!packet.empty()) {
                auto* kept = static_cast<std::byte*>(m_resource->allocate(packet.size(), 1));
                std::memcpy(kept, packet.data(), packet.size());
                packet = std::span<const std::byte>(kept, packet.size());
            }
            const bool parsed =
                m_binary ? parse_binary_row(packet, m_result.m_columns, *m_resource,
                                            m_result.m_fields)
                         : parse_text_row(packet, static_cast<std::size_t>(m_column_count),
                                          *m_resource, m_result.m_fields);
            if (!parsed) throw db::Error("malformed MySQL row", 2027);
            ++m_result.m_row_count;
            return false;
        }

        // The current result set ended with `status`: another follows?
        bool next_result(std::uint16_t status) {
            if ((status & SERVER_MORE_RESULTS_EXISTS) == 0) return true;
            m_extra = true;
            m_state = State::First;
            return false;
        }
        bool finish_extra(std::uint16_t status, std::uint16_t warnings) {
            m_result.m_warnings += warnings;
            m_result.m_status = status;
            return next_result(status);
        }

        db::ResultSet& m_result;
        mem::MemoryResource* m_resource;
        bool m_binary;
        bool m_extra = false;
        State m_state = State::First;
        std::uint64_t m_column_count = 0;
        std::uint64_t m_columns_seen = 0;
    };

    Task<db::ResultSet> read_result(mem::MemoryResource& resource,
                                    std::span<const std::byte> first, bool binary) {
        db::ResultSet result(resource);
        ResultParser parser(result, resource, binary);
        bool done = parser.feed(first);
        while (!done) {
            std::span<const std::byte> packet;
            SB_NEXT_PACKET_VIEW(m_channel, resource, packet);
            done = parser.feed(packet);
        }
        co_return result;
    }

    // The same, reading with blocking calls: a plain function, no frame.
    void read_result_sync(db::ResultSet& out, mem::MemoryResource& resource,
                          std::span<const std::byte> first, bool binary) {
        out.reset(resource);
        ResultParser parser(out, resource, binary);
        bool done = parser.feed(first);
        while (!done) done = parser.feed(m_channel.next_packet_sync(resource));
    }

    void mark_disconnected() noexcept { m_connected = false; }
    PacketChannel& channel() noexcept { return m_channel; }
    Bytes& request_buffer() noexcept { return m_request_buffer; }
    mem::MemoryResource& persistent() noexcept { return *m_persistent; }

private:
    std::array<bool, kMaxPipeline> m_pending_binary{};
    std::uint32_t m_pending_first = 0;
    std::uint32_t m_pending_count = 0;
    [[noreturn]] static void throw_server_error(std::span<const std::byte> packet) {
        ErrorPacket error;
        if (!parse_error(packet, error)) {
            throw db::Error("malformed MySQL error packet", 2027);
        }
        throw db::Error(error.m_code, error.m_state, error.m_message);
    }

    void throw_network(const char* operation, int code, int error) {
        char message[256];
        std::snprintf(message, sizeof(message), "MySQL %s failed: %s", operation,
                      std::strerror(error <= 0 ? errno : error));
        throw db::Error(message, code);
    }

    /* Handshake and authentication packets are scratch: they live in an arena
    carved from the request heap and released when the login ends, so logging
    in again (reconnect, NIC steering) costs the connection heap nothing. */
    Task<void> login() {
        mem::BumpArena login_scratch(m_request_heap, 8 * 1024, "login");
        const auto handshake_packet = co_await m_channel.read_packet(login_scratch);
        if (handshake_packet.empty()) {
            throw db::Error("empty MySQL handshake", 2027);
        }
        if (std::to_integer<std::uint8_t>(handshake_packet[0]) == 0xff) {
            throw_server_error(handshake_packet);
        }
        Handshake handshake;
        if (!parse_handshake(handshake_packet, handshake)) {
            throw db::Error("unsupported MySQL handshake", 2027);
        }
        m_handshake = handshake;

        char plugin[64]{};
        std::memcpy(plugin, handshake.m_auth_plugin, sizeof(plugin) - 1);
        std::array<std::uint8_t, 20> scramble{};
        std::memcpy(scramble.data(), handshake.m_scramble.data(),
                    std::min(handshake.m_scramble_length, scramble.size()));
        std::size_t scramble_length = std::min<std::size_t>(handshake.m_scramble_length, 20);

        bool tls = false;
        if (m_options.m_tls_mode != db::TlsMode::Disabled) {
            if ((handshake.m_capabilities & CLIENT_SSL) == 0) {
                if (m_options.m_tls_mode == db::TlsMode::Required) {
                    throw db::Error("MySQL server does not support TLS", 2026);
                }
            } else {
                Bytes ssl_request =
                    build_ssl_request(handshake, m_options, login_scratch);
                co_await m_channel.write_packet(ssl_request);
                co_await m_channel.start_tls(m_options);
                tls = true;
            }
        }
        auto build_response = [&](bool use_tls) {
            Bytes auth = auth_response_for(
                plugin, m_options.m_password,
                std::span<const std::uint8_t>(scramble.data(), scramble_length),
                login_scratch);
            return build_handshake_response(handshake, m_options, auth, plugin,
                                            login_scratch, use_tls);
        };

        Bytes response = build_response(tls);
        co_await m_channel.write_packet(response);

        for (;;) {
            const auto packet = co_await m_channel.read_packet(login_scratch);
            if (packet.empty()) throw db::Error("empty auth response", 2027);
            const auto header = std::to_integer<std::uint8_t>(packet[0]);
            if (header == 0x00) {
                OkPacket ok;
                if (!parse_ok(packet, ok)) {
                    throw db::Error("malformed auth OK packet", 2027);
                }
                m_connected = true;
                co_return;
            }
            if (header == 0xff) throw_server_error(packet);
            if (header == 0xfe) {
                Reader reader(packet);
                std::uint8_t ignored = 0;
                (void)reader.u8(ignored);
                std::string_view switched_plugin;
                if (reader.null_terminated(switched_plugin) != Reader::StringResult::Ok ||
                    switched_plugin.empty() || switched_plugin.size() >= sizeof(plugin)) {
                    throw db::Error("malformed MySQL auth switch", 2027);
                }
                std::memset(plugin, 0, sizeof(plugin));
                std::memcpy(plugin, switched_plugin.data(), switched_plugin.size());
                const auto remaining = reader.remaining_data();
                scramble_length =
                    std::min<std::size_t>(remaining.size(), scramble.size());
                std::memcpy(scramble.data(), remaining.data(), scramble_length);
                if (switched_plugin == "mysql_clear_password") {
                    if (!m_channel.tls_active()) {
                        throw db::Error("mysql_clear_password requires TLS", 2026);
                    }
                    Bytes clear = make_bytes(login_scratch);
                    clear.resize(m_options.m_password.size() + 1);
                    std::memcpy(clear.data(), m_options.m_password.data(),
                                m_options.m_password.size());
                    clear[m_options.m_password.size()] = std::byte{0};
                    co_await m_channel.write_packet(clear);
                    continue;
                }
                Bytes switched = build_response(m_channel.tls_active());
                co_await m_channel.write_packet(switched);
                continue;
            }
            if (header == 0x01) {
                const auto data = packet.subspan(1);
                const bool caching =
                    std::string_view(plugin) == "caching_sha2_password";
                const bool sha256 = std::string_view(plugin) == "sha256_password";
                if (caching && !data.empty() &&
                    std::to_integer<std::uint8_t>(data[0]) == 0x03) {
                    continue;  // fast auth succeeded; OK follows
                }
                if ((caching && !data.empty() &&
                     std::to_integer<std::uint8_t>(data[0]) == 0x04) ||
                    sha256) {
                    if (m_channel.tls_active()) {
                        Bytes clear = make_bytes(login_scratch);
                        clear.resize(m_options.m_password.size() + 1);
                        std::memcpy(clear.data(), m_options.m_password.data(),
                                    m_options.m_password.size());
                        clear[m_options.m_password.size()] = std::byte{0};
                        co_await m_channel.write_packet(clear);
                        continue;
                    }
                    // Request the server's RSA public key.
                    const std::array<std::byte, 1> request{std::byte{0x02}};
                    co_await m_channel.write_packet(request);
                    const auto key_packet = co_await m_channel.read_packet(login_scratch);
                    if (key_packet.empty()) {
                        throw db::Error("empty RSA key packet", 2027);
                    }
                    if (std::to_integer<std::uint8_t>(key_packet[0]) == 0xff) {
                        throw_server_error(key_packet);
                    }
                    std::span<const std::byte> pem = key_packet;
                    if (std::to_integer<std::uint8_t>(key_packet[0]) == 0x01) {
                        pem = key_packet.subspan(1);
                    }
                    Bytes encrypted = rsa_encrypt_password(
                        m_options.m_password,
                        std::span<const std::uint8_t>(scramble.data(), scramble_length),
                        {reinterpret_cast<const char*>(pem.data()), pem.size()},
                        login_scratch);
                    co_await m_channel.write_packet(encrypted);
                    continue;
                }
                if (caching && !data.empty() &&
                    std::to_integer<std::uint8_t>(data[0]) == '-') {
                    Bytes encrypted = rsa_encrypt_password(
                        m_options.m_password,
                        std::span<const std::uint8_t>(scramble.data(), scramble_length),
                        {reinterpret_cast<const char*>(data.data()), data.size()},
                        login_scratch);
                    co_await m_channel.write_packet(encrypted);
                    continue;
                }
                throw db::Error("unsupported MySQL auth continuation", 2061);
            }
            throw db::Error("unexpected MySQL auth packet", 2027);
        }
    }

    mem::MemoryResource* m_persistent;
    OwnedOptions m_owned_options;
    PacketChannel m_channel;
    db::ConnectionOptions m_options;
    /* Requests that must be assembled before they are framed (COM_STMT_EXECUTE
    with its bound values) are built in this connection's own heap, carved from
    the root on the connection's node.  The connection heap holds only
    connection state; requests never grow it. */
    mem::HeapArena m_request_heap;
    Bytes m_request_buffer;
    Handshake m_handshake{};
    bool m_connected = false;
};

MysqlStatement::MysqlStatement(MysqlConnection& connection,
                               mem::MemoryResource& persistent, std::uint32_t id,
                               std::uint16_t parameters, std::string_view sql)
    : m_connection(&connection),
      m_sql(mem::Allocator<char>(&persistent)),
      m_id(id),
      m_parameters(parameters) {
    m_sql.assign(sql);
}

/* The prepared-statement path keeps its body here rather than in
execute_impl(): one coroutine frame per execute, not two. */
Task<db::QueryResult> MysqlStatement::execute(
    std::span<const db::BindValue> parameters, mem::MemoryResource& result_resource) {
    try {
        if (m_closed) throw db::Error("prepared statement is closed", 1243);
        if (m_text_mode) {
            co_return db::QueryResult::ok(co_await execute_impl(parameters, result_resource));
        }
        Bytes& request = m_connection->request_buffer();
        request.clear();
        build_stmt_execute_payload(request, m_id, parameters, types_changed(parameters));
        PacketChannel& channel = m_connection->channel();
        if (!channel.try_send_command(std::span<const std::byte>(request))) {
            co_await channel.write_command(request);
        }
        std::span<const std::byte> first;
        SB_NEXT_PACKET_VIEW(channel, result_resource, first);
        if (!first.empty() && std::to_integer<std::uint8_t>(first[0]) == 0xff) {
            ErrorPacket error;
            if (!parse_error(first, error)) {
                throw db::Error("malformed MySQL error packet", 2027);
            }
            throw db::Error(error.m_code, error.m_state, error.m_message);
        }
        co_return db::QueryResult::ok(
            co_await m_connection->read_result(result_resource, first, true));
    } catch (const db::Error& error) {
        co_return db::QueryResult::err(error);
    } catch (const std::bad_alloc&) {
        co_return db::QueryResult::err(db::Error("MySQL client out of memory (heap exhausted)", 2008));
    } catch (...) {
        co_return db::QueryResult::err(
            db::Error("unexpected MySQL statement error"));
    }
}

/* Synchronous execute: COM_STMT_EXECUTE and its reply in this call. */
bool MysqlStatement::execute_into(std::span<const db::BindValue> parameters,
                                  db::ResultSet& out, mem::MemoryResource& resource,
                                  db::Error& error) {
    try {
        if (m_closed) throw db::Error("prepared statement is closed", 1243);
        if (m_text_mode) {
            if (!parameters.empty()) {
                throw db::Error("text-fallback statement cannot bind parameters", 1295);
            }
            return m_connection->query_into(std::string_view(m_sql.data(), m_sql.size()), out,
                                            resource, error);
        }
        Bytes& request = m_connection->request_buffer();
        request.clear();
        build_stmt_execute_payload(request, m_id, parameters, types_changed(parameters));
        PacketChannel& channel = m_connection->channel();
        const auto payload = std::span<const std::byte>(request);
        channel.send_command_sync(std::to_integer<std::uint8_t>(payload[0]), payload.subspan(1));
        m_connection->read_result_sync(out, resource, channel.next_packet_sync(resource), true);
        return true;
    } catch (const db::Error& failure) {
        error = failure;
    } catch (const std::bad_alloc&) {
        error = db::Error("MySQL client out of memory (heap exhausted)", 2008);
    } catch (...) {
        error = db::Error("unexpected MySQL statement error");
    }
    return false;
}

/* Pipelined execute: write COM_STMT_EXECUTE, leave the reply to
Connection::read_reply(). */
Task<db::Status> MysqlStatement::send_execute(std::span<const db::BindValue> parameters,
                                              mem::MemoryResource& scratch) {
    try {
        if (m_closed) throw db::Error("prepared statement is closed", 1243);
        if (m_text_mode) {
            if (!parameters.empty()) {
                throw db::Error("text-fallback statement cannot bind parameters", 1295);
            }
            co_return co_await m_connection->send_query(
                std::string_view(m_sql.data(), m_sql.size()), scratch);
        }
        if (m_connection->pipeline_full()) {
            auto status = co_await m_connection->read_reply(scratch);
            if (status.is_err()) co_return status;
        }
        Bytes& request = m_connection->request_buffer();
        request.clear();
        build_stmt_execute_payload(request, m_id, parameters, types_changed(parameters));
        PacketChannel& channel = m_connection->channel();
        if (!channel.try_send_command(std::span<const std::byte>(request))) {
            co_await channel.write_command(request);
        }
        m_connection->note_pending(true);
        co_return db::Status::ok();
    } catch (const db::Error& error) {
        co_return db::Status::err(error);
    } catch (const std::bad_alloc&) {
        co_return db::Status::err(db::Error("MySQL client out of memory (heap exhausted)", 2008));
    } catch (...) {
        co_return db::Status::err(db::Error("unexpected MySQL statement error"));
    }
}

Task<db::ResultSet> MysqlStatement::execute_impl(
    std::span<const db::BindValue> parameters, mem::MemoryResource& result_resource) {
    if (m_closed) throw db::Error("prepared statement is closed", 1243);
    if (m_text_mode) {
        if (!parameters.empty()) {
            throw db::Error("text-fallback statement cannot bind parameters", 1295);
        }
        db::ResultSet result = co_await m_connection->query_impl(
            std::string_view(m_sql.data(), m_sql.size()), result_resource);
        co_return result;
    }
    Bytes& request = m_connection->request_buffer();
    request.clear();
    build_stmt_execute_payload(request, m_id, parameters, types_changed(parameters));
    if (!m_connection->channel().try_send_command(std::span<const std::byte>(request))) {
        co_await m_connection->channel().write_command(request);
    }
    std::span<const std::byte> first;
    SB_NEXT_PACKET_VIEW(m_connection->channel(), result_resource, first);
    if (!first.empty() && std::to_integer<std::uint8_t>(first[0]) == 0xff) {
        ErrorPacket error;
        if (!parse_error(first, error)) {
            throw db::Error("malformed MySQL error packet", 2027);
        }
        throw db::Error(error.m_code, error.m_state, error.m_message);
    }
    db::ResultSet result =
        co_await m_connection->read_result(result_resource, first, true);
    co_return result;
}

Task<db::Status> MysqlStatement::close() {
    try {
        co_await close_impl();
        co_return db::Status::ok();
    } catch (const db::Error& error) {
        co_return db::Status::err(error);
    } catch (const std::bad_alloc&) {
        co_return db::Status::err(db::Error("MySQL client out of memory (heap exhausted)", 2008));
    } catch (...) {
        co_return db::Status::err(db::Error("unexpected MySQL close error"));
    }
}

Task<void> MysqlStatement::close_impl() {
    if (m_closed) co_return;
    m_closed = true;
    if (m_text_mode || !m_connection->healthy()) co_return;
    std::array<std::byte, 5> payload{
        std::byte{0x19},
        static_cast<std::byte>(m_id & 0xff),
        static_cast<std::byte>((m_id >> 8) & 0xff),
        static_cast<std::byte>((m_id >> 16) & 0xff),
        static_cast<std::byte>((m_id >> 24) & 0xff)};
    co_await m_connection->channel().write_command(payload);
}

class MysqlDriver final : public db::Driver {
public:
    explicit MysqlDriver(mem::MemoryResource& resource) : m_resource(&resource) {}

    [[nodiscard]] std::string_view name() const noexcept override { return "mysql"; }

    Task<db::ConnectResult> connect(const db::ConnectionOptions& options,
                                    mem::MemoryResource& persistent) override {
        (void)m_resource;
        if (options.m_protocol_version == 2) {
#ifdef SYSBENCH_HAVE_MYSQL_V2
            co_return co_await connect_v2(options, persistent);
#else
            co_return db::ConnectResult::err(db::Error(
                "MySQL protocol 2 requires a build with SYSBENCH_MYSQL_V2=ON", 2054));
#endif
        }
        if (options.m_protocol_version != 1) {
            co_return db::ConnectResult::err(db::Error("unsupported MySQL protocol revision", 2054));
        }
        auto* connection = mem::construct<MysqlConnection>(persistent, persistent, options);
        try {
            co_await connection->connect_async();
        } catch (const db::Error& error) {
            connection->close();
            connection->destroy(persistent);
            co_return db::ConnectResult::err(error);
        } catch (const std::exception& failure) {
            connection->close();
            connection->destroy(persistent);
            char message[256];
            std::snprintf(message, sizeof(message), "MySQL connect failed: %s", failure.what());
            co_return db::ConnectResult::err(db::Error(message, 2013));
        } catch (...) {
            connection->close();
            connection->destroy(persistent);
            co_return db::ConnectResult::err(
                db::Error("unexpected MySQL connect error"));
        }
        co_return db::ConnectResult::ok(static_cast<db::Connection*>(connection));
    }

    void destroy(mem::MemoryResource& resource) noexcept override {
        std::destroy_at(this);
        resource.deallocate(this, sizeof(*this), alignof(MysqlDriver));
    }

private:
    mem::MemoryResource* m_resource;
};

db::Driver* create_mysql_driver(mem::MemoryResource& resource) {
    return mem::construct<MysqlDriver>(resource, resource);
}

}  // namespace

ByteStream* create_byte_stream(mem::MemoryResource& resource) {
    return mem::construct<PacketChannel>(resource, resource);
}

void register_driver() {
    (void)db::DriverRegistry::instance().add("mysql", &create_mysql_driver);
}

}  // namespace sb::mysql
