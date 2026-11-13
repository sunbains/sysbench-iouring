#include "sysbench/mysql.hpp"

#include "sysbench/io_ring.hpp"
#include "sysbench/memory.hpp"
#include "sysbench/mysql_wire.hpp"
#include "sysbench/runtime.hpp"
#include "sysbench/socket_io.hpp"
#include "sysbench/task.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
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
#include <span>
#include <string_view>

namespace sb::mysql {
namespace {

constexpr std::size_t kMaxColumns = 4096;

class PacketChannel {
public:
    explicit PacketChannel(mem::MemoryResource& persistent)
        : m_io(*Runtime::current(), Runtime::current_worker_id()) {
        
        auto& pool = m_io.buffers();
        m_read_buffer = pool.acquire();
        m_write_buffer = pool.acquire();
        if (!m_read_buffer.valid() || !m_write_buffer.valid()) {
            throw db::Error("no registered MySQL socket buffers available", 2002);
        }
        (void)persistent;
    }

    ~PacketChannel() { close(); }

    PacketChannel(const PacketChannel&) = delete;
    PacketChannel& operator=(const PacketChannel&) = delete;

    Task<void> connect(const db::ConnectionOptions& options) {
        close();

        if (!options.socket_path.empty() ||
            (!options.host.empty() && options.host.front() == '/')) {
            const std::string_view path =
                !options.socket_path.empty() ? options.socket_path : options.host;
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
            co_return;
        }

        char host[256]{};
        char port[32]{};
        const std::size_t host_length = std::min(options.host.size(), sizeof(host) - 1);
        const std::size_t port_length = std::min(options.port.size(), sizeof(port) - 1);
        std::memcpy(host, options.host.data(), host_length);
        std::memcpy(port, options.port.data(), port_length);
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
    }

    [[nodiscard]] bool tls_active() const noexcept { return m_tls_active; }

    Task<void> start_tls(const db::ConnectionOptions& options) {
        close_tls();
        SSL_CTX* context = SSL_CTX_new(TLS_client_method());
        if (context == nullptr) throw tls_failure("SSL_CTX_new");
        SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION);
        SSL_CTX_set_options(context, SSL_OP_NO_COMPRESSION);
        if (options.tls_verify_server_cert) {
            SSL_CTX_set_verify(context, SSL_VERIFY_PEER, nullptr);
            if (!options.tls_ca.empty()) {
                char ca[4096]{};
                const std::size_t length =
                    std::min(options.tls_ca.size(), sizeof(ca) - 1);
                std::memcpy(ca, options.tls_ca.data(), length);
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
        load_identity(options.tls_cert, true);
        load_identity(options.tls_key, false);
        if (!options.tls_cert.empty() && !options.tls_key.empty() &&
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
        const std::size_t host_length = std::min(options.host.size(), sizeof(host) - 1);
        std::memcpy(host, options.host.data(), host_length);
        const bool is_path = host[0] == '/';
        in_addr ipv4{};
        in6_addr ipv6{};
        const bool is_ip = ::inet_pton(AF_INET, host, &ipv4) == 1 ||
                           ::inet_pton(AF_INET6, host, &ipv6) == 1;
        const char* verify_host = host_length == 0 ? "localhost" : host;
        if (!is_path && !is_ip && host_length != 0) {
            (void)SSL_set_tlsext_host_name(ssl, host);
        }
        if (options.tls_verify_server_cert) {
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
        m_tls_active = true;
    }

    Task<void> write_command(std::span<const std::byte> payload) {
        m_sequence = 0;
        co_await write_packet(payload);
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
                auto* target = static_cast<std::byte*>(m_write_buffer.data());
                std::memcpy(target, header.data(), header.size());
                if (chunk != 0) {
                    std::memcpy(target + 4, payload.data() + offset, chunk);
                }
                const int written = co_await write_fixed(m_io, m_fd, target,
                                                          static_cast<unsigned>(chunk + 4),
                                                          m_write_buffer.index());
                if (written < 0) {
                    close();
                    throw write_failure(written);
                }
                const std::size_t sent = static_cast<std::size_t>(written);
                if (sent < chunk + 4) {
                    co_await write_exact(target + sent, chunk + 4 - sent);
                }
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

    Task<std::span<const std::byte>> read_packet(mem::MemoryResource& resource) {
        std::array<std::byte, 4> header{};
        co_await read_exact(header.data(), header.size());
        PacketHeader parsed{};
        if (!parse_packet_header(header, parsed)) {
            throw db::Error("malformed MySQL packet header", 2027);
        }
        if (parsed.sequence != m_sequence) {
            throw db::Error("unexpected MySQL packet sequence", 2027);
        }
        ++m_sequence;
        if (parsed.length == 0x00ffffff) {
            throw db::Error("MySQL packets larger than 16 MiB are unsupported", 2027);
        }
        if (parsed.length == 0) {
            co_return std::span<const std::byte>();
        }
        auto* memory = static_cast<std::byte*>(resource.allocate(parsed.length, 1));
        co_await read_exact(memory, parsed.length);
        co_return std::span<const std::byte>(memory, parsed.length);
    }

    void close() noexcept {
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

    [[nodiscard]] bool healthy() const noexcept { return m_healthy && m_fd >= 0; }

private:
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
                const int received = co_await read_fixed(
                    m_io, m_fd, m_read_buffer.data(), m_read_buffer.size(),
                    m_read_buffer.index());
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
            co_await read_fixed(m_io, m_fd, m_read_buffer.data(),
                                m_read_buffer.size(), m_read_buffer.index());
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
    std::array<std::byte, 16384> m_tls_plain{};
    std::size_t m_tls_plain_offset = 0;
    std::size_t m_tls_plain_size = 0;
};

class MysqlConnection;

struct OwnedOptions {
    mem::String host;
    mem::String port;
    mem::String user;
    mem::String password;
    mem::String database;
    mem::String socket_path;
    mem::String tls_ca;
    mem::String tls_cert;
    mem::String tls_key;

    OwnedOptions(const db::ConnectionOptions& source, mem::MemoryResource& resource)
        : host(mem::Allocator<char>(&resource)),
          port(mem::Allocator<char>(&resource)),
          user(mem::Allocator<char>(&resource)),
          password(mem::Allocator<char>(&resource)),
          database(mem::Allocator<char>(&resource)),
          socket_path(mem::Allocator<char>(&resource)),
          tls_ca(mem::Allocator<char>(&resource)),
          tls_cert(mem::Allocator<char>(&resource)),
          tls_key(mem::Allocator<char>(&resource)),
          connect_timeout_ms(source.connect_timeout_ms),
          max_packet_size(source.max_packet_size),
          tls_mode(source.tls_mode),
          tls_verify_server_cert(source.tls_verify_server_cert) {
        host.assign(source.host);
        port.assign(source.port);
        user.assign(source.user);
        password.assign(source.password);
        database.assign(source.database);
        socket_path.assign(source.socket_path);
        tls_ca.assign(source.tls_ca);
        tls_cert.assign(source.tls_cert);
        tls_key.assign(source.tls_key);
    }

    [[nodiscard]] db::ConnectionOptions view() const noexcept {
        return db::ConnectionOptions{
            .host = host,
            .port = port,
            .user = user,
            .password = password,
            .database = database,
            .socket_path = socket_path,
            .connect_timeout_ms = connect_timeout_ms,
            .max_packet_size = max_packet_size,
            .tls_mode = tls_mode,
            .tls_verify_server_cert = tls_verify_server_cert,
            .tls_ca = tls_ca,
            .tls_cert = tls_cert,
            .tls_key = tls_key};
    }

    std::uint32_t connect_timeout_ms;
    std::uint32_t max_packet_size;
    db::TlsMode tls_mode;
    bool tls_verify_server_cert;
};

class MysqlStatement final : public db::PreparedStatement {
public:
    MysqlStatement(MysqlConnection& connection, mem::MemoryResource& persistent,
                   std::uint32_t id, std::uint16_t parameters, std::string_view sql);
    ~MysqlStatement() override = default;

    Task<db::QueryResult> execute(std::span<const db::BindValue> parameters,
                                  mem::MemoryResource& result_resource) override;
    Task<db::Status> close() override;
    Task<db::ResultSet> execute_impl(std::span<const db::BindValue> parameters,
                                     mem::MemoryResource& result_resource);
    Task<void> close_impl();
    [[nodiscard]] bool closed() const noexcept override { return m_closed; }
    [[nodiscard]] std::string_view sql() const noexcept override { return m_sql; }
    [[nodiscard]] std::uint32_t id() const noexcept { return m_id; }
    [[nodiscard]] std::uint16_t parameter_count() const noexcept { return m_parameters; }

    void destroy(mem::MemoryResource& resource) noexcept override {
        this->~MysqlStatement();
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
};

class MysqlConnection final : public db::Connection {
public:
    MysqlConnection(mem::MemoryResource& persistent, const db::ConnectionOptions& options)
        : m_persistent(&persistent),
          m_owned_options(options, persistent),
          m_channel(persistent),
          m_options(m_owned_options.view()),
          m_request_buffer(mem::Allocator<std::byte>(&persistent)) {}

    ~MysqlConnection() override { close(); }

    Task<void> connect_async() {
        co_await m_channel.connect(m_options);
        co_await login();
    }

    Task<db::QueryResult> query(std::string_view sql,
                                mem::MemoryResource& result_resource) override {
        try {
            co_return db::QueryResult::ok(co_await query_impl(sql, result_resource));
        } catch (const db::Error& error) {
            co_return db::QueryResult::err(error);
        } catch (...) {
            co_return db::QueryResult::err(db::Error("unexpected MySQL query error"));
        }
    }

    Task<db::ResultSet> query_impl(std::string_view sql,
                                   mem::MemoryResource& result_resource) {
        m_request_buffer.clear();
        m_request_buffer.reserve(1 + sql.size());
        Writer writer(m_request_buffer);
        writer.u8(0x03);
        writer.bytes(sql);
        co_await m_channel.write_command(m_request_buffer);
        const auto first = co_await m_channel.read_packet(result_resource);
        db::ResultSet result = co_await read_result(result_resource, first, false);
        co_return result;
    }

    Task<db::PrepareResult> prepare(std::string_view sql) override {
        try {
            co_return db::PrepareResult::ok(co_await prepare_impl(sql));
        } catch (const db::Error& error) {
            co_return db::PrepareResult::err(error);
        } catch (...) {
            co_return db::PrepareResult::err(
                db::Error("unexpected MySQL prepare error"));
        }
    }

    Task<db::PreparedStatement*> prepare_impl(std::string_view sql) {
        m_request_buffer.clear();
        m_request_buffer.reserve(1 + sql.size());
        Writer writer(m_request_buffer);
        writer.u8(0x16);
        writer.bytes(sql);
        co_await m_channel.write_command(m_request_buffer);

        mem::BumpArena scratch(*m_persistent, 16 * 1024, "prepare-response");
        const auto first = co_await m_channel.read_packet(scratch);
        if (!first.empty() && std::to_integer<std::uint8_t>(first[0]) == 0xff) {
            ErrorPacket error;
            if (!parse_error(first, error)) {
                throw db::Error("malformed MySQL error packet", 2027);
            }
            // MySQL 8 refuses a handful of commands in the prepared protocol
            // (notably BEGIN/COMMIT).  Keep the Lua statement API intact by
            // executing those as text commands.
            if (error.code == 1295 && sql.find('?') == std::string_view::npos) {
                auto* fallback = mem::construct<MysqlStatement>(
                    *m_persistent, *this, *m_persistent, std::uint32_t{0}, std::uint16_t{0}, sql);
                fallback->m_text_mode = true;
                co_return fallback;
            }
            throw db::Error(error.code, error.state, error.message);
        }
        PrepareOk prepared;
        if (!parse_prepare_ok(first, prepared)) {
            throw db::Error("malformed COM_STMT_PREPARE_OK", 2027);
        }

        for (std::uint16_t i = 0; i < prepared.parameters; ++i) {
            (void)co_await m_channel.read_packet(scratch);
        }
        if (prepared.parameters > 0) {
            const auto eof = co_await m_channel.read_packet(scratch);
            EofPacket ignored{};
            if (!parse_eof(eof, ignored)) {
                throw db::Error("missing EOF after prepared parameters", 2027);
            }
        }
        for (std::uint16_t i = 0; i < prepared.columns; ++i) {
            (void)co_await m_channel.read_packet(scratch);
        }
        if (prepared.columns > 0) {
            const auto eof = co_await m_channel.read_packet(scratch);
            EofPacket ignored{};
            if (!parse_eof(eof, ignored)) {
                throw db::Error("missing EOF after prepared columns", 2027);
            }
        }

        auto* statement = mem::construct<MysqlStatement>(*m_persistent, *this, *m_persistent,
                                                         prepared.statement_id,
                                                         prepared.parameters, sql);
        co_return statement;
    }

    Task<db::Status> reconnect() override {
        try {
            co_await reconnect_impl();
            co_return db::Status::ok();
        } catch (const db::Error& error) {
            co_return db::Status::err(error);
        } catch (...) {
            co_return db::Status::err(db::Error("unexpected MySQL reconnect error"));
        }
    }

    Task<void> reconnect_impl() {
        m_channel.close();
        m_connected = false;
        co_await m_channel.connect(m_options);
        co_await login();
    }

    void close() noexcept override {
        m_connected = false;
        m_channel.close();
    }

    [[nodiscard]] bool healthy() const noexcept override {
        return m_connected && m_channel.healthy();
    }

    [[nodiscard]] std::string_view driver_name() const noexcept override {
        return "mysql";
    }

    void destroy(mem::MemoryResource& resource) noexcept override {
        this->~MysqlConnection();
        resource.deallocate(this, sizeof(*this), alignof(MysqlConnection));
    }

    Task<db::ResultSet> read_result(mem::MemoryResource& resource,
                                    std::span<const std::byte> first, bool binary) {
        db::ResultSet result(resource);
        if (first.empty()) throw db::Error("empty MySQL response", 2027);
        const auto header = std::to_integer<std::uint8_t>(first[0]);
        if (header == 0xff) throw_server_error(first);
        if (header == 0x00) {
            OkPacket ok;
            if (!parse_ok(first, ok)) throw db::Error("malformed OK packet", 2027);
            result.has_rows = false;
            result.affected_rows = ok.affected_rows;
            result.last_insert_id = ok.last_insert_id;
            result.status = ok.status;
            result.warnings = ok.warnings;
            while ((result.status & SERVER_MORE_RESULTS_EXISTS) != 0) {
                const auto next = co_await m_channel.read_packet(resource);
                auto extra = co_await read_result(resource, next, binary);
                result.warnings += extra.warnings;
                result.status = extra.status;
            }
            co_return result;
        }

        Reader count_reader(first);
        std::uint64_t column_count = 0;
        if (!count_reader.lenenc(column_count) || column_count > kMaxColumns) {
            throw db::Error("invalid MySQL column count", 2027);
        }
        result.columns.reserve(static_cast<std::size_t>(column_count));
        for (std::uint64_t i = 0; i < column_count; ++i) {
            const auto packet = co_await m_channel.read_packet(resource);
            db::Column column;
            if (!parse_column(packet, column)) {
                throw db::Error("malformed MySQL column definition", 2027);
            }
            result.columns.push_back(column);
        }

        const auto terminator = co_await m_channel.read_packet(resource);
        EofPacket eof{};
        if (!parse_eof(terminator, eof)) {
            // Tolerate servers that answer with OK despite no DEPRECATE_EOF.
            OkPacket ok{};
            if (!parse_ok(terminator, ok)) {
                throw db::Error("missing metadata terminator", 2027);
            }
            eof.status = ok.status;
            eof.warnings = ok.warnings;
        }
        result.status = eof.status;
        result.warnings = eof.warnings;
        result.has_rows = true;

        for (;;) {
            const auto packet = co_await m_channel.read_packet(resource);
            if (!packet.empty() && std::to_integer<std::uint8_t>(packet[0]) == 0xff) {
                throw_server_error(packet);
            }
            if (!packet.empty() && std::to_integer<std::uint8_t>(packet[0]) == 0xfe &&
                packet.size() < 9) {
                EofPacket row_eof{};
                if (!parse_eof(packet, row_eof)) {
                    throw db::Error("malformed EOF packet", 2027);
                }
                result.status = row_eof.status;
                result.warnings = row_eof.warnings;
                break;
            }
            db::Row row(resource);
            const bool parsed = binary
                                    ? parse_binary_row(packet, result.columns, resource, row)
                                    : parse_text_row(packet,
                                                     static_cast<std::size_t>(column_count),
                                                     resource, row);
            if (!parsed) throw db::Error("malformed MySQL row", 2027);
            result.rows.push_back(std::move(row));
        }

        if ((result.status & SERVER_MORE_RESULTS_EXISTS) != 0) {
            for (;;) {
                const auto next = co_await m_channel.read_packet(resource);
                auto extra = co_await read_result(resource, next, binary);
                result.warnings += extra.warnings;
                result.status = extra.status;
                if ((extra.status & SERVER_MORE_RESULTS_EXISTS) == 0) break;
            }
        }
        co_return result;
    }

    void mark_disconnected() noexcept { m_connected = false; }
    PacketChannel& channel() noexcept { return m_channel; }
    Bytes& request_buffer() noexcept { return m_request_buffer; }
    mem::MemoryResource& persistent() noexcept { return *m_persistent; }

private:
    void throw_server_error(std::span<const std::byte> packet) {
        ErrorPacket error;
        if (!parse_error(packet, error)) {
            throw db::Error("malformed MySQL error packet", 2027);
        }
        throw db::Error(error.code, error.state, error.message);
    }

    void throw_network(const char* operation, int code, int error) {
        char message[256];
        std::snprintf(message, sizeof(message), "MySQL %s failed: %s", operation,
                      std::strerror(error <= 0 ? errno : error));
        throw db::Error(message, code);
    }

    Task<void> login() {
        const auto handshake_packet = co_await m_channel.read_packet(*m_persistent);
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
        std::memcpy(plugin, handshake.auth_plugin, sizeof(plugin) - 1);
        std::array<std::uint8_t, 20> scramble{};
        std::memcpy(scramble.data(), handshake.scramble.data(),
                    std::min(handshake.scramble_length, scramble.size()));
        std::size_t scramble_length = std::min<std::size_t>(handshake.scramble_length, 20);

        bool tls = false;
        if (m_options.tls_mode != db::TlsMode::Disabled) {
            if ((handshake.capabilities & CLIENT_SSL) == 0) {
                if (m_options.tls_mode == db::TlsMode::Required) {
                    throw db::Error("MySQL server does not support TLS", 2026);
                }
            } else {
                Bytes ssl_request =
                    build_ssl_request(handshake, m_options, *m_persistent);
                co_await m_channel.write_packet(ssl_request);
                co_await m_channel.start_tls(m_options);
                tls = true;
            }
        }
        auto build_response = [&](bool use_tls) {
            Bytes auth = auth_response_for(
                plugin, m_options.password,
                std::span<const std::uint8_t>(scramble.data(), scramble_length),
                *m_persistent);
            return build_handshake_response(handshake, m_options, auth, plugin,
                                            *m_persistent, use_tls);
        };

        Bytes response = build_response(tls);
        co_await m_channel.write_packet(response);

        for (;;) {
            const auto packet = co_await m_channel.read_packet(*m_persistent);
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
                    Bytes clear = make_bytes(*m_persistent);
                    clear.resize(m_options.password.size() + 1);
                    std::memcpy(clear.data(), m_options.password.data(),
                                m_options.password.size());
                    clear[m_options.password.size()] = std::byte{0};
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
                        Bytes clear = make_bytes(*m_persistent);
                        clear.resize(m_options.password.size() + 1);
                        std::memcpy(clear.data(), m_options.password.data(),
                                    m_options.password.size());
                        clear[m_options.password.size()] = std::byte{0};
                        co_await m_channel.write_packet(clear);
                        continue;
                    }
                    // Request the server's RSA public key.
                    const std::array<std::byte, 1> request{std::byte{0x02}};
                    co_await m_channel.write_packet(request);
                    const auto key_packet = co_await m_channel.read_packet(*m_persistent);
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
                        m_options.password,
                        std::span<const std::uint8_t>(scramble.data(), scramble_length),
                        {reinterpret_cast<const char*>(pem.data()), pem.size()},
                        *m_persistent);
                    co_await m_channel.write_packet(encrypted);
                    continue;
                }
                if (caching && !data.empty() &&
                    std::to_integer<std::uint8_t>(data[0]) == '-') {
                    Bytes encrypted = rsa_encrypt_password(
                        m_options.password,
                        std::span<const std::uint8_t>(scramble.data(), scramble_length),
                        {reinterpret_cast<const char*>(data.data()), data.size()},
                        *m_persistent);
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

Task<db::QueryResult> MysqlStatement::execute(
    std::span<const db::BindValue> parameters, mem::MemoryResource& result_resource) {
    try {
        co_return db::QueryResult::ok(co_await execute_impl(parameters, result_resource));
    } catch (const db::Error& error) {
        co_return db::QueryResult::err(error);
    } catch (...) {
        co_return db::QueryResult::err(
            db::Error("unexpected MySQL statement error"));
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
    build_stmt_execute_payload(request, m_id, parameters);
    co_await m_connection->channel().write_command(request);
    const auto first = co_await m_connection->channel().read_packet(result_resource);
    if (!first.empty() && std::to_integer<std::uint8_t>(first[0]) == 0xff) {
        ErrorPacket error;
        if (!parse_error(first, error)) {
            throw db::Error("malformed MySQL error packet", 2027);
        }
        throw db::Error(error.code, error.state, error.message);
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
        auto* connection = mem::construct<MysqlConnection>(persistent, persistent, options);
        try {
            co_await connection->connect_async();
        } catch (const db::Error& error) {
            connection->close();
            connection->destroy(persistent);
            co_return db::ConnectResult::err(error);
        } catch (...) {
            connection->close();
            connection->destroy(persistent);
            co_return db::ConnectResult::err(
                db::Error("unexpected MySQL connect error"));
        }
        co_return db::ConnectResult::ok(static_cast<db::Connection*>(connection));
    }

    void destroy(mem::MemoryResource& resource) noexcept override {
        this->~MysqlDriver();
        resource.deallocate(this, sizeof(*this), alignof(MysqlDriver));
    }

private:
    mem::MemoryResource* m_resource;
};

db::Driver* create_mysql_driver(mem::MemoryResource& resource) {
    return mem::construct<MysqlDriver>(resource, resource);
}

}  // namespace

void register_driver() {
    (void)db::DriverRegistry::instance().add("mysql", &create_mysql_driver);
}

}  // namespace sb::mysql
