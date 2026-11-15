// Copyright (C) 2026 Sunny Bains
// SPDX-License-Identifier: GPL-2.0-or-later

#include "mysql_transport.hpp"
#include "mysql_v2_ffi.h"

#include <ucontext.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

namespace sb::mysql {
namespace {

thread_local mem::MemoryResource* allocation_resource = nullptr;

class AllocationScope {
public:
    explicit AllocationScope(mem::MemoryResource& resource)
        : m_previous(std::exchange(allocation_resource, &resource)) {}
    ~AllocationScope() { allocation_resource = m_previous; }
private:
    mem::MemoryResource* m_previous;
};

struct AllocationHeader {
    mem::MemoryResource* resource;
    void* base;
    std::size_t size, alignment;
};

SbV2Bytes bytes(std::string_view value) {
    return {reinterpret_cast<const std::uint8_t*>(value.data()), value.size()};
}

// mysql-client-lib implements std::io::Read/Write. A separate stack lets its
// synchronous callbacks suspend without blocking the runtime worker. Each
// callback returns to this coroutine, which awaits the existing fixed-buffer
// socket operation before resuming the Rust stack on the same owning worker.
class RustCall {
public:
    RustCall(mem::MemoryResource& resource, ByteStream& stream)
        : m_resource(&resource), m_stream(&stream) {
        m_stack = resource.allocate(kStackBytes, 16);
        if (::getcontext(&m_rust_context) != 0) {
            resource.deallocate(m_stack, kStackBytes, 16);
            throw db::Error("cannot create Rust MySQL client context", 2000);
        }
        m_rust_context.uc_stack.ss_sp = m_stack;
        m_rust_context.uc_stack.ss_size = kStackBytes;
        m_rust_context.uc_link = nullptr;
        ::makecontext(&m_rust_context, &enter, 0);
    }
    ~RustCall() { m_resource->deallocate(m_stack, kStackBytes, 16); }

    SbV2Options options(const db::ConnectionOptions& source) {
        SbV2Options result{};
        result.user = bytes(source.m_user);
        result.password = bytes(source.m_password);
        result.database = bytes(source.m_database);
        result.max_packet_size = source.m_max_packet_size;
        result.tls_mode = static_cast<std::uint32_t>(source.m_tls_mode);
        result.tls_verify_server_cert = source.m_tls_verify_server_cert ? 1 : 0;
        result.server_name = bytes(source.m_host.empty() || source.m_host.front() == '/'
                                     ? std::string_view("localhost") : source.m_host);
        result.tls_ca = bytes(source.m_tls_ca);
        result.tls_cert = bytes(source.m_tls_cert);
        result.tls_key = bytes(source.m_tls_key);
        result.context = this;
        result.read = &read_callback;
        result.write = &write_callback;
        return result;
    }

    Task<db::Status> run(SbV2Command& command) {
        m_command = &command;
        m_io_error.reset();
        resume();
        while (m_state != State::Done) {
            try {
                if (m_state == State::Read) {
                    const auto count = co_await m_stream->read({m_destination, m_count});
                    m_bytes_read += count;
                    m_transferred = static_cast<std::ptrdiff_t>(count);
                } else {
                    co_await m_stream->write({m_source, m_count});
                    m_transferred = static_cast<std::ptrdiff_t>(m_count);
                }
            } catch (const db::Error& error) {
                m_io_error = error;
                m_transferred = -1;
            } catch (...) {
                m_io_error = db::Error("unexpected MySQL v2 transport error", 2013);
                m_transferred = -1;
            }
            resume();
        }
        m_command = nullptr;
        if (m_io_error) co_return db::Status::err(*m_io_error);
        if (command.error.code != 0) {
            co_return db::Status::err(db::Error(command.error.code,
                command.error.state, command.error.message, command.error.fatal != 0));
        }
        co_return db::Status::ok();
    }

    [[nodiscard]] std::uint64_t bytes_read() const noexcept { return m_bytes_read; }

private:
    enum class State { Done, Read, Write };
    static constexpr std::size_t kStackBytes = 256 * 1024;
    static thread_local RustCall* s_entering;

    static void enter() {
        RustCall* self = s_entering;
        for (;;) {
            sb_mysql_v2_run(self->m_command);
            self->m_state = State::Done;
            self->suspend();
        }
    }
    void resume() {
        AllocationScope allocation(*m_resource);
        RustCall* previous = std::exchange(s_entering, this);
        if (::swapcontext(&m_host_context, &m_rust_context) != 0) std::terminate();
        s_entering = previous;
    }
    void suspend() noexcept {
        if (::swapcontext(&m_rust_context, &m_host_context) != 0) std::terminate();
    }
    static std::ptrdiff_t read_callback(void* context, std::uint8_t* data,
                                        std::size_t count) noexcept {
        auto& self = *static_cast<RustCall*>(context);
        self.m_destination = reinterpret_cast<std::byte*>(data);
        self.m_count = count;
        self.m_state = State::Read;
        self.suspend();
        return self.m_transferred;
    }
    static std::ptrdiff_t write_callback(void* context, const std::uint8_t* data,
                                         std::size_t count) noexcept {
        auto& self = *static_cast<RustCall*>(context);
        self.m_source = reinterpret_cast<const std::byte*>(data);
        self.m_count = count;
        self.m_state = State::Write;
        self.suspend();
        return self.m_transferred;
    }

    mem::MemoryResource* m_resource;
    ByteStream* m_stream;
    void* m_stack;
    ucontext_t m_host_context{}, m_rust_context{};
    SbV2Command* m_command = nullptr;
    State m_state = State::Done;
    std::byte* m_destination = nullptr;
    const std::byte* m_source = nullptr;
    std::size_t m_count = 0;
    std::ptrdiff_t m_transferred = 0;
    std::uint64_t m_bytes_read = 0;
    std::optional<db::Error> m_io_error;
};
thread_local RustCall* RustCall::s_entering = nullptr;

struct StreamDeleter {
    mem::MemoryResource* resource;
    void operator()(ByteStream* stream) const noexcept { stream->destroy(*resource); }
};

class RustConnection;

class RustStatement final : public db::PreparedStatement {
public:
    RustStatement(RustConnection& connection, mem::MemoryResource& resource,
                  void* statement, std::string_view sql, std::uint64_t generation)
        : m_connection(&connection), m_resource(&resource), m_statement(statement),
          m_sql(sql, mem::Allocator<char>(&resource)), m_generation(generation) {}
    ~RustStatement() override {
        AllocationScope allocation(*m_resource);
        sb_mysql_v2_drop_statement(m_statement);
    }
    Task<db::QueryResult> execute(std::span<const db::BindValue> parameters,
                                  mem::MemoryResource& result_resource) override;
    Task<db::Status> close() override;
    [[nodiscard]] bool closed() const noexcept override { return m_closed; }
    [[nodiscard]] std::string_view sql() const noexcept override { return m_sql; }
    void destroy(mem::MemoryResource& resource) noexcept override { mem::destroy(resource, this); }
private:
    RustConnection* m_connection;
    mem::MemoryResource* m_resource;
    void* m_statement;
    mem::String m_sql;
    std::uint64_t m_generation;
    bool m_closed = false;
};

class RustConnection final : public db::Connection {
public:
    RustConnection(mem::MemoryResource& resource, const db::ConnectionOptions& options)
        : m_resource(&resource), m_options(options),
          m_host(options.m_host, mem::Allocator<char>(&resource)),
          m_port(options.m_port, mem::Allocator<char>(&resource)),
          m_user(options.m_user, mem::Allocator<char>(&resource)),
          m_password(options.m_password, mem::Allocator<char>(&resource)),
          m_database(options.m_database, mem::Allocator<char>(&resource)),
          m_socket(options.m_socket_path, mem::Allocator<char>(&resource)),
          m_tls_ca(options.m_tls_ca, mem::Allocator<char>(&resource)),
          m_tls_cert(options.m_tls_cert, mem::Allocator<char>(&resource)),
          m_tls_key(options.m_tls_key, mem::Allocator<char>(&resource)),
          m_stream(create_byte_stream(resource), StreamDeleter{&resource}),
          m_call(resource, *m_stream) {
        m_options.m_host = m_host;
        m_options.m_port = m_port;
        m_options.m_user = m_user;
        m_options.m_password = m_password;
        m_options.m_database = m_database;
        m_options.m_socket_path = m_socket;
        m_options.m_tls_ca = m_tls_ca;
        m_options.m_tls_cert = m_tls_cert;
        m_options.m_tls_key = m_tls_key;
    }
    ~RustConnection() override { close(); }

    Task<db::Status> connect() {
        co_await m_stream->connect(m_options);
        SbV2Command command{};
        command.operation = SB_V2_CONNECT;
        command.options = m_call.options(m_options);
        auto status = co_await m_call.run(command);
        if (status.is_ok()) m_client = command.client;
        co_return status;
    }

    Task<db::QueryResult> query(std::string_view sql,
                                mem::MemoryResource& result_resource) override {
        if (!healthy()) co_return db::QueryResult::err(db::Error("MySQL v2 connection is closed"));
        SbV2Command command{};
        command.operation = SB_V2_QUERY;
        command.client = m_client;
        command.sql = bytes(sql);
        co_return co_await execute_command(command, result_resource);
    }

    Task<db::PrepareResult> prepare(std::string_view sql) override {
        if (!healthy()) co_return db::PrepareResult::err(db::Error("MySQL v2 connection is closed"));
        SbV2Command command{};
        command.operation = SB_V2_PREPARE;
        command.client = m_client;
        command.sql = bytes(sql);
        auto status = co_await m_call.run(command);
        const bool text_fallback = status.is_err() && status.error().code() == 1295 &&
                                   sql.find('?') == std::string_view::npos;
        if (status.is_err() && !text_fallback) co_return db::PrepareResult::err(status.error());
        AllocationScope allocation(*m_resource);
        std::unique_ptr<void, decltype(&sb_mysql_v2_drop_statement)> statement(
            command.statement, &sb_mysql_v2_drop_statement);
        try {
            auto* result = mem::construct<RustStatement>(*m_resource, *this, *m_resource,
                                                         statement.get(), sql, m_generation);
            (void)statement.release();
            co_return db::PrepareResult::ok(static_cast<db::PreparedStatement*>(result));
        } catch (...) {
            co_return db::PrepareResult::err(db::Error("cannot allocate MySQL v2 statement", 2008));
        }
    }

    Task<db::Status> reconnect() override {
        close();
        try {
            auto status = co_await connect();
            if (status.is_err()) close();
            co_return status;
        } catch (const db::Error& error) {
            close();
            co_return db::Status::err(error);
        } catch (...) {
            close();
            co_return db::Status::err(db::Error("unexpected MySQL v2 reconnect error"));
        }
    }

    void close() noexcept override {
        AllocationScope allocation(*m_resource);
        sb_mysql_v2_drop_client(std::exchange(m_client, nullptr));
        m_stream->close();
        ++m_generation;
    }
    [[nodiscard]] bool healthy() const noexcept override { return m_client != nullptr && m_stream->healthy(); }
    [[nodiscard]] std::string_view driver_name() const noexcept override { return "mysql"; }
    void destroy(mem::MemoryResource& resource) noexcept override { mem::destroy(resource, this); }
    [[nodiscard]] std::uint64_t generation() const noexcept { return m_generation; }

    Task<db::Status> close_statement(void* statement) {
        SbV2Command command{};
        command.operation = SB_V2_CLOSE_STATEMENT;
        command.client = m_client;
        command.statement = statement;
        co_return co_await m_call.run(command);
    }

    Task<db::QueryResult> execute(void* statement, std::span<const db::BindValue> parameters,
                                  mem::MemoryResource& result_resource) {
        mem::Vector<SbV2Value> values{mem::Allocator<SbV2Value>(m_resource)};
        values.reserve(parameters.size());
        for (const auto& value : parameters) {
            values.push_back({static_cast<std::uint32_t>(value.m_kind), value.m_integer,
                              value.m_real, bytes(value.m_bytes)});
        }
        SbV2Command command{};
        command.operation = SB_V2_EXECUTE;
        command.client = m_client;
        command.statement = statement;
        command.parameters = values.data();
        command.parameter_count = values.size();
        co_return co_await execute_command(command, result_resource);
    }

private:
    Task<db::QueryResult> execute_command(SbV2Command& command, mem::MemoryResource& resource) {
        const auto before = m_call.bytes_read();
        auto status = co_await m_call.run(command);
        if (status.is_err()) co_return db::QueryResult::err(status.error());
        AllocationScope allocation(*m_resource);
        std::unique_ptr<void, decltype(&sb_mysql_v2_drop_result)> owner(
            command.result, &sb_mysql_v2_drop_result);
        try {
            const auto info = sb_mysql_v2_result_info(owner.get());
            if (info.columns > 4096) throw db::Error("too many MySQL v2 result columns", 2027);
            db::ResultSet result(resource);
            result.m_affected_rows = info.affected_rows;
            result.m_last_insert_id = info.last_insert_id;
            result.m_warnings = info.warnings;
            result.m_status = static_cast<std::uint16_t>(info.status & ~0x0008u);
            result.m_has_rows = info.columns != 0;
            result.m_bytes_read = m_call.bytes_read() - before;
            result.m_columns.reserve(info.columns);
            for (std::size_t i = 0; i < info.columns; ++i) {
                const auto column = sb_mysql_v2_column(owner.get(), i);
                const auto name = copy(column.name, resource);
                result.m_columns.push_back({name, column.type, column.flags, column.charset});
            }
            result.m_rows.reserve(info.rows);
            for (std::size_t r = 0; r < info.rows; ++r) {
                db::Row row(resource);
                row.m_fields.reserve(info.columns);
                for (std::size_t c = 0; c < info.columns; ++c) {
                    const auto field = sb_mysql_v2_field(owner.get(), r, c);
                    if (field.bytes.size > std::numeric_limits<std::uint32_t>::max()) {
                        throw db::Error("MySQL v2 field exceeds the result size limit", 2027);
                    }
                    const auto text = copy(field.bytes, resource);
                    row.m_fields.push_back({text.data(), static_cast<std::uint32_t>(text.size()),
                                            field.is_null != 0});
                }
                result.m_rows.push_back(std::move(row));
            }
            co_return db::QueryResult::ok(std::move(result));
        } catch (const db::Error& error) {
            co_return db::QueryResult::err(error);
        } catch (...) {
            co_return db::QueryResult::err(db::Error("cannot allocate MySQL v2 result", 2008));
        }
    }

    static std::string_view copy(SbV2Bytes source, mem::MemoryResource& resource) {
        if (source.size == 0) return "";
        auto* target = static_cast<char*>(resource.allocate(source.size, 1));
        std::memcpy(target, source.data, source.size);
        return {target, source.size};
    }

    mem::MemoryResource* m_resource;
    db::ConnectionOptions m_options;
    mem::String m_host, m_port, m_user, m_password, m_database, m_socket;
    mem::String m_tls_ca, m_tls_cert, m_tls_key;
    std::unique_ptr<ByteStream, StreamDeleter> m_stream;
    RustCall m_call;
    void* m_client = nullptr;
    std::uint64_t m_generation = 0;
};

Task<db::QueryResult> RustStatement::execute(std::span<const db::BindValue> parameters,
                                           mem::MemoryResource& resource) {
    if (m_closed || !m_connection->healthy() || m_generation != m_connection->generation()) {
        co_return db::QueryResult::err(db::Error(1243, "HY000", "MySQL v2 statement is closed or stale"));
    }
    if (m_statement == nullptr) {
        if (!parameters.empty()) co_return db::QueryResult::err(db::Error("text statement takes no parameters", 2031));
        co_return co_await m_connection->query(m_sql, resource);
    }
    co_return co_await m_connection->execute(m_statement, parameters, resource);
}

Task<db::Status> RustStatement::close() {
    if (m_closed) co_return db::Status::ok();
    m_closed = true;
    if (m_statement == nullptr || !m_connection->healthy() || m_generation != m_connection->generation()) {
        co_return db::Status::ok();
    }
    co_return co_await m_connection->close_statement(m_statement);
}

}  // namespace

Task<db::ConnectResult> connect_v2(const db::ConnectionOptions& options,
                                  mem::MemoryResource& persistent) {
    RustConnection* connection = nullptr;
    try {
        connection = mem::construct<RustConnection>(persistent, persistent, options);
        auto status = co_await connection->connect();
        if (status.is_ok()) co_return db::ConnectResult::ok(static_cast<db::Connection*>(connection));
        connection->destroy(persistent);
        co_return db::ConnectResult::err(status.error());
    } catch (const db::Error& error) {
        if (connection != nullptr) connection->destroy(persistent);
        co_return db::ConnectResult::err(error);
    } catch (...) {
        if (connection != nullptr) connection->destroy(persistent);
        co_return db::ConnectResult::err(db::Error("unexpected MySQL v2 connect error"));
    }
}

}  // namespace sb::mysql

extern "C" void* sb_mysql_v2_allocate(std::size_t size, std::size_t alignment) {
    using sb::mysql::AllocationHeader;
    auto* resource = sb::mysql::allocation_resource;
    if (resource == nullptr) return nullptr;
    alignment = std::max(alignment, alignof(AllocationHeader));
    if (size > std::numeric_limits<std::size_t>::max() - alignment - sizeof(AllocationHeader)) return nullptr;
    const auto total = size + alignment + sizeof(AllocationHeader);
    try {
        void* base = resource->allocate(total, alignment);
        const auto address = (reinterpret_cast<std::uintptr_t>(base) + sizeof(AllocationHeader) +
                              alignment - 1) & ~(alignment - 1);
        auto* header = reinterpret_cast<AllocationHeader*>(address) - 1;
        *header = {resource, base, total, alignment};
        return reinterpret_cast<void*>(address);
    } catch (...) {
        return nullptr;
    }
}

extern "C" void sb_mysql_v2_deallocate(void* pointer) {
    if (pointer == nullptr) return;
    const auto header = *(static_cast<sb::mysql::AllocationHeader*>(pointer) - 1);
    header.resource->deallocate(header.base, header.size, header.alignment);
}
