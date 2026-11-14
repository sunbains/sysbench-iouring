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

// Protocol-neutral database interface.  Lua and the benchmark runtime only see
// this layer; MySQL is the first registered implementation.  Adding PostgreSQL
// or another wire protocol means implementing Connection/PreparedStatement and
// registering a factory -- no Lua or scheduling changes.

#include "sysbench/memory.hpp"
#include "sysbench/result.hpp"
#include "sysbench/task.hpp"

#include <cstdint>
#include <exception>
#include <span>
#include <string_view>

namespace sb::db {

struct Field {
    const char* m_data = nullptr;
    std::uint32_t m_length = 0;
    bool m_is_null = true;
};

struct Column {
    std::string_view m_name;
    std::uint8_t m_type = 0;
    std::uint16_t m_flags = 0;
    std::uint32_t m_charset = 0;
};

struct Row {
    mem::Vector<Field> m_fields;
    explicit Row(mem::MemoryResource& resource)
        : m_fields(mem::Allocator<Field>(&resource)) {}
};

struct ResultSet {
    mem::Vector<Column> m_columns;
    mem::Vector<Row> m_rows;
    std::uint64_t m_affected_rows = 0;
    std::uint64_t m_last_insert_id = 0;
    std::uint16_t m_warnings = 0;
    std::uint16_t m_status = 0;
    bool m_has_rows = false;
    bool m_more_results = false;
    std::uint64_t m_bytes_read = 0;

    explicit ResultSet(mem::MemoryResource& resource)
        : m_columns(mem::Allocator<Column>(&resource)),
          m_rows(mem::Allocator<Row>(&resource)) {}
    ResultSet(ResultSet&&) noexcept = default;
    ResultSet& operator=(ResultSet&&) noexcept = default;
    ResultSet(const ResultSet&) = delete;
    ResultSet& operator=(const ResultSet&) = delete;
};

enum class BindKind : std::uint8_t { Null, Integer, Real, Bytes };

struct BindValue {
    BindKind m_kind = BindKind::Null;
    std::int64_t m_integer = 0;
    double m_real = 0.0;
    std::string_view m_bytes;

    static BindValue null() noexcept { return {}; }
    static BindValue from_int(std::int64_t value) noexcept {
        BindValue result;
        result.m_kind = BindKind::Integer;
        result.m_integer = value;
        return result;
    }
    static BindValue from_double(double value) noexcept {
        BindValue result;
        result.m_kind = BindKind::Real;
        result.m_real = value;
        return result;
    }
    static BindValue from_bytes(std::string_view value) noexcept {
        BindValue result;
        result.m_kind = BindKind::Bytes;
        result.m_bytes = value;
        return result;
    }
};

// Heap-free exception used on every SQL error path.
class Error final : public std::exception {
public:
    static constexpr std::size_t kMessageCapacity = 1024;

    Error(int code, std::string_view state, std::string_view message,
          bool fatal = true) noexcept;
    Error(std::string_view message, int network_code = 2013) noexcept;

    [[nodiscard]] int code() const noexcept { return m_code; }
    [[nodiscard]] const char* state() const noexcept { return m_state; }
    [[nodiscard]] const char* message() const noexcept { return m_message; }
    [[nodiscard]] bool fatal() const noexcept { return m_fatal; }
    [[nodiscard]] const char* what() const noexcept override { return m_message; }

private:
    int m_code = 0;
    bool m_fatal = true;
    char m_state[6] = "HY000";
    char m_message[kMessageCapacity]{};
};

enum class TlsMode : std::uint8_t { Disabled, Preferred, Required };

struct ConnectionOptions {
    std::string_view m_host = "127.0.0.1";
    std::string_view m_port = "3306";
    std::string_view m_user = "root";
    std::string_view m_password;
    std::string_view m_database;
    std::string_view m_socket_path;
    std::uint32_t m_protocol_version = 1;
    std::uint32_t m_connect_timeout_ms = 5000;
    std::uint32_t m_max_packet_size = 16 * 1024 * 1024;
    TlsMode m_tls_mode = TlsMode::Disabled;
    bool m_tls_verify_server_cert = false;
    std::string_view m_tls_ca;
    std::string_view m_tls_cert;
    std::string_view m_tls_key;
};

class Connection;
class PreparedStatement;

using QueryResult = Result<ResultSet, Error>;
using PrepareResult = Result<PreparedStatement*, Error>;
using Status = Result<void, Error>;
using ConnectResult = Result<Connection*, Error>;

class Connection {
public:
    virtual ~Connection() = default;

    virtual Task<QueryResult> query(std::string_view sql,
                                    mem::MemoryResource& result_resource) = 0;
    virtual Task<PrepareResult> prepare(std::string_view sql) = 0;
    // Run `total` copies of sql keeping up to `depth` in flight on the
    // connection: the next query is written as soon as a result is read.
    // Results are read and discarded.
    // Drivers that cannot keep several commands in flight run them one by one.
    virtual Task<Status> query_stream(std::string_view sql, std::uint32_t depth,
                                      std::uint64_t total,
                                      mem::MemoryResource& scratch) {
        (void)depth;
        for (std::uint64_t i = 0; i < total; ++i) {
            auto result = co_await query(sql, scratch);
            if (result.is_err()) co_return Status::err(result.error());
        }
        co_return Status::ok();
    }
    // Pipelining: send_query() writes a command without reading its reply;
    // read_reply() reads (and discards) the oldest outstanding reply.  Replies
    // come back in command order.  Drivers that cannot pipeline run the
    // command to completion in send_query() and never have replies pending.
    virtual Task<Status> send_query(std::string_view sql, mem::MemoryResource& scratch) {
        auto result = co_await query(sql, scratch);
        if (result.is_err()) co_return Status::err(result.error());
        co_return Status::ok();
    }
    virtual Task<Status> read_reply(mem::MemoryResource& scratch) {
        (void)scratch;
        co_return Status::ok();
    }
    [[nodiscard]] virtual std::uint32_t pending_replies() const noexcept { return 0; }
    virtual Task<Status> reconnect() = 0;
    virtual void close() noexcept = 0;
    [[nodiscard]] virtual bool healthy() const noexcept = 0;
    [[nodiscard]] virtual std::string_view driver_name() const noexcept = 0;

    // Explicit destruction keeps ownership in the custom allocator hierarchy.
    virtual void destroy(mem::MemoryResource& resource) noexcept = 0;
};

class PreparedStatement {
public:
    virtual ~PreparedStatement() = default;

    virtual Task<QueryResult> execute(std::span<const BindValue> parameters,
                                      mem::MemoryResource& result_resource) = 0;
    // Pipelined execute: write the command and leave its reply to
    // Connection::read_reply().  The default runs it to completion.
    virtual Task<Status> send_execute(std::span<const BindValue> parameters,
                                      mem::MemoryResource& scratch) {
        auto result = co_await execute(parameters, scratch);
        if (result.is_err()) co_return Status::err(result.error());
        co_return Status::ok();
    }
    virtual Task<Status> close() = 0;
    [[nodiscard]] virtual bool closed() const noexcept = 0;
    [[nodiscard]] virtual std::string_view sql() const noexcept = 0;
    virtual void destroy(mem::MemoryResource& resource) noexcept = 0;
};

class Driver {
public:
    using CreateFn = Driver* (*)(mem::MemoryResource&);

    virtual ~Driver() = default;
    [[nodiscard]] virtual std::string_view name() const noexcept = 0;
    virtual Task<ConnectResult> connect(const ConnectionOptions& options,
                                        mem::MemoryResource& persistent) = 0;
    virtual void destroy(mem::MemoryResource& resource) noexcept = 0;
};

// Fixed-capacity registry: protocol extensions are added at startup only.
class DriverRegistry {
public:
    static constexpr std::size_t kMaxDrivers = 8;

    static DriverRegistry& instance();

    bool add(std::string_view name, Driver::CreateFn create) noexcept;
    [[nodiscard]] Driver* create(std::string_view name,
                                 mem::MemoryResource& resource) const;
    [[nodiscard]] bool contains(std::string_view name) const noexcept;
    [[nodiscard]] std::size_t size() const noexcept { return m_count; }
    [[nodiscard]] std::string_view name_at(std::size_t index) const noexcept;

private:
    struct Entry {
        std::string_view m_name;
        Driver::CreateFn m_create = nullptr;
    };
    Entry m_entries[kMaxDrivers]{};
    std::size_t m_count = 0;
};

}  // namespace sb::db
