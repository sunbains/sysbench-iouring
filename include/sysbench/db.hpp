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
    const char* data = nullptr;
    std::uint32_t length = 0;
    bool is_null = true;
};

struct Column {
    std::string_view name;
    std::uint8_t type = 0;
    std::uint16_t flags = 0;
    std::uint32_t charset = 0;
};

struct Row {
    mem::Vector<Field> fields;
    explicit Row(mem::MemoryResource& resource)
        : fields(mem::Allocator<Field>(&resource)) {}
};

struct ResultSet {
    mem::Vector<Column> columns;
    mem::Vector<Row> rows;
    std::uint64_t affected_rows = 0;
    std::uint64_t last_insert_id = 0;
    std::uint16_t warnings = 0;
    std::uint16_t status = 0;
    bool has_rows = false;
    bool more_results = false;
    std::uint64_t bytes_read = 0;

    explicit ResultSet(mem::MemoryResource& resource)
        : columns(mem::Allocator<Column>(&resource)),
          rows(mem::Allocator<Row>(&resource)) {}
    ResultSet(ResultSet&&) noexcept = default;
    ResultSet& operator=(ResultSet&&) noexcept = default;
    ResultSet(const ResultSet&) = delete;
    ResultSet& operator=(const ResultSet&) = delete;
};

enum class BindKind : std::uint8_t { Null, Integer, Real, Bytes };

struct BindValue {
    BindKind kind = BindKind::Null;
    std::int64_t integer = 0;
    double real = 0.0;
    std::string_view bytes;

    static BindValue null() noexcept { return {}; }
    static BindValue from_int(std::int64_t value) noexcept {
        BindValue result;
        result.kind = BindKind::Integer;
        result.integer = value;
        return result;
    }
    static BindValue from_double(double value) noexcept {
        BindValue result;
        result.kind = BindKind::Real;
        result.real = value;
        return result;
    }
    static BindValue from_bytes(std::string_view value) noexcept {
        BindValue result;
        result.kind = BindKind::Bytes;
        result.bytes = value;
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
    std::string_view host = "127.0.0.1";
    std::string_view port = "3306";
    std::string_view user = "root";
    std::string_view password;
    std::string_view database;
    std::string_view socket_path;
    std::uint32_t connect_timeout_ms = 5000;
    std::uint32_t max_packet_size = 16 * 1024 * 1024;
    TlsMode tls_mode = TlsMode::Disabled;
    bool tls_verify_server_cert = false;
    std::string_view tls_ca;
    std::string_view tls_cert;
    std::string_view tls_key;
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
        std::string_view name;
        Driver::CreateFn create = nullptr;
    };
    Entry m_entries[kMaxDrivers]{};
    std::size_t m_count = 0;
};

}  // namespace sb::db
