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

// MySQL client/server protocol codec.  This layer is deliberately independent
// of io_uring, Lua and the scheduler, which makes it cheap to fuzz and test.

#include "sysbench/db.hpp"
#include "sysbench/memory.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace sb::mysql {

using Bytes = mem::Vector<std::byte>;

inline Bytes make_bytes(mem::MemoryResource& resource) {
    return Bytes{mem::Allocator<std::byte>(&resource)};
}

// Capability flags.
inline constexpr std::uint32_t CLIENT_LONG_PASSWORD = 0x00000001;
inline constexpr std::uint32_t CLIENT_LONG_FLAG = 0x00000004;
inline constexpr std::uint32_t CLIENT_CONNECT_WITH_DB = 0x00000008;
inline constexpr std::uint32_t CLIENT_SSL = 0x00000800;
inline constexpr std::uint32_t CLIENT_PROTOCOL_41 = 0x00000200;
inline constexpr std::uint32_t CLIENT_TRANSACTIONS = 0x00002000;
inline constexpr std::uint32_t CLIENT_SECURE_CONNECTION = 0x00008000;
inline constexpr std::uint32_t CLIENT_MULTI_RESULTS = 0x00020000;
inline constexpr std::uint32_t CLIENT_PS_MULTI_RESULTS = 0x00040000;
inline constexpr std::uint32_t CLIENT_PLUGIN_AUTH = 0x00080000;
inline constexpr std::uint32_t CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;

inline constexpr std::uint16_t SERVER_STATUS_IN_TRANS = 0x0001;
inline constexpr std::uint16_t SERVER_STATUS_AUTOCOMMIT = 0x0002;
inline constexpr std::uint16_t SERVER_MORE_RESULTS_EXISTS = 0x0008;

// Column types.
inline constexpr std::uint8_t TYPE_DECIMAL = 0;
inline constexpr std::uint8_t TYPE_TINY = 1;
inline constexpr std::uint8_t TYPE_SHORT = 2;
inline constexpr std::uint8_t TYPE_LONG = 3;
inline constexpr std::uint8_t TYPE_FLOAT = 4;
inline constexpr std::uint8_t TYPE_DOUBLE = 5;
inline constexpr std::uint8_t TYPE_NULL = 6;
inline constexpr std::uint8_t TYPE_TIMESTAMP = 7;
inline constexpr std::uint8_t TYPE_LONGLONG = 8;
inline constexpr std::uint8_t TYPE_INT24 = 9;
inline constexpr std::uint8_t TYPE_DATE = 10;
inline constexpr std::uint8_t TYPE_TIME = 11;
inline constexpr std::uint8_t TYPE_DATETIME = 12;
inline constexpr std::uint8_t TYPE_YEAR = 13;
inline constexpr std::uint8_t TYPE_NEWDATE = 14;
inline constexpr std::uint8_t TYPE_VARCHAR = 15;
inline constexpr std::uint8_t TYPE_BIT = 16;
inline constexpr std::uint8_t TYPE_TIMESTAMP2 = 17;
inline constexpr std::uint8_t TYPE_DATETIME2 = 18;
inline constexpr std::uint8_t TYPE_TIME2 = 19;
inline constexpr std::uint8_t TYPE_JSON = 245;
inline constexpr std::uint8_t TYPE_NEWDECIMAL = 246;
inline constexpr std::uint8_t TYPE_ENUM = 247;
inline constexpr std::uint8_t TYPE_SET = 248;
inline constexpr std::uint8_t TYPE_TINY_BLOB = 249;
inline constexpr std::uint8_t TYPE_MEDIUM_BLOB = 250;
inline constexpr std::uint8_t TYPE_LONG_BLOB = 251;
inline constexpr std::uint8_t TYPE_BLOB = 252;
inline constexpr std::uint8_t TYPE_VAR_STRING = 253;
inline constexpr std::uint8_t TYPE_STRING = 254;
inline constexpr std::uint8_t TYPE_GEOMETRY = 255;

inline constexpr std::uint16_t UNSIGNED_FLAG = 0x0020;

struct Handshake {
    std::uint32_t capabilities = 0;
    std::uint32_t connection_id = 0;
    std::uint32_t max_packet = 0;
    std::uint16_t status = 0;
    std::uint8_t charset = 0;
    std::array<std::uint8_t, 20> scramble{};
    std::size_t scramble_length = 0;
    char server_version[128]{};
    std::size_t server_version_length = 0;
    char auth_plugin[64] = "mysql_native_password";
    std::size_t auth_plugin_length = 16;
};

struct PacketHeader {
    std::uint32_t length = 0;
    std::uint8_t sequence = 0;
};

struct OkPacket {
    std::uint64_t affected_rows = 0;
    std::uint64_t last_insert_id = 0;
    std::uint16_t status = 0;
    std::uint16_t warnings = 0;
};

struct EofPacket {
    std::uint16_t warnings = 0;
    std::uint16_t status = 0;
};

struct ErrorPacket {
    std::uint16_t code = 0;
    char state[6] = "HY000";
    std::string_view message;
};

struct PrepareOk {
    std::uint32_t statement_id = 0;
    std::uint16_t columns = 0;
    std::uint16_t parameters = 0;
    std::uint16_t warnings = 0;
};

class Reader {
public:
    explicit Reader(std::span<const std::byte> data) : m_data(data) {}

    [[nodiscard]] bool u8(std::uint8_t& value) noexcept;
    [[nodiscard]] bool u16(std::uint16_t& value) noexcept;
    [[nodiscard]] bool u24(std::uint32_t& value) noexcept;
    [[nodiscard]] bool u32(std::uint32_t& value) noexcept;
    [[nodiscard]] bool u64(std::uint64_t& value) noexcept;
    [[nodiscard]] bool lenenc(std::uint64_t& value) noexcept;
    enum class StringResult { Ok, Null, Invalid };
    [[nodiscard]] StringResult lenenc_string(std::string_view& value) noexcept;
    [[nodiscard]] StringResult null_terminated(std::string_view& value) noexcept;
    [[nodiscard]] bool take(std::size_t count, std::span<const std::byte>& out) noexcept;
    [[nodiscard]] bool skip(std::size_t count) noexcept;
    [[nodiscard]] bool peek_u8(std::uint8_t& value) const noexcept;
    [[nodiscard]] std::size_t remaining() const noexcept { return m_data.size() - m_position; }
    [[nodiscard]] std::size_t position() const noexcept { return m_position; }
    [[nodiscard]] std::span<const std::byte> remaining_data() const noexcept {
        return m_data.subspan(m_position);
    }

private:
    [[nodiscard]] bool need(std::size_t count) const noexcept {
        return count <= m_data.size() - m_position;
    }
    std::span<const std::byte> m_data;
    std::size_t m_position = 0;
};

class Writer {
public:
    explicit Writer(Bytes& out) : m_out(&out) {}

    void u8(std::uint8_t value);
    void u16(std::uint16_t value);
    void u24(std::uint32_t value);
    void u32(std::uint32_t value);
    void u64(std::uint64_t value);
    void lenenc(std::uint64_t value);
    void bytes(std::span<const std::byte> value);
    void bytes(std::string_view value);
    void null_terminated(std::string_view value);
    void zeros(std::size_t count);
    void reserve(std::size_t count);

    [[nodiscard]] Bytes& buffer() noexcept { return *m_out; }

private:
    Bytes* m_out;
};

[[nodiscard]] bool parse_packet_header(std::span<const std::byte> data,
                                       PacketHeader& header) noexcept;
void write_packet_header(Bytes& out, std::uint32_t length, std::uint8_t sequence);
void append_packet(Bytes& out, std::uint8_t sequence,
                   std::span<const std::byte> payload);
[[nodiscard]] bool parse_handshake(std::span<const std::byte> payload,
                                   Handshake& handshake) noexcept;
[[nodiscard]] bool parse_ok(std::span<const std::byte> payload, OkPacket& packet) noexcept;
[[nodiscard]] bool parse_eof(std::span<const std::byte> payload, EofPacket& packet) noexcept;
[[nodiscard]] bool parse_error(std::span<const std::byte> payload,
                               ErrorPacket& packet) noexcept;
[[nodiscard]] bool parse_prepare_ok(std::span<const std::byte> payload,
                                    PrepareOk& packet) noexcept;
[[nodiscard]] bool parse_column(std::span<const std::byte> payload,
                                db::Column& column) noexcept;

[[nodiscard]] Bytes auth_response_native(std::string_view password,
                                         std::span<const std::uint8_t> scramble,
                                         mem::MemoryResource& resource);
[[nodiscard]] Bytes auth_response_sha2(std::string_view password,
                                       std::span<const std::uint8_t> scramble,
                                       mem::MemoryResource& resource);
[[nodiscard]] Bytes auth_response_for(std::string_view plugin,
                                      std::string_view password,
                                      std::span<const std::uint8_t> scramble,
                                      mem::MemoryResource& resource);
[[nodiscard]] Bytes rsa_encrypt_password(std::string_view password,
                                         std::span<const std::uint8_t> scramble,
                                         std::string_view public_key_pem,
                                         mem::MemoryResource& resource);
[[nodiscard]] std::uint32_t client_capabilities(const Handshake& handshake,
                                                const db::ConnectionOptions& options,
                                                bool ssl) noexcept;
[[nodiscard]] Bytes build_ssl_request(const Handshake& handshake,
                                      const db::ConnectionOptions& options,
                                      mem::MemoryResource& resource);
[[nodiscard]] Bytes build_handshake_response(const Handshake& handshake,
                                             const db::ConnectionOptions& options,
                                             std::span<const std::byte> auth_response,
                                             std::string_view plugin,
                                             mem::MemoryResource& resource,
                                             bool ssl = false);

[[nodiscard]] bool parse_text_row(std::span<const std::byte> payload,
                                  std::size_t column_count,
                                  mem::MemoryResource& resource, db::Row& row);
[[nodiscard]] bool parse_binary_row(std::span<const std::byte> payload,
                                    std::span<const db::Column> columns,
                                    mem::MemoryResource& resource, db::Row& row);

void build_stmt_execute_payload(Bytes& out, std::uint32_t statement_id,
                                std::span<const db::BindValue> parameters);

[[nodiscard]] std::string_view type_name(std::uint8_t type) noexcept;

}  // namespace sb::mysql
