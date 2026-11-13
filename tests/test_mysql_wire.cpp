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

#include "sysbench/memory.hpp"
#include "sysbench/mysql_wire.hpp"

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>

#include <array>
#include <cstring>
#include <string>
#include <vector>

using namespace sb;
using namespace sb::mysql;

namespace {

std::string to_hex(std::span<const std::byte> bytes) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (std::byte byte : bytes) {
        const auto value = std::to_integer<unsigned>(byte);
        result.push_back(digits[value >> 4]);
        result.push_back(digits[value & 0xf]);
    }
    return result;
}

Bytes fixture_handshake(mem::MemoryResource& resource) {
    Bytes payload = make_bytes(resource);
    Writer writer(payload);
    const std::uint32_t capabilities =
        CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH |
        CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA |
        CLIENT_MULTI_RESULTS;
    writer.u8(10);
    writer.null_terminated("8.0.36-test");
    writer.u32(42);
    writer.bytes("abcdefgh");
    writer.u8(0);
    writer.u16(static_cast<std::uint16_t>(capabilities & 0xffff));
    writer.u8(255);
    writer.u16(2);
    writer.u16(static_cast<std::uint16_t>(capabilities >> 16));
    writer.u8(21);
    writer.zeros(10);
    writer.bytes("ijklmnopqrst");
    writer.u8(0);
    writer.null_terminated("caching_sha2_password");
    return payload;
}

Bytes fixture_text_row(mem::MemoryResource& resource) {
    Bytes payload = make_bytes(resource);
    Writer writer(payload);
    writer.lenenc(3);
    writer.bytes("abc");
    writer.lenenc(0);
    writer.u8(0xfb);
    return payload;
}

Bytes fixture_column(mem::MemoryResource& resource, std::string_view name,
                     std::uint8_t type, std::uint16_t flags) {
    Bytes payload = make_bytes(resource);
    Writer writer(payload);
    writer.lenenc(3); writer.bytes("def");
    writer.lenenc(4); writer.bytes("test");
    writer.lenenc(1); writer.bytes("t");
    writer.lenenc(1); writer.bytes("t");
    writer.lenenc(name.size()); writer.bytes(name);
    writer.lenenc(name.size()); writer.bytes(name);
    writer.lenenc(12);
    writer.u16(45);
    writer.u32(255);
    writer.u8(type);
    writer.u16(flags);
    writer.u8(0);
    writer.u16(0);
    return payload;
}

}  // namespace

TEST("packet headers round trip and bounds-check") {
    mem::RootArena root(1024 * 1024);
    Bytes buffer = make_bytes(root);
    append_packet(buffer, 7, std::span<const std::byte>(
        reinterpret_cast<const std::byte*>("hello"), 5));
    CHECK_EQ(buffer.size(), 9u);
    PacketHeader header;
    CHECK(parse_packet_header(buffer, header));
    CHECK_EQ(header.m_length, 5u);
    CHECK_EQ(header.m_sequence, 7u);
    std::array<std::byte, 3> short_buffer{};
    CHECK(!parse_packet_header(short_buffer, header));
}

TEST("length-encoded integers use all wire widths") {
    mem::RootArena root(1024 * 1024);
    const std::uint64_t values[] = {0, 1, 250, 251, 65535, 65536, 0xffffff,
                                    0x1000000, 0xffffffffull, 0xffffffffffffffffull};
    for (std::uint64_t value : values) {
        Bytes buffer = make_bytes(root);
        Writer writer(buffer);
        writer.lenenc(value);
        Reader reader(buffer);
        std::uint64_t decoded = 0;
        CHECK(reader.lenenc(decoded));
        CHECK_EQ(decoded, value);
        CHECK_EQ(reader.remaining(), 0u);
    }
}

TEST("handshake v10 parsing") {
    mem::RootArena root(1024 * 1024);
    Bytes payload = fixture_handshake(root);
    Handshake handshake;
    CHECK(parse_handshake(payload, handshake));
    CHECK_EQ(std::string_view(handshake.m_server_version), "8.0.36-test");
    CHECK_EQ(handshake.m_connection_id, 42u);
    CHECK_EQ(handshake.m_charset, 255);
    CHECK_EQ(handshake.m_scramble_length, 20u);
    CHECK_EQ(std::string_view(reinterpret_cast<const char*>(handshake.m_scramble.data()), 20),
             "abcdefghijklmnopqrst");
    CHECK_EQ(std::string_view(handshake.m_auth_plugin), "caching_sha2_password");
    CHECK((handshake.m_capabilities & CLIENT_PROTOCOL_41) != 0);
    CHECK((handshake.m_capabilities & CLIENT_PLUGIN_AUTH) != 0);

    db::ConnectionOptions options;
    options.m_user = "bench";
    options.m_database = "test";
    Bytes auth = auth_response_native("password",
        std::span<const std::uint8_t>(handshake.m_scramble.data(), 20), root);
    Bytes response = build_handshake_response(handshake, options, auth,
                                             "mysql_native_password", root);
    Reader reader(response);
    std::uint32_t capabilities = 0;
    CHECK(reader.u32(capabilities));
    CHECK((capabilities & CLIENT_PROTOCOL_41) != 0);
    CHECK((capabilities & CLIENT_CONNECT_WITH_DB) != 0);
    std::uint32_t max_packet = 0;
    CHECK(reader.u32(max_packet));
    std::uint8_t charset = 0;
    CHECK(reader.u8(charset));
    CHECK_EQ(charset, 45);
    for (int i = 0; i < 23; ++i) {
        std::uint8_t zero = 1;
        CHECK(reader.u8(zero));
        CHECK_EQ(zero, 0);
    }
    std::string_view user;
    CHECK(reader.null_terminated(user) == Reader::StringResult::Ok);
    CHECK_EQ(user, "bench");
    std::uint64_t auth_length = 0;
    CHECK(reader.lenenc(auth_length));
    CHECK_EQ(auth_length, 20u);
}

TEST("authentication scramble matches MySQL vectors") {
    mem::RootArena root(1024 * 1024);
    const std::uint8_t scramble[20] = {
        '1','2','3','4','5','6','7','8','9','0',
        '1','2','3','4','5','6','7','8','9','0'};
    Bytes native = auth_response_native("password", scramble, root);
    CHECK_EQ(to_hex(native), "1957dce2724282e018f40d905824cb6361f88d41");
    Bytes sha2 = auth_response_sha2("password", scramble, root);
    CHECK_EQ(to_hex(sha2),
             "718d80fb92b693f7ee1fcad95bfe7a2be1f332ac2c3ec6cdc85893b65b240650");
    Bytes empty = auth_response_native("", scramble, root);
    CHECK(empty.empty());
}


TEST("TLS SSLRequest and handshake capability") {
    mem::RootArena root(1024 * 1024);
    Bytes payload = fixture_handshake(root);
    Handshake handshake;
    CHECK(parse_handshake(payload, handshake));
    db::ConnectionOptions options;
    options.m_user = "bench";
    Bytes request = build_ssl_request(handshake, options, root);
    CHECK_EQ(request.size(), 32u);
    Reader reader(request);
    std::uint32_t capabilities = 0;
    CHECK(reader.u32(capabilities));
    CHECK((capabilities & CLIENT_SSL) != 0);
    std::uint32_t max_packet = 0;
    CHECK(reader.u32(max_packet));
    std::uint8_t charset = 0;
    CHECK(reader.u8(charset));
    CHECK_EQ(charset, 45);
    for (int i = 0; i < 23; ++i) {
        std::uint8_t zero = 1;
        CHECK(reader.u8(zero));
        CHECK_EQ(zero, 0);
    }
    CHECK_EQ(reader.remaining(), 0u);

    Bytes auth = auth_response_sha2(
        "password", std::span<const std::uint8_t>(handshake.m_scramble.data(), 20),
        root);
    Bytes response = build_handshake_response(
        handshake, options, auth, "caching_sha2_password", root, true);
    Reader response_reader(response);
    std::uint32_t response_caps = 0;
    CHECK(response_reader.u32(response_caps));
    CHECK((response_caps & CLIENT_SSL) != 0);
}

TEST("OK, EOF, ERR, prepare-OK and column packets") {
    mem::RootArena root(1024 * 1024);
    Bytes ok_bytes = make_bytes(root);
    Writer ok_writer(ok_bytes);
    ok_writer.u8(0);
    ok_writer.lenenc(3);
    ok_writer.lenenc(9);
    ok_writer.u16(0x0002);
    ok_writer.u16(1);
    OkPacket ok;
    CHECK(parse_ok(ok_bytes, ok));
    CHECK_EQ(ok.m_affected_rows, 3u);
    CHECK_EQ(ok.m_last_insert_id, 9u);
    CHECK_EQ(ok.m_status, 2u);
    CHECK_EQ(ok.m_warnings, 1u);

    Bytes eof_bytes = make_bytes(root);
    Writer eof_writer(eof_bytes);
    eof_writer.u8(0xfe);
    eof_writer.u16(4);
    eof_writer.u16(0x0008);
    EofPacket eof;
    CHECK(parse_eof(eof_bytes, eof));
    CHECK_EQ(eof.m_warnings, 4u);
    CHECK_EQ(eof.m_status, 8u);

    Bytes err_bytes = make_bytes(root);
    Writer err_writer(err_bytes);
    err_writer.u8(0xff);
    err_writer.u16(1062);
    err_writer.bytes("#23000");
    err_writer.bytes("Duplicate entry");
    ErrorPacket error;
    CHECK(parse_error(err_bytes, error));
    CHECK_EQ(error.m_code, 1062u);
    CHECK_EQ(std::string_view(error.m_state), "23000");
    CHECK_EQ(error.m_message, "Duplicate entry");

    Bytes prepare_bytes = make_bytes(root);
    Writer prepare_writer(prepare_bytes);
    prepare_writer.u8(0);
    prepare_writer.u32(77);
    prepare_writer.u16(2);
    prepare_writer.u16(1);
    prepare_writer.u8(0);
    prepare_writer.u16(0);
    PrepareOk prepare;
    CHECK(parse_prepare_ok(prepare_bytes, prepare));
    CHECK_EQ(prepare.m_statement_id, 77u);
    CHECK_EQ(prepare.m_columns, 2u);
    CHECK_EQ(prepare.m_parameters, 1u);

    Bytes column_bytes = fixture_column(root, "c", TYPE_VAR_STRING, 0);
    db::Column column;
    CHECK(parse_column(column_bytes, column));
    CHECK_EQ(column.m_name, "c");
    CHECK_EQ(column.m_type, TYPE_VAR_STRING);
    CHECK_EQ(column.m_charset, 45u);
}

TEST("text rows preserve NULL and zero-length values") {
    mem::RootArena root(1024 * 1024);
    Bytes payload = fixture_text_row(root);
    db::Row row(root);
    CHECK(parse_text_row(payload, 3, root, row));
    CHECK_EQ(row.m_fields.size(), 3u);
    CHECK(!row.m_fields[0].m_is_null);
    CHECK_EQ(std::string_view(row.m_fields[0].m_data, row.m_fields[0].m_length), "abc");
    CHECK(!row.m_fields[1].m_is_null);
    CHECK_EQ(row.m_fields[1].m_length, 0u);
    CHECK(row.m_fields[2].m_is_null);
}

TEST("binary rows decode numeric and string columns") {
    mem::RootArena root(1024 * 1024);
    db::Column columns[5];
    columns[0].m_type = TYPE_TINY;
    columns[0].m_flags = 0;
    columns[1].m_type = TYPE_LONGLONG;
    columns[1].m_flags = UNSIGNED_FLAG;
    columns[2].m_type = TYPE_DOUBLE;
    columns[3].m_type = TYPE_VAR_STRING;
    columns[4].m_type = TYPE_NULL;

    Bytes payload = make_bytes(root);
    Writer writer(payload);
    writer.u8(0);
    writer.u8(0x04);  // column 0 null (bit 2)
    writer.u64(42);
    writer.u64(std::bit_cast<std::uint64_t>(3.5));
    writer.lenenc(5);
    writer.bytes("hello");

    db::Row row(root);
    CHECK(parse_binary_row(payload, columns, root, row));
    CHECK_EQ(row.m_fields.size(), 5u);
    CHECK(row.m_fields[0].m_is_null);
    CHECK_EQ(std::string_view(row.m_fields[1].m_data, row.m_fields[1].m_length), "42");
    CHECK_EQ(std::string_view(row.m_fields[2].m_data, row.m_fields[2].m_length), "3.5");
    CHECK_EQ(std::string_view(row.m_fields[3].m_data, row.m_fields[3].m_length), "hello");
    CHECK(row.m_fields[4].m_is_null);
}

TEST("prepared-statement execute payload") {
    mem::RootArena root(1024 * 1024);
    Bytes payload = make_bytes(root);
    const db::BindValue values[] = {
        db::BindValue::from_int(7),
        db::BindValue::from_bytes("abc"),
        db::BindValue::null(),
        db::BindValue::from_double(1.5),
    };
    build_stmt_execute_payload(payload, 123, values);
    Reader reader(payload);
    std::uint8_t command = 0;
    std::uint32_t statement_id = 0;
    std::uint8_t flags = 0;
    std::uint32_t iteration = 0;
    std::uint8_t bitmap = 0;
    std::uint8_t new_params = 0;
    CHECK(reader.u8(command));
    CHECK(reader.u32(statement_id));
    CHECK(reader.u8(flags));
    CHECK(reader.u32(iteration));
    CHECK(reader.u8(bitmap));
    CHECK(reader.u8(new_params));
    CHECK_EQ(command, 0x17);
    CHECK_EQ(statement_id, 123u);
    CHECK_EQ(flags, 0);
    CHECK_EQ(bitmap, 0x04);
    CHECK_EQ(new_params, 1);
}


TEST("caching_sha2 RSA full-auth encryption round trip") {
    mem::RootArena root(1024 * 1024);
    EVP_PKEY* key = EVP_PKEY_Q_keygen(nullptr, nullptr, "RSA", 2048);
    REQUIRE(key != nullptr);
    BIO* bio = BIO_new(BIO_s_mem());
    REQUIRE(bio != nullptr);
    CHECK_EQ(PEM_write_bio_PUBKEY(bio, key), 1);
    BUF_MEM* memory = nullptr;
    BIO_get_mem_ptr(bio, &memory);
    REQUIRE(memory != nullptr);
    const std::string_view pem(memory->data, memory->length);

    const std::uint8_t scramble[20] = {
        '1','2','3','4','5','6','7','8','9','0',
        '1','2','3','4','5','6','7','8','9','0'};
    const std::string_view password = "password";
    Bytes encrypted = rsa_encrypt_password(password, scramble, pem, root);
    CHECK(encrypted.size() == 256);

    EVP_PKEY_CTX* context = EVP_PKEY_CTX_new(key, nullptr);
    REQUIRE(context != nullptr);
    CHECK_EQ(EVP_PKEY_decrypt_init(context), 1);
    CHECK(EVP_PKEY_CTX_set_rsa_padding(context, RSA_PKCS1_OAEP_PADDING) > 0);
    CHECK(EVP_PKEY_CTX_set_rsa_oaep_md(context, EVP_sha1()) > 0);
    std::size_t plain_length = 0;
    CHECK_EQ(EVP_PKEY_decrypt(context, nullptr, &plain_length,
                              reinterpret_cast<const unsigned char*>(encrypted.data()),
                              encrypted.size()), 1);
    std::vector<unsigned char> plain(plain_length);
    CHECK_EQ(EVP_PKEY_decrypt(context, plain.data(), &plain_length,
                              reinterpret_cast<const unsigned char*>(encrypted.data()),
                              encrypted.size()), 1);
    plain.resize(plain_length);
    for (std::size_t i = 0; i < password.size(); ++i) {
        CHECK_EQ(plain[i], static_cast<unsigned char>(password[i]) ^ scramble[i % 20]);
    }
    CHECK_EQ(plain.back(), 0);
    EVP_PKEY_CTX_free(context);
    BIO_free(bio);
    EVP_PKEY_free(key);
}

TEST("wire codec throughput") {
    if (!::sb::test::perf_enabled()) return;
    mem::RootArena root(8 * 1024 * 1024);
    Bytes payload = fixture_text_row(root);
    db::Row row(root);
    constexpr std::size_t kOps = 5'000'000;
    const double ops = ::sb::test::measure_ops_per_second(kOps, [&] {
        for (std::size_t i = 0; i < kOps; ++i) {
            CHECK(parse_text_row(payload, 3, root, row));
        }
    });
    std::println("  text row decode: {:.1f} Mrows/s", ops / 1e6);
    CHECK(ops > 500'000.0);
}

int main(int argc, char** argv) {
    mem::GlobalAllocator::instance().initialize(512ull * 1024 * 1024);
    return ::sb::test::run_all(argc, argv);
}
