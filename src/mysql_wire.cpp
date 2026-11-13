#include "sysbench/mysql_wire.hpp"

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdio>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace sb::mysql {
namespace {

constexpr std::uint32_t kMaxPayload = 0x00ffffff;

void copy_string(char* destination, std::size_t capacity, std::string_view source,
                 std::size_t& length) {
    length = std::min(source.size(), capacity - 1);
    std::memcpy(destination, source.data(), length);
    destination[length] = '\0';
}

std::array<std::uint8_t, 20> sha1_digest(std::span<const std::byte> data) {
    std::array<std::uint8_t, 20> digest{};
    unsigned int length = 0;
    if (EVP_Digest(data.data(), data.size(), digest.data(), &length, EVP_sha1(),
                   nullptr) != 1 ||
        length != digest.size()) {
        throw std::runtime_error("SHA1 failed");
    }
    return digest;
}

std::array<std::uint8_t, 32> sha256_digest(std::span<const std::byte> data) {
    std::array<std::uint8_t, 32> digest{};
    unsigned int length = 0;
    if (EVP_Digest(data.data(), data.size(), digest.data(), &length, EVP_sha256(),
                   nullptr) != 1 ||
        length != digest.size()) {
        throw std::runtime_error("SHA256 failed");
    }
    return digest;
}

std::span<const std::byte> as_bytes(std::string_view value) {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

template <class T>
std::span<const std::byte> as_bytes(const T& value) {
    return {reinterpret_cast<const std::byte*>(&value), sizeof(T)};
}

void append_bytes(Bytes& out, std::span<const std::byte> data) {
    out.insert(out.end(), data.begin(), data.end());
}

db::Field text_field(mem::MemoryResource& resource, std::string_view text) {
    char* buffer = static_cast<char*>(resource.allocate(text.size() + 1, 1));
    std::memcpy(buffer, text.data(), text.size());
    buffer[text.size()] = '\0';
    return db::Field{buffer, static_cast<std::uint32_t>(text.size()), false};
}

template <class... Args>
db::Field formatted_field(mem::MemoryResource& resource, const char* format,
                          Args... args) {
    char temporary[96];
    const int count = std::snprintf(temporary, sizeof(temporary), format, args...);
    if (count < 0) return db::Field{};
    const std::size_t length = std::min<std::size_t>(
        static_cast<std::size_t>(count), sizeof(temporary) - 1);
    return text_field(resource, std::string_view(temporary, length));
}

std::uint64_t read_uint_le(std::span<const std::byte> data) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < data.size(); ++i) {
        value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(data[i]))
                 << (i * 8);
    }
    return value;
}

bool binary_date(Reader& reader, db::Row& row, mem::MemoryResource& resource,
                 bool time_only) {
    std::uint8_t length = 0;
    if (!reader.u8(length)) return false;
    if (length == 0) {
        row.fields.push_back(text_field(resource,
                                        time_only ? "00:00:00" : "0000-00-00 00:00:00"));
        return true;
    }
    std::uint16_t year = 0;
    std::uint8_t month = 0;
    std::uint8_t day = 0;
    std::uint8_t hour = 0;
    std::uint8_t minute = 0;
    std::uint8_t second = 0;
    if (time_only) {
        if (length >= 8) {
            std::uint8_t negative = 0;
            std::uint32_t days = 0;
            if (!reader.u8(negative) || !reader.u32(days) || !reader.u8(hour) ||
                !reader.u8(minute) || !reader.u8(second)) {
                return false;
            }
            const std::uint64_t total_hours =
                static_cast<std::uint64_t>(days) * 24u + hour;
            row.fields.push_back(formatted_field(
                resource, "%s%02llu:%02u:%02u", negative != 0 ? "-" : "",
                static_cast<unsigned long long>(total_hours),
                static_cast<unsigned>(minute), static_cast<unsigned>(second)));
            if (length == 12) {
                std::uint32_t micros = 0;
                if (!reader.u32(micros)) return false;
            }
        } else {
            row.fields.push_back(text_field(resource, "00:00:00"));
        }
        return true;
    }

    if (length >= 4) {
        if (!reader.u16(year) || !reader.u8(month) || !reader.u8(day)) return false;
    }
    bool has_time = length >= 7;
    if (has_time) {
        if (!reader.u8(hour) || !reader.u8(minute) || !reader.u8(second)) return false;
    }
    if (has_time) {
        row.fields.push_back(formatted_field(
            resource, "%04u-%02u-%02u %02u:%02u:%02u", year, month, day, hour,
            minute, second));
    } else {
        row.fields.push_back(
            formatted_field(resource, "%04u-%02u-%02u", year, month, day));
    }
    if (length == 11) {
        std::uint32_t micros = 0;
        if (!reader.u32(micros)) return false;
        (void)micros;
    }
    return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// Reader / Writer
// ---------------------------------------------------------------------------

bool Reader::u8(std::uint8_t& value) noexcept {
    if (!need(1)) return false;
    value = std::to_integer<std::uint8_t>(m_data[m_position++]);
    return true;
}

bool Reader::u16(std::uint16_t& value) noexcept {
    if (!need(2)) return false;
    value = static_cast<std::uint16_t>(read_uint_le(m_data.subspan(m_position, 2)));
    m_position += 2;
    return true;
}

bool Reader::u24(std::uint32_t& value) noexcept {
    if (!need(3)) return false;
    value = static_cast<std::uint32_t>(read_uint_le(m_data.subspan(m_position, 3)));
    m_position += 3;
    return true;
}

bool Reader::u32(std::uint32_t& value) noexcept {
    if (!need(4)) return false;
    value = static_cast<std::uint32_t>(read_uint_le(m_data.subspan(m_position, 4)));
    m_position += 4;
    return true;
}

bool Reader::u64(std::uint64_t& value) noexcept {
    if (!need(8)) return false;
    value = read_uint_le(m_data.subspan(m_position, 8));
    m_position += 8;
    return true;
}

bool Reader::lenenc(std::uint64_t& value) noexcept {
    std::uint8_t first = 0;
    if (!u8(first)) return false;
    if (first < 0xfb) {
        value = first;
        return true;
    }
    if (first == 0xfc) {
        std::uint16_t v = 0;
        if (!u16(v)) return false;
        value = v;
        return true;
    }
    if (first == 0xfd) {
        std::uint32_t v = 0;
        if (!u24(v)) return false;
        value = v;
        return true;
    }
    if (first == 0xfe) return u64(value);
    return false;
}

Reader::StringResult Reader::lenenc_string(std::string_view& value) noexcept {
    std::uint8_t first = 0;
    if (!peek_u8(first)) return StringResult::Invalid;
    if (first == 0xfb) {
        ++m_position;
        value = {};
        return StringResult::Null;
    }
    std::uint64_t length = 0;
    if (!lenenc(length)) return StringResult::Invalid;
    if (length > remaining()) return StringResult::Invalid;
    value = {reinterpret_cast<const char*>(m_data.data() + m_position),
             static_cast<std::size_t>(length)};
    m_position += static_cast<std::size_t>(length);
    return StringResult::Ok;
}

Reader::StringResult Reader::null_terminated(std::string_view& value) noexcept {
    const auto* start = reinterpret_cast<const char*>(m_data.data() + m_position);
    const std::size_t available = remaining();
    const void* terminator = std::memchr(start, '\0', available);
    if (terminator == nullptr) return StringResult::Invalid;
    const auto length = static_cast<const char*>(terminator) - start;
    value = {start, static_cast<std::size_t>(length)};
    m_position += static_cast<std::size_t>(length) + 1;
    return StringResult::Ok;
}

bool Reader::take(std::size_t count, std::span<const std::byte>& out) noexcept {
    if (!need(count)) return false;
    out = m_data.subspan(m_position, count);
    m_position += count;
    return true;
}

bool Reader::skip(std::size_t count) noexcept {
    if (!need(count)) return false;
    m_position += count;
    return true;
}

bool Reader::peek_u8(std::uint8_t& value) const noexcept {
    if (!need(1)) return false;
    value = std::to_integer<std::uint8_t>(m_data[m_position]);
    return true;
}

void Writer::u8(std::uint8_t value) {
    m_out->push_back(static_cast<std::byte>(value));
}

void Writer::u16(std::uint16_t value) {
    u8(static_cast<std::uint8_t>(value & 0xff));
    u8(static_cast<std::uint8_t>((value >> 8) & 0xff));
}

void Writer::u24(std::uint32_t value) {
    u8(static_cast<std::uint8_t>(value & 0xff));
    u8(static_cast<std::uint8_t>((value >> 8) & 0xff));
    u8(static_cast<std::uint8_t>((value >> 16) & 0xff));
}

void Writer::u32(std::uint32_t value) {
    for (int shift = 0; shift < 32; shift += 8) {
        u8(static_cast<std::uint8_t>((value >> shift) & 0xff));
    }
}

void Writer::u64(std::uint64_t value) {
    for (int shift = 0; shift < 64; shift += 8) {
        u8(static_cast<std::uint8_t>((value >> shift) & 0xff));
    }
}

void Writer::lenenc(std::uint64_t value) {
    if (value < 0xfb) {
        u8(static_cast<std::uint8_t>(value));
    } else if (value <= 0xffff) {
        u8(0xfc);
        u16(static_cast<std::uint16_t>(value));
    } else if (value <= 0xffffff) {
        u8(0xfd);
        u24(static_cast<std::uint32_t>(value));
    } else {
        u8(0xfe);
        u64(value);
    }
}

void Writer::bytes(std::span<const std::byte> value) {
    m_out->insert(m_out->end(), value.begin(), value.end());
}

void Writer::bytes(std::string_view value) {
    const auto* data = reinterpret_cast<const std::byte*>(value.data());
    m_out->insert(m_out->end(), data, data + value.size());
}

void Writer::null_terminated(std::string_view value) {
    bytes(value);
    u8(0);
}

void Writer::zeros(std::size_t count) {
    m_out->insert(m_out->end(), count, std::byte{0});
}

void Writer::reserve(std::size_t count) { m_out->reserve(count); }

// ---------------------------------------------------------------------------
// Packets
// ---------------------------------------------------------------------------

bool parse_packet_header(std::span<const std::byte> data, PacketHeader& header) noexcept {
    if (data.size() < 4) return false;
    const std::uint32_t length =
        static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(data[0])) |
        (static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(data[1])) << 8) |
        (static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(data[2])) << 16);
    header.length = length;
    header.sequence = std::to_integer<std::uint8_t>(data[3]);
    return true;
}

void write_packet_header(Bytes& out, std::uint32_t length, std::uint8_t sequence) {
    out.push_back(static_cast<std::byte>(length & 0xff));
    out.push_back(static_cast<std::byte>((length >> 8) & 0xff));
    out.push_back(static_cast<std::byte>((length >> 16) & 0xff));
    out.push_back(static_cast<std::byte>(sequence));
}

void append_packet(Bytes& out, std::uint8_t sequence,
                   std::span<const std::byte> payload) {
    if (payload.size() > kMaxPayload) {
        throw std::length_error("MySQL packet payload exceeds 16 MiB");
    }
    write_packet_header(out, static_cast<std::uint32_t>(payload.size()), sequence);
    append_bytes(out, payload);
}

bool parse_handshake(std::span<const std::byte> payload, Handshake& handshake) noexcept {
    Reader reader(payload);
    std::uint8_t protocol = 0;
    std::string_view version;
    if (!reader.u8(protocol) || protocol != 10) return false;
    if (reader.null_terminated(version) != Reader::StringResult::Ok) return false;
    copy_string(handshake.server_version, sizeof(handshake.server_version), version,
                handshake.server_version_length);

    if (!reader.u32(handshake.connection_id)) return false;
    std::span<const std::byte> auth1;
    if (!reader.take(8, auth1)) return false;
    std::uint8_t filler = 0;
    if (!reader.u8(filler)) return false;

    std::uint16_t capability_low = 0;
    if (!reader.u16(capability_low)) return false;
    if (reader.remaining() == 0) {
        handshake.capabilities = capability_low;
        return true;
    }

    if (!reader.u8(handshake.charset)) return false;
    if (!reader.u16(handshake.status)) return false;
    std::uint16_t capability_high = 0;
    if (!reader.u16(capability_high)) return false;
    handshake.capabilities = capability_low |
                             (static_cast<std::uint32_t>(capability_high) << 16);
    if (!reader.u8(reinterpret_cast<std::uint8_t&>(filler))) return false;  // auth length
    const std::size_t auth_length = filler;
    if (!reader.skip(10)) return false;

    const std::size_t part2_length =
        handshake.capabilities & CLIENT_SECURE_CONNECTION
            ? std::max<std::size_t>(13, auth_length > 8 ? auth_length - 8 : 0)
            : 0;
    std::span<const std::byte> auth2;
    const std::size_t available_part2 = std::min(part2_length, reader.remaining());
    if (available_part2 > 0 && !reader.take(available_part2, auth2)) return false;

    std::size_t scramble_length = 0;
    for (std::byte byte : auth1) {
        if (scramble_length < handshake.scramble.size()) {
            handshake.scramble[scramble_length++] =
                std::to_integer<std::uint8_t>(byte);
        }
    }
    for (std::byte byte : auth2) {
        if (scramble_length >= handshake.scramble.size()) break;
        const std::uint8_t value = std::to_integer<std::uint8_t>(byte);
        if (value == 0) break;
        handshake.scramble[scramble_length++] = value;
    }
    handshake.scramble_length = scramble_length;

    if (handshake.capabilities & CLIENT_PLUGIN_AUTH) {
        std::string_view plugin;
        if (reader.null_terminated(plugin) == Reader::StringResult::Ok && !plugin.empty()) {
            copy_string(handshake.auth_plugin, sizeof(handshake.auth_plugin), plugin,
                        handshake.auth_plugin_length);
        }
    }
    return true;
}

bool parse_ok(std::span<const std::byte> payload, OkPacket& packet) noexcept {
    if (payload.empty()) return false;
    const auto header = std::to_integer<std::uint8_t>(payload[0]);
    if (header != 0x00 && !(header == 0xfe && payload.size() >= 9)) return false;
    Reader reader(payload);
    std::uint8_t ignored = 0;
    if (!reader.u8(ignored)) return false;
    if (!reader.lenenc(packet.affected_rows)) return false;
    if (!reader.lenenc(packet.last_insert_id)) return false;
    if (!reader.u16(packet.status)) return false;
    if (!reader.u16(packet.warnings)) return false;
    return true;
}

bool parse_eof(std::span<const std::byte> payload, EofPacket& packet) noexcept {
    if (payload.empty() || std::to_integer<std::uint8_t>(payload[0]) != 0xfe) {
        return false;
    }
    Reader reader(payload);
    std::uint8_t ignored = 0;
    if (!reader.u8(ignored)) return false;
    if (reader.remaining() >= 2) (void)reader.u16(packet.warnings);
    if (reader.remaining() >= 2) (void)reader.u16(packet.status);
    return true;
}

bool parse_error(std::span<const std::byte> payload, ErrorPacket& packet) noexcept {
    if (payload.size() < 3 || std::to_integer<std::uint8_t>(payload[0]) != 0xff) {
        return false;
    }
    Reader reader(payload);
    std::uint8_t ignored = 0;
    if (!reader.u8(ignored)) return false;
    if (!reader.u16(packet.code)) return false;
    std::uint8_t next = 0;
    if (reader.peek_u8(next) && next == '#') {
        (void)reader.u8(next);
        std::span<const std::byte> state;
        if (!reader.take(5, state)) return false;
        for (std::size_t i = 0; i < 5; ++i) {
            packet.state[i] = static_cast<char>(std::to_integer<std::uint8_t>(state[i]));
        }
        packet.state[5] = '\0';
    }
    const auto remaining = reader.remaining_data();
    packet.message = {reinterpret_cast<const char*>(remaining.data()), remaining.size()};
    return true;
}

bool parse_prepare_ok(std::span<const std::byte> payload, PrepareOk& packet) noexcept {
    if (payload.empty() || std::to_integer<std::uint8_t>(payload[0]) != 0x00) return false;
    Reader reader(payload);
    std::uint8_t ignored = 0;
    if (!reader.u8(ignored)) return false;
    if (!reader.u32(packet.statement_id)) return false;
    if (!reader.u16(packet.columns)) return false;
    if (!reader.u16(packet.parameters)) return false;
    if (!reader.u8(ignored)) return false;
    if (!reader.u16(packet.warnings)) return false;
    return true;
}

bool parse_column(std::span<const std::byte> payload, db::Column& column) noexcept {
    Reader reader(payload);
    std::string_view catalog;
    std::string_view schema;
    std::string_view table;
    std::string_view org_table;
    std::string_view name;
    std::string_view org_name;
    if (reader.lenenc_string(catalog) != Reader::StringResult::Ok) return false;
    if (reader.lenenc_string(schema) != Reader::StringResult::Ok) return false;
    if (reader.lenenc_string(table) != Reader::StringResult::Ok) return false;
    if (reader.lenenc_string(org_table) != Reader::StringResult::Ok) return false;
    if (reader.lenenc_string(name) != Reader::StringResult::Ok) return false;
    if (reader.lenenc_string(org_name) != Reader::StringResult::Ok) return false;
    std::uint64_t fixed_length = 0;
    if (!reader.lenenc(fixed_length)) return false;
    std::uint16_t charset = 0;
    if (!reader.u16(charset)) return false;
    column.charset = charset;
    std::uint32_t column_length = 0;
    if (!reader.u32(column_length)) return false;
    if (!reader.u8(column.type)) return false;
    if (!reader.u16(column.flags)) return false;
    std::uint8_t decimals = 0;
    if (!reader.u8(decimals)) return false;
    if (!reader.skip(2)) return false;
    column.name = name;
    return true;
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

Bytes auth_response_native(std::string_view password,
                           std::span<const std::uint8_t> scramble,
                           mem::MemoryResource& resource) {
    Bytes response = make_bytes(resource);
    if (password.empty() || scramble.empty()) return response;
    const auto first = sha1_digest(as_bytes(password));
    const auto second = sha1_digest(as_bytes(first));
    Bytes input = make_bytes(resource);
    input.reserve(scramble.size() + second.size());
    append_bytes(input, {reinterpret_cast<const std::byte*>(scramble.data()),
                         scramble.size()});
    append_bytes(input, as_bytes(second));
    const auto third = sha1_digest(input);
    response.resize(first.size());
    for (std::size_t i = 0; i < first.size(); ++i) {
        response[i] = static_cast<std::byte>(first[i] ^ third[i]);
    }
    return response;
}

Bytes auth_response_sha2(std::string_view password,
                         std::span<const std::uint8_t> scramble,
                         mem::MemoryResource& resource) {
    Bytes response = make_bytes(resource);
    if (password.empty() || scramble.empty()) return response;
    const auto first = sha256_digest(as_bytes(password));
    const auto second = sha256_digest(as_bytes(first));
    Bytes input = make_bytes(resource);
    input.reserve(scramble.size() + second.size());
    append_bytes(input, as_bytes(second));
    append_bytes(input, {reinterpret_cast<const std::byte*>(scramble.data()),
                         scramble.size()});
    const auto third = sha256_digest(input);
    response.resize(first.size());
    for (std::size_t i = 0; i < first.size(); ++i) {
        response[i] = static_cast<std::byte>(first[i] ^ third[i]);
    }
    return response;
}

Bytes auth_response_for(std::string_view plugin, std::string_view password,
                        std::span<const std::uint8_t> scramble,
                        mem::MemoryResource& resource) {
    if (plugin == "caching_sha2_password") {
        return auth_response_sha2(password, scramble, resource);
    }
    if (plugin == "mysql_native_password" || plugin.empty()) {
        return auth_response_native(password, scramble, resource);
    }
    // Unknown plugin: let the server send an auth switch request rather than
    // guessing at a wire format.
    return make_bytes(resource);
}

Bytes rsa_encrypt_password(std::string_view password,
                           std::span<const std::uint8_t> scramble,
                           std::string_view public_key_pem,
                           mem::MemoryResource& resource) {
    Bytes result = make_bytes(resource);
    if (scramble.empty()) return result;

    Bytes plain = make_bytes(resource);
    plain.resize(password.size() + 1);
    for (std::size_t i = 0; i < password.size(); ++i) {
        plain[i] = static_cast<std::byte>(
            static_cast<std::uint8_t>(password[i]) ^ scramble[i % scramble.size()]);
    }
    plain[password.size()] = std::byte{0};

    BIO* bio = BIO_new_mem_buf(public_key_pem.data(),
                               static_cast<int>(public_key_pem.size()));
    if (bio == nullptr) throw db::Error("RSA: BIO allocation failed", 2061);
    EVP_PKEY* key = PEM_read_bio_PUBKEY(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (key == nullptr) throw db::Error("RSA: invalid public key", 2061);

    EVP_PKEY_CTX* context = EVP_PKEY_CTX_new(key, nullptr);
    EVP_PKEY_free(key);
    if (context == nullptr) throw db::Error("RSA: context allocation failed", 2061);

    std::size_t encrypted_length = 0;
    bool ok = EVP_PKEY_encrypt_init(context) == 1;
    if (ok) {
        ok = EVP_PKEY_CTX_set_rsa_padding(context, RSA_PKCS1_OAEP_PADDING) > 0;
    }
    if (ok) {
        ok = EVP_PKEY_CTX_set_rsa_oaep_md(context, EVP_sha1()) > 0;
    }
    if (ok) {
        ok = EVP_PKEY_encrypt(
                 context, nullptr, &encrypted_length,
                 reinterpret_cast<const unsigned char*>(plain.data()), plain.size()) == 1;
    }
    if (ok) {
        result.resize(encrypted_length);
        ok = EVP_PKEY_encrypt(context, reinterpret_cast<unsigned char*>(result.data()),
                              &encrypted_length,
                              reinterpret_cast<const unsigned char*>(plain.data()),
                              plain.size()) == 1;
        if (ok) result.resize(encrypted_length);
    }
    EVP_PKEY_CTX_free(context);
    if (!ok) throw db::Error("RSA password encryption failed", 2061);
    return result;
}

std::uint32_t client_capabilities(const Handshake& handshake,
                                  const db::ConnectionOptions& options,
                                  bool ssl) noexcept {
    std::uint32_t capabilities = CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG |
                                 CLIENT_PROTOCOL_41 | CLIENT_TRANSACTIONS |
                                 CLIENT_SECURE_CONNECTION | CLIENT_MULTI_RESULTS |
                                 CLIENT_PS_MULTI_RESULTS;
    if (!options.database.empty()) capabilities |= CLIENT_CONNECT_WITH_DB;
    if (handshake.capabilities & CLIENT_PLUGIN_AUTH) {
        capabilities |= CLIENT_PLUGIN_AUTH;
    }
    if (handshake.capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
        capabilities |= CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
    }
    if (ssl) capabilities |= CLIENT_SSL;
    return capabilities;
}

Bytes build_ssl_request(const Handshake& handshake,
                        const db::ConnectionOptions& options,
                        mem::MemoryResource& resource) {
    Bytes payload = make_bytes(resource);
    payload.reserve(32);
    Writer writer(payload);
    writer.u32(client_capabilities(handshake, options, true));
    writer.u32(16 * 1024 * 1024);
    writer.u8(45);
    writer.zeros(23);
    return payload;
}

Bytes build_handshake_response(const Handshake& handshake,
                               const db::ConnectionOptions& options,
                               std::span<const std::byte> auth_response,
                               std::string_view plugin,
                               mem::MemoryResource& resource,
                               bool ssl) {
    const std::uint32_t capabilities =
        client_capabilities(handshake, options, ssl);

    Bytes payload = make_bytes(resource);
    payload.reserve(128 + auth_response.size() + options.user.size());
    Writer writer(payload);
    writer.u32(capabilities);
    writer.u32(16 * 1024 * 1024);
    writer.u8(45);  // utf8mb4_general_ci
    writer.zeros(23);
    writer.null_terminated(options.user);

    if (capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
        writer.lenenc(auth_response.size());
        writer.bytes(auth_response);
    } else if (capabilities & CLIENT_SECURE_CONNECTION) {
        writer.u8(static_cast<std::uint8_t>(auth_response.size()));
        writer.bytes(auth_response);
    } else {
        writer.bytes(auth_response);
        writer.u8(0);
    }

    if (capabilities & CLIENT_CONNECT_WITH_DB) {
        writer.null_terminated(options.database);
    }
    if (capabilities & CLIENT_PLUGIN_AUTH) {
        writer.null_terminated(plugin);
    }
    return payload;
}

// ---------------------------------------------------------------------------
// Result rows
// ---------------------------------------------------------------------------

bool parse_text_row(std::span<const std::byte> payload, std::size_t column_count,
                    mem::MemoryResource& resource, db::Row& row) {
    (void)resource;
    row.fields.clear();
    row.fields.reserve(column_count);
    Reader reader(payload);
    for (std::size_t i = 0; i < column_count; ++i) {
        std::string_view value;
        const auto result = reader.lenenc_string(value);
        if (result == Reader::StringResult::Null) {
            row.fields.push_back(db::Field{});
        } else if (result == Reader::StringResult::Ok) {
            row.fields.push_back(db::Field{value.data(),
                                           static_cast<std::uint32_t>(value.size()),
                                           false});
        } else {
            return false;
        }
    }
    return true;
}

bool parse_binary_row(std::span<const std::byte> payload,
                      std::span<const db::Column> columns,
                      mem::MemoryResource& resource, db::Row& row) {
    if (payload.empty() || std::to_integer<std::uint8_t>(payload[0]) != 0x00) {
        return false;
    }
    Reader reader(payload);
    std::uint8_t ignored = 0;
    if (!reader.u8(ignored)) return false;

    const std::size_t bitmap_length = (columns.size() + 7 + 2) / 8;
    std::span<const std::byte> bitmap;
    if (!reader.take(bitmap_length, bitmap)) return false;

    row.fields.clear();
    row.fields.reserve(columns.size());

    auto is_null = [&](std::size_t index) {
        const std::size_t bit = index + 2;
        const auto byte = std::to_integer<std::uint8_t>(bitmap[bit / 8]);
        return (byte & (1u << (bit % 8))) != 0;
    };

    for (std::size_t i = 0; i < columns.size(); ++i) {
        const db::Column& column = columns[i];
        if (is_null(i) || column.type == TYPE_NULL) {
            row.fields.push_back(db::Field{});
            continue;
        }
        const bool is_unsigned = (column.flags & UNSIGNED_FLAG) != 0;
        switch (column.type) {
            case TYPE_TINY: {
                std::uint8_t raw = 0;
                if (!reader.u8(raw)) return false;
                if (is_unsigned) row.fields.push_back(formatted_field(resource, "%u", raw));
                else row.fields.push_back(
                    formatted_field(resource, "%d", static_cast<int>(static_cast<std::int8_t>(raw))));
                break;
            }
            case TYPE_SHORT:
            case TYPE_YEAR: {
                std::uint16_t raw = 0;
                if (!reader.u16(raw)) return false;
                if (is_unsigned || column.type == TYPE_YEAR)
                    row.fields.push_back(formatted_field(resource, "%u", raw));
                else
                    row.fields.push_back(
                        formatted_field(resource, "%d", static_cast<std::int16_t>(raw)));
                break;
            }
            case TYPE_LONG:
            case TYPE_INT24: {
                std::uint32_t raw = 0;
                if (!reader.u32(raw)) return false;
                if (is_unsigned) row.fields.push_back(formatted_field(resource, "%u", raw));
                else
                    row.fields.push_back(
                        formatted_field(resource, "%d", static_cast<std::int32_t>(raw)));
                break;
            }
            case TYPE_LONGLONG: {
                std::uint64_t raw = 0;
                if (!reader.u64(raw)) return false;
                if (is_unsigned)
                    row.fields.push_back(formatted_field(resource, "%llu",
                                                         static_cast<unsigned long long>(raw)));
                else
                    row.fields.push_back(formatted_field(resource, "%lld",
                                                         static_cast<long long>(raw)));
                break;
            }
            case TYPE_FLOAT: {
                std::uint32_t raw = 0;
                if (!reader.u32(raw)) return false;
                const float value = std::bit_cast<float>(raw);
                row.fields.push_back(formatted_field(resource, "%.9g",
                                                     static_cast<double>(value)));
                break;
            }
            case TYPE_DOUBLE: {
                std::uint64_t raw = 0;
                if (!reader.u64(raw)) return false;
                const double value = std::bit_cast<double>(raw);
                row.fields.push_back(formatted_field(resource, "%.17g", value));
                break;
            }
            case TYPE_DATE:
            case TYPE_NEWDATE:
            case TYPE_DATETIME:
            case TYPE_TIMESTAMP:
            case TYPE_DATETIME2:
            case TYPE_TIMESTAMP2:
                if (!binary_date(reader, row, resource, false)) return false;
                break;
            case TYPE_TIME:
            case TYPE_TIME2:
                if (!binary_date(reader, row, resource, true)) return false;
                break;
            case TYPE_DECIMAL:
            case TYPE_NEWDECIMAL:
            case TYPE_VARCHAR:
            case TYPE_VAR_STRING:
            case TYPE_STRING:
            case TYPE_ENUM:
            case TYPE_SET:
            case TYPE_JSON:
            case TYPE_BLOB:
            case TYPE_TINY_BLOB:
            case TYPE_MEDIUM_BLOB:
            case TYPE_LONG_BLOB:
            case TYPE_GEOMETRY:
            case TYPE_BIT: {
                std::string_view value;
                if (reader.lenenc_string(value) != Reader::StringResult::Ok) {
                    return false;
                }
                row.fields.push_back(db::Field{value.data(),
                                               static_cast<std::uint32_t>(value.size()),
                                               false});
                break;
            }
            default:
                return false;
        }
    }
    return reader.remaining() == 0;
}

void build_stmt_execute_payload(Bytes& out, std::uint32_t statement_id,
                                std::span<const db::BindValue> parameters) {
    Writer writer(out);
    writer.u8(0x17);
    writer.u32(statement_id);
    writer.u8(0);
    writer.u32(1);

    if (parameters.empty()) return;

    const std::size_t bitmap_length = (parameters.size() + 7) / 8;
    const std::size_t bitmap_start = out.size();
    writer.zeros(bitmap_length);
    for (std::size_t i = 0; i < parameters.size(); ++i) {
        if (parameters[i].kind == db::BindKind::Null) {
            out[bitmap_start + i / 8] |= static_cast<std::byte>(1u << (i % 8));
        }
    }
    writer.u8(1);  // new params bound

    for (const auto& parameter : parameters) {
        switch (parameter.kind) {
            case db::BindKind::Null:
                writer.u8(TYPE_NULL);
                writer.u8(0);
                break;
            case db::BindKind::Integer:
                writer.u8(TYPE_LONGLONG);
                writer.u8(0);
                break;
            case db::BindKind::Real:
                writer.u8(TYPE_DOUBLE);
                writer.u8(0);
                break;
            case db::BindKind::Bytes:
                writer.u8(TYPE_VAR_STRING);
                writer.u8(0);
                break;
        }
    }

    for (const auto& parameter : parameters) {
        switch (parameter.kind) {
            case db::BindKind::Null:
                break;
            case db::BindKind::Integer:
                writer.u64(static_cast<std::uint64_t>(parameter.integer));
                break;
            case db::BindKind::Real:
                writer.u64(std::bit_cast<std::uint64_t>(parameter.real));
                break;
            case db::BindKind::Bytes:
                writer.lenenc(parameter.bytes.size());
                writer.bytes(parameter.bytes);
                break;
        }
    }
}

std::string_view type_name(std::uint8_t type) noexcept {
    switch (type) {
        case TYPE_DECIMAL: return "DECIMAL";
        case TYPE_TINY: return "TINY";
        case TYPE_SHORT: return "SHORT";
        case TYPE_LONG: return "LONG";
        case TYPE_FLOAT: return "FLOAT";
        case TYPE_DOUBLE: return "DOUBLE";
        case TYPE_NULL: return "NULL";
        case TYPE_TIMESTAMP: return "TIMESTAMP";
        case TYPE_LONGLONG: return "LONGLONG";
        case TYPE_INT24: return "INT24";
        case TYPE_DATE: return "DATE";
        case TYPE_TIME: return "TIME";
        case TYPE_DATETIME: return "DATETIME";
        case TYPE_YEAR: return "YEAR";
        case TYPE_VARCHAR: return "VARCHAR";
        case TYPE_BIT: return "BIT";
        case TYPE_JSON: return "JSON";
        case TYPE_NEWDECIMAL: return "NEWDECIMAL";
        case TYPE_ENUM: return "ENUM";
        case TYPE_SET: return "SET";
        case TYPE_TINY_BLOB: return "TINY_BLOB";
        case TYPE_MEDIUM_BLOB: return "MEDIUM_BLOB";
        case TYPE_LONG_BLOB: return "LONG_BLOB";
        case TYPE_BLOB: return "BLOB";
        case TYPE_VAR_STRING: return "VAR_STRING";
        case TYPE_STRING: return "STRING";
        case TYPE_GEOMETRY: return "GEOMETRY";
        default: return "UNKNOWN";
    }
}

}  // namespace sb::mysql
