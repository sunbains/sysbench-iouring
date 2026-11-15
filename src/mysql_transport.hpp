#pragma once

#include "sysbench/db.hpp"

namespace sb::mysql {

// The protocol implementations share the same runtime-owned socket and fixed
// buffers. This stream carries bytes; framing belongs to each client library.
class ByteStream {
public:
    virtual ~ByteStream() = default;
    virtual Task<void> connect(const db::ConnectionOptions& options) = 0;
    virtual Task<std::size_t> read(std::span<std::byte> destination) = 0;
    virtual Task<void> write(std::span<const std::byte> source) = 0;
    virtual void close() noexcept = 0;
    [[nodiscard]] virtual bool healthy() const noexcept = 0;
    virtual void destroy(mem::MemoryResource& resource) noexcept = 0;
};

ByteStream* create_byte_stream(mem::MemoryResource& resource);
Task<db::ConnectResult> connect_v2(const db::ConnectionOptions& options,
                                  mem::MemoryResource& persistent);

}  // namespace sb::mysql
