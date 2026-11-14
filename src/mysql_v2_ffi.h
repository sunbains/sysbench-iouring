#pragma once

#include <stddef.h>
#include <stdint.h>

// Private C ABI. Rust owns opaque client/statement/result objects; all strings
// returned by getters are borrowed until the next getter or object destruction.
extern "C" {
struct SbV2Bytes { const uint8_t* data; size_t size; };
struct SbV2Error { int32_t code; uint8_t fatal; char state[6]; char message[1024]; };
struct SbV2Value { uint32_t kind; int64_t integer; double real; SbV2Bytes bytes; };
struct SbV2Options {
    SbV2Bytes user, password, database;
    uint32_t max_packet_size;
    uint32_t tls_mode;
    uint8_t tls_verify_server_cert;
    SbV2Bytes server_name, tls_ca, tls_cert, tls_key;
    void* context;
    ptrdiff_t (*read)(void*, uint8_t*, size_t);
    ptrdiff_t (*write)(void*, const uint8_t*, size_t);
};
enum SbV2Operation : uint32_t { SB_V2_CONNECT, SB_V2_QUERY, SB_V2_PREPARE,
                              SB_V2_EXECUTE, SB_V2_CLOSE_STATEMENT };
struct SbV2Command {
    uint32_t operation;
    void* client;
    void* statement;
    void* result;
    SbV2Bytes sql;
    const SbV2Value* parameters;
    size_t parameter_count;
    SbV2Options options;
    SbV2Error error;
};
struct SbV2ResultInfo {
    size_t columns, rows;
    uint64_t affected_rows, last_insert_id;
    uint16_t warnings, status;
};
struct SbV2Column { SbV2Bytes name; uint8_t type; uint16_t flags, charset; };
struct SbV2Field { SbV2Bytes bytes; uint8_t is_null; };

void sb_mysql_v2_run(SbV2Command* command);
void sb_mysql_v2_drop_client(void* client);
void sb_mysql_v2_drop_statement(void* statement);
void sb_mysql_v2_drop_result(void* result);
SbV2ResultInfo sb_mysql_v2_result_info(const void* result);
SbV2Column sb_mysql_v2_column(const void* result, size_t index);
SbV2Field sb_mysql_v2_field(void* result, size_t row, size_t column);
void* sb_mysql_v2_allocate(size_t size, size_t alignment);
void sb_mysql_v2_deallocate(void* pointer);
}
