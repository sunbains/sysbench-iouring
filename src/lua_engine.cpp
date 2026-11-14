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
#include "sysbench/layer_stats.hpp"
#include "sysbench/lua.hpp"
#include "sysbench/memory.hpp"

#include "sysbench/db.hpp"
#include "sysbench/io_ring.hpp"
#include "sysbench/mysql.hpp"
#include "sysbench/sharded_counter.hpp"
#include "sysbench/task.hpp"

#include <lua.hpp>

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <new>
#include <cmath>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <optional>
#include <string_view>
#include <utility>

namespace sb {
namespace {

std::atomic<bool> g_interrupted{false};
class LuaVm;
thread_local LuaVm* tls_vm = nullptr;

using Clock = std::chrono::steady_clock;

struct LuaFailure final : std::exception {
    char m_message[2048]{};
    LuaFailure(std::string_view text) {
        const std::size_t length = std::min(text.size(), sizeof(m_message) - 1);
        std::memcpy(m_message, text.data(), length);
        m_message[length] = '\0';
    }
    [[nodiscard]] const char* what() const noexcept override { return m_message; }
};

void copy_text(char* destination, std::size_t capacity, std::string_view source) {
    const std::size_t length = std::min(source.size(), capacity - 1);
    std::memcpy(destination, source.data(), length);
    destination[length] = '\0';
}

class Rng {
public:
    explicit Rng(std::uint64_t seed) {
        if (seed == 0) {
            seed = static_cast<std::uint64_t>(
                       std::chrono::steady_clock::now().time_since_epoch().count()) ^
                   (static_cast<std::uint64_t>(::getpid()) << 32);
        }
        m_state[0] = splitmix(seed);
        m_state[1] = splitmix(m_state[0]);
        if ((m_state[0] | m_state[1]) == 0) m_state[0] = 1;
    }

    [[nodiscard]] std::uint64_t next() noexcept {
        const std::uint64_t s0 = m_state[0];
        std::uint64_t s1 = m_state[1];
        const std::uint64_t result = s0 + s1;
        s1 ^= s0;
        m_state[0] = rotl(s0, 55) ^ s1 ^ (s1 << 14);
        m_state[1] = rotl(s1, 36);
        return result;
    }

    [[nodiscard]] double uniform_double() noexcept {
        return static_cast<double>(next() >> 11) * (1.0 / 9007199254740992.0);
    }

    [[nodiscard]] std::uint64_t uniform(std::uint64_t low,
                                        std::uint64_t high) noexcept {
        if (high <= low) return low;
        const std::uint64_t range = high - low + 1;
        if (range == 0) return next();
        const std::uint64_t limit = UINT64_MAX - (UINT64_MAX % range);
        std::uint64_t value = next();
        while (value >= limit) value = next();
        return low + value % range;
    }

    [[nodiscard]] std::uint32_t unique() noexcept {
        return static_cast<std::uint32_t>(++m_unique);
    }

private:
    static std::uint64_t splitmix(std::uint64_t& state) noexcept {
        std::uint64_t value = (state += 0x9e3779b97f4a7c15ull);
        value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ull;
        value = (value ^ (value >> 27)) * 0x94d049bb133111ebull;
        return value ^ (value >> 31);
    }
    static std::uint64_t rotl(std::uint64_t value, int shift) noexcept {
        return (value << shift) | (value >> (64 - shift));
    }
    std::uint64_t m_state[2]{};
    std::uint64_t m_unique = 0;
};

constexpr std::uint32_t kMaxBenchmarkThreads = 32768;

struct RunControl {
    Clock::time_point m_start{};
    Clock::time_point m_warmup_end{};
    Clock::time_point m_end{};
    std::uint64_t m_event_limit = 0;
    std::uint64_t m_rate = 0;
    bool m_fair_yield = false;
    ShardedCounter<std::uint64_t, kMaxBenchmarkThreads> m_events;
    alignas(mem::kCacheLine) std::atomic<bool> m_stop{false};
    // Benchmark tasks still running; the reporter task exits at zero.
    alignas(mem::kCacheLine) std::atomic<std::uint32_t> m_remaining{0};

    [[nodiscard]] bool stats_enabled(Clock::time_point now) const noexcept {
        return now >= m_warmup_end;
    }

};

class LuaVm;
struct LuaDriverHandle;
struct LuaConnection;
struct LuaStatement;
struct LuaParam;
struct LuaResult;
struct LuaHistogram;

enum class PendingKind {
    None,
    Connect,
    Query,
    QueryStream,
    Prepare,
    Reconnect,
    Execute,
    QuerySend,
    ExecuteSend,
    Flush,
    CloseStatement,
    BulkInsert,
    Sleep,
    MoreEventsYield
};

struct Pending {
    PendingKind m_kind = PendingKind::None;
    LuaDriverHandle* m_driver = nullptr;
    LuaConnection* m_connection = nullptr;
    LuaStatement* m_statement = nullptr;
    std::uint64_t m_sleep_ns = 0;
    std::uint32_t m_stream_depth = 1;
    std::uint64_t m_stream_total = 0;
    bool m_bool_result = false;
};

struct LuaDriverHandle {
    db::Driver* m_driver = nullptr;
    mem::MemoryResource* m_resource = nullptr;
};

struct LuaParam {
    enum class Kind : std::uint8_t { Integer, Real, Bytes };
    Kind m_kind = Kind::Integer;
    mem::MemoryResource* m_resource = nullptr;
    std::size_t m_capacity = 0;
    char* m_buffer = nullptr;
    std::size_t m_length = 0;
    std::int64_t m_integer = 0;
    double m_real = 0.0;
    bool m_is_null = true;

    LuaParam(mem::MemoryResource& owner, Kind kind_value, std::size_t capacity_value)
        : m_kind(kind_value), m_resource(&owner), m_capacity(capacity_value) {
        if (m_kind == Kind::Bytes) {
            if (m_capacity == 0) m_capacity = 1;
            m_buffer = static_cast<char*>(m_resource->allocate(m_capacity, 1));
        }
        if (m_kind == Kind::Integer) m_capacity = 0;
    }
    ~LuaParam() {
        if (m_buffer != nullptr) m_resource->deallocate(m_buffer, m_capacity, 1);
    }
    LuaParam(const LuaParam&) = delete;
    LuaParam& operator=(const LuaParam&) = delete;
};

struct LuaConnection {
    db::Connection* m_connection = nullptr;
    mem::MemoryResource* m_resource = nullptr;
    LuaVm* m_vm = nullptr;
    bool m_closed = false;
    /* Pipelining (con:pipeline(depth)): with a depth above 1, query() and
    execute() write the command and return at once; a reply is read only when
    `depth` commands are outstanding.  Any other operation on the connection
    first reads every outstanding reply. */
    std::uint32_t m_pipeline_depth = 0;
    int m_error_kind = 0;
    int m_error_code = 0;
    char m_error_state[6] = "HY000";
    char m_error_message[1024]{};
    /* A bulk INSERT is assembled in a request heap the connection owns, carved
    from the root on the connection's node when the first bulk insert starts.
    The statement (prefix, then rows) is built in place and sent from there, so
    neither the Lua heap nor the connection heap ever holds it. */
    static constexpr std::size_t kBulkFlushBytes = 1024 * 1024;
    static constexpr std::size_t kBulkHeapBytes = 4 * kBulkFlushBytes;
    struct BulkRequest {
        mem::HeapArena m_heap;
        mem::String m_sql;
        std::size_t m_prefix_length = 0;
        explicit BulkRequest(int node)
            : m_heap(mem::global_root(), kBulkHeapBytes, "bulk-request", node),
              m_sql(mem::Allocator<char>(&m_heap)) {
            m_sql.reserve(kBulkFlushBytes + 64 * 1024);
        }
        [[nodiscard]] bool has_rows() const noexcept { return m_sql.size() > m_prefix_length; }
        void restart() { m_sql.resize(m_prefix_length); }
    };
    std::optional<BulkRequest> m_bulk;

    LuaConnection(mem::MemoryResource& owner, LuaVm* context)
        : m_resource(&owner), m_vm(context) {}

    /* The connection's single current result, as in stock sysbench: one
    LuaResult and one result userdata, created on the first query and reused by
    every later one (valid until the connection's next query).  No allocation
    per query. */
    LuaResult* m_result = nullptr;
    void** m_result_box = nullptr;
    int m_result_ref = LUA_NOREF;

    ~LuaConnection();

    void clear_error() noexcept {
        m_error_kind = 0;
        m_error_code = 0;
        copy_text(m_error_state, sizeof(m_error_state), "HY000");
        m_error_message[0] = '\0';
    }

    void set_error(const db::Error& error, bool ignorable) noexcept {
        m_error_kind = ignorable ? 1 : 2;
        m_error_code = error.code();
        copy_text(m_error_state, sizeof(m_error_state), error.state());
        copy_text(m_error_message, sizeof(m_error_message), error.message());
    }
};

struct LuaStatement {
    db::PreparedStatement* m_statement = nullptr;
    LuaConnection* m_connection = nullptr;
    mem::MemoryResource* m_resource = nullptr;
    LuaParam* m_parameters[64]{};
    std::size_t m_parameter_count = 0;
    bool m_closed = false;

    ~LuaStatement() {
        if (m_statement != nullptr) {
            m_statement->destroy(*m_resource);
            m_statement = nullptr;
        }
    }
};

// The statement's bound Lua parameters as driver values.
void bind_values(const LuaStatement& statement, db::BindValue* values) noexcept {
    for (std::size_t i = 0; i < statement.m_parameter_count; ++i) {
        const LuaParam* parameter = statement.m_parameters[i];
        if (parameter == nullptr || parameter->m_is_null) {
            values[i] = db::BindValue::null();
        } else if (parameter->m_kind == LuaParam::Kind::Integer) {
            values[i] = db::BindValue::from_int(parameter->m_integer);
        } else if (parameter->m_kind == LuaParam::Kind::Real) {
            values[i] = db::BindValue::from_double(parameter->m_real);
        } else {
            values[i] = db::BindValue::from_bytes(
                std::string_view(parameter->m_buffer, parameter->m_length));
        }
    }
}

// Result storage lives in the VM's per-event bump arena.  Its destructor is
// deliberately never run: Lua's GC is asynchronous while the arena is rewound
// at the next event.  Results therefore have event lifetime, matching the way
// sysbench scripts consume them.
struct LuaResult {
    db::ResultSet m_result;
    std::size_t m_row_index = 0;
    bool m_freed = false;

    explicit LuaResult(db::ResultSet&& value) : m_result(std::move(value)) {}
    ~LuaResult() = default;
};

LuaConnection::~LuaConnection() {
    if (m_connection != nullptr) {
        m_connection->close();
        m_connection->destroy(*m_resource);
        m_connection = nullptr;
    }
    /* The result userdata may outlive the connection (a script can hold it):
    detach it.  Its memory stays valid until every finalizer has run. */
    if (m_result_box != nullptr) *m_result_box = nullptr;
    if (m_result != nullptr) mem::destroy(*m_resource, m_result);
}

void push_handle_box(lua_State* L, const char* meta, void* object);

// Make `set` the connection's current result and push its (reused) userdata.
void push_result(lua_State* L, LuaConnection& connection, db::ResultSet&& set) {
    if (connection.m_result == nullptr) {
        connection.m_result = mem::construct<LuaResult>(*connection.m_resource, std::move(set));
    } else {
        std::destroy_at(connection.m_result);
        std::construct_at(connection.m_result, std::move(set));
    }
    if (connection.m_result_ref == LUA_NOREF) {
        push_handle_box(L, "sysbench.result.handle", connection.m_result);
        connection.m_result_box = static_cast<void**>(lua_touserdata(L, -1));
        lua_pushvalue(L, -1);
        connection.m_result_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    } else {
        lua_rawgeti(L, LUA_REGISTRYINDEX, connection.m_result_ref);
    }
}

struct LuaHistogram {
    LatencyHistogram m_histogram;
    mem::MemoryResource* m_resource = nullptr;
    explicit LuaHistogram(mem::MemoryResource& owner) : m_resource(&owner) {}
};

// ---------------------------------------------------------------------------
// Lua VM
// ---------------------------------------------------------------------------

class LuaVm {
public:
    /* Metatables of the sysbench handle types, cached on first use so that
    checking a handle compares two pointers instead of looking its type name up
    in the registry (which interns the name string) on every call. */
    [[nodiscard]] const void* metatable_for(lua_State* L, const char* meta) {
        for (std::size_t i = 0; i < kMetaSlots; ++i) {
            if (m_meta_names[i] == meta) return m_meta_tables[i];
            if (m_meta_names[i] == nullptr) {
                luaL_getmetatable(L, meta);
                m_meta_tables[i] = lua_topointer(L, -1);
                lua_pop(L, 1);
                m_meta_names[i] = meta;
                return m_meta_tables[i];
            }
        }
        return nullptr;
    }

    LuaVm(mem::MemoryResource& resource, Options& options,
          const LuaRunOptions& run_options, std::string_view script_path,
          std::string_view command, int argc, char** argv, std::uint32_t tid,
          ThreadStats* stats, RunControl* control);
    ~LuaVm();

    LuaVm(const LuaVm&) = delete;
    LuaVm& operator=(const LuaVm&) = delete;

    // Where the MySQL session is allocated (the connection heap).
    void set_session_resource(mem::MemoryResource& resource) noexcept {
        m_session_resource = &resource;
    }

    bool load(mem::String& error);
    bool install_options(mem::String& error);
    void install_tid();
    [[nodiscard]] bool has_global(std::string_view name);
    [[nodiscard]] bool command_parallel(std::string_view command);
    [[nodiscard]] bool has_hook(std::string_view hook);

    Task<bool> call_global(std::string_view name, bool pass_tid, std::uint32_t tid);
    Task<bool> dispatch_command(std::string_view command);
    Task<bool> call_entry(std::string_view name, bool pass_tid, std::uint32_t tid,
                          bool pass_text, std::string_view text);
    bool call_hook(std::string_view hook, const StatsSnapshot& snapshot,
                   double interval, double total);

    [[nodiscard]] lua_State* state() const noexcept { return m_state; }
    [[nodiscard]] mem::MemoryResource& resource() noexcept { return *m_resource; }
    [[nodiscard]] const LuaRunOptions& run_options() const noexcept { return m_run_options; }
    [[nodiscard]] Options& options() noexcept { return *m_options; }
    [[nodiscard]] ThreadStats* stats() const noexcept { return m_stats; }
    [[nodiscard]] RunControl* control() const noexcept { return m_control; }
    [[nodiscard]] std::uint32_t tid() const noexcept { return m_tid; }
    [[nodiscard]] std::string_view script_path() const noexcept { return m_script_path; }

    [[nodiscard]] bool stats_enabled() const noexcept {
        return m_control == nullptr || m_control->stats_enabled(Clock::now());
    }
    [[nodiscard]] bool is_ignorable(int code) const noexcept;
    [[nodiscard]] mem::MemoryResource& result_resource() {
        return m_event_scope;
    }
    // RNG API
    [[nodiscard]] Rng& rng() noexcept { return m_rng; }
    void fill_random_string(std::string_view format, char* output);
    std::size_t fill_random_varstring(std::size_t min_length,
                                      std::size_t max_length, char* output);

    // Async bridge
    Pending m_pending;
    mem::String m_sql_buffer;
    db::Error m_last_error;
    bool m_has_last_error = false;

    Task<int> perform_pending(lua_State* coroutine);
    // Read pipelined replies until at most `keep` are outstanding.  False
    // (connection error set) when one fails.
    Task<bool> read_replies(LuaConnection* connection, std::uint32_t keep);

    bool tick_more_events() noexcept {
        ++m_more_events_calls;
        return m_control != nullptr && m_control->m_fair_yield &&
               (m_more_events_calls % 128u) == 0;
    }
    void mark_event_start(Clock::time_point now) {
        m_event_scope.reset();
        m_event_start = now;
        m_record_event = m_control == nullptr || m_control->stats_enabled(now);
        if (m_control != nullptr) {
            ++m_local_events;
            if (m_control->m_event_limit != 0) {
                m_control->m_events.add(m_tid, 1);
            }
        }
    }
    [[nodiscard]] bool recording_event() const noexcept { return m_record_event; }
    [[nodiscard]] std::uint64_t local_event_count() const noexcept {
        return m_local_events;
    }

    [[nodiscard]] bool more_events() noexcept {
        if (m_control == nullptr) return false;
        if (m_control->m_stop.load(std::memory_order_acquire) ||
            g_interrupted.load(std::memory_order_acquire)) {
            return false;
        }
        if ((m_local_events & 63u) == 0 && Clock::now() >= m_control->m_end) {
            return false;
        }
        if (m_control->m_event_limit != 0) {
            if ((m_local_events % m_event_check_batch) == 0) {
                m_cached_event_total =
                    m_control->m_events.sum(m_run_options.m_threads);
            }
            if (m_cached_event_total >= m_control->m_event_limit) return false;
        }
        return true;
    }

    [[nodiscard]] std::uint64_t rate_wait_ns(Clock::time_point now,
                                             Clock::time_point start) noexcept {
        if (m_rate_interval_ns <= 0) return 0;
        m_rate_slot_ns += m_rate_interval_ns;
        const std::int64_t elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - start).count();
        return m_rate_slot_ns > elapsed
                   ? static_cast<std::uint64_t>(m_rate_slot_ns - elapsed)
                   : 0;
    }

    void set_last_error(const db::Error& error) {
        m_last_error = error;
        m_has_last_error = true;
    }

private:
    friend struct LuaApi;

    bool create_state(mem::String& error);
    bool register_api(mem::String& error);
    bool load_module(std::string_view name, mem::String& error);
    bool load_script(mem::String& error);
    bool setup_base();
    bool push_options_table();

    mem::MemoryResource* m_resource;
    Options* m_options;
    mem::BumpArena m_event_arena;
    mem::MemoryResource* m_session_resource = nullptr;
    mem::ScopedAllocator m_event_scope;
    LuaRunOptions m_run_options;
    mem::String m_script_path;
    mem::String m_command;
    int m_argc;
    char** m_argv;
    std::uint32_t m_tid;
    ThreadStats* m_stats;
    RunControl* m_control;
    lua_State* m_state = nullptr;
    Rng m_rng;
    unsigned m_more_events_calls = 0;
    Clock::time_point m_event_start{};
    bool m_record_event = false;
    std::uint64_t m_local_events = 0;
    std::uint64_t m_cached_event_total = 0;
    std::uint64_t m_event_check_batch = 1;
    std::int64_t m_rate_slot_ns = 0;
    std::int64_t m_rate_interval_ns = 0;
    static constexpr std::size_t kMetaSlots = 8;
    const char* m_meta_names[kMetaSlots]{};
    const void* m_meta_tables[kMetaSlots]{};
};

// ---------------------------------------------------------------------------
// Lua allocator + userdata helpers
// ---------------------------------------------------------------------------

void* lua_allocate(void* user, void* pointer, std::size_t old_size,
                   std::size_t new_size) noexcept {
    auto* resource = static_cast<mem::MemoryResource*>(user);
    try {
        if (new_size == 0) {
            if (pointer != nullptr) resource->deallocate(pointer, old_size, 16);
            return nullptr;
        }
        if (pointer == nullptr) return resource->allocate(new_size, 16);
        void* replacement = resource->allocate(new_size, 16);
        std::memcpy(replacement, pointer, std::min(old_size, new_size));
        resource->deallocate(pointer, old_size, 16);
        return replacement;
    } catch (...) {
        return nullptr;
    }
}

/* LuaJIT (Lua 5.1 API) helpers. */

// A number with an integral value (LuaJIT numbers are doubles).
[[nodiscard]] bool is_integral(lua_State* L, int index) noexcept {
    if (lua_type(L, index) != LUA_TNUMBER) return false;
    const lua_Number value = lua_tonumber(L, index);
    return value == static_cast<lua_Number>(static_cast<lua_Integer>(value));
}

// luaL_tolstring for Lua 5.1: push tostring(value at index) and return it.
const char* to_display_string(lua_State* L, int index) {
    index = index < 0 && index > LUA_REGISTRYINDEX ? lua_gettop(L) + index + 1 : index;
    lua_getglobal(L, "tostring");
    lua_pushvalue(L, index);
    if (lua_pcall(L, 1, 1, 0) != 0) return lua_tostring(L, -1);
    return lua_tostring(L, -1);
}

constexpr const char* kDriverMeta = "sysbench.driver.handle";
constexpr const char* kConnectionMeta = "sysbench.connection.handle";
constexpr const char* kStatementMeta = "sysbench.statement.handle";
constexpr const char* kParamMeta = "sysbench.param.handle";
constexpr const char* kResultMeta = "sysbench.result.handle";
constexpr const char* kHistogramMeta = "sysbench.histogram.handle";

LuaVm* current_vm(lua_State* L) {
    if (tls_vm == nullptr) {
        luaL_error(L, "sysbench runtime context is unavailable");
    }
    return tls_vm;
}

template <class T>
T* check_handle(lua_State* L, int index, const char* meta) {
    /* Fast path: compare the userdata's metatable with this VM's cached one.
    Anything else (wrong type, another VM) takes luaL_checkudata's path, which
    also produces its error message. */
    if (LuaVm* vm = tls_vm; vm != nullptr && lua_type(L, index) == LUA_TUSERDATA &&
                            lua_getmetatable(L, index)) {
        const void* table = lua_topointer(L, -1);
        lua_pop(L, 1);
        if (table != nullptr && table == vm->metatable_for(L, meta)) {
            void** box = static_cast<void**>(lua_touserdata(L, index));
            if (*box != nullptr) return static_cast<T*>(*box);
        }
    }
    void** box = static_cast<void**>(luaL_checkudata(L, index, meta));
    if (box == nullptr || *box == nullptr) {
        luaL_error(L, "invalid sysbench handle");
    }
    return static_cast<T*>(*box);
}

void push_handle_box(lua_State* L, const char* meta, void* object) {
    void** box = static_cast<void**>(lua_newuserdata(L, sizeof(void*)));
    *box = object;
    luaL_getmetatable(L, meta);
    lua_setmetatable(L, -2);
}

template <class T>
void push_handle(lua_State* L, const char* meta, T* object) {
    void** box = static_cast<void**>(lua_newuserdata(L, sizeof(void*)));
    *box = object;
    luaL_getmetatable(L, meta);
    lua_setmetatable(L, -2);
}

template <class T, class... Args>
T* make_handle(lua_State* L, const char* meta, mem::MemoryResource& resource,
               Args&&... args) {
    T* object = mem::construct<T>(resource, std::forward<Args>(args)...);
    push_handle(L, meta, object);
    return object;
}

bool read_file(const char* path, mem::MemoryResource& resource,
               mem::Vector<char>& contents, mem::String& error) {
    const int fd = ::open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        error = mem::String{mem::Allocator<char>(&resource)};
        error.assign("cannot open '");
        error.append(path);
        error.append("': ");
        error.append(std::strerror(errno));
        return false;
    }
    struct stat status{};
    if (::fstat(fd, &status) != 0 || status.st_size < 0) {
        ::close(fd);
        error = mem::String{mem::Allocator<char>(&resource)};
        error.assign("cannot stat '");
        error.append(path);
        error.push_back('\'');
        return false;
    }
    contents.clear();
    contents.resize(static_cast<std::size_t>(status.st_size));
    std::size_t offset = 0;
    while (offset < contents.size()) {
        const ssize_t got = ::read(fd, contents.data() + offset, contents.size() - offset);
        if (got <= 0) {
            ::close(fd);
            error = mem::String{mem::Allocator<char>(&resource)};
            error.assign("cannot read '");
            error.append(path);
            error.push_back('\'');
            return false;
        }
        offset += static_cast<std::size_t>(got);
    }
    ::close(fd);
    return true;
}

std::string_view lua_directory() {
    const char* environment = std::getenv("SYSBENCH_LUA_DIR");
    if (environment != nullptr && environment[0] != '\0') return environment;
    if (::access(SYSBENCH_LUA_DIR "/sysbench.lua", R_OK) == 0) {
        return SYSBENCH_LUA_DIR;
    }
    if (::access(SYSBENCH_INSTALL_LUA_DIR "/sysbench.lua", R_OK) == 0) {
        return SYSBENCH_INSTALL_LUA_DIR;
    }
    return SYSBENCH_LUA_DIR;
}

std::string_view directory_of(std::string_view path) {
    const auto slash = path.find_last_of('/');
    if (slash == std::string_view::npos) return ".";
    return path.substr(0, slash == 0 ? 1 : slash);
}

}  // namespace

void request_lua_stop() noexcept {
    g_interrupted.store(true, std::memory_order_release);
}

namespace {

// ---------------------------------------------------------------------------
// LuaVm implementation
// ---------------------------------------------------------------------------

LuaVm::LuaVm(mem::MemoryResource& resource, Options& options,
             const LuaRunOptions& run_options, std::string_view script_path,
             std::string_view command, int argc, char** argv, std::uint32_t tid,
             ThreadStats* stats, RunControl* control)
    : m_sql_buffer(mem::Allocator<char>(&resource)),
      m_last_error(0, "", ""),
      m_resource(&resource),
      m_options(&options),
      m_event_arena(resource, 256 * 1024, "lua-event"),
      m_event_scope(m_event_arena),
      m_run_options(run_options),
      m_script_path(mem::Allocator<char>(&resource)),
      m_command(mem::Allocator<char>(&resource)),
      m_argc(argc),
      m_argv(argv),
      m_tid(tid),
      m_stats(stats),
      m_control(control),
      m_rng(run_options.m_seed == 0
               ? 0
               : run_options.m_seed + static_cast<std::uint64_t>(tid + 1) *
                                        0x9e3779b97f4a7c15ull) {
    m_script_path.assign(script_path);
    m_command.assign(command);
    if (m_run_options.m_threads > 1) {
        const std::uint64_t per_thread =
            std::max<std::uint64_t>(1, m_run_options.m_events / m_run_options.m_threads);
        m_event_check_batch = std::clamp<std::uint64_t>(per_thread / 8, 1, 64);
    } else {
        m_event_check_batch = 1;
    }
    if (m_control != nullptr && m_run_options.m_rate > 0) {
        const std::uint64_t threads = std::max<std::uint32_t>(1, m_run_options.m_threads);
        m_rate_interval_ns = static_cast<std::int64_t>(
            (1'000'000'000ull * threads) / std::max<std::uint64_t>(1, m_run_options.m_rate));
    }
}

LuaVm::~LuaVm() {
    if (m_state != nullptr) {
        lua_close(m_state);
        m_state = nullptr;
    }
}

bool LuaVm::create_state(mem::String& error) {
    m_state = lua_newstate(&lua_allocate, m_resource);
    if (m_state == nullptr) {
        error = mem::String{mem::Allocator<char>(m_resource)};
        error.assign("failed to allocate Lua state");
        return false;
    }
    luaL_openlibs(m_state);
    if (!register_api(error)) return false;
    if (!setup_base()) {
        error = mem::String{mem::Allocator<char>(m_resource)};
        error.assign("failed to construct sysbench Lua environment");
        return false;
    }

    // package.path: bundled modules, script directory, then Lua defaults.
    lua_getglobal(m_state, "package");
    lua_getfield(m_state, -1, "path");
    std::string_view existing;
    if (lua_isstring(m_state, -1)) existing = lua_tostring(m_state, -1);
    mem::String path{mem::Allocator<char>(m_resource)};
    auto append_module = [&](std::string_view directory, std::string_view pattern) {
        if (!path.empty()) path.push_back(';');
        path.append(directory);
        path.append(pattern);
    };
    append_module(lua_directory(), "/?.lua");
    append_module(lua_directory(), "/?/init.lua");
    const std::string_view script_directory = directory_of(m_script_path);
    append_module(script_directory, "/?.lua");
    append_module(script_directory, "/?/init.lua");
    append_module(".", "/?.lua");
    append_module(".", "/?/init.lua");
    if (!existing.empty()) {
        path.push_back(';');
        path.append(existing);
    }
    lua_pop(m_state, 1);
    lua_pushlstring(m_state, path.data(), path.size());
    lua_setfield(m_state, -2, "path");
    lua_pop(m_state, 1);
    return true;
}

bool LuaVm::load(mem::String& error) {
    if (!create_state(error)) return false;
    if (!install_options(error)) return false;

    const char* modules[] = {"sysbench.rand.lua", "sysbench.lua",
                             "sysbench.cmdline.lua", "sysbench.sql.lua",
                             "sysbench.histogram.lua"};
    for (const char* module : modules) {
        if (!load_module(module, error)) return false;
    }
    if (!load_script(error)) return false;
    return true;
}

bool LuaVm::load_module(std::string_view name, mem::String& error) {
    mem::String path{mem::Allocator<char>(m_resource)};
    path.assign(lua_directory());
    path.push_back('/');
    path.append(name);
    mem::Vector<char> contents{mem::Allocator<char>(m_resource)};
    if (!read_file(path.c_str(), *m_resource, contents, error)) {
        mem::String prefixed{mem::Allocator<char>(m_resource)};
        prefixed.assign("internal Lua module: ");
        prefixed.append(error);
        error = std::move(prefixed);
        return false;
    }
    if (luaL_loadbufferx(m_state, contents.data(), contents.size(), path.c_str(),
                         "t") != LUA_OK) {
        error = mem::String{mem::Allocator<char>(m_resource)};
        error.assign("failed to load internal module '");
        error.append(name);
        error.append("': ");
        if (const char* message = lua_tostring(m_state, -1)) error.append(message);
        lua_pop(m_state, 1);
        return false;
    }
    if (lua_pcall(m_state, 0, 0, 0) != LUA_OK) {
        error = mem::String{mem::Allocator<char>(m_resource)};
        error.assign("failed to execute internal module '");
        error.append(name);
        error.append("': ");
        if (const char* message = lua_tostring(m_state, -1)) error.append(message);
        lua_pop(m_state, 1);
        return false;
    }
    return true;
}

bool LuaVm::load_script(mem::String& error) {
    mem::Vector<char> contents{mem::Allocator<char>(m_resource)};
    if (!read_file(m_script_path.c_str(), *m_resource, contents, error)) return false;
    char* source = contents.data();
    std::size_t source_size = contents.size();
    if (source_size > 0 && source[0] == '#') {
        const void* newline = std::memchr(source, '\n', source_size);
        if (newline != nullptr) {
            const std::size_t skip =
                static_cast<std::size_t>(static_cast<const char*>(newline) - source) + 1;
            source += skip;
            source_size -= skip;
        }
    }
    if (luaL_loadbufferx(m_state, source, source_size,
                         m_script_path.c_str(), "t") != LUA_OK) {
        error = mem::String{mem::Allocator<char>(m_resource)};
        error.assign("failed to load benchmark '");
        error.append(m_script_path);
        error.append("': ");
        if (const char* message = lua_tostring(m_state, -1)) error.append(message);
        lua_pop(m_state, 1);
        return false;
    }
    if (lua_pcall(m_state, 0, 0, 0) != LUA_OK) {
        error = mem::String{mem::Allocator<char>(m_resource)};
        error.assign("failed to initialize benchmark '");
        error.append(m_script_path);
        error.append("': ");
        if (const char* message = lua_tostring(m_state, -1)) error.append(message);
        lua_pop(m_state, 1);
        return false;
    }
    return true;
}

bool LuaVm::setup_base() {
    lua_newtable(m_state);  // sysbench

    lua_pushinteger(m_state, m_tid);
    lua_setfield(m_state, -2, "tid");

    lua_pushliteral(m_state, SYSBENCH_VERSION);
    lua_setfield(m_state, -2, "version");
    lua_pushliteral(m_state, "sysbench-cpp " SYSBENCH_VERSION);
    lua_setfield(m_state, -2, "version_string");

    lua_newtable(m_state);  // sysbench.error
    lua_pushinteger(m_state, 0);
    lua_setfield(m_state, -2, "NONE");
    lua_pushinteger(m_state, 1);
    lua_setfield(m_state, -2, "RESTART_EVENT");
    lua_setfield(m_state, -2, "error");

    lua_newtable(m_state);  // sysbench.hooks
    lua_setfield(m_state, -2, "hooks");

    lua_newtable(m_state);  // sysbench.opt
    lua_setfield(m_state, -2, "opt");

    lua_newtable(m_state);  // sysbench.cmdline
    lua_pushstring(m_state, m_command.c_str());
    lua_setfield(m_state, -2, "command");
    lua_pushstring(m_state, m_script_path.c_str());
    lua_setfield(m_state, -2, "script_path");

    lua_newtable(m_state);  // argv
    for (int i = 0; i < m_argc; ++i) {
        lua_pushstring(m_state, m_argv[i]);
        lua_rawseti(m_state, -2, i);
    }
    lua_setfield(m_state, -2, "argv");
    lua_setfield(m_state, -2, "cmdline");

    lua_setglobal(m_state, "sysbench");
    return true;
}

bool LuaVm::install_options(mem::String& error) {
    (void)error;
    // sysbench.opt
    lua_getglobal(m_state, "sysbench");
    if (!lua_istable(m_state, -1)) {
        lua_pop(m_state, 1);
        return false;
    }
    if (!push_options_table()) {
        lua_pop(m_state, 1);
        return false;
    }
    lua_setfield(m_state, -2, "opt");
    lua_pop(m_state, 1);
    return true;
}

bool LuaVm::push_options_table() {
    lua_newtable(m_state);
    for (std::size_t i = 0; i < m_options->size(); ++i) {
        const OptionDef* option = m_options->at(i);
        if (option == nullptr) continue;
        mem::String key{mem::Allocator<char>(m_resource)};
        key.assign(option->m_name);
        for (char& character : key) {
            if (character == '-') character = '_';
        }
        lua_pushlstring(m_state, key.data(), key.size());
        switch (option->m_type) {
            case OptionType::Bool:
                lua_pushboolean(m_state, m_options->as_bool(option->m_name));
                break;
            case OptionType::Int:
                lua_pushinteger(m_state, m_options->as_int(option->m_name));
                break;
            case OptionType::Size:
                lua_pushinteger(m_state, static_cast<lua_Integer>(
                                           m_options->as_size(option->m_name)));
                break;
            case OptionType::Double:
                lua_pushnumber(m_state, m_options->as_double(option->m_name));
                break;
            case OptionType::String:
                lua_pushlstring(m_state, option->m_value.data(), option->m_value.size());
                break;
            case OptionType::List: {
                lua_newtable(m_state);
                std::string_view value(option->m_value);
                int index = 1;
                while (!value.empty()) {
                    const auto comma = value.find(',');
                    const std::string_view item =
                        comma == std::string_view::npos ? value : value.substr(0, comma);
                    lua_pushlstring(m_state, item.data(), item.size());
                    lua_rawseti(m_state, -2, index++);
                    if (comma == std::string_view::npos) break;
                    value.remove_prefix(comma + 1);
                }
                break;
            }
        }
        lua_rawset(m_state, -3);
    }
    return true;
}

void LuaVm::install_tid() {
    lua_getglobal(m_state, "sysbench");
    lua_pushinteger(m_state, m_tid);
    lua_setfield(m_state, -2, "tid");
    lua_pop(m_state, 1);
}

bool LuaVm::has_global(std::string_view name) {
    mem::String key{mem::Allocator<char>(m_resource)};
    key.assign(name);
    lua_getglobal(m_state, key.c_str());
    const bool result = lua_isfunction(m_state, -1);
    lua_pop(m_state, 1);
    return result;
}


bool LuaVm::command_parallel(std::string_view command) {
    lua_getglobal(m_state, "sysbench");
    lua_getfield(m_state, -1, "cmdline");
    lua_getfield(m_state, -1, "command_parallel");
    if (!lua_isfunction(m_state, -1)) {
        lua_pop(m_state, 3);
        return false;
    }
    mem::String name{mem::Allocator<char>(m_resource)};
    name.assign(command);
    lua_pushlstring(m_state, name.data(), name.size());
    const bool ok = lua_pcall(m_state, 1, 1, 0) == LUA_OK;
    const bool result = ok && lua_toboolean(m_state, -1) != 0;
    lua_pop(m_state, 3);
    return result;
}

bool LuaVm::is_ignorable(int code) const noexcept {
    std::string_view list = m_options->as_string("mysql-ignore-errors", "");
    while (!list.empty()) {
        const auto comma = list.find(',');
        const std::string_view item =
            comma == std::string_view::npos ? list : list.substr(0, comma);
        int value = 0;
        const auto parsed = std::from_chars(item.data(), item.data() + item.size(), value);
        if (parsed.ec == std::errc{} && value == code) return true;
        if (comma == std::string_view::npos) break;
        list.remove_prefix(comma + 1);
    }
    return false;
}


void LuaVm::fill_random_string(std::string_view format, char* output) {
    for (std::size_t i = 0; i < format.size(); ++i) {
        const char character = format[i];
        if (character == '#') {
            output[i] = static_cast<char>('0' + m_rng.uniform(0, 9));
        } else if (character == '@') {
            output[i] = static_cast<char>('a' + m_rng.uniform(0, 25));
        } else {
            output[i] = character;
        }
    }
}

std::size_t LuaVm::fill_random_varstring(std::size_t min_length,
                                         std::size_t max_length, char* output) {
    if (max_length == 0) return 0;
    if (min_length > max_length) min_length = 1;
    const std::size_t length = static_cast<std::size_t>(
        m_rng.uniform(min_length, max_length));
    for (std::size_t i = 0; i < length; ++i) {
        output[i] = static_cast<char>(m_rng.uniform(static_cast<unsigned>('0'),
                                                   static_cast<unsigned>('z')));
    }
    return length;
}

}  // namespace
}  // namespace sb

namespace sb {
namespace {

// ---------------------------------------------------------------------------
// C API helpers
// ---------------------------------------------------------------------------

template <class Function>
int protected_call(lua_State* L, Function&& function) {
    SB_LAYER(LuaBinding);
    try {
        return function();
    } catch (const std::exception& error) {
        return luaL_error(L, "%s", error.what());
    } catch (...) {
        return luaL_error(L, "unknown sysbench Lua API error");
    }
}

int l_rand_uniform_uint64(lua_State* L) {
    return protected_call(L, [&] {
        lua_pushinteger(L, static_cast<lua_Integer>(current_vm(L)->rng().next()));
        return 1;
    });
}

int l_rand_default(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        const lua_Integer a = luaL_checkinteger(L, 1);
        const lua_Integer b = luaL_checkinteger(L, 2);
        const std::uint64_t low = static_cast<std::uint64_t>(std::min(a, b));
        const std::uint64_t high = static_cast<std::uint64_t>(std::max(a, b));
        const std::string_view type = vm->options().as_string("rand-type", "uniform");
        std::uint64_t value = 0;
        if (type == "gaussian") {
            const double mean = (static_cast<double>(low) + static_cast<double>(high)) / 2.0;
            const double deviation = (static_cast<double>(high) - static_cast<double>(low)) / 6.0;
            const double u1 = std::max(vm->rng().uniform_double(), 1e-15);
            const double u2 = vm->rng().uniform_double();
            const double z = std::sqrt(-2.0 * std::log(u1)) * std::cos(2 * M_PI * u2);
            const double sampled = std::clamp(mean + z * deviation,
                                              static_cast<double>(low),
                                              static_cast<double>(high));
            value = static_cast<std::uint64_t>(sampled);
        } else if (type == "pareto") {
            const double shape = vm->options().as_double("rand-pareto-h", 0.2);
            const double exponent = std::log(shape) / std::log(1.0 - shape);
            const double sampled = static_cast<double>(low) +
                                   (static_cast<double>(high - low + 1) *
                                    std::pow(vm->rng().uniform_double(), exponent));
            value = static_cast<std::uint64_t>(sampled);
        } else if (type == "zipfian") {
            const double u = vm->rng().uniform_double();
            value = low + static_cast<std::uint64_t>(
                              static_cast<double>(high - low + 1) * u * u);
        } else {
            value = vm->rng().uniform(low, high);
        }
        lua_pushinteger(L, static_cast<lua_Integer>(value));
        return 1;
    });
}

int l_rand_uniform(lua_State* L) { return l_rand_default(L); }

int l_rand_gaussian(lua_State* L) {
    return protected_call(L, [&] {
        const double a = static_cast<double>(luaL_checkinteger(L, 1));
        const double b = static_cast<double>(luaL_checkinteger(L, 2));
        const double u1 = std::max(current_vm(L)->rng().uniform_double(), 1e-15);
        const double u2 = current_vm(L)->rng().uniform_double();
        const double z = std::sqrt(-2.0 * std::log(u1)) * std::cos(2 * M_PI * u2);
        const double mean = (a + b) / 2.0;
        const double deviation = (b - a) / 6.0;
        const double value = std::clamp(mean + z * deviation, a, b);
        lua_pushinteger(L, static_cast<lua_Integer>(value));
        return 1;
    });
}

int l_rand_pareto(lua_State* L) {
    return protected_call(L, [&] {
        const auto a = static_cast<double>(luaL_checkinteger(L, 1));
        const auto b = static_cast<double>(luaL_checkinteger(L, 2));
        const double value =
            a + (b - a + 1) * std::pow(current_vm(L)->rng().uniform_double(), 1.5);
        lua_pushinteger(L, static_cast<lua_Integer>(value));
        return 1;
    });
}

int l_rand_zipfian(lua_State* L) {
    return protected_call(L, [&] {
        const auto a = static_cast<double>(luaL_checkinteger(L, 1));
        const auto b = static_cast<double>(luaL_checkinteger(L, 2));
        const double u = current_vm(L)->rng().uniform_double();
        lua_pushinteger(L, static_cast<lua_Integer>(a + (b - a + 1) * u * u));
        return 1;
    });
}

int l_rand_unique(lua_State* L) {
    return protected_call(L, [&] {
        lua_pushinteger(L, current_vm(L)->rng().unique());
        return 1;
    });
}

int l_rand_str(lua_State* L) {
    return protected_call(L, [&] {
        std::size_t length = 0;
        const char* format = luaL_checklstring(L, 1, &length);
        LuaVm* vm = current_vm(L);
        std::array<char, 512> local{};
        char* output = length <= local.size()
                           ? local.data()
                           : static_cast<char*>(vm->result_resource().allocate(length, 1));
        vm->fill_random_string({format, length}, output);
        lua_pushlstring(L, output, length);
        return 1;
    });
}

int l_rand_varstr(lua_State* L) {
    return protected_call(L, [&] {
        const auto minimum = static_cast<std::size_t>(luaL_checkinteger(L, 1));
        const auto maximum = static_cast<std::size_t>(luaL_checkinteger(L, 2));
        LuaVm* vm = current_vm(L);
        std::array<char, 512> local{};
        char* output = maximum <= local.size()
                           ? local.data()
                           : static_cast<char*>(vm->result_resource().allocate(maximum, 1));
        const std::size_t length =
            vm->fill_random_varstring(minimum, maximum, output);
        lua_pushlstring(L, output, length);
        return 1;
    });
}

int l_rand_uniform_double(lua_State* L) {
    return protected_call(L, [&] {
        lua_pushnumber(L, current_vm(L)->rng().uniform_double());
        return 1;
    });
}

int l_more_events(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        RunControl* control = vm->control();
        if (control == nullptr) {
            lua_pushboolean(L, 0);
            return 1;
        }
        const bool more = vm->more_events();
        if (more && vm->tick_more_events()) {
            vm->m_pending.m_kind = PendingKind::MoreEventsYield;
            vm->m_pending.m_bool_result = true;
            return lua_yield(L, 0);
        }
        lua_pushboolean(L, more);
        return 1;
    });
}

int l_event_start(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        RunControl* control = vm->control();
        if (control == nullptr) return luaL_error(L, "event_start without a benchmark runtime");
        const auto now = Clock::now();
        vm->mark_event_start(now);
        if (vm->recording_event() && vm->stats() != nullptr) {
            vm->stats()->begin_event(now);
        }
        return 0;
    });
}

int l_event_stop(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        RunControl* control = vm->control();
        if (control == nullptr) return luaL_error(L, "event_stop without a benchmark runtime");
        const auto now = Clock::now();
        if (vm->recording_event() && vm->stats() != nullptr) {
            vm->stats()->end_event(now, false);
        }
        const std::uint64_t wait_ns = vm->rate_wait_ns(now, control->m_start);
        if (wait_ns > 0) {
            vm->m_pending.m_kind = PendingKind::Sleep;
            vm->m_pending.m_sleep_ns = wait_ns;
            return lua_yield(L, 0);
        }
        return 0;
    });
}

int l_sql_driver(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        std::string_view requested;
        if (!lua_isnoneornil(L, 1)) {
            std::size_t length = 0;
            const char* value = luaL_checklstring(L, 1, &length);
            requested = {value, length};
        } else {
            requested = vm->options().as_string("db-driver", "mysql");
        }
        mem::String name{mem::Allocator<char>(&vm->resource())};
        name.assign(requested);
        db::Driver* driver =
            db::DriverRegistry::instance().create(name, vm->resource());
        if (driver == nullptr) {
            lua_pushnil(L);
            return 1;
        }
        auto* handle = mem::construct<LuaDriverHandle>(
            vm->resource(), driver, &vm->resource());
        push_handle(L, kDriverMeta, handle);
        return 1;
    });
}

int l_sql_driver_name(lua_State* L) {
    return protected_call(L, [&] {
        auto* handle = check_handle<LuaDriverHandle>(L, 1, kDriverMeta);
        const auto name = handle->m_driver->name();
        lua_pushlstring(L, name.data(), name.size());
        return 1;
    });
}

int l_sql_connect(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        auto* driver = check_handle<LuaDriverHandle>(L, 1, kDriverMeta);
        vm->m_pending = Pending{};
        vm->m_pending.m_kind = PendingKind::Connect;
        vm->m_pending.m_driver = driver;
        return lua_yield(L, 0);
    });
}

int l_sql_disconnect(lua_State* L) {
    return protected_call(L, [&] {
        auto* connection = check_handle<LuaConnection>(L, 1, kConnectionMeta);
        if (connection->m_connection != nullptr) {
            connection->m_connection->close();
            connection->m_connection->destroy(*connection->m_resource);
            connection->m_connection = nullptr;
        }
        connection->m_closed = true;
        lua_pushboolean(L, 1);
        return 1;
    });
}

int l_sql_reconnect(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        auto* connection = check_handle<LuaConnection>(L, 1, kConnectionMeta);
        vm->m_pending = Pending{};
        vm->m_pending.m_kind = PendingKind::Reconnect;
        vm->m_pending.m_connection = connection;
        return lua_yield(L, 0);
    });
}

int l_sql_query_stream(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        auto* connection = check_handle<LuaConnection>(L, 1, kConnectionMeta);
        std::size_t length = 0;
        const char* query = luaL_checklstring(L, 2, &length);
        const auto depth = static_cast<std::uint32_t>(
            std::max<lua_Integer>(1, luaL_checkinteger(L, 3)));
        const auto total = static_cast<std::uint64_t>(
            std::max<lua_Integer>(0, luaL_checkinteger(L, 4)));
        if (connection->m_connection == nullptr || connection->m_closed) {
            connection->set_error(db::Error("MySQL connection is closed", 2006), false);
            lua_pushboolean(L, 0);
            return 1;
        }
        vm->m_sql_buffer.assign(query, length);
        if (vm->stats() != nullptr && vm->stats_enabled()) {
            const auto kind = classify_query(vm->m_sql_buffer);
            for (std::uint64_t i = 0; i < total; ++i) vm->stats()->record_query(kind);
        }
        vm->m_pending = Pending{};
        vm->m_pending.m_kind = PendingKind::QueryStream;
        vm->m_pending.m_connection = connection;
        vm->m_pending.m_stream_depth = depth;
        vm->m_pending.m_stream_total = total;
        return lua_yield(L, 0);
    });
}

// con:pipeline(depth): keep up to `depth` commands in flight (1 or 0: off).
int l_sql_pipeline(lua_State* L) {
    return protected_call(L, [&] {
        auto* connection = check_handle<LuaConnection>(L, 1, kConnectionMeta);
        connection->m_pipeline_depth = static_cast<std::uint32_t>(
            std::clamp<lua_Integer>(luaL_checkinteger(L, 2), 0, 256));
        lua_pushboolean(L, 1);
        return 1;
    });
}

// con:flush(): read every outstanding pipelined reply.
int l_sql_flush(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        auto* connection = check_handle<LuaConnection>(L, 1, kConnectionMeta);
        if (connection->m_connection == nullptr || connection->m_closed ||
            connection->m_connection->pending_replies() == 0) {
            lua_pushboolean(L, 1);
            return 1;
        }
        vm->m_pending = Pending{};
        vm->m_pending.m_kind = PendingKind::Flush;
        vm->m_pending.m_connection = connection;
        return lua_yield(L, 0);
    });
}

int l_sql_query(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        auto* connection = check_handle<LuaConnection>(L, 1, kConnectionMeta);
        std::size_t length = 0;
        const char* query = luaL_checklstring(L, 2, &length);
        if (connection->m_connection == nullptr || connection->m_closed) {
            connection->set_error(db::Error("MySQL connection is closed", 2006), false);
            lua_pushnil(L);
            return 1;
        }
        vm->m_sql_buffer.assign(query, length);
        SB_COUNT(Queries, 1);
        if (vm->stats() != nullptr && vm->stats_enabled()) {
            vm->stats()->record_query(classify_query(vm->m_sql_buffer));
        }
        vm->m_pending = Pending{};
        vm->m_pending.m_kind =
            connection->m_pipeline_depth > 1 ? PendingKind::QuerySend : PendingKind::Query;
        vm->m_pending.m_connection = connection;
        return lua_yield(L, 0);
    });
}

int l_sql_conn_error(lua_State* L) {
    return protected_call(L, [&] {
        auto* connection = check_handle<LuaConnection>(L, 1, kConnectionMeta);
        lua_pushinteger(L, connection->m_error_kind);
        lua_pushinteger(L, connection->m_error_code);
        lua_pushstring(L, connection->m_error_state);
        lua_pushstring(L, connection->m_error_message);
        return 4;
    });
}

int l_last_error(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        if (!vm->m_has_last_error) {
            lua_pushinteger(L, 0);
            lua_pushliteral(L, "");
            lua_pushliteral(L, "");
            return 3;
        }
        lua_pushinteger(L, vm->m_last_error.code());
        lua_pushstring(L, vm->m_last_error.state());
        lua_pushstring(L, vm->m_last_error.message());
        return 3;
    });
}

int l_sql_prepare(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        auto* connection = check_handle<LuaConnection>(L, 1, kConnectionMeta);
        std::size_t length = 0;
        const char* query = luaL_checklstring(L, 2, &length);
        if (connection->m_connection == nullptr || connection->m_closed) {
            connection->set_error(db::Error("MySQL connection is closed", 2006), false);
            lua_pushnil(L);
            return 1;
        }
        vm->m_sql_buffer.assign(query, length);
        vm->m_pending = Pending{};
        vm->m_pending.m_kind = PendingKind::Prepare;
        vm->m_pending.m_connection = connection;
        return lua_yield(L, 0);
    });
}

int l_sql_bulk_insert_init(lua_State* L) {
    return protected_call(L, [&] {
        auto* connection = check_handle<LuaConnection>(L, 1, kConnectionMeta);
        std::size_t length = 0;
        const char* prefix = luaL_checklstring(L, 2, &length);
        if (!connection->m_bulk) {
            Runtime& runtime = *Runtime::current();
            connection->m_bulk.emplace(
                runtime.worker_numa_node(Runtime::current_worker_id()));
        }
        auto& bulk = *connection->m_bulk;
        bulk.m_sql.assign(prefix, length);
        bulk.m_prefix_length = length;
        lua_pushboolean(L, 1);
        return 1;
    });
}

int l_sql_bulk_insert_next(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        auto* connection = check_handle<LuaConnection>(L, 1, kConnectionMeta);
        std::size_t length = 0;
        const char* row = luaL_checklstring(L, 2, &length);
        if (!connection->m_bulk) {
            return luaL_error(L, "bulk_insert_next() called before bulk_insert_init()");
        }
        auto& bulk = *connection->m_bulk;
        if (bulk.has_rows()) bulk.m_sql.push_back(',');
        bulk.m_sql.append(row, length);
        if (bulk.m_sql.size() >= LuaConnection::kBulkFlushBytes) {
            vm->m_pending = Pending{};
            vm->m_pending.m_kind = PendingKind::BulkInsert;
            vm->m_pending.m_connection = connection;
            return lua_yield(L, 0);
        }
        lua_pushboolean(L, 1);
        return 1;
    });
}

int l_sql_bulk_insert_done(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        auto* connection = check_handle<LuaConnection>(L, 1, kConnectionMeta);
        if (!connection->m_bulk || !connection->m_bulk->has_rows()) {
            lua_pushboolean(L, 1);
            return 1;
        }
        vm->m_pending = Pending{};
        vm->m_pending.m_kind = PendingKind::BulkInsert;
        vm->m_pending.m_connection = connection;
        return lua_yield(L, 0);
    });
}

int l_sql_bind_create(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        (void)check_handle<LuaStatement>(L, 1, kStatementMeta);
        const lua_Integer type = luaL_checkinteger(L, 2);
        const lua_Integer max_length = luaL_optinteger(L, 3, 0);
        LuaParam::Kind kind = LuaParam::Kind::Integer;
        if (type == 5 || type == 6) kind = LuaParam::Kind::Real;
        else if (type == 11 || type == 12) kind = LuaParam::Kind::Bytes;
        else if (type < 1 || type > 12) kind = LuaParam::Kind::Bytes;
        const std::size_t capacity =
            kind == LuaParam::Kind::Bytes
                ? static_cast<std::size_t>(std::max<lua_Integer>(max_length, 1))
                : 0;
        auto* parameter = mem::construct<LuaParam>(vm->resource(), vm->resource(),
                                                   kind, capacity);
        push_handle(L, kParamMeta, parameter);
        return 1;
    });
}

int l_sql_param_set(lua_State* L) {
    return protected_call(L, [&] {
        auto* parameter = check_handle<LuaParam>(L, 1, kParamMeta);
        if (lua_isnoneornil(L, 2)) {
            parameter->m_is_null = true;
            return 0;
        }
        parameter->m_is_null = false;
        switch (parameter->m_kind) {
            case LuaParam::Kind::Integer:
                parameter->m_integer = luaL_checkinteger(L, 2);
                break;
            case LuaParam::Kind::Real:
                parameter->m_real = luaL_checknumber(L, 2);
                break;
            case LuaParam::Kind::Bytes: {
                std::size_t length = 0;
                const char* value = luaL_checklstring(L, 2, &length);
                length = std::min(length, parameter->m_capacity);
                std::memcpy(parameter->m_buffer, value, length);
                parameter->m_length = length;
                break;
            }
        }
        return 0;
    });
}

int l_sql_param_set_rand_str(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        auto* parameter = check_handle<LuaParam>(L, 1, kParamMeta);
        std::size_t length = 0;
        const char* format = luaL_checklstring(L, 2, &length);
        if (parameter->m_kind != LuaParam::Kind::Bytes) {
            return luaL_error(L, "set_rand_str requires a string bind");
        }
        const std::size_t copied = std::min(length, parameter->m_capacity);
        vm->fill_random_string({format, copied}, parameter->m_buffer);
        parameter->m_length = copied;
        parameter->m_is_null = false;
        return 0;
    });
}

int l_sql_bind_param(lua_State* L) {
    return protected_call(L, [&] {
        auto* statement = check_handle<LuaStatement>(L, 1, kStatementMeta);
        const int count = lua_gettop(L) - 1;
        if (count < 0 || count > 64) return luaL_error(L, "too many statement parameters");
        statement->m_parameter_count = static_cast<std::size_t>(count);
        for (int i = 0; i < count; ++i) {
            statement->m_parameters[i] =
                check_handle<LuaParam>(L, i + 2, kParamMeta);
        }
        lua_pushboolean(L, 1);
        return 1;
    });
}

int l_sql_execute(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        auto* statement = check_handle<LuaStatement>(L, 1, kStatementMeta);
        if (statement->m_statement == nullptr || statement->m_closed) {
            lua_pushnil(L);
            return 1;
        }
        SB_COUNT(Queries, 1);
        if (vm->stats() != nullptr && vm->stats_enabled()) {
            vm->stats()->record_query(classify_query(statement->m_statement->sql()));
        }
        vm->m_pending = Pending{};
        vm->m_pending.m_kind = statement->m_connection->m_pipeline_depth > 1
                                   ? PendingKind::ExecuteSend
                                   : PendingKind::Execute;
        vm->m_pending.m_statement = statement;
        vm->m_pending.m_connection = statement->m_connection;
        return lua_yield(L, 0);
    });
}

int l_sql_stmt_close(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        auto* statement = check_handle<LuaStatement>(L, 1, kStatementMeta);
        if (statement->m_closed || statement->m_statement == nullptr) {
            lua_pushboolean(L, 1);
            return 1;
        }
        vm->m_pending = Pending{};
        vm->m_pending.m_kind = PendingKind::CloseStatement;
        vm->m_pending.m_statement = statement;
        return lua_yield(L, 0);
    });
}

int l_sql_result_info(lua_State* L) {
    return protected_call(L, [&] {
        auto* result = check_handle<LuaResult>(L, 1, kResultMeta);
        lua_pushinteger(L, static_cast<lua_Integer>(result->m_result.m_rows.size()));
        lua_pushinteger(L, static_cast<lua_Integer>(result->m_result.m_columns.size()));
        lua_pushinteger(L, static_cast<lua_Integer>(result->m_result.m_affected_rows));
        lua_pushinteger(L, static_cast<lua_Integer>(result->m_result.m_last_insert_id));
        lua_pushinteger(L, static_cast<lua_Integer>(result->m_result.m_warnings));
        lua_pushinteger(L, static_cast<lua_Integer>(result->m_result.m_status));
        return 6;
    });
}

int l_sql_result_fetch_row(lua_State* L) {
    return protected_call(L, [&] {
        auto* result = check_handle<LuaResult>(L, 1, kResultMeta);
        if (result->m_freed || result->m_row_index >= result->m_result.m_rows.size()) {
            lua_pushnil(L);
            return 1;
        }
        const db::Row& row = result->m_result.m_rows[result->m_row_index++];
        lua_createtable(L, static_cast<int>(row.m_fields.size()), 0);
        for (std::size_t i = 0; i < row.m_fields.size(); ++i) {
            const db::Field& field = row.m_fields[i];
            if (field.m_is_null) lua_pushnil(L);
            else lua_pushlstring(L, field.m_data, field.m_length);
            lua_rawseti(L, -2, static_cast<int>(i + 1));
        }
        return 1;
    });
}

int l_sql_result_free(lua_State* L) {
    return protected_call(L, [&] {
        auto* result = check_handle<LuaResult>(L, 1, kResultMeta);
        result->m_freed = true;
        return 0;
    });
}

int l_sql_more_results(lua_State*) { return 0; }
int l_sql_next_result(lua_State* L) {
    lua_pushnil(L);
    return 1;
}

int l_histogram_new(lua_State* L) {
    return protected_call(L, [&] {
        LuaVm* vm = current_vm(L);
        luaL_checkinteger(L, 1);
        luaL_checknumber(L, 2);
        luaL_checknumber(L, 3);
        auto* histogram = mem::construct<LuaHistogram>(vm->resource(), vm->resource());
        push_handle(L, kHistogramMeta, histogram);
        return 1;
    });
}

int l_histogram_update(lua_State* L) {
    return protected_call(L, [&] {
        auto* histogram = check_handle<LuaHistogram>(L, 1, kHistogramMeta);
        const double value = luaL_checknumber(L, 2);
        histogram->m_histogram.record(static_cast<std::uint64_t>(
            std::max(0.0, value) * 1'000'000.0));
        return 0;
    });
}

int l_histogram_print(lua_State* L) {
    return protected_call(L, [&] {
        auto* histogram = check_handle<LuaHistogram>(L, 1, kHistogramMeta);
        const double p50 = histogram->m_histogram.percentile(0.50) * 1000.0;
        const double p95 = histogram->m_histogram.percentile(0.95) * 1000.0;
        const double p99 = histogram->m_histogram.percentile(0.99) * 1000.0;
        std::println("histogram: observations={} p50={:.3f}ms p95={:.3f}ms p99={:.3f}ms",
                    static_cast<unsigned long long>(histogram->m_histogram.observations()),
                    p50, p95, p99);
        return 0;
    });
}

int gc_driver(lua_State* L) {
    auto** box = static_cast<void**>(luaL_checkudata(L, 1, kDriverMeta));
    if (box != nullptr && *box != nullptr) {
        auto* handle = static_cast<LuaDriverHandle*>(*box);
        if (handle->m_driver != nullptr) handle->m_driver->destroy(*handle->m_resource);
        mem::destroy(*handle->m_resource, handle);
        *box = nullptr;
    }
    return 0;
}

int gc_connection(lua_State* L) {
    auto** box = static_cast<void**>(luaL_checkudata(L, 1, kConnectionMeta));
    if (box != nullptr && *box != nullptr) {
        auto* connection = static_cast<LuaConnection*>(*box);
        if (connection->m_result_ref != LUA_NOREF) {
            luaL_unref(L, LUA_REGISTRYINDEX, connection->m_result_ref);
            connection->m_result_ref = LUA_NOREF;
        }
        mem::destroy(*connection->m_resource, connection);
        *box = nullptr;
    }
    return 0;
}

int gc_statement(lua_State* L) {
    auto** box = static_cast<void**>(luaL_checkudata(L, 1, kStatementMeta));
    if (box != nullptr && *box != nullptr) {
        auto* statement = static_cast<LuaStatement*>(*box);
        mem::destroy(*statement->m_resource, statement);
        *box = nullptr;
    }
    return 0;
}

int gc_param(lua_State* L) {
    auto** box = static_cast<void**>(luaL_checkudata(L, 1, kParamMeta));
    if (box != nullptr && *box != nullptr) {
        auto* parameter = static_cast<LuaParam*>(*box);
        mem::destroy(*parameter->m_resource, parameter);
        *box = nullptr;
    }
    return 0;
}

int gc_result(lua_State* L) {
    // Result storage belongs to the VM's event arena and is reclaimed wholesale
    // when the next event starts.  There is nothing to free per result.
    auto** box = static_cast<void**>(luaL_checkudata(L, 1, kResultMeta));
    if (box != nullptr) *box = nullptr;
    return 0;
}

int gc_histogram(lua_State* L) {
    auto** box = static_cast<void**>(luaL_checkudata(L, 1, kHistogramMeta));
    if (box != nullptr && *box != nullptr) {
        auto* histogram = static_cast<LuaHistogram*>(*box);
        mem::destroy(*histogram->m_resource, histogram);
        *box = nullptr;
    }
    return 0;
}

const luaL_Reg kApiFunctions[] = {
    {"sb_rand_uniform_uint64", l_rand_uniform_uint64},
    {"sb_rand_default", l_rand_default},
    {"sb_rand_uniform", l_rand_uniform},
    {"sb_rand_gaussian", l_rand_gaussian},
    {"sb_rand_pareto", l_rand_pareto},
    {"sb_rand_zipfian", l_rand_zipfian},
    {"sb_rand_unique", l_rand_unique},
    {"sb_rand_str", l_rand_str},
    {"sb_rand_varstr", l_rand_varstr},
    {"sb_rand_uniform_double", l_rand_uniform_double},
    {"sb_more_events", l_more_events},
    {"sb_event_start", l_event_start},
    {"sb_event_stop", l_event_stop},
    {"sb_sql_driver", l_sql_driver},
    {"sb_sql_driver_name", l_sql_driver_name},
    {"sb_sql_connect", l_sql_connect},
    {"sb_sql_disconnect", l_sql_disconnect},
    {"sb_sql_reconnect", l_sql_reconnect},
    {"sb_sql_query", l_sql_query},
    {"sb_sql_query_stream", l_sql_query_stream},
    {"sb_sql_pipeline", l_sql_pipeline},
    {"sb_sql_flush", l_sql_flush},
    {"sb_sql_conn_error", l_sql_conn_error},
    {"sb_last_error", l_last_error},
    {"sb_sql_prepare", l_sql_prepare},
    {"sb_sql_bulk_insert_init", l_sql_bulk_insert_init},
    {"sb_sql_bulk_insert_next", l_sql_bulk_insert_next},
    {"sb_sql_bulk_insert_done", l_sql_bulk_insert_done},
    {"sb_sql_bind_create", l_sql_bind_create},
    {"sb_sql_param_set", l_sql_param_set},
    {"sb_sql_param_set_rand_str", l_sql_param_set_rand_str},
    {"sb_sql_bind_param", l_sql_bind_param},
    {"sb_sql_execute", l_sql_execute},
    {"sb_sql_stmt_close", l_sql_stmt_close},
    {"sb_sql_result_info", l_sql_result_info},
    {"sb_sql_result_fetch_row", l_sql_result_fetch_row},
    {"sb_sql_result_free", l_sql_result_free},
    {"sb_sql_more_results", l_sql_more_results},
    {"sb_sql_next_result", l_sql_next_result},
    {"sb_histogram_new", l_histogram_new},
    {"sb_histogram_update", l_histogram_update},
    {"sb_histogram_print", l_histogram_print},
    {nullptr, nullptr}};

bool LuaVm::register_api(mem::String& error) {
    (void)error;
    lua_pushvalue(m_state, LUA_GLOBALSINDEX);
    luaL_setfuncs(m_state, kApiFunctions, 0);
    lua_pop(m_state, 1);

    struct Meta {
        const char* m_name;
        lua_CFunction m_gc;
    };
    const Meta metas[] = {
        {kDriverMeta, gc_driver},         {kConnectionMeta, gc_connection},
        {kStatementMeta, gc_statement},   {kParamMeta, gc_param},
        {kResultMeta, gc_result},         {kHistogramMeta, gc_histogram},
    };
    for (const auto& meta : metas) {
        luaL_newmetatable(m_state, meta.m_name);
        lua_pushcfunction(m_state, meta.m_gc);
        lua_setfield(m_state, -2, "__gc");
        lua_pop(m_state, 1);
    }
    return true;
}

}  // namespace
}  // namespace sb

namespace sb {
namespace {

Task<bool> LuaVm::read_replies(LuaConnection* connection, std::uint32_t keep) {
    while (connection->m_connection->pending_replies() > keep) {
        auto status = co_await connection->m_connection->read_reply(result_resource());
        if (status.is_err()) {
            connection->set_error(status.error(), is_ignorable(status.error().code()));
            if (m_stats != nullptr && stats_enabled()) m_stats->record_error();
            co_return false;
        }
    }
    co_return true;
}

Task<int> LuaVm::perform_pending(lua_State* coroutine) {
    Pending action = m_pending;
    m_pending = Pending{};
    try {
        /* Anything but a pipelined send reads the connection's outstanding
        replies first: the next reply on the wire is theirs. */
        if (action.m_kind != PendingKind::QuerySend && action.m_kind != PendingKind::ExecuteSend &&
            action.m_kind != PendingKind::Reconnect && action.m_connection != nullptr &&
            action.m_connection->m_connection != nullptr &&
            action.m_connection->m_connection->pending_replies() != 0) {
            if (!co_await read_replies(action.m_connection, 0)) {
                if (action.m_kind == PendingKind::Flush || action.m_kind == PendingKind::BulkInsert ||
                    action.m_kind == PendingKind::QueryStream) {
                    lua_pushboolean(coroutine, 0);
                } else {
                    lua_pushnil(coroutine);
                }
                co_return 1;
            }
        }
        switch (action.m_kind) {
            case PendingKind::Flush:
                action.m_connection->clear_error();
                lua_pushboolean(coroutine, 1);
                co_return 1;
            case PendingKind::QuerySend: {
                LuaConnection* connection = action.m_connection;
                const std::uint32_t depth = connection->m_pipeline_depth;
                if (connection->m_connection->pending_replies() >= depth &&
                    !co_await read_replies(connection, depth - 1)) {
                    lua_pushnil(coroutine);
                    co_return 1;
                }
                auto status = co_await connection->m_connection->send_query(
                    std::string_view(m_sql_buffer.data(), m_sql_buffer.size()), result_resource());
                if (status.is_err()) {
                    connection->set_error(status.error(), is_ignorable(status.error().code()));
                    if (m_stats != nullptr && stats_enabled()) m_stats->record_error();
                    lua_pushnil(coroutine);
                    co_return 1;
                }
                connection->clear_error();
                push_result(coroutine, *connection, db::ResultSet(result_resource()));
                co_return 1;
            }
            case PendingKind::ExecuteSend: {
                LuaStatement* statement = action.m_statement;
                LuaConnection* connection = statement->m_connection;
                const std::uint32_t depth = connection->m_pipeline_depth;
                if (connection->m_connection->pending_replies() >= depth &&
                    !co_await read_replies(connection, depth - 1)) {
                    lua_pushnil(coroutine);
                    co_return 1;
                }
                db::BindValue values[64];
                bind_values(*statement, values);
                auto status = co_await statement->m_statement->send_execute(
                    std::span<const db::BindValue>(values, statement->m_parameter_count),
                    result_resource());
                if (status.is_err()) {
                    connection->set_error(status.error(), is_ignorable(status.error().code()));
                    if (m_stats != nullptr && stats_enabled()) m_stats->record_error();
                    lua_pushnil(coroutine);
                    co_return 1;
                }
                connection->clear_error();
                push_result(coroutine, *connection, db::ResultSet(result_resource()));
                co_return 1;
            }
            case PendingKind::Connect: {
                try {
                    auto connect_result = co_await action.m_driver->m_driver->connect(
                        m_run_options.m_database,
                        m_session_resource != nullptr ? *m_session_resource : *m_resource);
                    if (connect_result.is_err()) throw connect_result.error();
                    db::Connection* connection = connect_result.value();
                    auto* handle = mem::construct<LuaConnection>(
                        *m_resource, *m_resource, this);
                    handle->m_connection = connection;
                    handle->clear_error();
                    push_handle(coroutine, kConnectionMeta, handle);
                    co_return 1;
                } catch (const db::Error& error) {
                    set_last_error(error);
                    lua_pushnil(coroutine);
                    co_return 1;
                }
            }
            case PendingKind::Query: {
                LuaConnection* connection = action.m_connection;
                try {
                    mem::MemoryResource& out = result_resource();
                    auto query_result = co_await connection->m_connection->query(
                        std::string_view(m_sql_buffer.data(), m_sql_buffer.size()), out);
                    if (query_result.is_err()) throw query_result.error();
                    connection->clear_error();
                    SB_LAYER(Dispatch);
                    push_result(coroutine, *connection, std::move(query_result.value()));
                    co_return 1;
                } catch (const db::Error& error) {
                    connection->set_error(error, is_ignorable(error.code()));
                    if (m_stats != nullptr && stats_enabled()) m_stats->record_error();
                    lua_pushnil(coroutine);
                    co_return 1;
                }
            }
            case PendingKind::QueryStream: {
                LuaConnection* connection = action.m_connection;
                try {
                    auto status = co_await connection->m_connection->query_stream(
                        std::string_view(m_sql_buffer.data(), m_sql_buffer.size()),
                        action.m_stream_depth, action.m_stream_total, result_resource());
                    if (status.is_err()) throw status.error();
                    connection->clear_error();
                    lua_pushboolean(coroutine, 1);
                    co_return 1;
                } catch (const db::Error& error) {
                    connection->set_error(error, is_ignorable(error.code()));
                    if (m_stats != nullptr && stats_enabled()) m_stats->record_error();
                    lua_pushboolean(coroutine, 0);
                    co_return 1;
                }
            }
            case PendingKind::Prepare: {
                LuaConnection* connection = action.m_connection;
                try {
                    auto prepare_result = co_await connection->m_connection->prepare(
                        std::string_view(m_sql_buffer.data(), m_sql_buffer.size()));
                    if (prepare_result.is_err()) throw prepare_result.error();
                    db::PreparedStatement* statement = prepare_result.value();
                    connection->clear_error();
                    auto* handle =
                        mem::construct<LuaStatement>(*m_resource);
                    handle->m_statement = statement;
                    handle->m_connection = connection;
                    handle->m_resource = m_resource;
                    push_handle(coroutine, kStatementMeta, handle);
                    co_return 1;
                } catch (const db::Error& error) {
                    connection->set_error(error, is_ignorable(error.code()));
                    if (m_stats != nullptr && stats_enabled()) m_stats->record_error();
                    lua_pushnil(coroutine);
                    co_return 1;
                }
            }
            case PendingKind::Reconnect: {
                LuaConnection* connection = action.m_connection;
                try {
                    auto status = co_await connection->m_connection->reconnect();
                    if (status.is_err()) throw status.error();
                    connection->clear_error();
                    if (m_stats != nullptr) m_stats->record_reconnect();
                    lua_pushboolean(coroutine, 1);
                } catch (const db::Error& error) {
                    connection->set_error(error, is_ignorable(error.code()));
                    if (m_stats != nullptr) m_stats->record_error();
                    lua_pushboolean(coroutine, 0);
                }
                co_return 1;
            }
            case PendingKind::Execute: {
                LuaStatement* statement = action.m_statement;
                LuaConnection* connection = statement->m_connection;
                try {
                    mem::MemoryResource& out = result_resource();
                    db::BindValue values[64];
                    bind_values(*statement, values);
                    auto execute_result = co_await statement->m_statement->execute(
                        std::span<const db::BindValue>(values, statement->m_parameter_count),
                        out);
                    if (execute_result.is_err()) throw execute_result.error();
                    connection->clear_error();
                    SB_LAYER(Dispatch);
                    push_result(coroutine, *connection, std::move(execute_result.value()));
                    co_return 1;
                } catch (const db::Error& error) {
                    connection->set_error(error, is_ignorable(error.code()));
                    if (m_stats != nullptr && stats_enabled()) m_stats->record_error();
                    lua_pushnil(coroutine);
                    co_return 1;
                }
            }
            case PendingKind::CloseStatement: {
                LuaStatement* statement = action.m_statement;
                try {
                    co_await statement->m_statement->close();
                } catch (const db::Error&) {
                    // The connection is already unusable; mark it closed locally.
                }
                statement->m_closed = true;
                lua_pushboolean(coroutine, 1);
                co_return 1;
            }
            case PendingKind::BulkInsert: {
                LuaConnection* connection = action.m_connection;
                auto& bulk = *connection->m_bulk;
                try {
                    if (m_stats != nullptr && stats_enabled()) {
                        m_stats->record_query(QueryKind::Write);
                    }
                    auto bulk_result = co_await connection->m_connection->query(
                        std::string_view(bulk.m_sql.data(), bulk.m_sql.size()),
                        result_resource());
                    if (bulk_result.is_err()) throw bulk_result.error();
                    bulk.restart();
                    connection->clear_error();
                    lua_pushboolean(coroutine, 1);
                } catch (const db::Error& error) {
                    bulk.restart();
                    connection->set_error(error, is_ignorable(error.code()));
                    if (m_stats != nullptr && stats_enabled()) m_stats->record_error();
                    lua_pushboolean(coroutine, 0);
                }
                co_return 1;
            }
            case PendingKind::Sleep:
                co_await sleep_for(std::chrono::nanoseconds(action.m_sleep_ns));
                co_return 0;
            case PendingKind::MoreEventsYield:
                co_await yield_now();
                lua_pushboolean(coroutine, action.m_bool_result ? 1 : 0);
                co_return 1;
            case PendingKind::None:
                break;
        }
    } catch (const std::exception& error) {
        if (action.m_connection != nullptr) {
            action.m_connection->set_error(db::Error(error.what(), 2027), false);
        } else {
            set_last_error(db::Error(error.what(), 2027));
        }
        lua_pushnil(coroutine);
        co_return 1;
    }
    lua_pushnil(coroutine);
    co_return 1;
}

namespace {

[[nodiscard]] mem::String lua_error_text(lua_State* L, mem::MemoryResource& resource) {
    mem::String result{mem::Allocator<char>(&resource)};
    if (lua_isstring(L, -1)) {
        std::size_t length = 0;
        const char* text = lua_tolstring(L, -1, &length);
        result.assign(text, length);
        lua_pop(L, 1);
        return result;
    }
    const char* text = to_display_string(L, -1);
    if (text != nullptr) result.assign(text);
    lua_pop(L, 2);
    return result;
}

void push_stat_table(lua_State* L, const StatsSnapshot& snapshot, double interval,
                     double total) {
    lua_newtable(L);
    auto number = [&](const char* name, double value) {
        lua_pushnumber(L, value);
        lua_setfield(L, -2, name);
    };
    auto integer = [&](const char* name, lua_Integer value) {
        lua_pushinteger(L, value);
        lua_setfield(L, -2, name);
    };
    number("time_interval", interval);
    number("time_total", total);
    integer("threads_running", static_cast<lua_Integer>(snapshot.m_threads_running));
    integer("events", static_cast<lua_Integer>(snapshot.m_events));
    integer("reads", static_cast<lua_Integer>(snapshot.m_reads));
    integer("writes", static_cast<lua_Integer>(snapshot.m_writes));
    integer("other", static_cast<lua_Integer>(snapshot.m_other));
    integer("errors", static_cast<lua_Integer>(snapshot.m_errors));
    integer("reconnects", static_cast<lua_Integer>(snapshot.m_reconnects));
    number("latency_min", static_cast<double>(snapshot.m_latency_min_ns) * 1e-9);
    number("latency_max", static_cast<double>(snapshot.m_latency_max_ns) * 1e-9);
    number("latency_avg", snapshot.m_latency_avg);
    number("latency_pct", snapshot.m_latency_percentile);
    number("latency_sum", static_cast<double>(snapshot.m_latency_sum_ns) * 1e-9);
    integer("bytes_read", static_cast<lua_Integer>(snapshot.m_bytes_read));
    integer("bytes_written", static_cast<lua_Integer>(snapshot.m_bytes_written));
}

}  // namespace

Task<bool> LuaVm::call_entry(std::string_view name, bool pass_tid,
                             std::uint32_t tid, bool pass_text,
                             std::string_view text) {
    mem::String function_name{mem::Allocator<char>(m_resource)};
    function_name.assign(name);
    lua_getglobal(m_state, function_name.c_str());
    if (!lua_isfunction(m_state, -1)) {
        lua_pop(m_state, 1);
        co_return false;
    }

    lua_State* coroutine = lua_newthread(m_state);
    const int reference = luaL_ref(m_state, LUA_REGISTRYINDEX);
    lua_xmove(m_state, coroutine, 1);
    int nargs = 0;
    if (pass_tid) {
        lua_pushinteger(coroutine, static_cast<lua_Integer>(tid));
        ++nargs;
    }
    if (pass_text) {
        lua_pushlstring(coroutine, text.data(), text.size());
        ++nargs;
    }

    tls_vm = this;
    int status = 0;
    {
        SB_LAYER(Lua);
        status = lua_resume(coroutine, nargs);
    }
    while (status == LUA_YIELD) {
        if (m_pending.m_kind == PendingKind::None) {
            luaL_unref(m_state, LUA_REGISTRYINDEX, reference);
            throw LuaFailure("Lua coroutine yielded without a pending sysbench I/O");
        }
        const int resumed = co_await perform_pending(coroutine);
        tls_vm = this;
        SB_LAYER(Lua);
        status = lua_resume(coroutine, resumed);
    }

    if (status != LUA_OK) {
        mem::String message = lua_error_text(coroutine, *m_resource);
        luaL_unref(m_state, LUA_REGISTRYINDEX, reference);
        throw LuaFailure(message);
    }
    luaL_unref(m_state, LUA_REGISTRYINDEX, reference);
    co_return true;
}

Task<bool> LuaVm::call_global(std::string_view name, bool pass_tid,
                              std::uint32_t tid) {
    return call_entry(name, pass_tid, tid, false, {});
}

Task<bool> LuaVm::dispatch_command(std::string_view command) {
    return call_entry("sb_run_named", false, 0, true, command);
}

bool LuaVm::call_hook(std::string_view hook, const StatsSnapshot& snapshot,
                      double interval, double total) {
    lua_getglobal(m_state, "sysbench");
    lua_getfield(m_state, -1, "hooks");
    mem::String name{mem::Allocator<char>(m_resource)};
    name.assign(hook);
    lua_getfield(m_state, -1, name.c_str());
    if (!lua_isfunction(m_state, -1)) {
        lua_pop(m_state, 3);
        return false;
    }
    lua_remove(m_state, -2);
    lua_remove(m_state, -2);
    push_stat_table(m_state, snapshot, interval, total);
    tls_vm = this;
    if (lua_pcall(m_state, 1, 0, 0) != LUA_OK) {
        mem::String message = lua_error_text(m_state, *m_resource);
        std::println(stderr, "sysbench: report hook '{}' failed: {}",
                     name.c_str(), message.c_str());
    }
    return true;
}

}  // namespace
}  // namespace sb

namespace sb {
namespace {

// ---------------------------------------------------------------------------
// Option discovery from sysbench.cmdline.options
// ---------------------------------------------------------------------------

bool collect_lua_options(LuaVm& vm, Options& options, mem::String& error) {
    lua_State* L = vm.state();
    lua_getglobal(L, "sysbench");
    if (!lua_istable(L, -1)) {
        lua_pop(L, 1);
        error = mem::String{mem::Allocator<char>(&vm.resource())};
        error.assign("sysbench table is missing");
        return false;
    }
    lua_getfield(L, -1, "cmdline");
    if (!lua_istable(L, -1)) {
        lua_pop(L, 2);
        return true;
    }
    lua_getfield(L, -1, "options");
    if (!lua_istable(L, -1)) {
        lua_pop(L, 3);
        return true;
    }

    lua_pushnil(L);
    while (lua_next(L, -2) != 0) {
        if (!lua_isstring(L, -2) || !lua_istable(L, -1)) {
            lua_pop(L, 1);
            continue;
        }
        std::size_t name_length = 0;
        const char* name_text = lua_tolstring(L, -2, &name_length);
        std::string_view name(name_text, name_length);

        lua_rawgeti(L, -1, 1);
        std::string_view description;
        if (lua_isstring(L, -1)) {
            std::size_t length = 0;
            const char* text = lua_tolstring(L, -1, &length);
            description = {text, length};
        }
        lua_pop(L, 1);

        lua_rawgeti(L, -1, 2);
        const int default_type = lua_type(L, -1);
        char default_buffer[64]{};
        std::string_view default_value;

        lua_rawgeti(L, -2, 3);
        int hint = 0;
        if (lua_isnumber(L, -1)) hint = static_cast<int>(lua_tointeger(L, -1));
        lua_pop(L, 1);

        OptionType type = OptionType::String;
        if (hint == 1) type = OptionType::Bool;
        else if (hint == 2) type = OptionType::Int;
        else if (hint == 3) type = OptionType::Size;
        else if (hint == 4) type = OptionType::Double;
        else if (hint == 5) type = OptionType::String;
        else if (hint == 6) type = OptionType::List;
        else {
            switch (default_type) {
                case LUA_TBOOLEAN: type = OptionType::Bool; break;
                case LUA_TNUMBER:
                    type = is_integral(L, -1) ? OptionType::Int
                                                 : OptionType::Double;
                    break;
                case LUA_TTABLE: type = OptionType::List; break;
                default: type = OptionType::String; break;
            }
        }

        switch (default_type) {
            case LUA_TBOOLEAN:
                default_value = lua_toboolean(L, -1) ? "on" : "off";
                break;
            case LUA_TNUMBER:
                if (is_integral(L, -1)) {
                    const auto integer = lua_tointeger(L, -1);
                    const auto result = std::to_chars(default_buffer,
                                                      default_buffer + sizeof(default_buffer),
                                                      integer);
                    default_value = {default_buffer,
                                     static_cast<std::size_t>(result.ptr - default_buffer)};
                } else {
                    const int written = std::snprintf(default_buffer,
                                                      sizeof(default_buffer), "%.17g",
                                                      lua_tonumber(L, -1));
                    default_value = {default_buffer,
                                     written > 0 ? static_cast<std::size_t>(written) : 0};
                }
                break;
            case LUA_TSTRING: {
                std::size_t length = 0;
                const char* text = lua_tolstring(L, -1, &length);
                default_value = {text, length};
                break;
            }
            case LUA_TTABLE:
            default:
                default_value = "";
                break;
        }

        if (!options.add(name, description, type, default_value, true)) {
            error = mem::String{mem::Allocator<char>(&vm.resource())};
            error.assign("too many Lua command line options");
            lua_pop(L, 3);
            return false;
        }
        lua_pop(L, 2);  // default value + option definition value
    }
    lua_pop(L, 3);
    return true;
}

void print_default_final_report(const StatsSnapshot& snapshot, double total,
                                std::uint32_t percentile) {
    const double interval = total > 0 ? total : 1.0;
    const std::uint64_t queries = snapshot.m_reads + snapshot.m_writes + snapshot.m_other;
    std::println("\nSQL statistics:");
    std::println("    queries performed:");
    std::println("        read:                            {}",
                static_cast<unsigned long long>(snapshot.m_reads));
    std::println("        write:                           {}",
                static_cast<unsigned long long>(snapshot.m_writes));
    std::println("        other:                           {}",
                static_cast<unsigned long long>(snapshot.m_other));
    std::println("        total:                           {}",
                static_cast<unsigned long long>(queries));
    std::println("    transactions:                        {} ({:.2f} per sec.)",
                static_cast<unsigned long long>(snapshot.m_events),
                static_cast<double>(snapshot.m_events) / interval);
    std::println("    queries:                             {} ({:.2f} per sec.)",
                static_cast<unsigned long long>(queries),
                static_cast<double>(queries) / interval);
    std::println("    ignored errors:                      {} ({:.2f} per sec.)",
                static_cast<unsigned long long>(snapshot.m_errors),
                static_cast<double>(snapshot.m_errors) / interval);
    std::println("    reconnects:                          {} ({:.2f} per sec.)",
                static_cast<unsigned long long>(snapshot.m_reconnects),
                static_cast<double>(snapshot.m_reconnects) / interval);
    std::println("\nGeneral statistics:");
    std::println("    total time:                          {:.4f}s", total);
    std::println("    total number of events:              {}",
                static_cast<unsigned long long>(snapshot.m_events));
    std::println("\nLatency (ms, {}%):", percentile);
    std::println("    min:                                  {:.3f}",
                static_cast<double>(snapshot.m_latency_min_ns) / 1e6);
    std::println("    avg:                                  {:.3f}",
                snapshot.m_latency_avg * 1000.0);
    std::println("    max:                                  {:.3f}",
                static_cast<double>(snapshot.m_latency_max_ns) / 1e6);
    std::println("    {}th percentile:                      {:.3f}", percentile,
                snapshot.m_latency_percentile * 1000.0);
    std::println("    sum:                                  {:.3f}",
                static_cast<double>(snapshot.m_latency_sum_ns) / 1e6);
    std::println("\nThreads fairness:");
    std::println("    events (avg/stddev):                  {:.4f}/{:.2f}",
                snapshot.m_thread_events_avg, snapshot.m_thread_events_stddev);
    std::println("");
}

void print_default_intermediate(const StatsSnapshot& interval,
                                const StatsSnapshot& total,
                                double interval_seconds, double total_seconds,
                                std::uint32_t percentile) {
    const double seconds = interval_seconds > 0 ? interval_seconds : 1.0;
    std::println("[ {:.0f}s ] thds: {} tps: {:.2f} qps: {:.2f} "
                 "(r/w/o: {:.2f}/{:.2f}/{:.2f}) lat (ms,{}%): {:.2f} "
                 "err/s: {:.2f} reconn/s: {:.2f}",
                total_seconds,
                static_cast<unsigned long long>(total.m_threads_running),
                static_cast<double>(interval.m_events) / seconds,
                static_cast<double>(interval.m_reads + interval.m_writes + interval.m_other) /
                    seconds,
                static_cast<double>(interval.m_reads) / seconds,
                static_cast<double>(interval.m_writes) / seconds,
                static_cast<double>(interval.m_other) / seconds, percentile,
                interval.m_latency_percentile * 1000.0,
                static_cast<double>(interval.m_errors) / seconds,
                static_cast<double>(interval.m_reconnects) / seconds);
}

void print_histogram(const StatsSnapshot& snapshot) {
    std::println("Latency histogram:");
    const std::uint64_t total = snapshot.m_histogram.m_total;
    if (total == 0) {
        std::println("    (no samples)");
        return;
    }
    std::uint64_t cumulative = 0;
    int printed = 0;
    for (std::size_t bin = 0; bin < LatencyHistogram::kBins; ++bin) {
        const std::uint64_t count = snapshot.m_histogram.m_counts[bin];
        if (count == 0) continue;
        cumulative += count;
        const double upper = LatencyHistogram::bin_upper_bound_seconds(bin) * 1000.0;
        std::println("    <= {:8.3f} ms: {:10}  {:6.2f}%  (cumulative {:6.2f}%)",
                    upper, static_cast<unsigned long long>(count),
                    100.0 * static_cast<double>(count) / static_cast<double>(total),
                    100.0 * static_cast<double>(cumulative) / static_cast<double>(total));
        if (++printed >= 40) {
            std::println("    ...");
            break;
        }
    }
}

}  // namespace

struct LuaEngine::Impl {
    mem::MemoryResource* m_resource;
    Options* m_options;
    mem::String m_script_path;
    mem::String m_command;
    int m_argc;
    char** m_argv;
    LuaVm* m_bootstrap = nullptr;
    bool m_loaded = false;

    Impl(mem::MemoryResource& owner, Options& option_table,
         std::string_view script, std::string_view command_name, int arg_count,
         char** arguments)
        : m_resource(&owner),
          m_options(&option_table),
          m_script_path(mem::Allocator<char>(&owner)),
          m_command(mem::Allocator<char>(&owner)),
          m_argc(arg_count),
          m_argv(arguments) {
        m_script_path.assign(script);
        m_command.assign(command_name);
        LuaRunOptions defaults{};
        m_bootstrap = mem::construct<LuaVm>(owner, owner, option_table, defaults,
                                          m_script_path, m_command, m_argc, m_argv, 0u,
                                          nullptr, nullptr);
    }

    ~Impl() {
        if (m_bootstrap != nullptr) {
            mem::destroy(*m_resource, m_bootstrap);
            m_bootstrap = nullptr;
        }
    }

    Task<void> run_benchmark_worker(
        const LuaRunOptions& run_options, std::uint32_t index,
        ThreadStats* thread_stats, RunControl* control,
        ShardedCounter<std::uint32_t, Runtime::kMaxWorkers>* failures);
    Task<void> run_command_worker(
        const LuaRunOptions& run_options, std::string_view command,
        std::uint32_t index,
        ShardedCounter<std::uint32_t, Runtime::kMaxWorkers>* failures);
    // Interim reports, run as a task on a worker: no main-thread loop.
    // The console thread: print a report every interval until the run ends.
    // @return false when the run stopped on a stall.
    bool report_until_done(Runtime& runtime, const LuaRunOptions& run_options,
                           ThreadStats** pointers, std::uint32_t threads,
                           RunControl* control);
};

/* Runs on the calling (console) thread, never on a worker: it sleeps on the
runtime's futex until the next report is due or the run ends. */
bool LuaEngine::Impl::report_until_done(Runtime& runtime, const LuaRunOptions& run_options,
                                        ThreadStats** pointers, std::uint32_t threads,
                                        RunControl* control) {
    const double percentile = static_cast<double>(run_options.m_percentile);
    StatsSnapshot previous =
        aggregate(std::span<ThreadStats* const>(pointers, threads), percentile);
    auto last_report = Clock::now();
    const auto interval = std::chrono::seconds(std::max(1u, run_options.m_report_interval));
    for (;;) {
        const auto due = last_report + interval;
        const auto before = Clock::now();
        const auto left = due > before ? std::chrono::duration_cast<std::chrono::nanoseconds>(due - before)
                                       : std::chrono::nanoseconds(0);
        if (runtime.wait_for(left)) return !runtime.stall_detected();
        const auto now = Clock::now();
        if (control->m_remaining.load(std::memory_order_acquire) == 0) continue;
        StatsSnapshot current =
            aggregate(std::span<ThreadStats* const>(pointers, threads), percentile);
        StatsSnapshot delta = current.difference(previous, percentile);
        const double interval_seconds = std::chrono::duration<double>(now - last_report).count();
        const double total_seconds = std::chrono::duration<double>(now - control->m_start).count();
        current.m_threads_running = delta.m_threads_running;
        const bool hooked = m_bootstrap->call_hook("report_intermediate", delta,
                                                   interval_seconds, total_seconds);
        if (!hooked) {
            print_default_intermediate(delta, current, interval_seconds, total_seconds,
                                       run_options.m_percentile);
        }
        previous = current;
        last_report = now;
    }
}

Task<void> LuaEngine::Impl::run_benchmark_worker(const LuaRunOptions& run_options,
                                                    std::uint32_t index,
                                                    ThreadStats* thread_stats,
                                                    RunControl* control,
                                                    ShardedCounter<std::uint32_t,
                                                                   Runtime::kMaxWorkers>* failures) {
    try {
        Runtime& runtime = *Runtime::current();
        const int node = runtime.worker_numa_node(Runtime::current_worker_id());
        mem::HeapArena lua_heap(mem::global_root(),
                                static_cast<std::size_t>(run_options.m_lua_heap_bytes),
                                "lua", node);
        mem::HeapArena session_heap(
            mem::global_root(),
            static_cast<std::size_t>(run_options.m_connection_heap_bytes),
            "connection", node);
        LuaVm vm(lua_heap, *m_options, run_options, m_script_path,
                 m_command, m_argc, m_argv, index, thread_stats, control);
        vm.set_session_resource(session_heap);
        mem::String error{mem::Allocator<char>(&vm.resource())};
        if (!vm.load(error)) throw LuaFailure(error);
        if (!vm.install_options(error)) throw LuaFailure("failed to install options");
        vm.install_tid();
        (void)co_await vm.call_global("init", false, 0);
        if (vm.has_global("thread_init")) {
            if (!(co_await vm.call_global("thread_init", true, index))) {
                throw LuaFailure("thread_init is not callable");
            }
        }
        thread_stats->set_active(true);
        if (!vm.has_global("thread_run")) throw LuaFailure("thread_run is not defined");
        if (!(co_await vm.call_global("thread_run", true, index))) {
            throw LuaFailure("thread_run is not callable");
        }
        thread_stats->set_active(false);
        (void)co_await vm.call_global("thread_done", true, index);
    } catch (const std::exception& error) {
        thread_stats->set_active(false);
        failures->add(Runtime::current_worker_id(), 1);
        control->m_stop.store(true, std::memory_order_release);
        std::println(stderr, "sysbench: worker {} failed: {}", index, error.what());
    }
    control->m_remaining.fetch_sub(1, std::memory_order_acq_rel);
    co_return;
}

Task<void> LuaEngine::Impl::run_command_worker(const LuaRunOptions& run_options,
                                               std::string_view command_name,
                                               std::uint32_t index,
                                               ShardedCounter<std::uint32_t,
                                                              Runtime::kMaxWorkers>* failures) {
    try {
        Runtime& runtime = *Runtime::current();
        const int node = runtime.worker_numa_node(Runtime::current_worker_id());
        mem::HeapArena lua_heap(mem::global_root(),
                                static_cast<std::size_t>(run_options.m_lua_heap_bytes),
                                "lua", node);
        mem::HeapArena session_heap(
            mem::global_root(),
            static_cast<std::size_t>(run_options.m_connection_heap_bytes),
            "connection", node);
        LuaVm vm(lua_heap, *m_options, run_options, m_script_path,
                 m_command, m_argc, m_argv, index, nullptr, nullptr);
        vm.set_session_resource(session_heap);
        mem::String error{mem::Allocator<char>(&vm.resource())};
        if (!vm.load(error)) throw LuaFailure(error);
        if (!vm.install_options(error)) throw LuaFailure("failed to install options");
        vm.install_tid();
        (void)co_await vm.call_global("init", false, 0);
        if (!(co_await vm.dispatch_command(command_name))) {
            throw LuaFailure("command is not defined");
        }
    } catch (const std::exception& error) {
        failures->add(Runtime::current_worker_id(), 1);
        std::println(stderr, "sysbench: command worker {} failed: {}", index,
                     error.what());
    }
    co_return;
}

LuaEngine::LuaEngine(mem::MemoryResource& resource, Options& options,
                     std::string_view script_path, std::string_view command,
                     int argc, char** argv) {
    m_impl = mem::construct<Impl>(resource, resource, options, script_path, command,
                                 argc, argv);
}

LuaEngine::~LuaEngine() {
    if (m_impl != nullptr) {
        Impl* impl = m_impl;
        m_impl = nullptr;
        mem::MemoryResource* resource = impl->m_resource;
        mem::destroy(*resource, impl);
    }
}

bool LuaEngine::load(mem::String& error) {
    if (m_impl->m_loaded) return true;
    if (!m_impl->m_bootstrap->load(error)) return false;
    m_impl->m_loaded = true;
    return true;
}

bool LuaEngine::collect_options(mem::String& error) {
    if (!m_impl->m_loaded) {
        error = mem::String{mem::Allocator<char>(m_impl->m_resource)};
        error.assign("Lua engine must be loaded before collecting options");
        return false;
    }
    return collect_lua_options(*m_impl->m_bootstrap, *m_impl->m_options, error);
}

bool LuaEngine::install_options(mem::String& error) {
    return m_impl->m_bootstrap->install_options(error);
}

bool LuaEngine::command_parallel(std::string_view command) {
    return m_impl->m_bootstrap->command_parallel(command);
}

bool LuaEngine::has_global(std::string_view name) {
    return m_impl->m_bootstrap->has_global(name);
}

void LuaEngine::print_help() const {
    std::println("Usage: sysbench [options] SCRIPT COMMAND\n");
    std::println("Commands: run, prepare, cleanup, warmup, help\n");
    std::println("Options:");
    for (std::size_t i = 0; i < m_impl->m_options->size(); ++i) {
        const OptionDef* option = m_impl->m_options->at(i);
        if (option == nullptr) continue;
        char display[192]{};
        const std::size_t length =
            std::min(option->m_name.size(), sizeof(display) - 1);
        for (std::size_t j = 0; j < length; ++j) {
            display[j] = option->m_name[j] == '_' ? '-' : option->m_name[j];
        }
        std::println("  --{:<28} {} (default: {})", display,
                    option->m_description.c_str(), option->m_value.c_str());
    }
}

int LuaEngine::call_help() {
    if (m_impl->m_bootstrap->has_global("help")) {
        lua_State* L = m_impl->m_bootstrap->state();
        lua_getglobal(L, "help");
        tls_vm = m_impl->m_bootstrap;
        if (lua_pcall(L, 0, 0, 0) != LUA_OK) {
            std::println(stderr, "sysbench: help failed: {}",
                         lua_tostring(L, -1));
            lua_pop(L, 1);
            return 1;
        }
        return 0;
    }
    print_help();
    return 0;
}

int LuaEngine::run(Runtime& runtime, const LuaRunOptions& run_options) {
    Impl* impl = m_impl;
    const std::uint32_t threads = std::max(1u, run_options.m_threads);
    if (threads > kMaxBenchmarkThreads) {
        std::println(stderr, "sysbench: too many benchmark threads (max {})",
                     kMaxBenchmarkThreads);
        return 1;
    }
    g_interrupted.store(false, std::memory_order_release);

    mem::MemoryResource& main_resource = *impl->m_resource;
    mem::BumpArena stats_arena(
        main_resource,
        std::max<std::size_t>(64 * 1024,
                              sizeof(ThreadStats) * threads + 64 * threads),
        "stats");
    auto* stats = static_cast<ThreadStats*>(
        stats_arena.allocate(sizeof(ThreadStats) * threads, alignof(ThreadStats)));
    auto* pointers = static_cast<ThreadStats**>(
        stats_arena.allocate(sizeof(ThreadStats*) * threads, alignof(ThreadStats*)));
    for (std::uint32_t i = 0; i < threads; ++i) {
        ::new (static_cast<void*>(stats + i)) ThreadStats(i);
        pointers[i] = &stats[i];
    }

    RunControl control;
    control.m_start = Clock::now();
    control.m_warmup_end = control.m_start + std::chrono::seconds(run_options.m_warmup_seconds);
    if (run_options.m_time_seconds > 0) {
        control.m_end = control.m_start +
                      std::chrono::seconds(run_options.m_warmup_seconds +
                                           run_options.m_time_seconds);
    } else {
        control.m_end = control.m_start + std::chrono::hours(24 * 365);
    }
    control.m_event_limit = run_options.m_events;
    control.m_rate = run_options.m_rate;
    control.m_fair_yield = runtime.worker_count() < threads;

    ShardedCounter<std::uint32_t, Runtime::kMaxWorkers> failures;
    control.m_remaining.store(threads, std::memory_order_release);
    for (std::uint32_t i = 0; i < threads; ++i) {
        const std::uint32_t worker = i % std::max(1u, runtime.worker_count());
        runtime.spawn_on(worker, [impl, &run_options, i, pointers, &control,
                                  &failures] {
            return impl->run_benchmark_worker(run_options, i, pointers[i], &control,
                                              &failures);
        });
    }

    /* The calling thread is the console thread: it prints the interval
    reports (if configured) and the final report, and otherwise sleeps.  The
    workers detect stalls and wake it when the last task ends. */
    if (run_options.m_report_interval != 0) {
        if (!impl->report_until_done(runtime, run_options, pointers, threads, &control)) return 1;
    } else if (!runtime.wait()) {
        return 1;
    }

    const auto now = Clock::now();
    const double total_seconds =
        std::chrono::duration<double>(now - control.m_start).count();
    StatsSnapshot final_snapshot =
        aggregate(std::span<ThreadStats* const>(pointers, threads),
                  static_cast<double>(run_options.m_percentile));
    final_snapshot.m_threads_running = threads;
    if (!impl->m_bootstrap->call_hook("report_cumulative", final_snapshot,
                                    total_seconds, total_seconds)) {
        print_default_final_report(final_snapshot, total_seconds,
                                   run_options.m_percentile);
    }
    sb::layer::report();
    if (run_options.m_histogram) print_histogram(final_snapshot);

    for (std::uint32_t i = 0; i < threads; ++i) stats[i].~ThreadStats();
    return failures.sum(runtime.worker_count()) == 0 ? 0 : 1;
}

int LuaEngine::run_command(Runtime& runtime, const LuaRunOptions& run_options,
                           std::string_view command, bool parallel) {
    Impl* impl = m_impl;
    const std::uint32_t jobs = parallel ? std::max(1u, run_options.m_threads) : 1;
    ShardedCounter<std::uint32_t, Runtime::kMaxWorkers> failures;
    for (std::uint32_t i = 0; i < jobs; ++i) {
        const std::uint32_t worker = i % std::max(1u, runtime.worker_count());
        runtime.spawn_on(worker, [impl, &run_options, command, i, &failures] {
            return impl->run_command_worker(run_options, command, i, &failures);
        });
    }
    if (!runtime.wait()) return 1;
    return failures.sum(runtime.worker_count()) == 0 ? 0 : 1;
}

}  // namespace sb
