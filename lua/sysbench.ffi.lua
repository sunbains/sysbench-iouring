-- The C entry points of the hot path (event loop, rand, query, execute,
-- param:set, results), called through LuaJIT's FFI as sysbench does:
-- the JIT compiles each call into the trace.  The engine hands them over as
-- function pointers (sb_ffi()), so no symbol has to be exported.
--
-- An entry that must wait returns 2 (YIELD) after recording the request; the
-- caller then runs coroutine.yield(), and the engine performs the request on
-- its runtime and resumes the coroutine with the request's results.

local ffi = require("ffi")

ffi.cdef[[
typedef struct sbx_connection sbx_connection;
typedef struct sbx_statement sbx_statement;
typedef struct {
   int64_t integer;
   double real;
   char *buffer;
   size_t length;
   size_t capacity;
   bool is_null;
   uint8_t kind;
} sbx_param;
typedef struct {
   const char *data;
   uint32_t length;
   bool is_null;
} sbx_field;
]]

local raw = sb_ffi()
local function entry(name, ctype)
   return ffi.cast(ctype, raw[name])
end

sysbench.ffi = {
   DONE = 0,
   FAILED = -1,
   YIELD = 2,
   more_events = entry("more_events", "int (*)(void)"),
   event_start = entry("event_start", "int (*)(void)"),
   event_stop = entry("event_stop", "int (*)(void)"),
   rand_default = entry("rand_default", "double (*)(int64_t, int64_t)"),
   rand_uniform = entry("rand_uniform", "double (*)(int64_t, int64_t)"),
   rand_gaussian = entry("rand_gaussian", "double (*)(int64_t, int64_t)"),
   rand_pareto = entry("rand_pareto", "double (*)(int64_t, int64_t)"),
   rand_zipfian = entry("rand_zipfian", "double (*)(int64_t, int64_t)"),
   rand_unique = entry("rand_unique", "double (*)(void)"),
   rand_uniform_double = entry("rand_uniform_double", "double (*)(void)"),
   rand_string = entry("rand_string", "void (*)(const char *, size_t, char *)"),
   rand_varstring = entry("rand_varstring", "int (*)(size_t, size_t, char *)"),
   query = entry("query", "int (*)(sbx_connection *, const char *, size_t)"),
   execute = entry("execute", "int (*)(sbx_statement *)"),
   result_info = entry("result_info", "double (*)(sbx_connection *, int)"),
   fetch_row = entry("fetch_row", "const sbx_field *(*)(sbx_connection *, uint32_t *)"),
   result_free = entry("result_free", "void (*)(sbx_connection *)"),
   -- The C object behind a handle userdata (a box holding one pointer).
   object = function(ctype, ud)
      return ffi.cast(ctype .. " **", ud)[0]
   end,
}
