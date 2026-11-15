-- SQL API compatible with sysbench, implemented over the C++ MySQL client.
-- query(), execute(), param:set() and the result calls go through the FFI
-- (sysbench.ffi.lua).  A call that cannot complete inline (the io_uring
-- architecture, pipelining) yields the Lua coroutine; the C++ runtime
-- performs it and resumes the coroutine.

local ffi = require("ffi")
local F = sysbench.ffi
local ffi_query = F.query
local ffi_execute = F.execute
local ffi_result_info = F.result_info
local ffi_fetch_row = F.fetch_row
local ffi_result_free = F.result_free
local ffi_copy = ffi.copy
local ffi_string = ffi.string
local DONE, YIELD = F.DONE, F.YIELD
local yield = coroutine.yield

sysbench.sql = {}

sysbench.sql.type = {
   NONE = 0,
   TINYINT = 1,
   SMALLINT = 2,
   INT = 3,
   BIGINT = 4,
   FLOAT = 5,
   DOUBLE = 6,
   TIME = 7,
   DATE = 8,
   DATETIME = 9,
   TIMESTAMP = 10,
   CHAR = 11,
   VARCHAR = 12
}

sysbench.sql.error = {
   NONE = 0,
   IGNORABLE = 1,
   FATAL = 2
}

local driver_methods = {}
local connection_methods = {}
local statement_methods = {}
local result_methods = {}
local param_methods = {}

local driver_mt = { __index = driver_methods }
local connection_mt = { __index = connection_methods }
local statement_mt = { __index = statement_methods }
-- nrows, nfields, affected_rows, last_insert_id, warnings and status are
-- read from the driver only when a script asks for one (most never do).
local result_info_index = {
   nrows = 1, nfields = 2, affected_rows = 3, last_insert_id = 4, warnings = 5,
   status = 6,
}
local result_mt = {
   __index = function(rs, key)
      local method = result_methods[key]
      if method ~= nil then return method end
      local index = result_info_index[key]
      if index ~= nil then
         return ffi_result_info(rawget(rs, "__c"), index)
      end
      return nil
   end
}

local param_mt = { __index = param_methods }

function sysbench.sql.driver(driver_name)
   local ud = sb_sql_driver(driver_name)
   if ud == nil then
      error("failed to initialize the DB driver", 2)
   end
   -- The name is fixed: read once, so drv:name() in an event stays in Lua.
   return setmetatable({__ud = ud, __name = sb_sql_driver_name(ud)}, driver_mt)
end

function driver_methods.connect(self)
   local ud = sb_sql_connect(self.__ud)
   if ud == nil then
      local code, state, msg = sb_last_error()
      error(string.format("connection creation failed: (%s) %s",
                          tostring(code), tostring(msg)), 2)
   end
   local c = F.object("sbx_connection", ud)
   -- A connection has one current result (as in sysbench), valid until
   -- its next query: one table for the connection's life, so a query
   -- allocates nothing.
   return setmetatable({__ud = ud, __p = c, __rs = setmetatable({__c = c}, result_mt),
                        closed = false}, connection_mt)
end

function driver_methods.name(self)
   return self.__name
end

function connection_methods.disconnect(self)
   if self.closed then return true end
   self.closed = true
   return sb_sql_disconnect(self.__ud)
end

function connection_methods.reconnect(self)
   local ok = sb_sql_reconnect(self.__ud)
   if not ok then
      return self:check_error(nil, "<reconnect>")
   end
   return true
end

function connection_methods.check_error(self, rs, query)
   if rs ~= nil then
      return rs
   end

   local kind, code, state, msg = sb_sql_conn_error(self.__ud)
   if kind == sysbench.sql.error.NONE then
      error("SQL API error", 3)
   end

   local errdesc = {
      connection = self,
      query = query,
      sql_errno = code,
      sql_state = state,
      sql_errmsg = msg
   }

   if kind == sysbench.sql.error.FATAL and
      type(sysbench.hooks.sql_error_ignorable) == "function" and
      sysbench.hooks.sql_error_ignorable(errdesc)
   then
      kind = sysbench.sql.error.IGNORABLE
   end

   if kind == sysbench.sql.error.IGNORABLE then
      errdesc.errcode = sysbench.error.RESTART_EVENT
      error(errdesc, 3)
   end

   error(string.format("SQL error, errno = %d, state = '%s': %s",
                       code, state, msg), 2)
end

function connection_methods.query(self, query)
   local r = ffi_query(self.__p, query, #query)
   if r == DONE or (r == YIELD and yield() ~= nil) then
      return self.__rs
   end
   return self:check_error(nil, query)
end

-- Run `total` copies of query keeping up to `depth` of them in flight on this
-- connection (a steady stream); results are discarded.
function connection_methods.query_stream(self, query, depth, total)
   if not sb_sql_query_stream(self.__ud, query, depth, total) then
      return self:check_error(nil, query)
   end
   return true
end

-- Pipelining: with depth > 1, query() and execute() write the command and
-- return at once with an empty result; a reply is read (and discarded) only
-- when `depth` commands are outstanding.  Any other call on the connection
-- reads the outstanding replies first.  An error reply surfaces on the call
-- that reads it.  depth 0 or 1 turns pipelining off.
function connection_methods.pipeline(self, depth)
   return sb_sql_pipeline(self.__ud, depth)
end

-- Read every outstanding pipelined reply.
function connection_methods.flush(self)
   if not sb_sql_flush(self.__ud) then
      return self:check_error(nil, "<pipeline flush>")
   end
   return true
end

function connection_methods.more_results(self)
   return false
end

function connection_methods.next_result(self)
   return nil
end

function connection_methods.query_row(self, query)
   local rs = self:query(query)
   if rs == nil or rs.nrows == 0 then
      return nil
   end
   return unpack(rs:fetch_row(), 1, rs.nfields)
end

function connection_methods.bulk_insert_init(self, query)
   return sb_sql_bulk_insert_init(self.__ud, query)
end

function connection_methods.bulk_insert_next(self, value)
   return sb_sql_bulk_insert_next(self.__ud, value)
end

function connection_methods.bulk_insert_done(self)
   local ok = sb_sql_bulk_insert_done(self.__ud)
   if not ok then
      return self:check_error(nil, "<bulk insert>")
   end
   return true
end

function connection_methods.prepare(self, query)
   local ud = sb_sql_prepare(self.__ud, query)
   if ud == nil then
      return self:check_error(nil, query)
   end
   return setmetatable({__ud = ud, __p = F.object("sbx_statement", ud),
                        connection = self, closed = false}, statement_mt)
end

function statement_methods.bind_create(self, btype, max_len)
   local ud = sb_sql_bind_create(self.__ud, btype, max_len or 0)
   local p = F.object("sbx_param", ud)
   return setmetatable({__ud = ud, __c = p, __kind = p.kind,
                        __capacity = tonumber(p.capacity)}, param_mt)
end

-- Written straight into the C parameter (sbx_param); kind 0 integer, 1 real,
-- 2 bytes.
function param_methods.set(self, value)
   local p = self.__c
   if value == nil then
      p.is_null = true
      return
   end
   p.is_null = false
   local kind = self.__kind
   if kind == 0 then
      if type(value) ~= "number" then value = tonumber(value) end
      p.integer = value
   elseif kind == 1 then
      if type(value) ~= "number" then value = tonumber(value) end
      p.real = value
   else
      if type(value) ~= "string" then value = tostring(value) end
      local length = #value
      if length > self.__capacity then length = self.__capacity end
      ffi_copy(p.buffer, value, length)
      p.length = length
   end
end

local rand_string = F.rand_string

-- Generated straight into the C parameter's buffer.
function param_methods.set_rand_str(self, fmt)
   if self.__kind ~= 2 then
      error("set_rand_str requires a string bind", 2)
   end
   local p = self.__c
   local length = #fmt
   if length > self.__capacity then length = self.__capacity end
   rand_string(fmt, length, p.buffer)
   p.length = length
   p.is_null = false
end

function statement_methods.bind_param(self, ...)
   local count = select('#', ...)
   local parameters = {...}
   for i = 1, count do
      parameters[i] = parameters[i].__ud
   end
   return sb_sql_bind_param(self.__ud, unpack(parameters, 1, count))
end

function statement_methods.execute(self)
   local r = ffi_execute(self.__p)
   if r == DONE or (r == YIELD and yield() ~= nil) then
      return self.connection.__rs
   end
   return self.connection:check_error(nil, "<prepared statement>")
end

function statement_methods.next_result(self)
   return nil
end

function statement_methods.close(self)
   if not self.closed then
      self.closed = true
      sb_sql_stmt_close(self.__ud)
   end
   return true
end

local field_count = ffi.new("uint32_t[1]")

function result_methods.fetch_row(self)
   local fields = ffi_fetch_row(self.__c, field_count)
   if fields == nil then return nil end
   local row = {}
   for i = 0, field_count[0] - 1 do
      local field = fields[i]
      if not field.is_null then
         row[i + 1] = ffi_string(field.data, field.length)
      end
   end
   return row
end

function result_methods.free(self)
   ffi_result_free(self.__c)
   return true
end
