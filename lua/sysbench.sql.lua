-- SQL API compatible with sysbench, implemented over the C++ async MySQL
-- client.  Calls that perform network I/O yield the Lua coroutine; the C++
-- runtime resumes it from an io_uring completion.

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
local result_mt = { __index = result_methods }

-- A connection has one current result (as in stock sysbench), valid until its
-- next query: the result table is created once and refilled per query, so a
-- query allocates nothing.
local function current_result(connection, ud)
   local rs = connection.__rs
   if rs == nil then
      rs = setmetatable({__ud = ud}, result_mt)
      connection.__rs = rs
   end
   rs.__ud = ud
   rs.nrows, rs.nfields, rs.affected_rows, rs.last_insert_id, rs.warnings,
      rs.status = sb_sql_result_info(ud)
   rs.row_index = 0
   rs.freed = false
   return rs
end
local param_mt = { __index = param_methods }

function sysbench.sql.driver(driver_name)
   local ud = sb_sql_driver(driver_name)
   if ud == nil then
      error("failed to initialize the DB driver", 2)
   end
   return setmetatable({__ud = ud}, driver_mt)
end

function driver_methods.connect(self)
   local ud = sb_sql_connect(self.__ud)
   if ud == nil then
      local code, state, msg = sb_last_error()
      error(string.format("connection creation failed: (%s) %s",
                          tostring(code), tostring(msg)), 2)
   end
   return setmetatable({__ud = ud, closed = false}, connection_mt)
end

function driver_methods.name(self)
   return sb_sql_driver_name(self.__ud)
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
   local ud = sb_sql_query(self.__ud, query)
   if ud == nil then
      return self:check_error(nil, query)
   end
   return current_result(self, ud)
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
   return setmetatable({__ud = ud, connection = self, closed = false},
                       statement_mt)
end

function statement_methods.bind_create(self, btype, max_len)
   local ud = sb_sql_bind_create(self.__ud, btype, max_len or 0)
   return setmetatable({__ud = ud}, param_mt)
end

function param_methods.set(self, value)
   sb_sql_param_set(self.__ud, value)
end

function param_methods.set_rand_str(self, fmt)
   sb_sql_param_set_rand_str(self.__ud, fmt)
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
   local ud = sb_sql_execute(self.__ud)
   if ud == nil then
      return self.connection:check_error(nil, "<prepared statement>")
   end
   return current_result(self.connection, ud)
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

function result_methods.fetch_row(self)
   if self.freed then return nil end
   return sb_sql_result_fetch_row(self.__ud)
end

function result_methods.free(self)
   if not self.freed then
      self.freed = true
      sb_sql_result_free(self.__ud)
   end
   return true
end
