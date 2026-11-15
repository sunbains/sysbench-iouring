-- Exercise the bundled module with a session-aware SQL double. The real Lua
-- engine loads this fixture, discovers the module's options and runs one event.
require("mysql_handler")

local handler_init = init
local handler_thread_init = thread_init
local handler_event = event
local handler_thread_done = thread_done
local handler_restart = sysbench.hooks.before_restart_event

function init() end
function thread_init() end
function thread_done() end

local function equal(actual, expected)
   assert(actual == expected,
          string.format("expected %s, got %s", tostring(expected), tostring(actual)))
end

function event()
   local original_options = sysbench.opt
   local log, connections, frees = {}, {}, 0
   local driver_name = "mysql"
   local driver = {}
   function driver:name() return driver_name end
   function driver:connect()
      local connection = {opened = {}, closed = false}
      connections[#connections + 1] = connection
      function connection:query(sql)
         assert(not self.closed, "query on closed connection")
         log[#log + 1] = sql
         local table_name, operation = sql:match("^HANDLER (sbtest%d+) (%u+)")
         if table_name then
            if operation == "OPEN" then
               assert(not self.opened[table_name], "handler already open")
               self.opened[table_name] = true
            else
               assert(self.opened[table_name], "handler is not open in this session")
               if operation == "CLOSE" then
                  self.opened[table_name] = nil
               else
                  equal(operation, "READ")
               end
            end
         else
            assert(sql:match("^DROP TABLE IF EXISTS sbtest%d+$"), sql)
         end
         return {free = function() frees = frees + 1 end}
      end
      function connection:reconnect()
         assert(next(self.opened) == nil, "handlers must close before reconnect")
         log[#log + 1] = "RECONNECT"
         self.opened = {}
      end
      function connection:disconnect()
         assert(next(self.opened) == nil, "handlers must close before disconnect")
         self.closed = true
         log[#log + 1] = "DISCONNECT"
      end
      return connection
   end
   sysbench.sql.driver = function() return driver end
   sysbench.rand.uniform = function(low, high)
      assert(low <= high)
      return high
   end
   sysbench.rand.default = sysbench.rand.uniform

   local function reset(mode)
      log, connections, frees = {}, {}, 0
      sysbench.opt = {
         handler_mode = mode, handler_reads = 2,
         tables = 2, table_size = 10, range_size = 3,
         secondary = false, reconnect = 0, threads = 1
      }
      handler_init()
      handler_thread_init()
      equal(log[1], "HANDLER sbtest1 OPEN")
      equal(log[2], "HANDLER sbtest2 OPEN")
   end
   local function finish()
      handler_thread_done()
      equal(log[#log - 2], "HANDLER sbtest1 CLOSE")
      equal(log[#log - 1], "HANDLER sbtest2 CLOSE")
      equal(log[#log], "DISCONNECT")
      assert(connections[1].closed)
   end

   reset("point")
   handler_event()
   equal(log[3], "HANDLER sbtest2 READ `PRIMARY` = (10)")
   equal(log[4], log[3])
   equal(frees, 2)
   finish()

   reset("range")
   handler_event()
   equal(log[3], "HANDLER sbtest2 READ `PRIMARY` >= (8) LIMIT 3")
   equal(frees, 2)
   -- A range larger than the table must still use a valid starting ID.
   sysbench.opt.range_size = 20
   handler_event()
   equal(log[5], "HANDLER sbtest2 READ `PRIMARY` >= (1) LIMIT 20")
   finish()

   for _, mode in ipairs({"point", "range"}) do
      reset(mode)
      sysbench.opt.secondary = true
      handler_event()
      assert(log[3]:find("READ `xid`", 1, true))
      finish()
   end

   reset("scan")
   handler_event()
   equal(log[3], "HANDLER sbtest2 READ FIRST LIMIT 3")
   equal(log[4], "HANDLER sbtest2 READ NEXT LIMIT 3")
   handler_event()
   equal(log[5], log[3]) -- each event resets the cursor
   equal(frees, 4)
   finish()

   reset("point")
   sysbench.opt.reconnect = 2
   handler_event()
   equal(#log, 4)
   handler_event()
   equal(log[7], "HANDLER sbtest1 CLOSE")
   equal(log[8], "HANDLER sbtest2 CLOSE")
   equal(log[9], "RECONNECT")
   equal(log[10], "HANDLER sbtest1 OPEN")
   equal(log[11], "HANDLER sbtest2 OPEN")
   handler_event() -- reads must work on the reopened session
   finish()

   reset("point")
   handler_restart({sql_errno = 1213}) -- a deadlock does not lose handlers
   equal(#log, 2)
   for _, code in ipairs({2013, 2055, 2006, 2011}) do
      connections[1].opened = {} -- simulate the caller reconnecting
      handler_restart({sql_errno = code})
      handler_event()
   end
   finish()

   -- Reuse OLTP's parallel preparation and cleanup commands.
   local created = {}
   create_table = function(_, _, number) created[#created + 1] = number end
   sysbench.opt.tables = 5
   sysbench.opt.threads = 2
   sysbench.tid = 1
   cmd_prepare()
   equal(table.concat(created, ","), "2,4")
   equal(sysbench.cmdline.commands.prepare[2], sysbench.cmdline.PARALLEL_COMMAND)
   log = {}
   cleanup()
   equal(#log, 5)
   equal(log[1], "DROP TABLE IF EXISTS sbtest1")
   equal(log[5], "DROP TABLE IF EXISTS sbtest5")

   local function invalid(option, value)
      local previous = sysbench.opt[option]
      sysbench.opt[option] = value
      local ok = pcall(handler_init)
      sysbench.opt[option] = previous
      assert(not ok, "accepted invalid option " .. option)
   end
   invalid("handler_mode", "unknown")
   invalid("handler_reads", 0)
   invalid("handler_reads", 1.5)
   invalid("tables", 0)
   invalid("table_size", -1)
   invalid("reconnect", -1)
   sysbench.opt.handler_mode = "range"
   invalid("range_size", 0)
   driver_name = "pgsql"
   assert(not pcall(handler_init), "accepted a non-MySQL driver")
   sysbench.opt = original_options
end
