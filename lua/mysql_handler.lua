#!/usr/bin/env sysbench
-- Copyright (C) 2026 Sunny Bains
-- SPDX-License-Identifier: GPL-2.0-or-later

-- Exercise MySQL's SQL HANDLER interface over the text protocol. Dataset
-- preparation, warmup and cleanup use the standard OLTP sbtest tables.
require("oltp_common")

sysbench.cmdline.options.handler_mode =
   {"HANDLER read mode: point, range, scan", "point"}
sysbench.cmdline.options.handler_reads =
   {"Number of HANDLER READ commands per event", 1}

local common_init = init

local function positive_integer(name)
   local value = sysbench.opt[name]
   assert(value > 0 and value == math.floor(value),
          name:gsub("_", "-") .. " must be a positive integer")
end

function init()
   common_init()
   assert(sysbench.sql.driver():name() == "mysql",
          "mysql_handler requires the MySQL driver")
   assert(sysbench.opt.handler_mode == "point" or
          sysbench.opt.handler_mode == "range" or
          sysbench.opt.handler_mode == "scan",
          "handler-mode must be point, range, or scan")
   positive_integer("tables")
   positive_integer("table_size")
   positive_integer("handler_reads")
   if sysbench.opt.handler_mode ~= "point" then
      positive_integer("range_size")
   end
   assert(sysbench.opt.reconnect >= 0 and
          sysbench.opt.reconnect == math.floor(sysbench.opt.reconnect),
          "reconnect must be a non-negative integer")
end

local function open_handlers()
   for t = 1, sysbench.opt.tables do
      con:query(string.format("HANDLER sbtest%d OPEN", t))
   end
end

local function close_handlers()
   for t = 1, sysbench.opt.tables do
      con:query(string.format("HANDLER sbtest%d CLOSE", t))
   end
end

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()
   transactions = 0
   open_handlers()
end

function thread_done()
   close_handlers()
   con:disconnect()
end

function event()
   local t = sysbench.rand.uniform(1, sysbench.opt.tables)
   local index = sysbench.opt.secondary and "`xid`" or "`PRIMARY`"

   for i = 1, sysbench.opt.handler_reads do
      local query
      if sysbench.opt.handler_mode == "point" then
         local id = sysbench.rand.default(1, sysbench.opt.table_size)
         query = string.format("HANDLER sbtest%d READ %s = (%d)", t, index, id)
      elseif sysbench.opt.handler_mode == "range" then
         local last_start = math.max(1, sysbench.opt.table_size -
                                       sysbench.opt.range_size + 1)
         local id = sysbench.rand.default(1, last_start)
         query = string.format("HANDLER sbtest%d READ %s >= (%d) LIMIT %d",
                               t, index, id, sysbench.opt.range_size)
      else
         -- Each event starts a new scan; subsequent reads advance its cursor.
         query = string.format("HANDLER sbtest%d READ %s LIMIT %d", t,
                               i == 1 and "FIRST" or "NEXT",
                               sysbench.opt.range_size)
      end
      con:query(query):free()
   end

   transactions = transactions + 1
   if sysbench.opt.reconnect > 0 and
      transactions % sysbench.opt.reconnect == 0 then
      close_handlers()
      con:reconnect()
      open_handlers()
   end
end

-- A new session has no open handlers. Replace oltp_common's prepared-statement
-- recovery hook for callers that reconnect before restarting a failed event.
function sysbench.hooks.before_restart_event(errdesc)
   if errdesc.sql_errno == 2013 or errdesc.sql_errno == 2055 or
      errdesc.sql_errno == 2006 or errdesc.sql_errno == 2011 then
      open_handlers()
   end
end
