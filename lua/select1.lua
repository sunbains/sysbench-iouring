-- Copyright (C) 2026 Sunny Bains
--
-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

-- Floor test: SELECT 1 through the full client/server round trip, with no
-- storage-engine work.  --stream-depth=N keeps N queries in flight per
-- connection (pipelining) instead of waiting for each reply.

sysbench.cmdline.options = {
   stream_depth =
      {"Queries in flight per connection (pipelining); 1 waits for every reply", 1},
}

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()
   if sysbench.opt.stream_depth > 1 then
      con:pipeline(sysbench.opt.stream_depth)
   end
end

function event()
   con:query("SELECT 1")
end

function thread_done()
   con:flush()
   con:disconnect()
end

function prepare() end
function cleanup() end
