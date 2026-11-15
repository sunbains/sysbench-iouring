#!/usr/bin/env sysbench
-- Copyright (C) 2026 Sunny Bains
-- SPDX-License-Identifier: GPL-2.0-or-later

-- sql-rust revision 2 extends MySQL with INSERT ... RETURNING. Reuse the
-- insert benchmark's dataset commands and connection/reconnect lifecycle.
require("oltp_insert")

local common_init = init
function init()
   common_init()
   assert(sysbench.opt.mysql_protocol == 2,
          "oltp_insert_returning requires --mysql-protocol=2 and a sql-rust server")
end

function event()
   local t = sysbench.rand.uniform(1, sysbench.opt.tables)
   local id = sysbench.opt.auto_inc and 0 or sysbench.rand.unique() - 2147483648
   local k = sysbench.rand.default(1, sysbench.opt.table_size)
   local result = con:query(string.format(
      "INSERT INTO sbtest%d (id, k, c, pad) VALUES (%d, %d, '%s', '%s') RETURNING id",
      t, id, k, get_c_value(), get_pad_value()))
   assert(result.nrows == 1 and result.affected_rows == 1,
          "INSERT RETURNING must return one inserted row and its affected-row count")
   assert(result:fetch_row()[1] ~= nil, "INSERT RETURNING did not return an ID")
   result:free()
   check_reconnect()
end
