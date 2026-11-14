function thread_init()
   con = sysbench.sql.driver():connect()
   stmt = con:prepare("INSERT INTO t(c) VALUES (?) RETURNING id")
   param = stmt:bind_create(sysbench.sql.type.CHAR, 10)
   stmt:bind_param(param)
end

function event()
   local result = con:query(
      "INSERT INTO t(c) VALUES (''), ('41') RETURNING c, id, nullable")
   assert(result.nrows == 2 and result.nfields == 3)
   assert(result.affected_rows == 2 and result.last_insert_id == 41)
   assert(result.warnings == 3 and result.status == 2)
   local row = result:fetch_row()
   assert(row[1] == "" and row[2] == "41" and row[3] == nil)
   row = result:fetch_row()
   assert(row[1] == "41" and row[2] == "42" and row[3] == "x")
   assert(result:fetch_row() == nil)
   result:free()

   param:set("value")
   result = stmt:execute()
   assert(result.nrows == 1 and result.affected_rows == 1)
   assert(result.last_insert_id == 42 and result:fetch_row()[1] == "42")
   result:free()

   result = con:query("SELECT 1")
   assert(result.nrows == 1 and result:fetch_row()[1] == "1")
   result:free()
end

function thread_done()
   stmt:close()
   con:disconnect()
end
