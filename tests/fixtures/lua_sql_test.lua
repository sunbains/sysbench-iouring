sysbench.cmdline.options = {
   test_option = {"A custom integer option", 3},
   flag_option = {"A custom boolean option", true},
   string_option = {"A custom string option", "abc"}
}

function thread_init()
   assert(sysbench.opt.test_option == 3, "test_option export")
   assert(sysbench.opt.flag_option == true, "flag_option export")
   assert(sysbench.opt.string_option == "abc", "string_option export")

   drv = sysbench.sql.driver()
   con = drv:connect()
   stmt = con:prepare("SELECT ?")
   param = stmt:bind_create(sysbench.sql.type.INT)
   stmt:bind_param(param)
end

function prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()
   con:query("INSERT INTO t VALUES (1)")
   con:disconnect()
end

function event()
   local rs = con:query("SELECT 1")
   local row = rs:fetch_row()
   assert(row[1] == "1", "text result")
   rs:free()

   param:set(7)
   local rs2 = stmt:execute()
   local row2 = rs2:fetch_row()
   assert(row2[1] == "42", "binary result")
   rs2:free()

   con:bulk_insert_init("INSERT INTO t VALUES")
   con:bulk_insert_next("(1)")
   con:bulk_insert_done()
end

function thread_done()
   stmt:close()
   con:disconnect()
end
