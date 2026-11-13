-- Command line compatibility layer.  Option values are exported by the C++
-- runtime into sysbench.opt; this module provides the script-facing helpers.
sysbench.cmdline = sysbench.cmdline or {}
sysbench.cmdline.options = sysbench.cmdline.options or {}
sysbench.cmdline.commands = sysbench.cmdline.commands or {}

sysbench.cmdline.ARG_NULL = 0
sysbench.cmdline.ARG_BOOL = 1
sysbench.cmdline.ARG_INT = 2
sysbench.cmdline.ARG_SIZE = 3
sysbench.cmdline.ARG_DOUBLE = 4
sysbench.cmdline.ARG_STRING = 5
sysbench.cmdline.ARG_LIST = 6
sysbench.cmdline.ARG_FILE = 7
sysbench.cmdline.ARG_MAX = 8

sysbench.cmdline.PARALLEL_COMMAND = true

function sysbench.cmdline.read_cmdline_options()
   return true
end

function sysbench.cmdline.command_defined(name)
   return type(sysbench.cmdline.commands) == "table" and
      sysbench.cmdline.commands[name] ~= nil and
      sysbench.cmdline.commands[name][1] ~= nil
end

function sysbench.cmdline.command_parallel(name)
   return sysbench.cmdline.command_defined(name) and
      sysbench.cmdline.commands[name][2] == sysbench.cmdline.PARALLEL_COMMAND
end

function sysbench.cmdline.call_command(name)
   if not sysbench.cmdline.command_defined(name) then
      return false
   end

   local rc = sysbench.cmdline.commands[name][1]()

   if rc == nil then
      return true
   else
      return rc and true or false
   end
end

function sysbench.cmdline.run_named(name)
   if sysbench.cmdline.command_defined(name) then
      local ok = sysbench.cmdline.call_command(name)
      if ok == false then error("command failed: " .. name, 2) end
      return true
   end
   local fn = _G[name]
   if type(fn) ~= "function" then
      error("unknown command: " .. name, 2)
   end
   local rc = fn()
   if rc == false then error("command failed: " .. name, 2) end
   return true
end

function sysbench.cmdline.print_test_options()
   if type(sysbench.cmdline.options) ~= "table" then return end
   local names = {}
   for name in pairs(sysbench.cmdline.options) do table.insert(names, name) end
   table.sort(names)
   for _, name in ipairs(names) do
      local def = sysbench.cmdline.options[name]
      if type(def) == "table" and def[1] then
         print(string.format("  --%-28s %s", name, def[1]))
      end
   end
end

function sb_run_named(name)
   return sysbench.cmdline.run_named(name)
end
