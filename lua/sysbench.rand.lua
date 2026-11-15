-- Random number API compatible with sysbench's sysbench.rand.  Everything an
-- event calls goes through the FFI (sysbench.ffi.lua), so it stays in the
-- JIT's trace.
sysbench.rand = {}

local ffi = require("ffi")
local F = sysbench.ffi
local rand_default = F.rand_default
local rand_uniform = F.rand_uniform
local rand_gaussian = F.rand_gaussian
local rand_pareto = F.rand_pareto
local rand_zipfian = F.rand_zipfian
local rand_unique = F.rand_unique
local rand_uniform_double = F.rand_uniform_double
local rand_string = F.rand_string
local rand_varstring = F.rand_varstring
local ffi_string = ffi.string

-- One buffer per VM for generated strings, grown on demand (sysbench
-- allocates one per call).
local buffer_size = 512
local buffer = ffi.new("char[?]", buffer_size)
local function scratch(length)
   if length > buffer_size then
      buffer_size = length
      buffer = ffi.new("char[?]", buffer_size)
   end
   return buffer
end

function sysbench.rand.uniform_uint64()
   return sb_rand_uniform_uint64()
end

function sysbench.rand.default(a, b)
   return rand_default(a, b)
end

function sysbench.rand.uniform(a, b)
   return rand_uniform(a, b)
end

function sysbench.rand.gaussian(a, b)
   return rand_gaussian(a, b)
end

function sysbench.rand.pareto(a, b)
   return rand_pareto(a, b)
end

function sysbench.rand.zipfian(a, b)
   return rand_zipfian(a, b)
end

function sysbench.rand.unique()
   return rand_unique()
end

function sysbench.rand.string(fmt)
   local length = #fmt
   local out = scratch(length)
   rand_string(fmt, length, out)
   return ffi_string(out, length)
end

function sysbench.rand.varstring(min_len, max_len)
   assert(min_len <= max_len)
   assert(max_len > 0)
   local out = scratch(max_len)
   return ffi_string(out, rand_varstring(min_len, max_len, out))
end

function sysbench.rand.uniform_double()
   return rand_uniform_double()
end
