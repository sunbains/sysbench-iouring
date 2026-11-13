-- Random number API compatible with sysbench's sysbench.rand.
sysbench.rand = {}

function sysbench.rand.uniform_uint64()
   return sb_rand_uniform_uint64()
end

function sysbench.rand.default(a, b)
   return sb_rand_default(a, b)
end

function sysbench.rand.uniform(a, b)
   return sb_rand_uniform(a, b)
end

function sysbench.rand.gaussian(a, b)
   return sb_rand_gaussian(a, b)
end

function sysbench.rand.pareto(a, b)
   return sb_rand_pareto(a, b)
end

function sysbench.rand.zipfian(a, b)
   return sb_rand_zipfian(a, b)
end

function sysbench.rand.unique()
   return sb_rand_unique()
end

function sysbench.rand.string(fmt)
   return sb_rand_str(fmt)
end

function sysbench.rand.varstring(min_len, max_len)
   assert(min_len <= max_len)
   assert(max_len > 0)
   return sb_rand_varstr(min_len, max_len)
end

function sysbench.rand.uniform_double()
   return sb_rand_uniform_double()
end
