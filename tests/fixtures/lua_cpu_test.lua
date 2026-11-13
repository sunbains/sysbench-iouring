sysbench.cmdline.options = {
   local_iters = {"Iterations per event", 32}
}

function thread_init()
   assert(sysbench.opt.local_iters >= 1)
end

function event()
   local sum = 0
   for i = 1, sysbench.opt.local_iters do
      sum = sum + sysbench.rand.uniform(1, 10)
      local formatted = sysbench.rand.string("###-@@")
      assert(#formatted == 6)
      local variable = sysbench.rand.varstring(2, 5)
      assert(#variable >= 2 and #variable <= 5)
   end
   assert(sum > 0)
end
