-- Lua 5.1 compatibility used by the bundled sysbench scripts.
unpack = unpack or table.unpack
getn = getn or table.getn

-- sysbench-compatible Lua runtime (PUC Lua 5.4, no FFI required).
-- Copyright (C) 2026 sysbench-cpp contributors.
-- Derived from sysbench's GPLv2 Lua interface.

sysbench.hooks = sysbench.hooks or {}

function thread_run(thread_id)
   while sb_more_events() do
      sb_event_start()

      local success, ret
      repeat
         success, ret = pcall(event, thread_id)

         if not success then
            if type(ret) == "table" and
               ret.errcode == sysbench.error.RESTART_EVENT
            then
               if sysbench.hooks.before_restart_event then
                  sysbench.hooks.before_restart_event(ret)
               end
            else
               error(ret, 2)
            end
         end
      until success

      if ret then
         break
      end

      sb_event_stop()
   end
end

function sysbench.report_csv(stat)
   local seconds = stat.time_interval
   print(string.format("%.0f,%u,%4.2f,%4.2f,%4.2f,%4.2f,%4.2f,%4.2f,%4.2f,%4.2f",
                       stat.time_total,
                       stat.threads_running,
                       stat.events / seconds,
                       (stat.reads + stat.writes + stat.other) / seconds,
                       stat.reads / seconds,
                       stat.writes / seconds,
                       stat.other / seconds,
                       stat.latency_pct * 1000,
                       stat.errors / seconds,
                       stat.reconnects / seconds))
end

function sysbench.report_json(stat)
   local seconds = stat.time_interval
   io.write(string.format([[
  {
    "time": %4.0f,
    "threads": %u,
    "tps": %4.2f,
    "qps": {
      "total": %4.2f,
      "reads": %4.2f,
      "writes": %4.2f,
      "other": %4.2f
    },
    "latency": %4.2f,
    "errors": %4.2f,
    "reconnects": %4.2f
  }
]], stat.time_total, stat.threads_running,
    stat.events / seconds,
    (stat.reads + stat.writes + stat.other) / seconds,
    stat.reads / seconds, stat.writes / seconds, stat.other / seconds,
    stat.latency_pct * 1000, stat.errors / seconds, stat.reconnects / seconds))
end

function sysbench.report_default(stat)
   local seconds = stat.time_interval
   print(string.format("[ %.0fs ] thds: %u tps: %4.2f qps: %4.2f " ..
                          "(r/w/o: %4.2f/%4.2f/%4.2f) lat (ms,%u%%): %4.2f " ..
                          "err/s %4.2f reconn/s: %4.2f",
                       stat.time_total,
                       stat.threads_running,
                       stat.events / seconds,
                       (stat.reads + stat.writes + stat.other) / seconds,
                       stat.reads / seconds,
                       stat.writes / seconds,
                       stat.other / seconds,
                       sysbench.opt.percentile,
                       stat.latency_pct * 1000,
                       stat.errors / seconds,
                       stat.reconnects / seconds))
end
