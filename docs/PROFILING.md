# Profiling and cache behavior

Profiling was performed on the development host:

- Intel Xeon Platinum 8260, 96 logical CPUs, 2 NUMA nodes
  (node0: 0-23,48-71; node1: 24-47,72-95)
- Linux 6.14
- RelWithDebInfo build, GCC 14

## Tooling

```sh
perf stat -e cycles,instructions,cache-references,cache-misses,\
L1-dcache-loads,L1-dcache-load-misses,LLC-loads,LLC-load-misses \
  ./build/sysbench26 lua/empty-test.lua --threads=4 --events=2000000 run

perf stat -e ... \
  ./build/sysbench26 oltp_point_select ... --threads=4 --time=2 run
```

`perf` did not permit kernel-level PMU access for every event on this host and
multiplexed counters at ~37–50%. Repeated runs varied by 10–20%, so the
numbers below are representative rather than exact. Coherency was additionally
checked with `perf c2c record/report`. `test_numa` uses `move_pages(2)` and
per-worker threads to verify CPU/allocation locality directly.

## What was found and fixed

The first profile (single run) showed 5.99B cycles, 0.67 IPC, 7.86M cache
references and 1.57M LLC misses for 2M Lua events. The main causes:

1. `RunControl::events_started` was a single shared atomic incremented by every
   worker for every event. This caused a cache line to bounce between all
   worker cores even when no event limit was configured.
2. `Runtime::resumes_` was another shared atomic incremented on every
   coroutine resume.
3. `ThreadStats` instances were not cache-line aligned, and latency min/max
   used compare-exchange loops on every event.
4. Rate limiting used a shared atomic next-slot counter.
5. Coroutine frame allocation used one global mutex-protected pool.
6. Worker control blocks and network buffers were first-touched by the main
   thread on one NUMA node, then used by workers on both nodes.

Changes:

- Per-thread event counting with a shared update only every 64 events in
  time-based runs (exact shared counting only for `--events`, where an exact
  stop condition matters more than coherency).
- Per-worker resume accounting; no shared atomic on the resume path.
- `ThreadStats alignas(64)` and owner-local min/max fast paths.
- Per-thread local rate pacing (rate split across benchmark tasks).
- Per-NUMA-node frame pools with 1 MiB slabs and per-thread size-class caches
  (no global frame mutex, no per-frame root allocation).
- Worker control blocks are page-aligned and `mbind`-bound; worker heaps,
  registered buffers, task frames and event arenas are node-local.
- Workers are affined to their node's CPU mask; the main/reporting thread is
  pinned to a reserved CPU on its home node.
- Top-level task frames are allocated on the target worker's node.

## Result

2M Lua events, 4 threads:

| Metric | Before (one run) | Final (range of 3 runs) |
|---|---:|---:|
| Cycles | 5.99B | 3.02–3.58B |
| IPC | 0.67 | 1.02–1.22 |
| LLC loads | 2.85M | 0.15–0.50M |
| LLC misses | 1.57M | 0.09–0.40M |
| Wall time | 0.418 s | 0.203–0.240 s |

PMU multiplexing makes raw counts noisy; repeated runs still showed roughly a
twofold cycle/wall-time improvement after time checks were batched and the
remaining shared counters were sharded.

`perf c2c` initially reported 18 remote HITMs on a hot shared counter line.
After converting event counts, runtime task counts, resumes and failure counts
to cache-line-separated sharded counters indexed by thread ID, a 1-second
4-thread run showed **1 remote HITM** across 6 shared lines, which are
read-mostly option/configuration pages. Event-limit runs use the same sharded
counters; the shared aggregate is read every configured batch, so multi-thread
event limits can overshoot by up to `batch * threads`.

The MySQL point-select path is dominated by protocol and the intentional
submit/reap thread handoff; it measured 0.53 IPC, 1.63% cache-miss and 0.80%
LLC-miss rates (4 tasks), so cache misses are not its bottleneck. NUMA-on
versus NUMA-off measurements showed:

- MySQL point-select, 4 tasks: ~74k vs ~69k events/s
- Lua empty events, 4 tasks: ~0.20–0.24 s vs ~0.39 s for 2M events

The io_uring socketpair benchmark uses one ring per endpoint, registered
buffers, a submitter worker and a dedicated eventfd-driven reaper thread. It
measured ~208k fixed ops/s outside `perf` and ~160k under `perf stat`
(1.39% cache misses, 0.35% LLC misses). The split is deliberate: submitter
threads never synchronously wait; the cross-thread completion handoff is the
cost.

## Allocation / free churn

Reduced by:

- registered network buffers reused for the connection lifetime;
- per-connection reusable MySQL request buffer;
- reusable Lua SQL/bulk buffers;
- 256 KiB event arenas reset in O(1), so normal events do not return blocks to
  the worker heap;
- allocation-free random-string formatting;
- per-node 1 MiB frame slabs + per-thread caches (frame pool throughput rose
  from ~58M to ~123M alloc/free pairs/s in the unit benchmark);
- no per-release OS allocation in steady state.

The `test_memory`, `test_runtime`, `test_io_uring`, `test_mysql_*`, `test_lua`
and `test_numa` targets assert these behaviors and throughput floors.

## Final lock-free submit/reap optimization

The last profiling pass targeted the submit/reap path directly. Findings, in
order of impact:

1. **`IORING_SETUP_COOP_TASKRUN` was the dominant stall.**  In cooperative
   mode the kernel defers ring notification to task work run in the submitting
   task's context.  With a separate reaper thread polling the CQ eventfd, the
   notification did not fire while the submitting worker was spinning in user
   space; it ran only when that worker next entered the kernel.  Every I/O paid
   roughly the full spin budget.  Removing the flag took the MySQL mock from
   ~14k to ~46k queries/s and the io_uring round trip from ~490k to ~747k
   fixed ops/s.
2. **The per-socket reaper could not be woken for shutdown.**  It blocked in
   `io_uring_wait_cqe_timeout` with a 100 ms timeout and was joined when a
   connection closed: 100 ms of dead time per connection teardown.  The ring
   now registers a CQ eventfd and the reaper polls that plus a shutdown
   eventfd; teardown dropped from 100.2 ms to ~0.1 ms, which also removed the
   fixed cost that had masked the real per-query throughput.
3. **Nested task starts went through the ready queue.**  A MySQL query nests
   several `Task<>` levels (`query -> write_command -> write_packet ->
   write_exact`).  `TaskAwaiter::await_suspend` queued each child for another
   scheduler iteration; a 2,000-query run showed ~42,000 worker resumes.
   Starting children with symmetric transfer cut that to ~4,000 (2 per query).
4. **Spin-check granularity was 64 `pause` instructions** (about 3.7 µs on this
   CPU).  Checking the wake sequence every pass, with the clock sampled only
   every 64 passes, moved the handoff latency into the hundreds of
   nanoseconds.  The submitter spin budget is 50 µs and is only used while the
   runtime is undersubscribed.
5. **The mock server dribbled packets.**  It sent each response packet with two
   separate `send()` calls, so the client's read buffer kept emptying and it
   issued ~6 io_uring operations per query instead of 2.  Batching each
   response into one write (as a real server's net-buffer flush does) removed
   four round trips per query.

Result on the same machine (96 vCPU, 2 NUMA nodes, kernel 6.14, GCC 14):

| Path | Before | After |
|---|---|---|
| io_uring fixed-buffer round trip | ~105k ops/s (9.5 µs/op) | ~747k ops/s (1.34 µs/op) |
| MySQL mock query | ~8.2k qps (122 µs/query) | ~46k qps (21.8 µs/query) |
| Connection teardown | 100.2 ms | 0.11 ms |
| Worker resumes per query | ~21 | 2 |

The remaining end-to-end cost is kernel-side: one `io_uring_enter` submission
per operation (~160 ns for a NOP submit/reap pair measured separately) and
socket loopback processing.  The user-space handoff itself - lock-free injection
push, sequentially-consistent sequence bump observed by the spinning worker -
measures a few hundred nanoseconds, and no wait in the path sleeps on an
eventfd while the runtime is undersubscribed.
