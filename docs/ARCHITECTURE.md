# Architecture

An illustrated, animated walkthrough is in [architecture.html](architecture.html).

## Memory hierarchy

```
RootArena (one mmap, MAP_NORESERVE, optional MAP_POPULATE)
├── NUMA node 0
│   ├── Worker HeapArena(s)    (mbind node 0, boundary-tag, 4–64 MiB)
│   │   ├── LuaState allocations
│   │   ├── MySQLConnection / PreparedStatement / requests / TLS state
│   │   ├── ThreadStats
│   │   └── per-task BumpArena (event results, 256 KiB base block)
│   │       └── ScopedAllocator (reset each event)
│   ├── worker control blocks  (page-aligned, mbind node 0)
│   ├── per-socket rings/registered buffers (mbind node 0)
│   └── frame slabs + frame free lists / thread caches (node 0)
├── NUMA node 1
│   └── ... same layout ...
└── Main HeapArena             (home node: options, bootstrap Lua, stats)
```

- `RootArena` is the only OS mapping. Every other allocator carves from it.
- `HeapArena` is a coalescing boundary-tag allocator with segregated bins,
  used for long-lived objects where Lua, statements and results can be freed in
  arbitrary order.
- `BumpArena` grows in chained blocks from its parent heap. `ScopedAllocator`
  marks and rewinds it in O(1). Events use one scope; results are valid until
  the next event. This removes per-query frees and GC pressure from the hot
  path.
- `FramePool` recycles coroutine frames through 64-byte size classes up to
  8 KiB plus power-of-two large classes. Frames are carved from the root and
  never returned to the OS. A magic guard detects invalid/double frees.
- `mem::Allocator<T>`, `Vector<T>` and `String` make STL containers use a
  chosen resource. The application never calls `new`/`malloc` directly.

## Runtime and scheduling

`Runtime` starts at most **64** worker threads and owns *every* thread in the
process: workers, the per-socket reaper threads, and any fixture thread created
through `Runtime::start_thread()`.  All spin, yield, sleep and wake decisions
are made by the Runtime scheduler; no other module contains a spin loop.
Each `Worker` is page-aligned, bound to one NUMA node, and owns:

- a node-bound `HeapArena`;
- intrusive local ready/completed/deferred/timer lists;
- a lock-free power-of-two injection ring for cross-thread spawns and I/O
  completion handoff, with an eventfd used only to wake a parked worker;
- a `wake_seq` counter that a spinning worker polls.

Worker assignment is round-robin across online NUMA nodes. Each worker is
affined to a CPU mask containing only its node's CPUs (excluding the main
thread's reserved home-node CPU), so it cannot migrate across nodes. Worker
heaps, control blocks, registered buffers and frame slabs are `mbind`-bound
before first touch. The main/reporting thread is pinned to a reserved CPU on
its home node.

`Task<T>` promises are lazy. Only the runtime starts them. Awaiting a child
schedules it on the same worker. Tasks are never stolen, because Lua states,
connections, arenas and registered buffers are thread-affine. The final awaiter
uses symmetric transfer. Awaited child frames are destroyed on a per-worker
deferred list after the current resume unwinds; top-level frames are destroyed
by the completion queue.

Timer and rate-limit sleeps are worker-local user-space timers.  Workers never
wait for io_uring completions directly; a Runtime-owned reaper posts completed
coroutines into the worker's injection ring.

Scheduling is subscription-aware.  `Runtime::undersubscribed()` is true while
the number of live top-level tasks does not exceed the worker count.  In that
state a worker that has I/O outstanding spins up to 50 microseconds on its
`wake_seq` (checking every pass, sampling the clock only periodically) and a
reaper spins on its CQ before sleeping.  When tasks outnumber workers the CPU
is more valuable than latency: workers and reapers park immediately, so a
runnable task - even one whose I/O is on another socket - gets the cycle.  When
`threads > workers`, `sb_more_events()` also yields every 128 iterations.

## io_uring transport

- **One io_uring per socket**, with its own registered buffer pool (read/write
  slots) bound to the owning worker's NUMA node.
- **Submit and reap are split across threads.** The socket-owning runtime
  worker is the sole SQ submitter: it drains a 512-entry lock-free
  power-of-two ring and calls non-blocking `io_uring_submit`; it never waits
  for completions.
- Each ring has a **Runtime-owned, node-pinned reaper thread**.  The reaper
  does not use `io_uring_enter` to wait: the ring has an eventfd registered via
  `io_uring_register_eventfd`, and the reaper blocks in `poll()` on it plus a
  shutdown eventfd.  This gives zero idle wakeups and immediate teardown.
- **No `IORING_SETUP_COOP_TASKRUN`.**  That mode defers completion
  notification to task work run in the submitting task's context.  Because the
  submitter is a worker that may be spinning in user space and the waiter is a
  different thread, notification would stall until the submitter next entered
  the kernel - adding roughly the whole spin budget to every operation.
- The Runtime decides whether the reaper spins (undersubscribed) or blocks
  (oversubscribed); `SocketIo` contains no spin or yield.
- `IoCompletion` objects live in coroutine frames; user_data points at them,
  so a completion requires no allocation.
- Network reads/writes always use registered buffers. Connect/accept are
  native io_uring operations.
- Ring/buffer/reaper state is released with the connection.  The reaper thread
  is woken through a shutdown eventfd and joined by the Runtime.

## Database interface

```
db::DriverRegistry → db::Driver → db::Connection
                                   ├── query(sql, resource) -> Task<Result<ResultSet, Error>>
                                   ├── prepare(sql) -> Task<Result<PreparedStatement*, Error>>
                                   └── reconnect() -> Task<Result<void, Error>>

db::PreparedStatement::execute(binds, resource) -> Task<Result<ResultSet, Error>>
db::PreparedStatement::close() -> Task<Result<void, Error>>
driver->connect(options, resource) -> Task<Result<Connection*, Error>>
```

Every fallible operation returns the Rust-style `sb::Result<T, E>`; call sites
branch on `is_err()`/`value()` explicitly instead of relying on exceptions.
The interface is protocol-neutral. `mysql::register_driver()` installs the only
implementation. Drivers return results allocated from a caller-provided
resource; this lets the Lua layer choose event-lifetime storage.

### MySQL client

`PacketChannel` wraps a non-blocking socket, two registered buffer leases, the
MySQL packet sequence and chunked packet framing. `MysqlConnection` implements:

- Handshake v10 and capability negotiation (protocol 4.1, plugin auth, secure
  connection, multi-result, optional TLS, no deprecated EOF).
- `mysql_native_password` and `caching_sha2_password`:
  - fast auth SHA-256 scramble;
  - full auth public-key request and RSA-OAEP/SHA-1 encryption;
  - auth-switch handling.
- COM_QUERY text result sets, COM_STMT_PREPARE/EXECUTE/CLOSE with binary row
  decoding, COM_QUIT-free close, COM_STMT metadata parsing.
- Transparent text fallback for commands MySQL 8 rejects in the prepared
  protocol (notably BEGIN/COMMIT).
- Multi-result draining, chunked bulk inserts, error packet decoding into a
  heap-free `db::Error`.

`mysql_wire` is a pure codec and can be fuzzed/tested without sockets.

## Lua bridge

Each benchmark task has its own `lua_State` created with a custom allocator on
the worker heap. The original internal modules are replaced by PUC Lua 5.4
modules that call a small C API (`sb_sql_*`, `sb_rand_*`, `sb_event_*`).

The key mechanism is coroutine suspension:

1. `con:query()` invokes a C function.
2. The C function records the operation and calls `lua_yield`.
3. The C++ task driving `thread_run` performs the MySQL I/O with `co_await`.
4. The C++ layer pushes the result onto the suspended Lua stack and calls
   `lua_resume`.
5. Lua's `pcall(event)` sees the query result as a normal return value.

This keeps the original single-threaded event code unchanged, including
`pcall`, restart-event handling, hooks and `thread_run`.

Report hooks run on a separate bootstrap state on the main thread; benchmark
states live only on workers.

## TLS transport

`PacketChannel` optionally wraps the plaintext MySQL byte stream in TLS:

1. After HandshakeV10 and capability negotiation, it sends a 32-byte
   `SSLRequest` packet with `CLIENT_SSL`.
2. `TlsSession` creates an `SSL_CTX` and an `SSL` object whose read/write BIOs
   are memory BIOs. `SSL_connect`, `SSL_read` and `SSL_write` never touch the
   socket directly.
3. Encrypted bytes are moved between those BIOs and the socket with the same
   io_uring fixed-buffer read/write helpers used by plain MySQL.
4. Once handshaken, MySQL packet payloads are encrypted before crossing the
   ring; packet sequencing/parsing is unchanged.
5. Full caching_sha2/sha256 auth sends a NUL-terminated password over TLS
   instead of doing an RSA key exchange. `mysql_clear_password` is accepted only
   over TLS. Verification can use system roots or `--mysql-ssl-ca`, with
   hostname/IP checks; client cert/key files are optional.

## Allocation churn and buffer reuse

- Network buffers are registered and leased for the connection lifetime.
- MySQL connection request packets reuse a per-connection `Bytes` buffer.
- Lua SQL text buffers and bulk-insert buffers retain capacity across events.
- Event result memory uses a 256 KiB per-task bump arena reset in O(1); ordinary
  OLTP events do not hit the worker heap after warmup.
- Random-string APIs format directly into stack or event-arena bytes; no
  per-call heap strings.
- Coroutine frames use node slabs plus per-thread free caches.
- TLS plaintext/encrypted buffers are members of the connection, not per-call.
- Non-trivial API types are passed by const reference; only move-only results
  are returned by value.

## Sharded counters

Hot counters never share a cache line:

- `ShardedCounter<T, N>` stores one cache-line-aligned atomic per shard.
  Updates use the caller's thread ID (`runtime worker ID` or benchmark
  `sysbench.tid`) as the shard index with relaxed ordering.
- Runtime task counters (`outstanding`, `resumes`) are sharded by worker ID.
- Lua event-limit counting is sharded by benchmark thread ID. Multi-threaded
  event-limit checks aggregate only the active shards at a batch interval
  (up to 64 events), so the stop point can overshoot by at most
  `batch * threads`; single-thread limits are exact.
- Benchmark failures are sharded by worker ID.
- `ThreadStats` is one per benchmark thread ID; latency histograms and all
  event/query counters are contained in that cache-line-aligned shard.
- `IoRing` counters are naturally per-socket (one ring per socket).
- Routing cursors and allocator state (root bump offset, task outstanding
  bookkeeping) are not statistics counters; they use atomics only where a
  global ordering is required.

## Statistics

`ThreadStats` is cacheline-separated atomics: event/query/error/reconnect
counters, latency min/max/sum and a log-binned histogram. Reports aggregate
without stopping workers. `StatsSnapshot::difference()` computes interval
statistics from histogram differences.

## Test strategy

Every subsystem has a separately linked test executable:

| Target | Coverage |
|---|---|
| `test_memory` | alignment, exhaustion, coalescing, nested scopes, leaks, frame reuse, throughput |
| `test_runtime` | task values, exceptions, timers, worker-affine allocators, spawn/yield throughput |
| `test_io_uring` | per-socket registered buffers, fixed read/write, connect/accept, submit/reap split, IOPS |
| `test_mysql_wire` | framing, lenenc, handshake, auth vectors, RSA roundtrip, rows, prepared payload, decode throughput |
| `test_mysql_connection` | full handshake/query/prepared flow against a coroutine, io_uring-only MySQL server, caching_sha2 fast auth, QPS |
| `test_stats` | classification, percentiles, aggregation, interval deltas, throughput |
| `test_options` | two-pass CLI parsing, typed values, custom options, errors, lookup throughput |
| `test_lua` | original-style scripts, custom options, SQL text/prepared/bulk, command dispatch, coroutine mock server, event throughput |
| `test_numa` | topology, mbind locality of heaps/frames, 64-worker cap, per-worker CPU/allocation node checks |

Performance assertions use generous lower bounds so they detect order-of-
magnitude regressions without being machine-specific. Set
`SYSBENCH_SKIP_PERF=1` to skip them.

`test_numa` additionally verifies topology, mbind locality of heaps/frames,
the 64-worker cap, and that worker CPUs and allocations belong to the worker's
NUMA node. See `docs/PROFILING.md` for cache/profiling methodology and results.

## Conventions

- Class data members use the `m_` prefix (`m_ring`, `m_worker_id`).
- All fallible APIs return `Result<T, E>`; errors are values, not exceptions.
- All socket I/O is a coroutine awaiter, including test fixtures.
- Only the Runtime creates threads or spins/yields.
- Hot-path cost is measured and reported in nanoseconds.
