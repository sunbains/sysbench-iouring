# sysbench-cpp

A C++26 port of [sysbench](https://github.com/akopytov/sysbench), a great tool
that I have used forever. This port is built around a strict hierarchy of
pre-reserved allocators, a coroutine runtime that owns all worker scheduling,
io_uring with registered network buffers, an async MySQL wire client, and a
sysbench-compatible Lua benchmark interface.

The reference implementation from [sysbench](https://github.com/akopytov/sysbench)
was used for the Lua API, benchmark scripts, command names, option semantics,
statistics and output formats.

## Highlights

- **C++26**, coroutines, lambdas, no application-level `new`/`malloc` calls.
- **Hierarchical memory**: one up-front root reservation; worker heaps;
  per-event scoped arenas; Lua and MySQL allocations all flow through it.
- **Runtime-owned scheduling**: a hard cap of 64 worker threads.  Every thread
  in the process - workers, per-socket reapers and test fixtures - is created,
  tracked and joined by the Runtime, and **all spin/yield/sleep decisions live
  in the Runtime scheduler**.  It discovers NUMA topology, binds node-local
  worker heaps/control blocks, pins node-affinity CPU masks, and uses intrusive
  coroutine queues plus symmetric transfer.  When the runtime is
  *undersubscribed* (tasks <= workers) workers and reapers briefly spin for I/O
  completions; when *oversubscribed* they park, so the CPU goes to whatever
  task is runnable - including one waiting on another socket.
- **io_uring**: **one ring per socket**. The owning worker is the sole SQ
  submitter and drains a lock-free power-of-two submission ring.  A
  Runtime-owned, node-pinned reaper thread per ring blocks in `poll()` on the
  ring's CQ eventfd, reaps completions and posts suspended coroutines back to
  the worker.  Rings are deliberately created without
  `IORING_SETUP_COOP_TASKRUN`: that mode defers completion notification to task
  work run in the submitting task's context, which stalls the eventfd while the
  submitter spins in user space.  All socket reads/writes use
  `READ_FIXED`/`WRITE_FIXED` over the socket's node-bound registered buffers;
  connect/accept also use io_uring.  Every socket operation is a coroutine
  awaiter - there is no blocking I/O anywhere, including the test servers.
- **Rust-style `Result<T, E>`** on every fallible interface
  (`sb::Result`): query, prepare, execute, connect, reconnect and statement
  close return `Result<T, db::Error>` from their coroutines, so error handling
  is explicit at each call site rather than implicit exception propagation.
- **MySQL protocol behind a DB interface**: text queries, multi-result
  draining, prepared statements (binary protocol), bulk inserts,
  `mysql_native_password` and `caching_sha2_password` fast/full RSA auth,
  plus configurable TLS (`off` / `preferred` / `required`, optional CA and
  client certificate). Adding another protocol only requires a new
  `db::Driver` implementation and registry entry.
- **Sharded counters** for events, task counts, resumes, failures and all
  per-thread latency/query statistics; each cache-line-aligned shard is indexed
  by the owning worker or benchmark thread ID.
- **Lua 5.4 compatibility layer**: original-style `sysbench.rand`,
  `sysbench.sql`, `sysbench.cmdline`, `sysbench.hooks`, `sysbench.histogram`,
  `thread_init`/`event`/`thread_done`, custom commands and custom options.
  Calls such as `con:query()` yield the Lua coroutine while C++ awaits
  io_uring.
- **Independent tests** for every subsystem plus performance assertions.

## Requirements

- Linux with io_uring (kernel 5.19+; tested on 6.14)
- CMake 3.28+, Ninja recommended
- GCC 14+ or Clang 20+ with C++26 coroutines
- `liburing`
- `libnuma` development files
- Lua 5.4 development files
- OpenSSL development files (caching_sha2 RSA auth and TLS)
- Cargo/Rust for the optional sql-rust protocol revision 2 client

Ubuntu:

```sh
sudo apt install build-essential cmake ninja-build liburing-dev libnuma-dev liblua5.4-dev libssl-dev
```

## Build

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
```

Optional: `-DSYSBENCH_NATIVE=ON`, `-DSYSBENCH_WERROR=ON`.

Protocol revision 2 uses the Rust library from a sibling `mysql-client-lib`
checkout and is enabled automatically when that checkout exists. To select
another location or explicitly enable it:

```sh
cmake -S . -B build -DSYSBENCH_MYSQL_V2=ON \
  -DSYSBENCH_MYSQL_CLIENT_LIB_DIR="$HOME/dev/mysql-client-lib"
cmake --build build -j
```

`-DSYSBENCH_MYSQL_V2=OFF` builds without Rust. Cargo artifacts and compiler
scratch files for the bridge are kept under the build directory.

## Test

```sh
ctest --test-dir build --output-on-failure       # correctness
ctest --test-dir build -L perf --output-on-failure
```

Individual subsystem binaries:

```
build/tests/test_memory
build/tests/test_runtime
build/tests/test_io_uring
build/tests/test_mysql_wire
build/tests/test_mysql_connection
build/tests/test_stats
build/tests/test_options
build/tests/test_lua
build/tests/test_numa
```

`SYSBENCH_SKIP_PERF=1` skips throughput assertions.

## Quick start

CPU benchmark:

```sh
./build/sysbench26 lua/prime-test.lua --threads=8 --time=10 run
```

Bundled test names resolve under `lua/`:

```sh
./build/sysbench26 empty-test --threads=4 --time=5 run
```

MySQL dataset and OLTP benchmark:

```sh
./build/sysbench26 oltp_read_write \
  --mysql-host=127.0.0.1 --mysql-port=3306 \
  --mysql-user=bench --mysql-password=secret --mysql-db=sbtest \
  --tables=4 --table-size=10000 --threads=8 prepare

./build/sysbench26 oltp_read_write \
  --mysql-host=127.0.0.1 --mysql-port=3306 \
  --mysql-user=bench --mysql-password=secret --mysql-db=sbtest \
  --tables=4 --table-size=10000 --threads=8 --time=60 \
  --report-interval=1 --histogram run

# The same benchmark over TLS, verifying the server certificate:
./build/sysbench26 oltp_read_write ... \
  --mysql-ssl=required --mysql-ssl-verify-server-cert=on \
  --mysql-ssl-ca=/etc/mysql/ca.pem run

./build/sysbench26 oltp_read_write ... cleanup
```

All reference OLTP variants are included: `oltp_point_select`,
`oltp_read_only`, `oltp_write_only`, `oltp_read_write`, `oltp_insert`,
`oltp_delete`, `oltp_update_index`, `oltp_update_non_index`,
`select_random_points`, `select_random_ranges`, `bulk_insert`.

The `mysql_handler` module exercises MySQL's
[SQL HANDLER interface](https://dev.mysql.com/doc/refman/8.4/en/handler.html).
It uses the same OLTP tables and connection options:

```sh
./build/sysbench26 mysql_handler --mysql-db=sbtest --tables=4 --table-size=10000 prepare
./build/sysbench26 mysql_handler --mysql-db=sbtest --tables=4 --table-size=10000 \
  --handler-mode=point --handler-reads=1 --threads=8 --time=60 run
./build/sysbench26 mysql_handler --mysql-db=sbtest --tables=4 cleanup
```

`--handler-mode=point` (default) reads random IDs through the primary key;
`range` reads from a random ID with `--range-size` rows per command;
`scan` issues `READ FIRST`, then `READ NEXT` for subsequent commands in the
event. `--handler-reads` sets commands per event (default 1). Scans restart
each event and may return empty results after reaching the end of a table.
`--secondary=on` uses the OLTP `xid` index for point/range reads.
Each worker opens its own handlers, closes them on completion, and reopens
them after `--reconnect=N` events. Use a storage engine that supports HANDLER,
such as InnoDB or MyISAM. HANDLER returns all columns and does not provide
the same consistency guarantees as SELECT.

Script `help`:

```sh
./build/sysbench26 oltp_read_write --help
./build/sysbench26 oltp_read_write help
```

For a sql-rust server, `--mysql-protocol=2` selects the Rust client and sends
the connection attribute `sqlrust_protocol=2` on every connection, including
reconnects. This enables the server's PostgreSQL-style `INSERT ... RETURNING`
extension; it is a sql-rust revision of the MySQL protocol. Standard MySQL
servers do not implement this SQL extension. The default revision 1 uses the
existing C++ client.

```sh
./build/sysbench26 oltp_insert_returning --mysql-protocol=2 --mysql-db=sbtest prepare
./build/sysbench26 oltp_insert_returning --mysql-protocol=2 --mysql-db=sbtest \
  --threads=4 --time=10 run
./build/sysbench26 oltp_insert_returning --mysql-protocol=2 --mysql-db=sbtest cleanup
```

Both `con:query()` and prepared `stmt:execute()` expose returned rows through
`fetch_row()` alongside `affected_rows`, `last_insert_id`, `warnings`, and
`status`. The Rust adapter retains the statement counts from the final OK
packet and drains additional results before the next command. Its Rust
allocations use the project's arena hierarchy, and socket I/O still uses the
runtime's registered io_uring buffers. Each connection also reserves a 256 KiB
stack to adapt the Rust client's synchronous transport callbacks to coroutines.
The Rust library supports TLS 1.2/1.3 with `--mysql-ssl=preferred|required`,
optional CA/hostname verification, and client certificates using the existing
`--mysql-ssl-*` options. `caching_sha2_password` full authentication is supported
over TLS. Preferred mode falls back only when the server does not advertise
TLS; certificate or TLS handshake failures return errors.

```sh
./build/sysbench26 oltp_insert_returning --mysql-protocol=2 --mysql-db=sbtest \
  --mysql-ssl=required --mysql-ssl-verify-server-cert=on \
  --mysql-ssl-ca=/path/to/ca.pem run
```

## Important options

| Option | Meaning |
|---|---|
| `--threads=N` | Benchmark tasks (coroutines), not OS threads |
| `--workers=N` | Runtime worker threads, capped at 64 (0 = min(threads, CPUs)) |
| `--numa=on/off` | NUMA-aware placement and node-local memory (default on) |
| `--pin-workers=on/off` | Pin workers to their node's CPU mask (default on) |
| `--time=S`, `--events=N`, `--rate=N` | Stop conditions / aggregate event rate |
| `--warmup-time=S` | Warm up before recording statistics |
| `--report-interval=S` | Intermediate reports; 0 disables |
| `--percentile=N`, `--histogram` | Latency reporting |
| `--memory-size=4G` | Up-front root reservation (virtual with `MAP_NORESERVE`) |
| `--populate-memory` | Fault in/resident the root reservation up front |
| `--db-driver=mysql` | Only `mysql` is registered in this build |
| `--mysql-host/port/user/password/db/socket` | Connection options |
| `--mysql-protocol=1/2` | C++ MySQL client (default 1), or Rust sql-rust revision 2 client |
| `--mysql-ssl=off/preferred/required` | MySQL TLS mode (default off) |
| `--mysql-ssl-verify-server-cert` | Verify CA and hostname/IP |
| `--mysql-ssl-ca`, `--mysql-ssl-cert`, `--mysql-ssl-key` | TLS trust/client identity files |
| `--seed=N`, `--rand-seed=N` | Deterministic per-thread RNG seed |
| `--workers` smaller than `--threads` | Tasks yield periodically for fairness |

Application memory:

- The root `mmap` is the only OS memory allocation made by the project.
- Worker heaps, event arenas, Lua states, MySQL packets, result sets, options,
  strings, vectors and coroutine frames all use the custom hierarchy.
- Worker memory is **NUMA-local**: worker heap ranges, worker control blocks,
  registered buffer pools and per-node frame slabs are `mbind`-bound before
  first touch. Worker threads are pinned to a CPU mask containing only their
  node's CPUs, and the main/reporting thread runs on its own reserved home-node
  CPU. `test_numa` verifies CPU and allocation locality.
- Coroutine frames use per-NUMA-node slab pools with per-thread size-class
  caches; memory is recycled and never returned to the OS.
- Per-query churn is reduced with reusable MySQL request buffers, reusable Lua
  SQL/bulk buffers, registered network buffers, a 256 KiB per-event arena, and
  allocation-free random-string formatting.
- libc/pthread/OpenSSL/Lua-library-internal bookkeeping allocations outside the
  project's control are not intercepted.

## Measured on this machine

96 vCPU Linux 6.14, 2 NUMA nodes, local MySQL 8.4, RelWithDebInfo:

| Path | Result |
|---|---|
| Bump allocation | ~218 Mops/s (4.6 ns/op) |
| Heap alloc+free pairs | ~62 Mops/s (16 ns/pair) |
| Coroutine frame alloc+free (thread cache) | ~130 Mops/s (7.7 ns/pair) |
| Runtime spawn+run | ~0.6 M tasks/s (1.7 µs/task) |
| Coroutine yield | ~21 M switches/s (48 ns/switch) |
| io_uring fixed-buffer socketpair, submit/reap split | ~750k ops/s (1.33 µs/op) |
| MySQL mock round trip (write + read + server) | ~21.8 µs/query |
| MySQL text row decode | ~52 M rows/s |
| Lua CPU events | ~3.4 M events/s |
| MySQL `oltp_point_select`, 4 tasks | ~74k events/s |
| MySQL `oltp_read_write`, 4 tasks | ~48k qps / ~2.4k transactions/s |
| MySQL `oltp_read_write` over TLS, 4 tasks | ~34k qps / ~1.7k transactions/s |

The per-operation cost inside this code is now in nanoseconds: the
reaper-to-worker completion handoff is a lock-free push plus a spin-observed
sequence bump (~100-300 ns), not an eventfd wake.  The ~1.3 µs/op end-to-end
figure above is dominated by the kernel's `io_uring_enter` submission and
socket loopback processing, and the MySQL mock figure additionally contains a
full TCP round trip through a scripted server.

`perf stat`/`perf c2c` profiling showed a ~50% cycle/wall-time reduction in
the CPU event loop, from 0.67 to ~1.0–1.2 IPC, and a drop from 18 to 1 remote L1
HITM transfers on the hot shared line after moving to batched per-thread event
counting. See `docs/PROFILING.md`.

## Distribution packages

After configuring/building, the following targets create installable packages
under `build/packages/`:

```sh
cmake --build build --target deb       # Debian/DPKG .deb
cmake --build build --target dpkg      # alias for deb
cmake --build build --target rpm       # RPM .rpm
cmake --build build --target packages  # both
```

Outputs:

- `build/packages/sysbench-cpp_<version>_amd64.deb`
- `build/packages/sysbench-cpp-<version>-1.x86_64.rpm`

The packages install `usr/bin/sysbench26`, the Lua benchmark tree under
`usr/share/sysbench-cpp/lua`, and documentation under
`usr/share/doc/sysbench-cpp`. DEB dependencies are generated with
`dpkg-shlibdeps`; RPM `AutoReq`/`AutoProv` are enabled. License:
GPL-2.0-or-later.

## Design notes and limitations

- **Result lifetime**: Lua result objects are allocated in the task's per-event
  arena and are valid until the next event starts.  This is what original OLTP
  scripts assume; freeing is O(1) per event rather than GC-dependent.  Do not
  retain a result across `event()` calls.
- **LuaJIT FFI is not used**.  The public `sysbench.*` API is reimplemented on
  PUC Lua 5.4 and calls yield/resume coroutines for network I/O.
- **MySQL TLS is supported** with `--mysql-ssl=off|preferred|required`, optional
  CA/hostname verification and client certificates. TLS uses OpenSSL memory
  BIOs; encrypted records are still carried by io_uring registered buffers.
  Both common authentication plugins work, including caching_sha2 full auth
  over RSA or cleartext over TLS. MySQL 8 commands rejected by the prepared
  protocol (e.g. `BEGIN`) fall back to text queries transparently.
- **Only MySQL is registered**.  `db::Driver`, `db::Connection`,
  `db::PreparedStatement` and `DriverRegistry` are protocol-neutral.
- **No compression, SSL, LOCAL INFILE, or client-side multi-statements**.
- **io_uring is one-ring-per-socket**. Each ring has two registered buffers
  (read/write) bound to the owning worker's NUMA node. The worker submits; a
  dedicated node-pinned reaper thread waits and posts completions. This adds
  one cross-thread handoff per I/O operation in exchange for never blocking a
  submitter on completion, and the worker uses a bounded 20 µs spin before
  parking when its own I/O is outstanding. Kernel/library-internal allocations
  (ring pages, pthread stacks, OpenSSL) are outside the project allocator;
  user-space data buffers are node-local.
- **API types**: non-trivial objects (`LuaRunOptions`, `StatsSnapshot`,
  `ConnectionOptions`, `Runtime::Config`) are passed by `const&`; only
  move-only results (`Task`, `ResultSet`, strings/vectors) are returned by
  value.
- **Multi-threaded `--events` limits are approximate** by up to
  `check_batch * threads` because sharded counters are aggregated periodically;
  single-thread limits are exact.
- **Reporting hooks** run on the main reporting state and are intended for
  output/statistics only; they must not issue blocking database calls.
- **Benchmark scripts** are GPLv2, derived from sysbench; see `NOTICE` and
  `COPYING`.
