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

- **C++26 coroutine runtime**: one up-front memory reservation, node-local
  worker heaps, per-event arenas, no `malloc` per query.
- **Two network architectures**: sysbench's blocking send/recv with
  one worker per connection (default, synchronous calls from Lua), or a
  per-worker io_uring with many connections per worker (`--io-blocking=off`).
- **Pipelining** (`--stream-depth=N`): N commands in flight per connection.
- **MySQL wire client** behind a protocol-neutral `db::` interface: text and
  prepared protocols, `caching_sha2_password`, TLS.
- **sysbench-compatible Lua API** on LuaJIT (vendored, linked statically); the
  hot path (event loop, rand, query, execute, param:set) uses the FFI.

Design and control flow: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Requirements

- Linux with io_uring (kernel 5.19+; tested on 6.14)
- CMake 3.28+, Ninja recommended
- GCC 14+ or Clang 20+ with C++26 coroutines
- `liburing`
- `libnuma` development files
- OpenSSL development files (caching_sha2 RSA auth and TLS)
- Cargo/Rust for the optional sql-rust protocol revision 2 client

Ubuntu:

```sh
sudo apt install build-essential cmake ninja-build liburing-dev libnuma-dev libssl-dev
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
| `--threads=N` | Benchmark connections (one Lua coroutine each) |
| `--io-blocking=on/off` | Blocking send/recv, one worker per connection (default on); off: per-worker io_uring |
| `--stream-depth=N` | Commands in flight per connection (default 1: wait for each reply) |
| `--connections-per-worker=N` | io_uring mode: connections per worker (default 2) |
| `--workers=N` | Override the worker count (max 64) |
| `--io-busy-poll=off/on/auto` | io_uring mode: NAPI busy polling while waiting |
| `--io-multishot-recv`, `--io-direct-send`, `--io-send-zc` | io_uring mode: receive/send variants |
| `--numa=on/off`, `--pin-workers=on/off` | NUMA placement and CPU pinning (default on) |
| `--time=S`, `--events=N`, `--rate=N` | Stop conditions / aggregate event rate |
| `--warmup-time=S`, `--report-interval=S` | Warm-up and intermediate reports |
| `--percentile=N`, `--histogram` | Latency reporting |
| `--memory-size=4G` | Up-front root reservation |
| `--mysql-host/port/user/password/db/socket` | Connection options |
| `--mysql-ssl=off/preferred/required` (+ `-ca/-cert/-key/-verify-server-cert`) | TLS |
| `--seed=N`, `--rand-seed=N` | Deterministic per-thread RNG seed |

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

## Limitations

- A Lua result is valid until the connection's next query (as in
  sysbench); a pipelined query returns an empty result.
- Only MySQL is registered; no compression, LOCAL INFILE or client-side
  multi-statements.
- Benchmark scripts are GPLv2, derived from sysbench; see `NOTICE` and
  `COPYING`.
