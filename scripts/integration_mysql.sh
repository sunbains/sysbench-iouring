#!/usr/bin/env bash
set -euo pipefail

: "${MYSQL_HOST:=127.0.0.1}"
: "${MYSQL_PORT:=3306}"
: "${MYSQL_USER:=root}"
: "${MYSQL_PASSWORD:=}"
: "${MYSQL_DB:=sbtest}"
: "${SYSBENCH:=./build/sysbench26}"
: "${MYSQL_SSL:=off}"
: "${TABLES:=2}"
: "${TABLE_SIZE:=1000}"
: "${THREADS:=4}"
: "${TIME:=5}"

ARGS=(
  --mysql-host="$MYSQL_HOST"
  --mysql-port="$MYSQL_PORT"
  --mysql-user="$MYSQL_USER"
  --mysql-password="$MYSQL_PASSWORD"
  --mysql-db="$MYSQL_DB"
  --tables="$TABLES"
  --table-size="$TABLE_SIZE"
  --mysql-ssl="$MYSQL_SSL"
)

for benchmark in oltp_point_select oltp_read_only oltp_write_only oltp_read_write; do
  echo "=== ${benchmark}: prepare ==="
  "$SYSBENCH" "$benchmark" "${ARGS[@]}" --threads="$THREADS" prepare
  echo "=== ${benchmark}: run ==="
  "$SYSBENCH" "$benchmark" "${ARGS[@]}" --threads="$THREADS" --time="$TIME" run
  echo "=== ${benchmark}: cleanup ==="
  "$SYSBENCH" "$benchmark" "${ARGS[@]}" cleanup
done
