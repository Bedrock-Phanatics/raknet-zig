#!/usr/bin/env bash
# Linux only. Build first: zig build interop && (cd tests/interop/go && go build -o ../../../zig-out/bin/raknet-interop-go .)
set -u
zig_bin=${ZIG_BIN:-./zig-out/bin/raknet-interop}
go_bin=${GO_BIN:-./zig-out/bin/raknet-interop-go}
seconds=${SECONDS_PER_CASE:-2}
port=${BASE_PORT:-19400}

run() {
  local server=$1 client=$2 connections=$3 payload=$4
  port=$((port + 1))
  timeout $((seconds + 10)) "$server" server "127.0.0.1:$port" $((seconds + 3)) &
  sleep 0.7
  timeout $((seconds + 10)) "$client" client "127.0.0.1:$port" "$connections" "$payload" "$seconds" 500
  wait
}

for pair in "$zig_bin $zig_bin" "$go_bin $go_bin" "$go_bin $zig_bin" "$zig_bin $go_bin"; do
  set -- $pair
  for connections in ${CONNECTIONS:-1 10 50 100}; do
    for payload in ${PAYLOADS:-32 128 512 1200 8192 65536}; do
      run "$1" "$2" "$connections" "$payload"
    done
  done
done
