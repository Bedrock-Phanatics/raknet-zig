#!/usr/bin/env bash
set -euo pipefail
exec python3 tests/interop/scale.py \
  --zig "${ZIG_BIN:-./zig-out/bin/raknet-interop}" \
  --go "${GO_BIN:-./zig-out/bin/raknet-interop-go}" \
  --connections "${CONNECTIONS:-1 10 100 500 1000 2000 4096}" \
  --payloads "${PAYLOADS:-32 128 512 1200 8192}" \
  --pairs "${PAIRS:-zig-go go-go go-zig zig-zig}" \
  --seconds "${SECONDS_PER_CASE:-20}" --samples "${SAMPLES:-3}" \
  --warmup-ms "${WARMUP_MS:-3000}" --window "${WINDOW:-32}" \
  --interval-ms "${INTERVAL_MS:-0}" --listeners "${LISTENERS:-1}" \
  --ack-ms "${ACK_MS:-0}" --receive-batch "${RECEIVE_BATCH:-32}" \
  --server-cpus "${SERVER_CPUS:-}" --client-cpus "${CLIENT_CPUS:-}" \
  --port "${BASE_PORT:-19400}" --output "${OUTPUT:-zig-out/interop-$(date +%Y%m%d-%H%M%S)}"
