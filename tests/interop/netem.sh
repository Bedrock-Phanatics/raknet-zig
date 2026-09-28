#!/usr/bin/env bash
set -euo pipefail
if [ "$(readlink /proc/self/ns/net)" = "$(readlink /proc/1/ns/net)" ]; then
  echo 'Use unshare -n; refusing to modify the host network.' >&2
  exit 2
fi
delay=$1
loss=$2
shift 2
ip link set lo up
tc qdisc add dev lo root netem limit 100000 delay "${delay}ms" loss "${loss}%" reorder "${REORDER_PERCENT:-0}%" duplicate "${DUPLICATE_PERCENT:-0}%"
exec python3 tests/interop/scale.py "$@"
