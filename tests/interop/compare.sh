#!/usr/bin/env bash
# usage: compare.sh <zig-interop> <go-interop> <output-dir> [rounds]
set -euo pipefail
zig=$(realpath "$1")
go=$(realpath "$2")
out=$3
rounds=${4:-3}
here=$(dirname "$(realpath "$0")")
mkdir "$out"
out=$(realpath "$out")
common=(--zig "$zig" --go "$go" --server-cpus "${SERVER_CPUS:-0}" --client-cpus "${CLIENT_CPUS:-2,4,6,8}" --go-cpus 4 --window 1 --samples 1)

run() {
  local name=$1 round=$2
  shift 2
  local pairs="zig-go go-go"
  if [ $((round % 2)) = 1 ]; then pairs="go-go zig-go"; fi
  python3 "$here/scale.py" "${common[@]}" --pairs "$pairs" --output "$out/$name-r$round" "$@"
}

for round in $(seq 0 $((rounds - 1))); do
  run paced-100 "$round" --connections 100 --payloads 128 --interval-ms 100 --seconds 10 --warmup-ms 1000
  run paced-1000 "$round" --connections 1000 --payloads 128 --interval-ms 100 --seconds 10 --warmup-ms 1000
  run paced-4096 "$round" --connections 4096 --payloads 128 --interval-ms 400 --seconds 10 --warmup-ms 1000
  run paced-1000-8k "$round" --connections 1000 --payloads 8192 --interval-ms 1000 --seconds 10 --warmup-ms 1000
  run saturated-100 "$round" --connections 100 --payloads 128 --seconds 10 --warmup-ms 2000
  run saturated-1000 "$round" --connections 1000 --payloads 128 --seconds 10 --warmup-ms 2000
  for impairment in "10 1" "25 0"; do
    set -- $impairment
    for payload in 128 8192; do
      unshare -rn bash "$here/netem.sh" "$1" "$2" "${common[@]}" --pairs "$([ $((round % 2)) = 1 ] && echo 'go-go zig-go' || echo 'zig-go go-go')" \
        --connections 100 --payloads "$payload" --seconds 8 --warmup-ms 1000 --output "$out/rtt$(($1 * 2))-loss$2-$payload-r$round"
    done
  done
done
