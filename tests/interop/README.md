# Interop benchmarks

These tools compare raknet-zig with [sandertv/go-raknet](https://github.com/sandertv/go-raknet)
over real UDP sockets. They need Linux, Zig 0.16.0, Go and Python 3.

## Build

```sh
zig build interop -Doptimize=ReleaseFast
(cd tests/interop/go && go build -o ../../../zig-out/bin/raknet-interop-go .)
python3 tests/interop/test_scale.py
```

The Go binary uses go-raknet v1.15.2 from `go/go.mod`. It does not expose
retransmission counts, so Go results report `-1`.

## Compare the servers

```sh
bash tests/interop/compare.sh zig-out/bin/raknet-interop \
  zig-out/bin/raknet-interop-go zig-out/compare
```

The Go client drives both servers. Three rounds alternate server order and run:

- paced load: 100 and 1,000 connections at ten 128-byte requests per second,
  4,096 connections at 2.5 per second, and 1,000 connections at one 8 KiB
  request per second
- overload: 100 and 1,000 connections with one outstanding request and no pacing
- impaired networks: 100 connections at 20 ms RTT with 1% loss and at 50 ms RTT,
  with 128-byte and 8 KiB payloads

Set `SERVER_CPUS` and `CLIENT_CPUS` for your CPU topology. The defaults pin the
server to CPU 0 and the client to 2,4,6,8. Impaired cases use `unshare -rn` and
never touch the host network.

## Single runs

`scale.py` runs one configuration and keeps its configuration, binary hashes,
logs and samples. Output directories must be new.

```sh
python3 tests/interop/scale.py --pairs zig-go --connections 1000 \
  --payloads 128 --window 1 --interval-ms 100 --seconds 10 --samples 3 \
  --output zig-out/paced-1000
```

`--pairs` takes `server-client` pairs such as `zig-go` and `go-go`. Pass
`--baseline-zig` to interleave an older raknet-zig server with `--zig` for
before/after comparisons. `--window 1` keeps one request outstanding per
connection, `--interval-ms` paces it, and `--payloads 0` cycles through several
sizes. `netem.sh <one-way-delay-ms> <loss-percent> <scale.py args>` runs the
same command inside a private network namespace; ten milliseconds adds about
20 ms of RTT.

## Reading results

- A sample is valid only with no connection failures, payload mismatches or
  undrained requests, and with every session kept. Medians include invalid
  samples, so always check `valid_samples`.
- RTT is localhost echo time measured by the client. Each connection keeps its
  last 2,048 samples. It is not player ping.
- Throughput counts replies during the measured period only.
- CPU is per process, where 100% is one logical CPU. RSS is sampled at phase
  boundaries, not peak memory.
- Kernel drops come from the server socket's `/proc/net/udp` counter.

Protocol behavior such as ordering, wraparound, allocation failure and shutdown
is covered by `zig build test`, not by these runs.
