# Listener scalability measurements

Build with Zig 0.16.0 and the Go version supported by `go.mod`:

```sh
zig build interop
(cd tests/interop/go && go build -o ../../../zig-out/bin/raknet-interop-go .)
python3 tests/interop/test_scale.py
```

`scale.py` drives these existing implementations. It does not implement RakNet.
The pinned Go replacement is Lunar's fork; `go.mod` records its exact revision.
Run a single case first, then the longer matrix:

```sh
python3 tests/interop/scale.py --output zig-out/smoke \
  --pairs zig-go --connections 100 --payloads 128 --seconds 5 --samples 1
python3 tests/interop/scale.py --output zig-out/scaling \
  --pairs zig-go --connections '1 10 100 500 1000 2000 4096' \
  --payloads 128 --seconds 20 --samples 3 --warmup-ms 3000 \
  --window 1 --server-cpus 0 --client-cpus 2,4,6,8
```

CPU lists are examples. Inspect the machine's topology and use disjoint physical
cores when possible. All process threads inherit the selected affinity. Set
`--go-cpus` to control Go's `GOMAXPROCS`. A Zig client uses a task per connection;
compare all four pairs before attributing a throughput limit to the server.
Windows ignores the Linux affinity flags and uses native process CPU/RSS sampling.

For a remote generator, run the server binary directly on its own machine and
the client binary on the generator machine using the server's reachable IP.
RTT timestamps are generated and checked on the client, so clock synchronization
is unnecessary. `scale.py` launches both processes locally; remote CPU/RSS
collection must be performed on each host separately.

`run.sh` is an environment-variable wrapper around this runner. Its complete
default matrix is intentionally long: seven connection counts, five payload
sizes, four implementation pairs, three samples, and 20 measured seconds each.

## Workloads

### Upstream go-raknet comparison

The main harness uses Lunar's fork. To reproduce the root README's comparison
with upstream `github.com/sandertv/go-raknet` v1.15.2, run on Linux from the
repository root with Zig 0.16.0, Go and Python 3 on `PATH`:

```sh
zig build interop -Dtarget=x86_64-linux -Doptimize=ReleaseFast
python3 tests/interop/upstream.py --zig zig-out/bin/raknet-interop \
  --output zig-out/upstream-comparison --server-cpus 0 --client-cpus 2,4,6,8
```

Choose CPU IDs for your topology. This builds an isolated copy of the Go harness
with upstream v1.15.2 and no module replacement. Only the unavailable
`MetricsSnapshot()` diagnostic is replaced; retransmission counts are `-1`
(unavailable). The echo and client logic are unchanged.

The comparison runs three samples per server at 100 and 1,000 connections,
alternating server order between rounds. Each sample measures ten seconds after
two seconds of warm-up, with 128-byte payloads and one outstanding request per
connection. Both servers use the same upstream Go client. `GOMAXPROCS=4` applies
to Go processes; CPU affinity limits each server to one logical CPU and the
client to four separate physical cores on the recorded host. Each run directory
retains configuration, binary hashes, logs and samples, including failures.

### Available options

| Options | Workload |
| --- | --- |
| `--window 0` | Established idle sessions, with protocol keepalives |
| `--window 1` | Reliable ordered request/reply |
| `--window 32` | Pipelined saturation, also capped at 256 KiB outstanding payload per connection |
| `--window 1 --interval-ms 100` | Up to ten sends/second per connection; initial phases staggered across connections |
| `--payloads '32 128 512 1200 8192'` | Separate fixed-size cases; 8192 requires fragmentation |
| `--payloads 0` | Each connection cycles through all five sizes |
| `--churn-rounds 5` | Five client cohorts connect/disconnect against one persistent listener per sample |
| `--listeners 1`, `2`, `4` | Linux reuse-port listener sharding |
| `--ack-ms 0`, `1`, `2`, `5`, `10` | Candidate server ACK delay; library default stays zero |
| `--receive-batch 32`, `64`, `128`, `256` | Receive capacity sweep |
| `--send-batch 32`, `64`, `128`, `256` | Benchmark-only send capacity sweep; library default stays 64 |
| `--receive-buffer 1048576` | Linux receive-buffer request; the benchmark prints actual kernel-granted capacity |

Connection establishment is a separate phase with at most 64 concurrent dials.
`ramp_us` measures the entire cohort; setup percentiles measure each actual dial,
excluding its wait for admission to the ramp. Derive successful setups/second
from `(connections - failures) * 1e6 / ramp_us`.

For loss and RTT on Linux, use a disposable network namespace as root:

```sh
sudo unshare -n bash tests/interop/netem.sh 10 1 \
  --zig zig-out/bin/raknet-interop --go zig-out/bin/raknet-interop-go \
  --pairs zig-go --connections 100 --payloads '128 8192' \
  --seconds 20 --samples 3 --window 1 --output zig-out/rtt20-loss1
```

The first two arguments are one-way delay in milliseconds and independent loss
percentage on each transmitted datagram. Ten milliseconds adds roughly 20 ms
to an echo round trip. Loss percentages are per datagram, not per message or
round trip. The script refuses the host network namespace. Neither script
changes host socket-buffer sysctls. Add netem reorder/duplicate settings inside
the same disposable namespace for those experiments.

## Before/after and validity

Save the instrumented baseline binary before changing production code. Pass it
as `--baseline-zig`; `--zig` selects the candidate. The runner interleaves them
and reverses order on alternate samples. Both use the same `--client-zig`
(candidate by default) or Go generator. Pass a baseline built with matching
benchmark arguments and instrumentation. `--send-batch` requires the extended
candidate CLI and should be omitted for older baselines.

Each output directory retains configuration, SHA-256 binary hashes, complete
stdout/stderr, every raw sample, and median summaries. Output directories must
be new so evidence is never silently overwritten. Keep invalid samples:
handshake failures, incomplete drains, payload mismatches, early exits, and
incorrect measurement durations make `valid=false`. The median file reports
`valid_samples`; a numeric median alone is not an acceptance result.
Sampled session populations must also stay at or above the requested count.

After an interrupted run, repeat the exact command with `--resume`. Completed
samples are reused only when configuration and binary hashes still match.
Churn runs require a fresh directory because their persistent server state
cannot be recovered after interruption.

The server is terminated after client results. That cleanup is not a graceful
shutdown test. Protocol shutdown, allocation failure, ordering, wraparound,
timer fairness, and malformed-input behavior are covered by `zig build test`.
Churn retains the server across cohorts but likewise is not a proof of leak
absence; inspect its retained memory and session-count diagnostics.
The last churn cohort waits eleven seconds for the default idle expiry and
checks that the persistent server reports zero remaining sessions.

## Metric definitions and limits

- Throughput counts measured-phase messages delivered before the phase ends;
  replies during the bounded three-second drain do not increase throughput.
- RTT includes late replies to measured-phase sends. Each connection retains
  its most recent 2048 RTT samples. Reported percentiles describe this bounded
  sample, not every packet in the run. Setup percentiles include all successes.
- Echo validation checks length, the application marker, and the trailing
  sequence marker. It is not a full-byte corruption or exactly-once oracle;
  protocol payload, duplicate and ordering correctness rely on the regression
  tests as well as these runtime checks.
- CPU uses process-time deltas aligned to client phase markers. 100% means one
  logical CPU. Generator CPU is measured separately. Marker handling and
  sampling add small timing error. RSS is the larger endpoint measurement,
  not an OS-wide peak. KiB/requested connection subtracts pre-ramp server RSS;
  it is not valid as KiB/established connection when dials fail.
- `snapshots` retain listener counters about once per second, per shard.
  Compare deltas over matching snapshot intervals. Snapshot timing is not
  exactly the entire measurement window. `statistics_ns` measures the detailed
  O(N) snapshot separately.
- `network` reports native receive syscall attempts/datagrams and percentiles
  from the last 2048 active `poll` calls. Poll duration **includes receive wait**;
  it is not pure event-loop processing time. Native attempts include EAGAIN.
- Portable `receive_calls` / `send_calls` count backend API calls. A `std.Io`
  provider can issue multiple syscalls inside a call; do not call these syscall
  counts. Native `recvmmsg` counters identify actual syscall batching.
  Zig 0.16.0 Threaded sends in chunks of at most 64 datagrams even when the
  benchmark's send queue is larger. A queue occupancy of 256 is not one 256-packet
  `sendmmsg` syscall.
- Timer lateness is cumulative deadline-to-service time. Its maximum includes
  earlier phases. Deferred receive counts are pending packets carried to later
  turns, not packet loss, and the same packet may be counted on multiple turns.
- Linux kernel receive drops are socket counter deltas from `/proc/net/udp`.
  They distinguish host UDP overflow from protocol rejection/retransmission.
- Quota allocation/free counters count successful allocation/free operations;
  in-place resize/remap updates bytes and peak bytes without counting a new
  allocation. RSS also includes allocator retention, stacks, and bookkeeping.
- Jain fairness and min/max delivered messages expose unequal session progress.
  Idle fairness is zero by definition because no application messages are sent.

Use `perf record` / `perf report` on the server process for attribution. Keep
profiling runs separate from throughput acceptance runs, and do not run builds
or other benchmark processes alongside them. `tests/bench` provides useful Core
and scheduler microbenchmarks, but its smaller configurations bypass the actual
Listener path and cannot establish listener scalability.
