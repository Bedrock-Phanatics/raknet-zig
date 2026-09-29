# Performance audit

This audit covers the latency work in revisions `3d662c5` and `e0cc61d` and the
retransmission timeout change after them. Every comparison uses the
[sandertv/go-raknet](https://github.com/sandertv/go-raknet) v1.15.2 client from
`go/`, except where the raknet-zig client is named.

## Setup

Ubuntu on WSL2 (kernel 6.6.87.2), AMD Ryzen 5 5500, about 8 GiB RAM, Zig 0.16.0
ReleaseFast and Go 1.27.1. The server ran on CPU 0 and the client on CPUs
2,4,6,8. `net.core.rmem_max` was left at 212,992 bytes. Before/after runs were
interleaved with alternating order. RTT is localhost echo time seen by the
client, not player ping. The current comparison with go-raknet's server is in
[results.json](results.json) and the root README.

## Where latency comes from

- At healthy load the server's own contribution is small. With the raknet-zig
  client at 1,000 connections sending ten 128-byte requests per second, kernel
  receive timestamps put server residence (arrival to reply sent) at p50
  30–40 µs and p99 280–330 µs.
- Server CPU at that load was about 30% user and 70% kernel. The deadline heap was
  the largest user-space cost, at 1.5 updates and about 9 swaps per datagram.
- Zig 0.16.0's `Io.Threaded` rounds waits below 1 ms down to zero. Any timer
  less than 1 ms away made the listener spin until it fired. go-raknet clients
  ACK every 100 ms, which kept timers in that range and the loop spinning.
- go-raknet clients' 100 ms ACKs also outlasted raknet-zig's 50 ms minimum
  retransmission timeout. Each spurious timeout cut the congestion window to one
  MTU, and slow start grew it by one MTU per ACK datagram even when an ACK covered
  many, so fragmented replies were released about one datagram per 100 ms.
- Even after that fix, the timeout settled just below the slowest delayed ACKs.
  The remaining spurious retransmissions kept the window near 10 KiB, so each
  8 KiB reply at 50 ms RTT waited a second round trip for window space.

## Changes

1. **No sub-millisecond spinning.** When nothing is queued and less than 1 ms
   remains, the Linux listener waits with `ppoll` at nanosecond precision. The
   wait cannot be canceled but is bounded below 1 ms.
2. **Compact deadline heap entries.** Entries point at the map's key and index
   instead of copying the key, and removal no longer hashes it. A microbenchmark
   of one echo's heap work fell from 2.27 to 0.42 µs at 1,000 sessions and from
   2.9 to 0.52 µs at 4,096.
3. **Larger listener receive buffer on Linux.** Listeners request 4 MiB, which the
   kernel caps at `rmem_max`. On this host that doubled the buffer to about 416 KiB.
4. **Congestion control with delayed ACKs.** Slow start grows per acknowledged
   datagram, and an ACK naming a datagram's first send yields an RTT sample even
   after it was resent under a new sequence number.
5. **Retransmission timeout above delayed ACKs.** The timeout stays 25% above a
   slowly fading peak of recent RTT samples, so ACK delays that recur stop firing
   it. The 50 ms minimum still applies to fast peers.

Changes 1–3 (`1992904` → `3d662c5`), medians of three samples:

| Workload | Server CPU | Kernel drops | RTT p99 (ms) |
| --- | --- | --- | --- |
| 1,000 connections, 128 B, 10/s | 45.8% → 20.0% | 0 → 0 | 1.41 → 1.35 |
| 1,000 connections, 1,200 B, 10/s | 47.5% → 21.1% | 539 → 0 | 1.29 → 1.44 |
| 4,096 connections, 128 B, 2.5/s | 40.2% → 27.0% | 0 → 0 | 1.09 → 0.94 |
| 100 connections, 20 ms RTT, 1% loss | 44.6% → 9.5% | 0 → 0 | 138 → 145 |

p99 changes are within the spread between samples.

Change 4 (`3d662c5` → `e0cc61d`). Ranges cover each sample; single values are
one run.

| Workload | Echoes/s | RTT p99 (ms) |
| --- | --- | --- |
| 1 connection, 8 KiB, loopback | 17 → 1,100 | 300 → 0.52 |
| 1 connection, 1,200 B, loopback | 76 → 7,577 | 100 → 0.18 |
| 100 connections, 8 KiB, 50 ms RTT | 336–347 → 1,180–1,194 | 400 → 150 |
| 100 connections, 8 KiB, 20 ms RTT, 1% loss | 702–738 → 1,215–1,231 | 388–400 → 283–288 |
| 100 connections, 8 KiB, 100 ms RTT, 5% loss | 188–193 → 241–244 | 1,400–1,437 → 1,177–1,188 |
| 1,000 connections, 128 B, 10/s | unchanged | 1.10–1.95 → 1.14–1.38 |
| 1,000 connections, 8 KiB, 1/s | unchanged | 0.42–0.43 → 0.33–0.35 |
| 100 connections, 128 B, 20 ms RTT, 1% loss | unchanged | 142–146 → 155–157 |

The last row is the only regression seen. With raknet-zig on both ends and
mixed 32 B–8 KiB payloads, delivery rose from 31,796–32,443 to 33,212–34,550
echoes/s.

Change 5, medians of three samples at 100 connections unless noted:

| Workload | Echoes/s | RTT p50 / p99 (ms) |
| --- | --- | --- |
| 8 KiB, 50 ms RTT | 1,177 → 1,566 | 99.3 / 149.8 → 50.5 / 149.9 |
| 128 B, 20 ms RTT, 1% loss | 4,308 → 4,282 | 20.2 / 154.9 → 20.2 / 161.4 |
| 128 B, 100 ms RTT, 1% loss | 934 → 941 | 100.2 / 307.9 → 100.2 / 336.1 |
| 8 KiB, 20 ms RTT, 1% loss | 1,231 → 1,221 | 79.7 / 283.6 → 79.8 / 294.5 |
| 8 KiB, 100 ms RTT, 5% loss | 242 → 241 | 360.6 / 1,225 → 350.6 / 1,174 |
| 1,000 connections, 128 B, 10/s | unchanged | 0.3 / 1.1 → 0.2 / 1.0 |

A lost final datagram now waits slightly longer for a go-raknet client, whose
ACKs set the peak. With the raknet-zig client, which ACKs promptly, p99 at 20 ms
RTT with 1% loss stayed at 69.7 → 69.9 ms and at 100 ms RTT 199.8 → 200.2 ms.

## Rejected

- **Restoring the congestion window after a spurious timeout** matched change 4
  on latency but left one or two connections undrained after three seconds in
  closed-loop 8 KiB overload.
- **RTT sampling without the slow-start change** did not fix the collapse:
  8 KiB on one connection stayed at 10 echoes/s with a 300 ms p99.

## Remaining limits

- Against clients that ACK every 100 ms, 8 KiB replies at 50 ms RTT reach 1,570
  echoes/s against go-raknet's 1,820, and p95 is still about two round trips
  while the window grows. go-raknet's server has no congestion window.
- With one outstanding request, a lost last datagram is recovered only by the
  retransmission timer, so p99 under loss is roughly RTT plus one timeout.
- In closed-loop 8 KiB overload with the go-raknet client, most samples leave one
  or two connections undrained after three seconds, before and after change 5.
  Overload is not a supported operating point, but the cause is not yet known.
- Memory is higher than go-raknet's: 336 against 105 MiB RSS at 4,096 paced
  sessions.
- The receive buffer request only helps up to `rmem_max`. Raise that limit for
  large player counts.
- Windows and multi-listener sharding were not measured in this pass.
