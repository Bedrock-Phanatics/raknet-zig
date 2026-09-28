# Performance and scalability audit

The retained changes reduce scheduler overhead and session memory, and improve
healthy paced workloads through 4,096 connections. **They do not meet the broad
4k saturation-scaling target.** Overload, generator limits and backend waiting
costs remain measurable. Failed samples and rejected experiments are included.

## Scope and environment

The audit compares production revision `0b58343` with the working-tree changes,
using the actual Listener, callbacks, endpoint map, deadline queue, UDP sockets,
ACK/recovery handling, and default per-session limits. The Core-only sessions
microbenchmark is not used to establish Listener scalability.

Linux runs use Zig 0.16.0 ReleaseFast in Ubuntu under WSL2, kernel
6.6.87.2-microsoft-standard-WSL2, 12 logical / 6 physical CPUs and about 8 GiB RAM.
Single-listener runs pin the server to CPU 0 and the Go generator to 2,4,6,8.
Lunar Go is pinned by `go.mod` to `2049463566ca`; Linux Go is 1.27.1.
The host UDP receive default and maximum were both 212,992 bytes. Host sysctls
were not changed. Windows results must be read separately.

The main matrix uses three interleaved samples, 20 measured seconds, a 3-second
warm-up, 128-byte reliable ordered payloads, and one outstanding request per
connection. The common generators cap concurrent dials at 64. CPU and RSS are
sampled externally at measurement phase boundaries. Throughput excludes drain
deliveries, while RTT includes them. Each connection retains its last 2,048 RTT
samples. See [measurement definitions](README.md) for remaining limitations.

Usage interruptions stopped the first long matrix. Completed samples were
retained; missing samples resumed only after binary hash/configuration checks.
The resumed environment produced lower absolute throughput. Interpret adjacent
before/after pairs and repeated medians, not historical absolute rates as a
single stable machine-capacity claim. The 1,000-connection first pair straddled
the interruption and is excluded from the report tables and exported summary;
its raw logs remain available.

Raw configurations, binary hashes, logs and per-sample JSON are under
`zig-out/audit/`. These are local artifacts and are not committed build inputs.
The portable summary, configurations and binary hashes are preserved in
[audit-results.json](audit-results.json); reproduction commands and metric
definitions are in [README.md](README.md).

## Findings supported by profiles and measurements

1. **Redundant scheduler work is real.** At 100 connections the original path
   made 4.64 global upserts per received datagram; the candidate makes 1.91.
   Callback sends, receipt queuing and final incoming-packet processing formerly
   scheduled the same session repeatedly. Work is now deferred during a logical
   incoming/timer operation, followed by one effective update. Rejected input
   does not refresh an otherwise unchanged deadline.
2. **Heap movement repeatedly hashed endpoints.** The initial 1,000-connection
   userspace profile attributed 19.21% to `getIndex` and 6.35% to equality, with
   stacks leading through deadline heap swaps. Heap entries now retain stable
   pointers to values in the fully reserved index map. Swaps update those
   indices directly. The map does not rehash and entries are removed before
   session destruction; no Session pointer is retained by the scheduler.
3. **Eager metadata consumed operational headroom.** Recovery descriptors/heap,
   outbound queue slots, and split metadata allocated their configured maxima
   even for nearly idle connections. They now allocate lazily and remain
   bounded by the same limits. Sequence-indexed recovery slots, resequencing,
   retained size classes, control reservations and the 512 MiB quota remain.
4. **A receive API batch was not a syscall batch.** Zig 0.16.0's Threaded POSIX
   backend loops over `recvmsg`. Its packed-buffer API and the `recvmmsg` timeout
   problem explain that choice. This transport already owns fixed-stride UDP
   slots, so a bounded nonblocking `recvmmsg` drain can serve it. Threaded still
   owns waiting/cancellation when the socket is empty. Other I/O providers and
   Windows retain the portable path. IPv4, IPv6, empty and truncated datagrams,
   timeouts and custom-provider fallback have regression coverage.
5. **High-count overload is not solved by these changes.** Saturated 4,096-client
   loopback runs record hundreds of thousands to over a million kernel receive
   drops, retransmission storms, incomplete drains and declining live-session
   populations. Such runs are invalid as successful throughput results. The
   baseline can shed more sessions and therefore deliver higher aggregate
   throughput to its remaining subset; requested connections are not equivalent
   to a stable established population.
6. **Sub-millisecond timeout rounding causes empty-receive spinning.** The
   installed Zig 0.16.0 `Io.Threaded.batchAwaitConcurrent` floors the remaining
   duration before `poll`. A focused empty-socket check with an 800 us deadline
   returned early. A one-millisecond rounding allowance removed most empty
   attempts, but the paired two-listener median fell from 32,062 to 28,347 msg/s
   and p99 rose from 262 to 386 ms. Four listeners improved from 16,840 to 21,438
   msg/s and CPU fell from 377% to 170%. The adjustment and its stronger timeout
   assertion were **reverted**: a provider is allowed to return a spurious
   timeout, and this workaround did not meet the latency acceptance criterion.
   A cancellable high-resolution readiness wait is a remaining backend issue.

The post-scheduler kernel-inclusive 4,096-connection profile put `getIndex` at
1.16%, heap removal at 4.83%, heap upsert at 2.70%, timer processing at 3.62%, and
incoming Core work at 3.27%. Kernel locking/network work was substantial. These
profiles use different connection counts and sampling domains; their percentages
are attribution evidence, not a controlled percentage-speedup calculation.

## Controlled before/after scaling

Actual Listener, Lunar Go generator, one server CPU, 128-byte payload, window 1,
20 seconds measured after 3 seconds warm-up. Three samples except the
1,000-connection row, which excludes the interrupted pair and uses two.
`B` is the instrumented original; `A` includes scheduler/lazy-memory/native
receive changes. Later provider/flag guards preserve this Threaded UDP path.
CPU 100% is one logical CPU. RTT is milliseconds; RSS is MiB. Memory per
connection is KiB after subtracting pre-ramp RSS, divided by **requested**
connections. Retx is the client's complete-run counter (includes ramp/drain).
`Incomplete` is the median number of connections with an undrained request.
All rows had zero reported connection failures and payload mismatches.

| Connections | Version | msg/s | MiB/s | CPU % | RTT p50 / p95 / p99 ms | RSS MiB | KiB/requested | Client retx | Incomplete | Valid samples |
| ---: | :---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 100 | B | 95,997 | 11.72 | 99.5 | 1.008 / 1.329 / 1.590 | 14.50 | 139.52 | 0 | 0 | 3/3 |
| 100 | A | 100,870 | 12.31 | 99.9 | 0.948 / 1.253 / 1.492 | 10.25 | 94.72 | 0 | 0 | 3/3 |
| 500 | B | 69,251 | 8.45 | 100.0 | 2.141 / 51.537 / 100.776 | 75.00 | 151.81 | 104,469 | 0 | 3/3 |
| 500 | A | 75,536 | 9.22 | 99.6 | 1.986 / 44.267 / 100.528 | 52.88 | 106.24 | 104,128 | 0 | 3/3 |
| 1,000 | B | 40,194.5 | 4.905 | 97.3 | 3.469 / 120.681 / 251.647 | 147.68 | 150.33 | 271,617.5 | 5.5 | 0/2 |
| 1,000 | A | 44,963 | 5.485 | 96.9 | 3.201 / 113.972 / 239.882 | 103.19 | 104.64 | 268,342.5 | 2.5 | 1/2 |
| 2,000 | B | 35,125 | 4.29 | 97.6 | 4.417 / 238.171 / 435.876 | 284.36 | 145.14 | 753,100 | 163 | 0/3 |
| 2,000 | A | 35,263 | 4.30 | 96.8 | 4.501 / 243.850 / 448.750 | 192.86 | 98.23 | 767,503 | 190 | 0/3 |
| 4,096 | B | 21,203 | 2.59 | 97.0 | 57.697 / 499.690 / 867.204 | 544.88 | 136.00 | 2,673,462 | 2,570 | 0/3 |
| 4,096 | A | 15,895 | 1.94 | 96.7 | 135.802 / 718.692 / 1172.993 | 398.12 | 99.28 | 677,853 | 570 | 0/3 |

Throughput retention relative to each version's 100-connection result is
100/72.1/41.9/36.6/22.1% before and 100/74.9/44.6/35.0/15.8% after.
This is aggregate throughput retention, not per-core parallel efficiency.
The 1k+ overload rows are **not capacity successes**. Baseline 4k shed many more
sessions: Jain fairness was 0.777 versus 0.962, and median kernel drops were
1,300,494 versus 373,308. A faster surviving subset is not better service to 4k.
The patch therefore does not meet the requested broad saturation-scaling target.

The successful 100/500 cases improved throughput by 5.1%/9.1%. At 100 connections,
server CPU per delivered message fell approximately 10.37 to 9.91 us; at 500,
14.45 to 13.19 us. These include all protocol/network server CPU, not only Core.

### Paced and idle cases

Three samples, 128-byte payloads, staggered ten sends/second/connection,
10 seconds measured, one-second warm-up:

| Connections | Version | msg/s | Server CPU % | RTT p50 / p95 / p99 ms | RSS MiB | Failures / incomplete |
| ---: | :---: | ---: | ---: | --- | ---: | --- |
| 100 | B | 994 | 5.0 | 0.241 / 0.368 / 0.583 | 14.00 | 0 / 0 |
| 100 | A | 994 | 4.6 | 0.205 / 0.361 / 0.547 | 9.63 | 0 / 0 |
| 1,000 | B | 9,955 | 28.8 | 0.750 / 1.927 / 2.595 | 128.25 | 0 / 0 |
| 1,000 | A | 9,959 | 26.0 | 0.563 / 1.485 / 2.039 | 83.13 | 0 / 0 |

All twelve paced samples above completed without kernel drops or client
retransmission. At 1k, CPU/message fell about 28.9 to 26.1 us and p99 improved
21.4%. Median complete ramp time fell 362 to 230 ms (about 2,764 to 4,356
connections/s); this includes bounded dial admission, not only map insertion.
Successful dial p50/p95/p99 were 19.803/45.396/47.812 ms before and
10.373/36.828/41.868 ms after. Separate rehash/allocation time was not instrumented;
there is no claim that hashmap reservation alone caused the ramp improvement.
The 4k ten-send/s case exceeded this host's healthy envelope and is not claimed
as a win.

At **4,096 sessions with one send every 400 ms**, all six interleaved samples
were valid (ten measured seconds, one-second warm-up). Delivery was 10,228 to
10,230 msg/s; CPU fell 49.5% to 46.1%; p50/p95/p99 fell from
0.565/2.218/4.730 ms to 0.479/1.801/3.759 ms. RSS fell 522.25 to 335.88 MiB.
Jain fairness remained above 0.99995. Client retransmits including ramp were
519 versus 251. This is evidence of healthy thousands-of-session operation at
a controlled offered load, not a claim that 4k saturated streams are repaired.

At 4,096 sessions sending **8192 bytes once per second**, the baseline completed
none of three samples: median incomplete count 262, Jain 0.938, and RSS
559.63 MiB (RSS includes memory outside the quota). The candidate completed all
three, delivering 4,093 msg/s / 31.98 MiB/s, with zero incomplete connections,
Jain 0.99994 and RSS 407.88 MiB. Candidate p50/p95/p99 were
0.582/21.993/120.907 ms. Its individual p99 values ranged from 78.9 to 392.9 ms,
so fragmentation under this load still has substantial tail variability.
Median CPU was 66.0% before and 61.2% after; client retransmits including ramp
fell from 204,553 to 12,064. Baseline and candidate did not serve the same surviving
population, so this supports improved completion/headroom rather than an
unqualified throughput-speedup percentage.

Idle established populations remained intact in all three samples per version
and count. At 4,096 sessions RSS fell from 521.88 to 333.75 MiB (36.0%). Exact
session-quota usage fell from 528,850,328 to 339,579,736 bytes, leaving about
7.65 versus 188.15 MiB under the unchanged 512 MiB quota. These short idle samples
include protocol keepalives and are not zero-work sleep benchmarks.

## Batching, sharding and secondary costs

At 1k connections, sequential five-second/three-sample receive-capacity screens
gave 42,751 / 34,272 / 38,782 / 41,948 msg/s for 32 / 64 / 128 / 256 slots.
Bigger buffers did not give a consistent gain, so the default remains 32.
Larger batches also increased carried-over receive work. A requested 1 MiB
kernel receive buffer was clamped to 425,984 bytes; it did not repair 4k overload.

Initial sequential send-capacity screens appeared to favor 128/256 entries,
but the final interleaved 64-versus-256 comparison did not reproduce that win:
39,416 versus 38,252 msg/s and p99 241.1 versus 265.9 ms (three five-second
samples at 1k). All six completed. The default therefore remains 64. This also
illustrates why changing host conditions make sequential screen results
insufficient for selecting defaults. The underlying Threaded send path still
splits API batches into at most 64 datagrams per `sendmmsg` call.

The isolated native-receive ablation used the same lazy-memory binary as its
baseline: 100-client saturated medians rose 92,641 to 96,810 msg/s; at 1k,
67,053 to 72,384. p99 was 1.680 to 1.598 ms and 188.658 to 188.208 ms respectively.
There was no meaningful 4k saturation gain. These are separate runs, not numbers
to combine arithmetically with the main table. `recvmmsg` is retained for its
measured single-listener gain and bounded, provider-specific scope.

In the main candidate runs, actual `recvmmsg` datagrams per attempted syscall
(including empty attempts) were 31.85 / 30.14 / 22.19 / 22.97 / 4.73 at
100 / 500 / 1k / 2k / 4k connections. Portable backend call counters are not
syscall counts. The native ablation predates the final drain-count correction;
its before/after generators match each other, but its absolute rates should not
be mixed with the final matrix.

Median sampled active-poll p50/p95/p99, in microseconds, were:

| Connections | Poll p50 / p95 / p99 us |
| ---: | --- |
| 100 | 276 / 354 / 404 |
| 500 | 327 / 491 / 582 |
| 1,000 | 429.5 / 688 / 869 |
| 2,000 | 529 / 910 / 1592 |
| 4,096 | 31 / 896 / 1621 |

These are the last 2048 active polls per sample and include receive wait, not
isolated CPU processing latency. Timer-lateness maxima and cumulative work
deferrals are retained in the machine-readable results; they are observed
values, not a proof of a universal upper latency bound under overload.

Fifteen-second sharding runs (three samples) used server CPUs 0,2,4,6 and client
CPUs 8,9,10,11. The generator therefore had two physical cores with SMT, unlike
the four physical cores in the single-listener main table.

| Listeners | msg/s | Server / client CPU % | p99 ms | Incomplete | Jain | Valid |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 1 | 20,077 | 96.5 / 380.6 | 1001.2 | 241 | 0.9773 | 0/3 |
| 2 | 24,477 | 193.0 / 378.3 | 339.8 | 0 | 1.0000 | 3/3 |
| 4 | 14,782 | 378.6 / 365.7 | 416.4 | 0 | 0.9999 | 3/3 |

Stable session counts and correct echoes support reuse-port affinity in these
runs. They do not establish linear multicore scaling. Four listeners made about
10.7 million backend receive calls for 544 thousand received datagrams between
snapshots. Profiles attributed 5.67% to Socket.receiveMany, 4.28% to clock reads,
3.20% to Threaded.batchAwaitConcurrent and 2.06% to do_sys_poll; the rounding
experiment above explains an important part of that waste. Generator saturation,
loopback kernel work, smaller send batches and retransmissions also matter.

Detailed O(N) statistics snapshots were typically tens to hundreds of
microseconds at 1k and up to roughly 1.9 ms in the recorded 4k overload snapshots.
At once per second they do not justify per-packet atomic statistics. Consumers
should not collect them on every packet. After scheduler hashing was removed
from swaps, endpoint lookup was not a dominant profile entry; a custom table or
IPv4-only key would not address the remaining bottleneck.

The low-loss 100-client path still performs roughly one quota allocation/free
per echo. Queued payloads own application bytes, construction fills TX scratch,
recovery owns retransmittable wire data, and listener send batches copy bytes
until their final flush. Those copies protect real lifetimes. No borrowed-send
API or allocator pool was added without isolated ownership/performance evidence.

## Cross-implementation and traffic checks

The final candidate passed all 24 short 100-connection checks: all four
server/client combinations, each of 32/128/512/1200/8192 bytes and mixed payloads.
These three-second screens check compatibility, not stable throughput medians.
The one/ten-connection, five-second, three-sample Zig-server/Go-client medians
were 6,292 / 60,002 msg/s, with all samples valid.

At 1,000 connections, 128 bytes, window 1, five seconds after a one-second
warm-up, each server was pinned to CPU 0 and each client to CPUs 2,4,6,8:

| Server / client | msg/s | Server / client CPU % | RTT p50 / p95 / p99 ms | Server RSS MiB | Valid samples |
| --- | ---: | --- | --- | ---: | ---: |
| Zig / Go | 35,075 | 96.0 / 274.8 | 4.176 / 143.508 / 284.335 | 101.0 | 3/3 |
| Go / Go | 15,323 | 95.2 / 221.4 | 10.621 / 286.039 / 470.501 | 32.0 | 3/3 |
| Go / Zig | 11,467 | 94.7 / 378.7 | 21.070 / 366.949 / 822.543 | 30.75 | 1/3 |
| Zig / Zig | 30,772 | 97.2 / 388.8 | 9.376 / 146.573 / 357.014 | 83.38 | 2/3 |

Two Go/Zig and one Zig/Zig samples had one handshake timeout each. These remain
in the medians and are not accepted throughput results. The Zig generator's
task-per-connection cost is visible; it must not be mistaken for server-only
regression. The pinned Lunar implementation uses a 512-entry resend cap,
per-connection mutexes, send wakeups/goroutines and different ACK timing, while
this Listener has a single owner and 1,024 recovery slots. The Go echo wrapper
also starts a goroutine per connection. Lower Go RSS is real in this comparison,
but defaults, state representation and ownership differ; no resource limits
were weakened to match it.

The 100-connection pipelined screens (window 32, five seconds, three samples)
completed all samples. Median delivery was 150,609 msg/s at 128 bytes and
10,175 msg/s at 8192 bytes; p99 was 297.4 and 690.6 ms. Those high RTTs make these
overload characterization results, not evidence of low-latency service.

Five 100-connection mixed-payload cohorts reused one Listener with each client
implementation. All ten cohorts completed, and the final eleven-second cleanup
wait left zero sessions and zero recovery bytes. The quota retained 4,120 bytes
with Go and 8,216 bytes with Zig: one remaining allocation for the session map,
which is freed at Listener destruction. This is retained table capacity, not
live-session state. RSS stabilized around 18.6 MiB and 12.5 MiB during later
cohorts and fell to 13.3 MiB and 7.1 MiB after cleanup. Five cohorts cannot prove
absence of every long-duration leak; allocation-failure and destruction tests
provide complementary evidence.

### Loss, RTT, ACK delay and reordering

All **90 short screens** completed without reported echo mismatches, failed
connections or incomplete drains: 0/20/100 ms added RTT, 0/1/5% independent loss
per datagram, ACK delays 0/1/2/5/10 ms, and payloads 128/8192 bytes. Each used
20 connections, window 4, one-second warm-up and three measured seconds in a
disposable network namespace. These are one-sample screens, not stable capacity
medians. Echo validation checks length and boundary markers; full payload,
duplicate and ordering correctness also depend on the protocol regression tests.

For the unchanged zero-delay ACK default:

| Added RTT ms | Loss % | 128-byte msg/s | 128-byte p99 ms | 8192-byte msg/s | 8192-byte p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 0 | 48,116 | 9.3 | 1,273 | 98.0 |
| 0 | 1 | 36,962 | 10.5 | 1,111 | 145.6 |
| 0 | 5 | 15,610 | 69.2 | 718 | 265.9 |
| 20 | 0 | 3,840 | 21.9 | 700 | 223.0 |
| 20 | 1 | 3,564 | 42.6 | 515 | 298.2 |
| 20 | 5 | 2,622 | 153.3 | 307 | 457.2 |
| 100 | 0 | 773 | 104.1 | 360 | 1298.4 |
| 100 | 1 | 721 | 209.2 | 193 | 1409.6 |
| 100 | 5 | 558 | 413.8 | 81 | 1521.9 |

Fragmentation plus loss/RTT has a real latency cost even when delivery completes.
At 20 ms RTT/1% loss, ACK delay 0/1/2/5/10 ms produced 3,564/3,645/3,553/3,603/3,652
msg/s for 128 bytes. CPU was 4.3/12.3/12.3/11.7/22.0%; ACK records/datagram stayed
near 1.01–1.02. At 8192 bytes, rates were 515/525/512/540/566 and CPU
6.3/13.7/15.0/21.0/21.0%. Delayed ACKs reduced some ACK traffic and improved some
fragmented tails, but the short screens and increased CPU do not justify a
universal default change. NACK urgency and existing ACK behavior are retained.

With 20 ms added RTT, 1% loss, 25% reordering and 2% duplication, all six
ten-second samples completed (three per payload). Median p99 was 41.4 ms at
128 bytes and 303.4 ms at 8192; delivery was 4,260 and 379 msg/s. These validate
bounded runtime behavior under this impairment model, not every possible loss
pattern or a multi-hour soak.

### Windows is a separate backend result

Native Windows uses Zig 0.16.0 and Go 1.25.6. These three-sample runs use
128-byte payloads, window 1, five measured seconds and one-second warm-up.
The runner uses Windows process CPU/working-set sampling; Linux affinity flags
are not applied, so Go servers can use more than one CPU. Do not compare these
absolute numbers directly with the CPU-pinned Linux table.

| Connections | Server / client | msg/s | Server / client CPU % | p99 ms | Valid samples |
| ---: | --- | ---: | --- | ---: | ---: |
| 100 | Zig / Go | 15,357 | 99.7 / 133.1 | 11.9 | 3/3 |
| 100 | Go / Go | 27,844 | 152.5 / 188.8 | 14.1 | 3/3 |
| 100 | Go / Zig | 27,298 | 180.3 / 193.8 | 9.8 | 3/3 |
| 100 | Zig / Zig | 16,111 | 98.4 / 120.0 | 9.1 | 3/3 |
| 1,000 | Zig / Go | 8,452 | 99.7 / 162.2 | 951.9 | 3/3 |
| 1,000 | Go / Go | 8,812 | 141.6 / 172.5 | 786.3 | 3/3 |
| 1,000 | Go / Zig | 8,492 | 122.5 / 187.8 | 1181.1 | 0/3 |
| 1,000 | Zig / Zig | 12,224 | 95.3 / 220.6 | 97.2 | 1/3 |

Every Zig-server snapshot reported maximum receive occupancy **one**. This
matches `Io.Threaded.netReceiveWindows`, which fills only the first message;
the fallback uses AFD polling. The Zig listener saturated roughly one CPU.
ETW/AFD attribution was not collected, so the relative cost of wakeups versus
protocol work is not quantified. No speculative Windows batching backend was
added.

At 1k, Go/Zig samples had 7/4/1 handshake failures; the last also had 998
incomplete drains and zero measured deliveries. Zig/Zig had incomplete drains
in two samples. Their medians retain those failures and are not capacity wins.
Even the completed Zig/Go and Go/Go cases had high tail latency. This audit does
not establish low-latency 1k Windows service or repair its generator/backend
limitations.

## Per-session accounting

The following bytes were measured from the compiled types and the first actual
connection callback with production defaults and MTU 1492. The established
connection has already allocated the first eight recovery heap/descriptors.

| Subsystem | Original eager bytes | Candidate bytes at first connection callback |
| --- | ---: | ---: |
| Recovery slots + heap + descriptors | 86,016 | 57,568 |
| Split metadata | 3,328 | 0 |
| Outbound queue metadata | 14,336 | 0 |
| Receive/reliable windows | 8,192 | 8,192 |
| Ordered metadata | 768 | 768 |
| Sequenced channel state | 256 | 256 |
| ACK decode scratch | 2,048 | 2,048 |
| Receipt state and scratch | 8,184 | 8,184 |
| TX scratch | 1,492 | 1,492 |

The candidate Session object is 4,048 bytes, including the 2,280-byte Transmitter;
do not count that Transmitter twice. Accounted candidate state/scratch totals
82,556 bytes, excluding retained wire/payload data and listener bookkeeping.
Lazy arrays remove 46,112 eager bytes per established session. Allocator RSS is
different from exact requested bytes; compare both quota counters and RSS.

The 4,096-session configuration has a 512 MiB shared session quota. Lazy metadata
improves headroom but does not reserve each session's worst-case payload budgets.
Traffic can still exhaust the quota. 8,192/10,000 single-listener tests are outside
the unchanged default 4,096-connection limit; raising limits and quota merely to
pass the benchmark would not establish sane defaults.

## Decisions and unchanged behavior

- Retain the bounded indexed heap. A timer wheel, intrusive Session scheduler,
  custom endpoint table and immediate-work queue would add lifetime and fairness
  obligations that the current attribution does not justify.
- Keep ACK delay at zero. Overload screens were invalid; loss/RTT screens
  showed tradeoffs rather than a reproducible universal improvement.
- Keep 1,024 recovery entries and all protocol/resource limits. Lunar's 512
  resend entries are a different design point, not a reason to reduce capacity.
- Keep byte windows and inline Transmitter state for now. They account for
  8,192 bytes and 2,280 bytes respectively; their wraparound/channel semantics and
  allocator-free public initialization do not warrant another structural change
  without separate evidence. The retained memory work addresses the larger cost.
- Do not share per-session scratch or introduce borrowed send lifetimes. Current
  copies establish ownership for asynchronous recovery and queued sends.
- Keep detailed statistics as an O(N) snapshot. Once-per-second measurements are
  small relative to packet processing, so no per-packet atomics were added.
- Keep the single-owner Listener. Go's pinned Conn uses mutexes, atomics, a send
  signal/goroutine and delayed ACKs; the benchmark echo server also uses a
  goroutine per accepted connection. CPU budgets and configured limits must be
  considered when comparing it with one Zig Listener.

## Verification

- 179/179 tests pass in Windows and Linux ReleaseSafe on the retained code,
  with 100,000 deterministic malformed-input iterations.
- 179/179 tests pass in final Windows ReleaseFast. An earlier Linux Debug
  version passed 178/178 before the custom-provider regression was added.
- Linux `zig build bench` completed its protocol/Core/network workloads.
  It ran during final build verification; its timings are not used as acceptance
  numbers or compared with the isolated Listener measurements.
- `zig fmt --check`, `gofmt -l`, and `git diff --check` passed. Native Go
  `go test ./...` compiled the harness; it reports no Go test files.
- Allocation-failure tests cover lazy queue/recovery growth and split metadata.
- Scheduler stress covers 4,096 entries, churn, removal and stable index pointers.
- Existing tests retain ACK/NACK, ranges, wraparound, retransmission, ordering,
  expiry, work budgeting, resource limits and graceful-close coverage.
- All four Zig/Go pairings passed a mixed-payload smoke run with the Go race
  detector. No race report appeared in those runtime logs. This is bounded
  evidence, not a proof that every possible race or leak is absent.
- Python runner checks pass on Windows and Linux for phase parsing, retaining
  failed samples, process cleanup, persistent-server churn and completed-run resume.
- The 60-second existing soak workload (100 connections, 512 bytes, pipelined)
  completed with zero reported mismatches, incomplete drains or failures;
  Jain fairness was 0.99699. The server reported zero drops/malformed/rejected
  messages and returned to zero sessions before shutdown. It still retransmitted
  235,076 datagrams under saturation. This is a one-minute check, not a long soak.
  The first wrapper attempt failed on CRLF shell line endings; the successful
  run used an LF-normalized temporary copy of the existing script.

## Remaining limits and next work

- Single-listener saturation at 2k/4k still loses packets and fails drains.
  The improved memory headroom is not an admission-control or congestion fix.
- Multi-listener scaling is limited by generator CPU, loopback/kernel work,
  small batches and early Threaded timeouts. The rejected rounding experiment
  is not present in production. A cancellable high-resolution backend wait
  requires a separate latency-controlled implementation and comparison.
- Windows single-datagram receives and high-count generator failures remain.
  No Windows-native batched receive or ETW attribution was implemented.
- The ACK/loss grid is exploratory; longer repeated WAN tests are needed before
  choosing adaptive ACK behavior. No physical remote-generator run or full
  Minecraft Bedrock application session was performed.
- Hardware cache-miss/cycle attribution, formal lock/race proofs, exhaustive
  allocation failures across every transport callback, and multi-hour soaks
  were not performed. Passing bounded checks does not prove leak/deadlock
  absence under all workloads.
- The byte windows, inline transmitter state and remaining owned copies were
  retained. No timer wheel, custom session table, shared scratch or borrowed
  send lifetime was introduced without evidence justifying its complexity.
