# Security policy

## Threat model

The UDP peer is untrusted. It may spoof source addresses, truncate or concatenate
packets, advertise extreme counts, replay handshakes, send overlapping ACK ranges,
force sequence-number wraparound, duplicate or conflict split fragments, reorder
traffic, or attempt CPU, memory, and bandwidth exhaustion.

The implementation's security boundaries are:

- checked cursors for all wire reads and exact structural validation before
  allocation;
- 24-bit serial-number comparisons with half-range ambiguity rejected;
- canonical, sorted, non-overlapping ACK/NACK ranges and independent work caps;
- fixed receive/reliable windows and bounded ordered and reassembly stores;
- duplicate split-fragment equality checks, collision teardown, byte quotas, and
  expiry;
- stateless, endpoint-bound, rotating HMAC-SHA256 handshake cookies before a
  session can be allocated;
- keyed per-source and global token buckets for offline traffic;
- a hard listener-wide allocator quota around the session hash table and all
  remotely-created session state;
- exact congestion/recovery preflight before a multi-datagram send, preventing
  partial state commitment when backpressured;
- bounded batch, ACK, retransmission, handshake, and expiry work per iteration.

The listener is single-owner. Sharing it or a session concurrently without
external serialization is outside the supported model. Application callbacks are
trusted code and can still consume unbounded resources, block the event loop, or
copy borrowed messages indefinitely. The embedding application is responsible
for access control, Minecraft protocol validation, authentication, encryption,
and abuse controls above RakNet.

## Operational hardening

- Prefer `ReleaseSafe` for Internet-facing builds unless profiling justifies a
  different safety mode.
- Use a finite poll timeout so retransmission and idle expiry run during quiet
  periods.
- Reduce connection, split, ordered-byte, recovery-byte, and global session-byte
  limits to the smallest values your workload needs.
- Apply network-layer filtering and aggregate rate limits upstream when exposed
  to volumetric attacks. A userspace UDP library cannot prevent link saturation.
- Treat advertisement strings as public, attacker-queryable data.
- Do not expose allocator diagnostics, packet bodies, cookies, or endpoint data
  in unauthenticated error responses or high-volume logs.

## Reporting a vulnerability

Do not publish exploit details before a fix is available. Report the affected
revision, target OS/architecture, Zig version, minimal reproducer or packet bytes,
and the security impact to the repository owner through a private channel. Avoid
including production secrets, player data, or live server addresses.

There is currently no declared long-term support window. Consumers should pin a
reviewed revision and rerun the adversarial tests when updating Zig or this
library.

