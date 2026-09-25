# Configuration Reference

`raknet.Config` groups transport settings by purpose. Values are copied into each
client or server session and cannot change while that session is active.

Default origins are:

- **Protocol:** required by RakNet or its wire representation.
- **Reference:** aligned with established RakNet implementations.
- **Compatibility:** chosen from observed Bedrock interoperability.
- **Policy:** a local resource or abuse bound.

## Protocol limits

| Field | Default | Scope | Origin | Memory and exceed behavior |
| --- | ---: | --- | --- | --- |
| `minimum_mtu` | 576 B | Endpoint | Compatibility | Rejects smaller negotiated MTUs. |
| `maximum_mtu` | 1,492 B | Endpoint | Compatibility | Bounds per-session scratch and emitted datagrams. |
| `maximum_datagram_size` | 2,048 B | Endpoint | Policy | Sets each receive slot; larger datagrams are dropped. |
| `maximum_frame_payload` | 8 KiB | Session | Policy | Oversized decoded frames are rejected. |
| `maximum_acknowledged_datagrams` | 4,096 | Session | Policy | Bounds ACK/NACK expansion and work. |
| `receive_window` | 4,096 packets | Session | Reference | Allocates one receive bit per slot; large jumps are rejected. |
| `reliable_window` | 4,096 packets | Session | Reference | Allocates one reliable bit per slot; large jumps close the session. |
| `maximum_order_channels` | 32 | Session | Compatibility | Allocates per-channel state; larger channel IDs are rejected. |
| `maximum_split_parts` | 8,192 | Session | Compatibility | Rejects larger split counts. |
| `maximum_split_bytes` | 4 MiB | Message | Policy | Rejects larger outbound or reassembled messages. |

## Session limits

| Field | Default | Scope | Origin | Memory and exceed behavior |
| --- | ---: | --- | --- | --- |
| `maximum_retransmissions` | 4,096 packets | Session | Policy | Preallocates recovery metadata; excess reliable sends fail. |
| `maximum_recovery_bytes` | 16 MiB | Session | Policy | Independently caps retained wire data; excess sends fail. |
| `maximum_ordered_packets` | 4,096 packets | Session | Policy | Preallocates ordered metadata; excess packets close the session. |
| `maximum_ordered_bytes` | 16 MiB | Session | Policy | Caps buffered out-of-order payload; excess closes the session. |
| `maximum_concurrent_splits` | 64 | Session | Policy | Preallocates assembly slots; a new assembly beyond it is left unacknowledged for retry. |
| `maximum_split_bytes_per_connection` | 16 MiB | Session | Policy | Caps all incomplete fragment payloads; excess fragments are left unacknowledged for retry. |
| `maximum_split_parts_per_connection` | 16,384 | Session | Policy | Caps fragment metadata across assemblies; excess assemblies are left unacknowledged for retry. |
| `maximum_queued_outbound_packets` | 256 packets | Session | Policy | Preallocates queue slots; excess application sends return backpressure. |
| `maximum_queued_outbound_bytes` | 16 MiB | Session | Policy | Caps queued owned payloads; excess application sends return backpressure. |
| `reserved_control_queue_packets` | 16 packets | Session | Policy | Reserves existing queue slots for control traffic. |
| `reserved_control_queue_bytes` | 64 KiB | Session | Policy | Reserves existing queue bytes for control traffic. |

## Listener limits

| Field | Default | Scope | Origin | Memory and exceed behavior |
| --- | ---: | --- | --- | --- |
| `maximum_pending_handshakes` | 4,096 sources | Listener | Policy | Sizes rate-limit state; sources sharing a slot share its budget. |
| `maximum_connections` | 4,096 sessions | Listener | Policy | Sizes session/deadline tables; new clients receive a capacity response. |

`ServerOptions.maximum_session_memory_bytes` is the listener-wide allocation
ceiling for session-owned state. It should be sized together with
`maximum_connections`; allocation failure rejects the new session or closes the
session whose required state cannot be retained.

## Timing

All timing values use monotonic milliseconds.

| Field | Default | Scope | Origin | Exceed behavior |
| --- | ---: | --- | --- | --- |
| `maximum_ack_delay_ms` | 0 ms | Session | Policy | Values above 10 ms are invalid; zero flushes after the input batch. |
| `split_timeout_ms` | 15,000 ms | Session | Reference | Expired incomplete assemblies are released. |
| `idle_timeout_ms` | 10,000 ms | Session | Policy | Idle sessions are closed. |
| `minimum_rto_ms` | 50 ms | Session | Compatibility | Lower RTT estimates are clamped. |
| `maximum_rto_ms` | 5,000 ms | Session | Compatibility | Higher retransmission delays are clamped. |
| `shutdown_timeout_ms` | 5,000 ms | Session | Compatibility | A local close that is not acknowledged by then is forced. |

## Batching

| Field | Default | Scope | Origin | Memory and exceed behavior |
| --- | ---: | --- | --- | --- |
| `maximum_ack_records` | 256 records | Session | Policy | Allocates ACK scratch once; extra ranges are split across datagrams. |
| `maximum_packets_per_iteration` | 256 work units | Event-loop turn | Policy | Defers remaining packets and timers to the next turn. |

Endpoint options such as `receive_batch_size`, handshake limits, socket buffers,
rate limits, and the listener memory quota remain in `ServerOptions` or
`ClientOptions` because they do not alter RakNet session semantics.
