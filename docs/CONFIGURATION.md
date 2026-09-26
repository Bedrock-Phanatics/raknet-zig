# Configuration Reference

`raknet.Config` values are copied into each session. Configure them before connecting or listening.

## Protocol limits

| Field | Default | Behavior |
| --- | ---: | --- |
| `minimum_mtu` | 576 B | Rejects smaller negotiated MTUs. |
| `maximum_mtu` | 1,492 B | Bounds per-session scratch and emitted datagrams. |
| `maximum_datagram_size` | 2,048 B | Sets each receive slot. Larger datagrams are dropped. |
| `maximum_frame_payload` | 8 KiB | Oversized decoded frames are rejected. |
| `maximum_acknowledged_datagrams` | 4,096 | Bounds ACK/NACK expansion and work. |
| `receive_window` | 4,096 packets | Allocates one boolean per slot. Accepted forward jumps slide past older holes. |
| `maximum_datagram_gap` | 65,536 packets | Maximum forward distance from the oldest pending datagram. Larger jumps are rejected without ACK or state changes. Must be at least `receive_window` and below the 24-bit half-range. |
| `reliable_window` | 4,096 packets | Allocates one boolean per slot. Large jumps close the session. |
| `maximum_order_channels` | 32 | Allocates per-channel state. Larger channel IDs are rejected. |
| `maximum_split_parts` | 8,192 | Rejects larger split counts. |
| `maximum_split_bytes` | 4 MiB | Rejects larger outbound or reassembled messages. |

The default datagram gap allows recovery across sixteen default receive windows without allocating for the gap. Window advancement stays bounded by `receive_window`, and NACK reporting by `maximum_acknowledged_datagrams`.

## Session limits

| Field | Default | Behavior |
| --- | ---: | --- |
| `maximum_retransmissions` | 1,024 packets | Preallocates recovery metadata and bounds unacknowledged reliable datagrams. Further sends wait for ACKs. |
| `maximum_recovery_bytes` | 16 MiB | Independently caps retained wire data. Excess sends fail. |
| `maximum_ordered_packets` | 4,096 packets | Ordered metadata grows lazily up to this limit. Excess packets close the session. |
| `maximum_ordered_bytes` | 16 MiB | Caps buffered out-of-order payload. Excess closes the session. |
| `maximum_concurrent_splits` | 64 | Preallocates assembly slots. A new assembly beyond it is left unacknowledged for retry. |
| `maximum_split_bytes_per_connection` | 16 MiB | Caps all incomplete fragment payloads. Excess fragments are left unacknowledged for retry. |
| `maximum_split_parts_per_connection` | 16,384 | Caps fragment metadata across assemblies. Excess assemblies are left unacknowledged for retry. |
| `maximum_queued_outbound_packets` | 256 packets | Preallocates queue slots. Excess application sends return backpressure. |
| `maximum_queued_outbound_bytes` | 16 MiB | Caps queued owned payloads. Excess application sends return backpressure. |
| `reserved_control_queue_packets` | 16 packets | Reserves existing queue slots for control traffic. |
| `reserved_control_queue_bytes` | 64 KiB | Reserves existing queue bytes for control traffic. |

## Listener limits

| Field | Default | Behavior |
| --- | ---: | --- |
| `maximum_pending_handshakes` | 4,096 sources | Sizes rate-limit state. Sources sharing a slot share its budget. |
| `maximum_connections` | 4,096 sessions | Sizes session/deadline tables. New clients receive a capacity response. |

`ServerOptions.maximum_session_memory_bytes` caps session memory across the listener. Allocation failure rejects a new session or closes the affected session.

## Timing

All timing values use monotonic milliseconds.

| Field | Default | Behavior |
| --- | ---: | --- |
| `maximum_ack_delay_ms` | 0 ms | Values above 10 ms are invalid. Zero flushes after the input batch. |
| `split_timeout_ms` | 15,000 ms | Expired incomplete assemblies are released. |
| `idle_timeout_ms` | 10,000 ms | Idle sessions, and reliable datagrams unacknowledged for this long, are closed. |
| `minimum_rto_ms` | 50 ms | Minimum retransmission timeout. |
| `maximum_rto_ms` | 5,000 ms | Maximum retransmission timeout. |
| `shutdown_timeout_ms` | 5,000 ms | A local close that is not acknowledged by then is forced. |

## Batching

| Field | Default | Behavior |
| --- | ---: | --- |
| `maximum_ack_records` | 256 records | Allocates ACK scratch once. Extra ranges are split across datagrams. |
| `maximum_packets_per_iteration` | 256 work units | Defers remaining packets and timers to the next turn. |

Receive batching, handshake options, socket buffers, `reuse_port`, and rate limits are configured through `ServerOptions` or `ClientOptions`.
