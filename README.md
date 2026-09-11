# raknet-zig

A bounded RakNet transport for Minecraft: Bedrock Edition, built for Zig 0.16.
It provides a client, a UDP listener, offline and connected handshakes,
reliability, ordering, retransmission, congestion control, and split-packet
reassembly.

The implementation is designed for hostile network input. Attacker-controlled
counts do not directly create unbounded allocations or loops, and protocol state
is constrained by explicit connection, memory, packet, window, and work limits.

> [!NOTE]
> The package is currently version 0.1.0. Until 1.0, public APIs may change
> between releases.

## Features

- RakNet offline discovery and connection handshakes
- Connected client and server sessions
- Reliable, ordered, and sequenced delivery modes
- ACK/NACK handling, RTT estimation, retransmission, and congestion control
- Bounded split-packet reassembly and ordered-packet storage
- IPv4 and IPv6 address codecs
- Batched listener reads where supported by the Zig I/O backend
- Deadline-driven server maintenance without listener-wide session scans
- Stateless HMAC handshake cookies and per-source/global rate limiting
- Aggregate memory quotas for remotely created session state
- Unit, integration, adversarial, deterministic fuzz, and microbenchmark targets

## Requirements

- Zig 0.16.x
- A `std.Io` provider from the embedding application

## Installation

Add the package to `build.zig.zon`:

```sh
zig fetch --save git+https://github.com/Bedrock-Phanatics/raknet-zig.git
```

Expose the module to your executable in `build.zig`:

```zig
const target = b.standardTargetOptions(.{});
const optimize = b.standardOptimizeOption(.{});

const app = b.addExecutable(.{
    .name = "app",
    .root_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    }),
});

const dependency = b.dependency("zig_raknet", .{
    .target = target,
    .optimize = optimize,
});
app.root_module.addImport("raknet", dependency.module("raknet"));
```

## Server API

```zig
const std = @import("std");
const raknet = @import("raknet");

const App = struct {};

fn onConnected(_: *anyopaque, _: *raknet.Session) error{ApplicationFailure}!void {}

fn onMessage(
    _: *anyopaque,
    session: *raknet.Session,
    payload: raknet.BorrowedPayload,
) error{ApplicationFailure}!void {
    session.send(payload.bytes, .reliable_ordered, 0) catch
        return error.ApplicationFailure;
}

fn onDisconnected(_: *anyopaque, _: *raknet.Session) void {}

pub fn main(init: std.process.Init) !void {
    const address = try std.Io.net.IpAddress.parseLiteral("0.0.0.0:19132");
    const listener = try raknet.Server.listen(init.gpa, init.io, address, .{
        .advertisement = "MCPE;Example Server;11;1.21;0;20;0;world;Survival;1;19132;19133;",
        .maximum_session_memory_bytes = 256 * 1024 * 1024,
    });
    defer listener.destroy();

    var app: App = .{};
    while (true) {
        const stats = try listener.poll(
            .{ .duration = .fromMilliseconds(10) },
            .{
                .context = &app,
                .connected = onConnected,
                .message = onMessage,
                .disconnected = onDisconnected,
            },
        );
        _ = stats;
    }
}
```

`Server.listen` binds the UDP socket and owns all listener resources.
`Server.poll` processes one available receive batch and any due session work.
`Server.destroy` closes the socket, destroys every session, and releases all
listener allocations.

`Server.poll` returns `PollStats` counters for received datagrams, dropped or
malformed traffic, expired/failed sessions, and failure classes suitable for
operational metrics.

A `Session` is owned by its listener. Use `Session.send`, `Session.isConnected`,
and `Session.rttMs` only while the session is live. Do not retain a session
pointer after its disconnect callback or after destroying the listener.

## Client API

```zig
const std = @import("std");
const raknet = @import("raknet");

fn onMessage(_: *anyopaque, payload: raknet.BorrowedPayload) error{ApplicationFailure}!void {
    _ = payload;
}

pub fn main(init: std.process.Init) !void {
    const server = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:19132");
    const client = try raknet.Client.connect(init.gpa, init.io, server, .{});
    defer client.destroy();

    try client.send("hello", .reliable_ordered, 0);

    var context: u8 = 0;
    _ = try client.poll(
        .{ .duration = .fromMilliseconds(10) },
        &context,
        onMessage,
    );
}
```

`Client.connect` completes the offline and connected handshakes before returning.
`Client.poll` processes one incoming datagram. `Client.close` closes the transport;
`Client.destroy` also releases all client resources.

## Payload ownership

Callback payloads are borrowed:

- `BorrowedPayload.bytes` is valid only until the synchronous callback returns.
- Wrapping or slicing the bytes does not extend their lifetime.
- Call `payload.toOwned(allocator)` before queueing, deferring, or retaining data.
- `OwnedPayload` copies exactly the payload length and must be released once with
  `deinit`.

Callbacks execute inside `poll`. Keep them short and move expensive work to an
application-owned queue after copying any required payload.

## Sending and backpressure

`Client.send` and `Session.send` accept a payload, a
`raknet.protocol.frame.Reliability`, and an order channel. Available reliability
modes are:

- `unreliable`
- `unreliable_sequenced`
- `reliable`
- `reliable_ordered`
- `reliable_sequenced`
- `unreliable_with_ack_receipt`
- `reliable_with_ack_receipt`
- `reliable_ordered_with_ack_receipt`

ACK-receipt variants preserve their RakNet wire mode, but this version does not
yet expose application receipt callbacks.

`error.CongestionWindowFull` is retryable: no packet is emitted or reserved when
the current congestion window cannot fit the message. Retry after processing
incoming ACKs. Other resource, transport, or invariant failures may close the
connection. Application callbacks may return only `error.ApplicationFailure`;
the listener reports it separately from malformed traffic.

## Configuration

`raknet.Config` contains client/server protocol limits. `raknet.ServerOptions`
and `raknet.ClientOptions` contain endpoint-specific settings. Defaults are safe
general-purpose starting points, but production deployments should set limits
from expected traffic, concurrency, and available memory.

The most important controls are:

| Setting | Default | Purpose |
| --- | ---: | --- |
| `maximum_connections` | 4,096 | Maximum live server sessions |
| `maximum_datagram_size` | 2,048 B | Maximum accepted UDP datagram |
| `maximum_frame_payload` | 8 KiB | Maximum decoded frame payload |
| `maximum_retransmissions` | 4,096 | Recovery records per connection |
| `maximum_ordered_bytes` | 16 MiB | Retained ordered payload bytes per connection |
| `maximum_split_bytes` | 4 MiB | Maximum reassembled message size |
| `maximum_split_bytes_per_connection` | 16 MiB | Aggregate split payload storage per connection |
| `maximum_packets_per_iteration` | 256 | Per-turn protocol work bound |
| `idle_timeout_ms` | 10,000 | Connected-session idle timeout |
| `maximum_session_memory_bytes` | 512 MiB | Server-wide session-state quota |
| `receive_batch_size` | 32 | Listener receive slots per poll |
| `handshake_timeout_ms` | 5,000 | Connected-handshake deadline |

See [`src/config.zig`](src/config.zig), [`src/server.zig`](src/server.zig), and
[`src/client.zig`](src/client.zig) for the complete option set.

Operational guidance:

- Size `maximum_connections` and `maximum_session_memory_bytes` together. The
  lower effective limit wins.
- Use a short finite poll timeout so protocol deadlines continue to advance when
  the socket is idle.
- Keep `maximum_datagram_size` at or above `maximum_mtu`. Negotiated MTU controls
  emitted datagram size.
- Split-part, split-byte, concurrent-assembly, recovery, and ordered-storage
  limits should be reviewed as one memory budget.
- `Listener`, `Session`, and `Client` are single-owner objects. Call them from one
  event-loop context; the packet path intentionally uses no locks.

## Security model

The parser validates complete datagrams before committing receive state or
calling application code. Wire-derived packet counts, ACK ranges, windows,
reassembly state, recovery state, and per-turn work are bounded.

Server handshake cookies and rate limits reduce spoofed-source allocation and
amplification. They do not authenticate players or application payloads. Deploy
application authentication, encryption, authorization, and abuse controls above
this transport as required.

## Scope

This package implements RakNet transport. It does not implement Minecraft login,
packet versions, encryption, compression, resource packs, or gameplay protocol.
Applications remain responsible for those layers and for validating compatibility
with the Bedrock versions and platforms they support.

## Development

```sh
zig build test
zig build test -Doptimize=ReleaseSafe
zig build bench
```

The default test run includes unit, loopback integration, adversarial, and
deterministic malformed-input coverage. Increase the fuzz workload with
`-Dfuzz-iterations=<count>`. Benchmarks are isolated in-memory regression tools,
not network-throughput claims.

## License

Licensed under the [MIT License](LICENSE).