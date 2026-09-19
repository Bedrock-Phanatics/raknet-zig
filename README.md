# raknet-zig

A bounded RakNet transport for Minecraft: Bedrock Edition, built for Zig 0.16.
It provides a client, a UDP listener, offline and connected handshakes,
reliability, ordering, retransmission, congestion control, and split-packet
reassembly.

<p align="center">
    Join our <a href="https://discord.gg/Yv9qPRQNc3">Discord</a>!
</p>

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
- Batched reads and deadline-driven maintenance
- Stateless handshake cookies, rate limiting, and session memory quotas
- Optional bounded Minecraft batch decoding for zlib and Snappy
- IPv4 and IPv6 support

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
        .config = .{ .listener = .{ .maximum_connections = 1_024 } },
    });
    defer listener.destroy();

    var app: App = .{};
    while (true) {
        const stats = try listener.poll(
            .none,
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

`Server.listen` binds the UDP socket and owns the listener resources.
`Server.poll` processes an available receive batch, dispatches callbacks, runs
due timers, and returns `PollStats`. `Server.destroy` closes all sessions and
releases the listener.

Custom event loops can call `Server.nextDeadline` to read the earliest protocol
deadline and `Server.processTimers` to run due work without receiving a packet.
Deadlines and `now_ms` use monotonic milliseconds from `std.Io.Clock.awake`.
Both `poll` methods treat their timeout as an upper bound and shorten it to the
next protocol deadline.

A `Session` is owned by its listener. Use `Session.send`, `Session.isConnected`,
and `Session.statistics` only while the session is live. Do not retain a session
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
        .none,
        &context,
        onMessage,
    );
}
```

`Client.connect` completes both handshakes before returning. `Client.poll`
processes one datagram and due timer work. `Client.close` closes the transport;
`Client.destroy` also releases its resources. Custom loops can use
`Client.nextDeadline` and `Client.processTimers` with the same clock contract as
the server.

## Payload ownership

Callback payloads are borrowed:

- `BorrowedPayload.bytes` is valid only until the synchronous callback returns.
- Wrapping or slicing the bytes does not extend their lifetime.
- Call `payload.toOwned(allocator)` before queueing, deferring, or retaining data.
- `OwnedPayload` copies exactly the payload length and must be released once with
  `deinit`.

Callbacks execute inside `poll`. Keep them short and move expensive work to an
application-owned queue after copying any required payload.

## Optional Minecraft batch codec

```zig
var decoder = try raknet.minecraft.batch.Decoder.init(allocator, .{});
defer decoder.deinit();

_ = try decoder.decodeBorrowed(batch, .declared, now_ms, context, onPacket);
var owned = try decoder.decodeOwned(batch, .declared, now_ms);
defer owned.deinit();
```

Borrowed packet bytes expire when their callback returns. Pass `.disabled`
before compression is negotiated. Encrypted batches must be authenticated and
decrypted before decoding.

## Sending and backpressure

```zig
if (!session.isConnected()) return;

try session.send("immediate", .reliable_ordered, 0);

if (!try session.trySend("hello", .reliable_ordered, 0)) {
    _ = try session.queueSend("hello", .reliable_ordered, 0);
}
_ = try session.flush();

const pending = try session.queueSend("cancel me", .reliable, 0);
_ = session.cancelSend(pending);

const statistics = session.statistics();
_ = statistics;
```

## Configuration

`raknet.Config` groups protocol, session, listener, timing, and batching
settings. Endpoint settings live in `raknet.ServerOptions` and
`raknet.ClientOptions`. Defaults are bounded starting points. Production deployments should tune them for expected traffic,
concurrency, and available memory.

The most important controls are:

| Setting | Default | Purpose |
| --- | ---: | --- |
| `config.listener.maximum_connections` | 4,096 | Maximum live server sessions |
| `config.protocol.maximum_datagram_size` | 2,048 B | Maximum accepted UDP datagram |
| `config.protocol.maximum_frame_payload` | 8 KiB | Maximum decoded frame payload |
| `config.session.maximum_retransmissions` | 4,096 | Recovery records per connection |
| `config.session.maximum_recovery_bytes` | 16 MiB | Retained recovery bytes per connection |
| `config.session.maximum_queued_outbound_packets` | 256 | Queued messages per connection |
| `config.session.maximum_queued_outbound_bytes` | 16 MiB | Queued payload bytes per connection |
| `config.session.maximum_ordered_bytes` | 16 MiB | Retained ordered payload bytes per connection |
| `config.protocol.maximum_split_bytes` | 4 MiB | Maximum reassembled message size |
| `config.session.maximum_split_bytes_per_connection` | 16 MiB | Aggregate split payload storage per connection |
| `config.batching.maximum_packets_per_iteration` | 256 | Per-turn protocol work bound |
| `config.timing.idle_timeout_ms` | 10,000 | Connected-session idle timeout |
| `maximum_session_memory_bytes` | 512 MiB | Server-wide session-state quota |
| `receive_batch_size` | 32 | Listener receive slots per poll |
| `handshake_timeout_ms` | 5,000 | Connected-handshake deadline |

See [Configuration](docs/CONFIGURATION.md) for units, scope, memory costs, and
limit behavior.

Operational guidance:

- Size `config.listener.maximum_connections` and `maximum_session_memory_bytes`
  together.
- Poll timeouts are upper bounds. Use `.none` to wait until traffic or the next
  protocol deadline, or pass a shorter timeout for application work.
- Keep `config.protocol.maximum_datagram_size` at or above
  `config.protocol.maximum_mtu`. Negotiated MTU controls emitted datagram size.
- Review split, recovery, and ordered-storage limits as one memory budget.
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

This package implements RakNet transport and an optional Minecraft packet-batch
codec. It does not implement Minecraft login, packet versions, encryption,
resource packs, or gameplay protocol. Applications remain responsible for those
layers and for validating compatibility with supported Bedrock versions.

## Development

```sh
zig build test
zig build test -Doptimize=ReleaseSafe
zig build bench
```

The default test run includes unit, integration, adversarial, and deterministic
fuzz coverage. Increase the workload with `-Dfuzz-iterations=<count>`.

## License

Licensed under the [MIT License](LICENSE).
