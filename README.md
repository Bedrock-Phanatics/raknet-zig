# zig-raknet

A bounded, event-loop-oriented RakNet transport for Minecraft: Bedrock Edition,
written for Zig 0.16. The library includes offline discovery and connection
handshakes, connected handshakes, ACK/NACK processing, reliability and ordering,
split-packet reassembly, retransmission, RTT estimation, congestion control, IPv4
and IPv6 address codecs, a client, and a UDP listener.

The implementation treats every network byte as hostile. Wire-derived counts do
not directly allocate memory or drive unbounded loops. All receive windows,
ordering queues, split assemblies, recovery data, ACK work, connections, and
per-poll work have explicit limits. The server additionally applies stateless
HMAC cookies before session allocation, per-source and global token buckets, and
a listener-wide byte quota for all remotely-created session state.

## Requirements and build

- Zig 0.16.0 or newer within the 0.16 language/API line
- A `std.Io` provider supplied by the embedding application

```text
zig build test
zig build -Doptimize=ReleaseSafe test
zig build bench
```

Add the package as a dependency and import its `raknet` module. `build.zig.zon`
contains the package metadata required by Zig's package manager.

## Server sketch

```zig
const std = @import("std");
const raknet = @import("raknet");

fn connected(_: *anyopaque, session: *raknet.Server.Session) !void {
    _ = session;
}

fn message(_: *anyopaque, session: *raknet.Server.Session, payload: []const u8) !void {
    // payload is borrowed and is valid only for this callback.
    try session.send(payload, .reliable_ordered, 0);
}

pub fn serve(allocator: std.mem.Allocator, io: std.Io) !void {
    const bind_address = try std.Io.net.IpAddress.parseLiteral("0.0.0.0:19132");
    const listener = try raknet.Server.listen(allocator, io, bind_address, .{
        .advertisement = "MCPE;My Server;11;1.21;0;20;0;name;Survival;1;19132;19133;",
        .maximum_session_memory_bytes = 256 * 1024 * 1024,
    });
    defer listener.destroy();

    var app: u8 = 0;
    while (true) {
        _ = try listener.poll(
            .{ .duration = .fromMilliseconds(10) },
            .{ .context = &app, .connected = connected, .message = message },
        );
    }
}
```

`Listener`, each `Session`, and `Client` are single-owner objects. Call their
methods from one event-loop context; the packet path deliberately has no locks.
Callbacks run synchronously from `poll`. Copy a delivered payload if it must
outlive its callback. A session pointer is borrowed from its listener and must
not be retained after disconnection or listener destruction.

`send` is transactional and backpressured. It returns
`error.CongestionWindowFull` before emitting or reserving any packet when the
current congestion window cannot fit the entire message. Retry after processing
ACKs. Reliable wire copies are retained only in the bounded recovery store.

## Configuration

`Config` holds protocol limits shared by client and server. `ServerOptions` adds
listener limits such as the receive batch, offline rate limits, the aggregate
session-memory quota, and maintenance cadence. Defaults are conservative general
purpose values, not a promise that every workload should use them unchanged.

Important operational rules:

- Set `maximum_connections` and `maximum_session_memory_bytes` together. The
  lower effective limit wins, and a quota failure drops a new handshake without
  destabilizing existing sessions.
- Poll with a short finite timeout. Timeout polls perform recovery and expiry
  maintenance and return zeroed statistics rather than surfacing `error.Timeout`.
- `maximum_split_parts` defaults to 2048 because observed Bedrock ecosystems can
  exceed 1,400 fragments; byte and concurrent-assembly caps remain authoritative.
- `maximum_datagram_size` is an input boundary, while negotiated MTU determines
  emitted datagram size.
- Application callback errors propagate through the packet-processing path; the
  listener counts rejected input as malformed. Keep callbacks short and move
  expensive work to an application queue.

## Scope

This is a RakNet transport, not a Minecraft login, encryption, compression, or
game-protocol implementation. It does not authenticate application payloads.
Stateless handshake cookies reduce spoofed-source allocation and amplification;
they are not player identity credentials.

The benchmark target measures isolated in-memory codec/window operations. It is
useful for regressions but is not a network-throughput claim. Validate limits,
loss behavior, and interoperability against the Bedrock versions and platforms
you deploy before exposing a service to the public Internet.

See [SECURITY.md](SECURITY.md) for the threat model and reporting guidance.

