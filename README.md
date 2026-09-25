# raknet-zig

A RakNet transport library for Minecraft: Bedrock Edition, written for Zig 0.16. It provides client and server connections, reliable and ordered delivery, retransmission, congestion control, and split packet reassembly.

> Version 0.1.0. Public APIs may change before 1.0.

## Install

Requires Zig 0.16.x and a `std.Io` provider.

```sh
zig fetch --save git+https://github.com/Bedrock-Phanatics/raknet-zig.git
```

Add the module to your `build.zig` after creating your executable as `exe`:

```zig
const raknet_dep = b.dependency("zig_raknet", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("raknet", raknet_dep.module("raknet"));
```

## Use

### Server

```zig
const std = @import("std");
const raknet = @import("raknet");

fn onConnected(_: *anyopaque, _: *raknet.Session) error{ApplicationFailure}!void {}

fn onMessage(
    _: *anyopaque,
    session: *raknet.Session,
    payload: raknet.BorrowedPayload,
) error{ApplicationFailure}!void {
    session.send(payload.bytes, .reliable_ordered, 0) catch
        return error.ApplicationFailure;
}

pub fn main(init: std.process.Init) !void {
    const address = try std.Io.net.IpAddress.parseLiteral("0.0.0.0:19132");
    const listener = try raknet.Server.listen(init.gpa, init.io, address, .{
        .advertisement = "MCPE;Example;11;1.21;0;20;0;world;Survival;1;19132;19133;",
    });
    defer listener.destroy();

    var context: u8 = 0;
    while (true) {
        _ = try listener.poll(.none, .{
            .context = &context,
            .connected = onConnected,
            .message = onMessage,
        });
    }
}
```

`poll` receives packets and advances protocol timers. Use `nextDeadline` and `processTimers` when integrating with another event loop. Sessions belong to the listener and become invalid after disconnect.

### Client

```zig
const std = @import("std");
const raknet = @import("raknet");

fn onMessage(_: *anyopaque, _: raknet.BorrowedPayload) error{ApplicationFailure}!void {}

pub fn main(init: std.process.Init) !void {
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:19132");
    const client = try raknet.Client.connect(init.gpa, init.io, address, .{});
    defer client.destroy();

    try client.send("hello", .reliable_ordered, 0);

    var context: u8 = 0;
    while (true) {
        _ = client.poll(.none, &context, onMessage) catch |err| switch (err) {
            error.Timeout => continue,
            else => return err,
        };
    }
}
```

Payload bytes are borrowed for the duration of their callback. Call `payload.toOwned(allocator)` to keep them afterward, and release the owned payload when done. Keep each client or listener on one event loop.

`send` queues a copy of the payload. Use `trySend` to reject sends under backpressure, or `queueSend` and `flush` to control queued sends. `cancelSend` can cancel a queued send before transmission.

## Minecraft batches

The optional `raknet.minecraft.batch` codec handles Bedrock packet framing and compression. Decode a batch only after the application has authenticated and decrypted it, and use the compression mode negotiated for that connection. The decoder supports borrowed and owned results with configurable size limits.

## Configuration and scope

Client and server options expose timeouts, connection limits, memory budgets, and per-poll work limits. Defaults are bounded; tune them for your traffic and deployment. See the [configuration guide](docs/CONFIGURATION.md) for the available settings.

The library validates packet structure and bounds allocation and protocol work on untrusted traffic. It implements the RakNet transport, not Bedrock login, authentication, or encryption. Applications remain responsible for those layers.

## Development

```sh
zig build test
zig build
```

## License

See [LICENSE](LICENSE).
