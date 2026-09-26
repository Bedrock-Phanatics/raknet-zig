# raknet-zig

A RakNet client and server library for Minecraft: Bedrock Edition, written in Zig.

> Version 0.2.4. Public APIs may change before 1.0.

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

Payloads are valid only during their callback. Use `payload.toOwned(allocator)` to retain a copy and release it when done. Keep each client or listener on one event loop.

`send` sends immediately or queues a copy, returning an error if the queue is full. `trySend` returns `false` under backpressure without queuing. The caller can reuse its buffer after either call.

`Client.close()` starts graceful shutdown. Keep calling `poll()` or `processTimers()` until `isClosed()` is true. `Session.close()` requires continued listener polling. Closing rejects new sends and drains pending data until the shutdown timeout. Use `destroy()` for immediate teardown.

## Minecraft batches

`raknet.minecraft.batch` handles Bedrock packet framing and compression. Decode authenticated, decrypted data using the connection's negotiated compression mode and appropriate size limits.

## Configuration and scope

See the [configuration guide](docs/CONFIGURATION.md) for timeouts, connection limits, memory budgets, and per-poll work limits.

Applications handle Bedrock login, authentication, and encryption.

## Development

```sh
zig build test
zig build
```

## License

See [LICENSE](LICENSE).
