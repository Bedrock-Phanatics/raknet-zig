# raknet-zig

A RakNet client and server library for Minecraft: Bedrock Edition, written in Zig.

> Version 0.2.6. Public APIs may change before 1.0.

## Install

Requires Zig 0.17.x and a `std.Io` provider.

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

Minecraft batch handling is provided by [Bedwire](https://github.com/Bedrock-Phanatics/bedwire)

## Configuration and scope

See the [configuration guide](docs/CONFIGURATION.md) for timeouts, connection limits, memory budgets, and per-poll work limits.

Applications handle Bedrock login, authentication, and encryption.

## Benchmarks vs go-raknet

Compared with [sandertv/go-raknet v1.15.2](https://github.com/sandertv/go-raknet/tree/v1.15.2)
using its own client against both servers, with reliable ordered echoes over
loopback. RTT is localhost echo time, not player ping. Medians of three rounds on
a Ryzen 5 5500 under WSL2, each server pinned to one CPU.

**Paced load**, one request per connection per interval:

| Workload | Server | RTT p50 / p99 (ms) | Server CPU | RSS (MiB) |
| --- | --- | --- | ---: | ---: |
| 100 conns, 128 B, 10/s | raknet-zig | 0.152 / 0.289 | 3.3% | 9.8 |
|  | go-raknet | 0.192 / 0.696 | 6.9% | 11.2 |
| 1,000 conns, 128 B, 10/s | raknet-zig | 0.259 / 1.244 | 16.9% | 83.4 |
|  | go-raknet | 0.418 / 8.506 | 36.6% | 38.4 |
| 4,096 conns, 128 B, 2.5/s | raknet-zig | 0.207 / 1.387 | 25.7% | 335.8 |
|  | go-raknet | 0.379 / 232.503 | 70.1% | 105.1 |
| 1,000 conns, 8 KiB, 1/s | raknet-zig | 0.172 / 0.350 | 10.6% | 100.4 |
|  | go-raknet | 0.215 / 2.968 | 28.8% | 42.5 |

**Impaired network and overload**, one outstanding request per connection:

| Workload | Server | Echoes/s | RTT p50 / p99 (ms) | Completed runs |
| --- | --- | ---: | --- | ---: |
| 100 conns, 128 B, 20 ms RTT, 1% loss | raknet-zig | 4,317 | 20.2 / 158.2 | 3/3 |
|  | go-raknet | 3,822 | 20.2 / 296.0 | 2/3 |
| 100 conns, 8 KiB, 20 ms RTT, 1% loss | raknet-zig | 1,250 | 79.6 / 279.7 | 3/3 |
|  | go-raknet | 1,338 | 20.4 / 621.4 | 0/3 |
| 100 conns, 8 KiB, 50 ms RTT | raknet-zig | 1,570 | 50.5 / 149.3 | 3/3 |
|  | go-raknet | 1,820 | 50.3 / 57.9 | 3/3 |
| 100 conns, 128 B, saturated | raknet-zig | 109,325 | 0.874 / 1.598 | 3/3 |
|  | go-raknet | 65,013 | 0.591 / 5.151 | 3/3 |
| 1,000 conns, 128 B, saturated | raknet-zig | 90,915 | 3.475 / 276.844 | 3/3 |
|  | go-raknet | 64,542 | 1.530 / 341.676 | 2/3 |

Both servers delivered the full paced load. Incomplete go-raknet runs had
failed connections or undrained requests. go-raknet's server has no congestion
window, which helps it on large replies to its own client. raknet-zig uses more
memory per session. Saturated runs show overload behavior, not healthy latency.

See [the samples](tests/interop/results.json), [how to reproduce them](tests/interop/README.md)
and [the performance audit](tests/interop/AUDIT.md).

## Development

```sh
zig build test
zig build
```

## License

See [LICENSE](LICENSE).
