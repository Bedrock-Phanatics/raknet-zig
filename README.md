# raknet-zig

A RakNet client and server library for Minecraft: Bedrock Edition, written in Zig.

> Version 0.2.6. Public APIs may change before 1.0.

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

Minecraft batch handling is provided by [Bedwire](https://github.com/Bedrock-Phanatics/bedwire)

## Configuration and scope

See the [configuration guide](docs/CONFIGURATION.md) for timeouts, connection limits, memory budgets, and per-poll work limits.

Applications handle Bedrock login, authentication, and encryption.

## Benchmarks vs go-raknet

Measured on September 28, 2026 against upstream
[sandertv/go-raknet v1.15.2](https://github.com/sandertv/go-raknet/tree/v1.15.2).
Both servers used the same upstream Go client, 128-byte reliable ordered echoes,
and one outstanding request per connection. These are loopback transport tests,
not Minecraft gameplay benchmarks.

Medians of three runs, each with ten measured seconds after two seconds of
warm-up; server order alternated between rounds. Host: Ryzen 5 5500, Ubuntu on
WSL2, about 8 GiB RAM, Zig 0.16.0 ReleaseFast and Go 1.27.1. Each server was pinned
to one logical CPU; the client used four separate physical cores.

| Connections | Server | Echoes/s | RTT p50 / p95 / p99 (ms) | Server RSS (MiB) | Completed runs |
| ---: | --- | ---: | --- | ---: | ---: |
| 100 | raknet-zig | 53,502 | 0.875 / 1.315 / 58.743 | 13.00 | 3/3 |
| 100 | go-raknet | 53,019 | 0.517 / 1.415 / 4.361 | 78.63 | 3/3 |
| 1,000 | raknet-zig | 78,881 | 2.051 / 59.871 / 294.048 | 115.25 | 3/3 |
| 1,000 | go-raknet | 53,210* | 1.821 / 9.575 / 364.520 | 101.38 | 0/3 |

At 100 connections, throughput was similar: individual runs ranged from
50,126–71,432 echoes/s for Zig and 51,785–58,146 for Go. Go had lower p99 latency;
Zig used less RSS. At 1,000 connections, Zig completed all runs but both servers
had high tail latency. **The starred Go result includes incomplete drains in
every run and is not a successful capacity result.** No throughput speedup ratio
is claimed from those failed runs.

Completed means no reported connection failures, payload mismatches or incomplete
drains, with the requested session population maintained. Throughput excludes
drained replies; RTT includes them and uses each connection's last 2,048 samples.
RSS is sampled at measurement boundaries, not peak memory. Defaults and internal
resource limits differ between libraries. These short, single-core WSL2 results
do not establish Windows, WAN or multicore performance.

See [individual samples and build metadata](tests/interop/upstream-results.json)
and [reproduction instructions](tests/interop/README.md#upstream-go-raknet-comparison).
The earlier [scalability audit](tests/interop/AUDIT.md) used Lunar's fork and is a
separate comparison.

## Development

```sh
zig build test
zig build
```

## License

See [LICENSE](LICENSE).
