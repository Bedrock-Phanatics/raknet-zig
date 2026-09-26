const std = @import("std");
const raknet = @import("raknet");
const Client = raknet.Client;
const Config = raknet.Config;
const Options = raknet.ClientOptions;

test "client and server complete a real loopback handshake" {
    var io_instance: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer io_instance.deinit();
    const io = io_instance.io();
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var server_config: Config = .{};
    server_config.protocol.maximum_mtu = 1200;
    server_config.listener.maximum_connections = 1;
    var listener = try raknet.Server.listen(std.testing.allocator, io, address, .{ .advertisement = "MCPE;interop", .config = server_config });
    defer listener.destroy();
    const Harness = struct {
        listener: *raknet.Server,
        connected: std.atomic.Value(bool) = .init(false),
        message_data: [32]u8 = undefined,
        message_len: usize = 0,
        fail_messages: bool = false,
        disconnected: usize = 0,
        fn onConnect(raw: *anyopaque, _: *raknet.Session) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.connected.store(true, .release);
        }
        fn onMessage(raw: *anyopaque, _: *raknet.Session, payload: raknet.BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail_messages or payload.bytes.len > self.message_data.len) return error.ApplicationFailure;
            @memcpy(self.message_data[0..payload.bytes.len], payload.bytes);
            self.message_len = payload.bytes.len;
        }
        fn onDisconnect(raw: *anyopaque, _: *raknet.Session) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.disconnected += 1;
        }
        fn run(self: *@This(), io_value: std.Io) !void {
            _ = io_value;
            while (!self.connected.load(.acquire)) {
                _ = try self.listener.poll(.none, .{ .context = self, .connected = onConnect, .message = onMessage });
            }
        }
    };
    var harness: Harness = .{ .listener = listener };
    var server_task = try io.concurrent(Harness.run, .{ &harness, io });
    defer server_task.cancel(io) catch {};
    const client = Client.connect(std.testing.allocator, io, listener.socket.value.address, .{ .handshake_retry_ms = 10 }) catch |err| {
        std.debug.print("client connect failed: {any}\n", .{err});
        return err;
    };
    defer client.destroy();
    try server_task.await(io);
    try std.testing.expect(harness.connected.load(.acquire));
    try std.testing.expectEqual(listener.handshake_handler.server_guid, client.server_guid);
    try std.testing.expectEqual(@as(u16, 1200), client.mtu);

    const CapacityHarness = struct {
        listener: *raknet.Server,
        harness: *Harness,
        stop: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This(), io_value: std.Io) !void {
            _ = io_value;
            while (!self.stop.load(.acquire)) {
                _ = try self.listener.poll(.{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }, .{ .context = self.harness, .connected = Harness.onConnect, .message = Harness.onMessage });
            }
        }
    };
    var capacity_harness: CapacityHarness = .{ .listener = listener, .harness = &harness };
    var capacity_task = try io.concurrent(CapacityHarness.run, .{ &capacity_harness, io });
    try std.testing.expectError(error.NoFreeIncomingConnections, Client.connect(std.testing.allocator, io, listener.socket.value.address, .{ .handshake_retry_ms = 10 }));
    capacity_harness.stop.store(true, .release);
    try capacity_task.await(io);

    try client.send("\xfehello", .reliable_ordered, 0);
    for (0..4) |_| {
        _ = try listener.poll(.none, .{ .context = &harness, .connected = Harness.onConnect, .message = Harness.onMessage });
        if (harness.message_len != 0) break;
    }
    try std.testing.expectEqualStrings("\xfehello", harness.message_data[0..harness.message_len]);

    const canceled = try client.queueSend("\xfecanceled", .reliable_ordered, 0);
    try std.testing.expectEqual(raknet.CancelResult.canceled, client.cancelSend(canceled));
    _ = try client.queueSend("\xfequeued", .reliable_ordered, 0);
    const outbound_deadline = client.outbound_deadline_ms.?;
    try std.testing.expectEqual(outbound_deadline, client.nextDeadline().?);
    try client.processTimers(outbound_deadline);
    try std.testing.expectEqual(@as(usize, 0), client.core.outboundCount(.application));
    harness.message_len = 0;
    for (0..4) |_| {
        _ = try listener.poll(.none, .{ .context = &harness, .connected = Harness.onConnect, .message = Harness.onMessage });
        if (harness.message_len != 0) break;
    }
    try std.testing.expectEqualStrings("\xfequeued", harness.message_data[0..harness.message_len]);

    var session_iterator = listener.sessions.valueIterator();
    const session = session_iterator.next().?.*;
    _ = try session.queueSend("\xfeworld", .reliable_ordered, 0);
    const server_outbound_deadline = session.outbound_deadline_ms.?;
    try std.testing.expectEqual(server_outbound_deadline, listener.nextDeadline().?);
    _ = try listener.processTimers(server_outbound_deadline, .{ .context = &harness, .connected = Harness.onConnect, .message = Harness.onMessage });
    try std.testing.expectEqual(@as(usize, 0), session.core.outboundCount(.application));
    const ClientCollector = struct {
        data: [32]u8 = undefined,
        len: usize = 0,
        fn collect(raw: *anyopaque, payload: raknet.BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            @memcpy(self.data[0..payload.bytes.len], payload.bytes);
            self.len = payload.bytes.len;
        }
    };
    var collector: ClientCollector = .{};
    for (0..8) |_| {
        _ = try client.poll(.none, &collector, ClientCollector.collect);
        if (collector.len != 0) break;
    }
    try std.testing.expectEqualStrings("\xfeworld", collector.data[0..collector.len]);

    var client_address = client.socket.value.address;
    client_address.ip4.bytes = .{ 127, 0, 0, 1 };
    try listener.socket.send(client_address, &.{ 0x84, 0 });
    _ = try client.poll(.none, &collector, ClientCollector.collect);
    try std.testing.expectEqual(@as(u64, 1), client.rejected_datagrams);
    try std.testing.expect(!client.isClosed());

    harness.fail_messages = true;
    try client.send("\xfefail", .reliable_ordered, 0);
    var failed_sessions: usize = 0;
    var malformed: usize = 0;
    var application_failures: usize = 0;
    for (0..4) |_| {
        const stats = try listener.poll(.none, .{
            .context = &harness,
            .connected = Harness.onConnect,
            .message = Harness.onMessage,
            .disconnected = Harness.onDisconnect,
        });
        failed_sessions += stats.sessions_failed;
        malformed += stats.malformed;
        application_failures += stats.application_failures;
        if (listener.sessions.count() == 0) break;
    }
    try std.testing.expectEqual(@as(usize, 1), failed_sessions);
    try std.testing.expectEqual(@as(usize, 0), malformed);
    try std.testing.expectEqual(@as(usize, 1), application_failures);
    try std.testing.expectEqual(@as(u32, 0), listener.sessions.count());
    try std.testing.expectEqual(@as(usize, 0), listener.deadlines.count());
    try std.testing.expectEqual(@as(usize, 1), harness.disconnected);

    const retransmission_deadline = client.core.nextRetransmissionDeadline().?;
    try std.testing.expectEqual(retransmission_deadline, client.nextDeadline().?);
    try client.processTimers(retransmission_deadline);
    try std.testing.expect(client.core.nextRetransmissionDeadline().? > retransmission_deadline);
    try std.testing.expectError(error.Timeout, client.poll(.none, &collector, ClientCollector.collect));
    try std.testing.expectError(error.ConnectionTimedOut, client.processTimers(std.math.maxInt(u64)));
    try std.testing.expect(client.nextDeadline() == null);
}

test "repeated reconnects survive shutdown with queued traffic" {
    var io_instance: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer io_instance.deinit();
    const io = io_instance.io();
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var config: Config = .{};
    config.listener.maximum_connections = 1;
    var listener = try raknet.Server.listen(std.testing.allocator, io, address, .{ .advertisement = "MCPE;reconnect", .config = config });
    defer listener.destroy();

    const Harness = struct {
        listener: *raknet.Server,
        connected: std.atomic.Value(usize) = .init(0),
        target: usize = 0,

        fn onConnect(raw: *anyopaque, _: *raknet.Session) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.connected.fetchAdd(1, .release);
        }
        fn onMessage(_: *anyopaque, _: *raknet.Session, _: raknet.BorrowedPayload) !void {}
        fn run(self: *@This()) !void {
            while (self.connected.load(.acquire) < self.target) {
                _ = try self.listener.poll(.none, .{ .context = self, .connected = onConnect, .message = onMessage });
            }
        }
    };
    var harness: Harness = .{ .listener = listener };
    for (0..3) |iteration| {
        harness.target = iteration + 1;
        var task = try io.concurrent(Harness.run, .{&harness});
        defer task.cancel(io) catch {};
        const client = try Client.connect(std.testing.allocator, io, listener.socket.value.address, .{ .handshake_retry_ms = 10 });
        try task.await(io);
        try std.testing.expectEqual(iteration + 1, harness.connected.load(.acquire));
        try std.testing.expectEqual(@as(u32, 1), listener.sessions.count());
        try client.send("\xfetraffic", .reliable_ordered, 0);
        client.destroy();
        _ = try listener.processTimers(std.math.maxInt(u64), .{ .context = &harness, .connected = Harness.onConnect, .message = Harness.onMessage });
        try std.testing.expectEqual(@as(u32, 0), listener.sessions.count());
    }
}

test "handshake deadline and cancellation release every resource" {
    var io_instance: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer io_instance.deinit();
    const io = io_instance.io();
    var silent = try raknet.advanced.net.Socket.bind(io, try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0"), 2048);
    defer silent.close();
    const options: Options = .{ .handshake_timeout_ms = 60, .handshake_retry_ms = 10 };
    try std.testing.expectError(error.Timeout, Client.connect(std.testing.allocator, io, silent.value.address, options));

    const Connect = struct {
        fn run(io_value: std.Io, address: std.Io.net.IpAddress) !void {
            const client = try Client.connect(std.testing.allocator, io_value, address, .{ .handshake_timeout_ms = 60_000, .handshake_retry_ms = 10 });
            client.destroy();
        }
    };
    var task = try io.concurrent(Connect.run, .{ io, silent.value.address });
    var probe: [2048]u8 = undefined;
    _ = try silent.receive(&probe, .{ .duration = .{ .raw = .fromMilliseconds(1_000), .clock = .awake } });
    try std.testing.expectError(error.Canceled, task.cancel(io));
}

test "graceful client close delivers queued data before the disconnect" {
    var io_instance: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer io_instance.deinit();
    const io = io_instance.io();
    var listener = try raknet.Server.listen(std.testing.allocator, io, try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0"), .{ .advertisement = "MCPE;close" });
    defer listener.destroy();

    const Harness = struct {
        listener: *raknet.Server,
        received: std.atomic.Value(usize) = .init(0),
        received_before_disconnect: usize = 0,
        disconnected: std.atomic.Value(bool) = .init(false),

        fn onConnect(_: *anyopaque, _: *raknet.Session) !void {}
        fn onMessage(raw: *anyopaque, _: *raknet.Session, _: raknet.BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.received.fetchAdd(1, .release);
        }
        fn onDisconnect(raw: *anyopaque, _: *raknet.Session) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.received_before_disconnect = self.received.load(.acquire);
            self.disconnected.store(true, .release);
        }
        fn run(self: *@This()) !void {
            while (!self.disconnected.load(.acquire)) {
                _ = try self.listener.poll(.none, .{ .context = self, .connected = onConnect, .message = onMessage, .disconnected = onDisconnect });
            }
        }
    };
    var harness: Harness = .{ .listener = listener };
    var server_task = try io.concurrent(Harness.run, .{&harness});
    defer server_task.cancel(io) catch {};

    const client = try Client.connect(std.testing.allocator, io, listener.socket.value.address, .{ .handshake_retry_ms = 10 });
    defer client.destroy();
    for (0..3) |_| _ = try client.queueSend("\xfefinal", .reliable_ordered, 0);
    const started = std.Io.Clock.awake.now(io);
    client.close();
    client.close();
    try std.testing.expectError(error.ConnectionClosed, client.queueSend("\xfelate", .reliable_ordered, 0));
    const Ignore = struct {
        fn message(_: *anyopaque, _: raknet.BorrowedPayload) !void {}
    };
    var unused: u8 = 0;
    for (0..1_000) |_| {
        if (client.isClosed()) break;
        _ = client.poll(.{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } }, &unused, Ignore.message) catch |err| switch (err) {
            error.Timeout => continue,
            else => return err,
        };
    }
    try std.testing.expect(client.isClosed());
    const elapsed = started.durationTo(std.Io.Clock.awake.now(io));
    try std.testing.expect(elapsed.nanoseconds < @as(i96, client.core.config.timing.shutdown_timeout_ms) * std.time.ns_per_ms);
    try std.testing.expectEqual(@as(usize, 0), client.core.recovery_state.count());
    try std.testing.expectEqual(@as(usize, 0), client.core.outbound_state.countAll());
    try std.testing.expect(client.nextDeadline() == null);
    try server_task.await(io);
    try std.testing.expectEqual(@as(usize, 3), harness.received_before_disconnect);
}
