const std = @import("std");
const builtin = @import("builtin");
const raknet = @import("raknet");
const Client = raknet.Client;
const Config = raknet.Config;
const Options = raknet.ClientOptions;

test "public ping receives an opaque advertisement over loopback" {
    var io_instance: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer io_instance.deinit();
    const io = io_instance.io();
    var listener = try raknet.Server.listen(std.testing.allocator, io, try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0"), .{ .advertisement = "MCPE;loopback" });
    defer listener.destroy();
    const Harness = struct {
        listener: *raknet.Server,
        fn connected(_: *anyopaque, _: *raknet.Session) !void {}
        fn message(_: *anyopaque, _: *raknet.Session, _: raknet.BorrowedPayload) !void {}
        fn run(self: *@This()) !void {
            const stats = try self.listener.poll(.none, .{ .context = self, .connected = connected, .message = message });
            try std.testing.expectEqual(@as(usize, 1), stats.datagrams);
        }
    };
    var harness: Harness = .{ .listener = listener };
    var task = try io.concurrent(Harness.run, .{&harness});
    defer task.cancel(io) catch {};
    var buffer: [1492]u8 = undefined;
    const pong = try raknet.ping(io, listener.localAddress(), &buffer, 1_000);
    try task.await(io);
    try std.testing.expectEqual(listener.handshake_handler.server_guid, pong.server_guid);
    try std.testing.expectEqualStrings("MCPE;loopback", pong.advertisement);
    try std.testing.expectError(error.InvalidConfiguration, raknet.ping(io, listener.localAddress(), buffer[0..34], 1_000));
}

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
    try std.testing.expect(listener.localAddress().ip4.port != 0);
    const Harness = struct {
        listener: *raknet.Server,
        connected: std.atomic.Value(bool) = .init(false),
        message_data: [32]u8 = undefined,
        message_len: usize = 0,
        fail_messages: bool = false,
        disconnected: usize = 0,
        user_data_on_disconnect: bool = false,
        fn onConnect(raw: *anyopaque, session: *raknet.Session) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (session.userData() != null) return error.ApplicationFailure;
            session.setUserData(self);
            if (session.userData() != raw) return error.ApplicationFailure;
            session.setUserData(self.listener);
            if (session.userData() != @as(*anyopaque, @ptrCast(self.listener))) return error.ApplicationFailure;
            session.setUserData(null);
            if (session.userData() != null) return error.ApplicationFailure;
            session.setUserData(self);
            self.connected.store(true, .release);
        }
        fn onMessage(raw: *anyopaque, session: *raknet.Session, payload: raknet.BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (session.userData() != raw) return error.ApplicationFailure;
            if (self.fail_messages or payload.bytes.len > self.message_data.len) return error.ApplicationFailure;
            @memcpy(self.message_data[0..payload.bytes.len], payload.bytes);
            self.message_len = payload.bytes.len;
        }
        fn onDisconnect(raw: *anyopaque, session: *raknet.Session) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.user_data_on_disconnect = session.userData() == raw;
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
    const client = Client.connect(std.testing.allocator, io, listener.localAddress(), .{ .handshake_retry_ms = 10 }) catch |err| {
        std.debug.print("client connect failed: {any}\n", .{err});
        return err;
    };
    defer client.destroy();
    try std.testing.expect(client.localAddress().ip4.port != 0);
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

    var client_address = client.localAddress();
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
    try std.testing.expect(harness.user_data_on_disconnect);

    const retransmission_deadline = client.core.nextRetransmissionDeadline().?;
    try std.testing.expectEqual(retransmission_deadline, client.nextDeadline().?);
    try client.processTimers(retransmission_deadline);
    try std.testing.expect(client.core.nextRetransmissionDeadline().? > retransmission_deadline);

    client.core.config.batching.maximum_packets_per_iteration = 1;
    try client.receipts.append(.{ .acknowledge = 0 });
    client.ack_deadline_ms = 0;
    var invalid = [_]u8{0};
    for (client.messages[0..3]) |*message| message.* = .{ .from = client.server, .data = &invalid, .control = &.{}, .flags = @bitCast(@as(u8, 0)) };
    client.pending_message_index = 0;
    client.pending_message_count = 3;
    _ = try client.poll(.none, &collector, ClientCollector.collect);
    try std.testing.expectEqual(@as(usize, 1), client.pending_message_index);
    _ = try client.poll(.none, &collector, ClientCollector.collect);
    try std.testing.expectEqual(@as(usize, 1), client.pending_message_index);
    try std.testing.expect(client.receipts.isEmpty());
    while (client.pending_message_count != 0) _ = try client.poll(.none, &collector, ClientCollector.collect);
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

test "listener local address supports port zero and reuse port" {
    const io = std.testing.io;
    const ipv4 = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    if (builtin.os.tag != .linux) {
        try std.testing.expectError(error.ReusePortUnsupported, raknet.Server.listen(std.testing.allocator, io, ipv4, .{ .advertisement = "MCPE;reuse", .reuse_port = true }));
        return;
    }
    for ([_][]const u8{ "127.0.0.1:0", "[::1]:0" }) |literal| {
        const address = try std.Io.net.IpAddress.parseLiteral(literal);
        const first = try raknet.Server.listen(std.testing.allocator, io, address, .{ .advertisement = "MCPE;reuse", .reuse_port = true });
        defer first.destroy();
        const bound = first.localAddress();
        switch (bound) {
            .ip4 => |value| try std.testing.expect(value.port != 0),
            .ip6 => |value| try std.testing.expect(value.port != 0),
        }
        const second = try raknet.Server.listen(std.testing.allocator, io, bound, .{ .advertisement = "MCPE;reuse", .reuse_port = true });
        defer second.destroy();
        const third = try raknet.Server.listen(std.testing.allocator, io, bound, .{ .advertisement = "MCPE;reuse", .reuse_port = true });
        defer third.destroy();
        try std.testing.expectEqual(bound, second.localAddress());
        try std.testing.expectEqual(bound, third.localAddress());
    }
}

test "IPv6 client local address uses the bound port" {
    if (builtin.os.tag != .linux) return;
    var io_instance: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer io_instance.deinit();
    const io = io_instance.io();
    const address = try std.Io.net.IpAddress.parseLiteral("[::1]:0");
    const listener = try raknet.Server.listen(std.testing.allocator, io, address, .{ .advertisement = "MCPE;IPv6" });
    defer listener.destroy();
    const Harness = struct {
        listener: *raknet.Server,
        connected: bool = false,
        fn onConnect(raw: *anyopaque, _: *raknet.Session) error{ApplicationFailure}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.connected = true;
        }
        fn onMessage(_: *anyopaque, _: *raknet.Session, _: raknet.BorrowedPayload) error{ApplicationFailure}!void {}
        fn run(self: *@This()) !void {
            while (!self.connected) _ = try self.listener.poll(.none, .{ .context = self, .connected = onConnect, .message = onMessage });
        }
    };
    var harness: Harness = .{ .listener = listener };
    var task = try io.concurrent(Harness.run, .{&harness});
    defer task.cancel(io) catch {};
    const client = try Client.connect(std.testing.allocator, io, listener.localAddress(), .{ .handshake_retry_ms = 10 });
    defer client.destroy();
    try task.await(io);
    try std.testing.expect(client.localAddress().ip6.port != 0);
    try std.testing.expect(listener.localAddress().ip6.port != 0);
}

test "client readiness wakes on data and disconnect without polling" {
    var io_instance: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer io_instance.deinit();
    const io = io_instance.io();
    var listener = try raknet.Server.listen(std.testing.allocator, io, try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0"), .{ .advertisement = "MCPE;ready" });
    defer listener.destroy();

    const Harness = struct {
        listener: *raknet.Server,
        sessions: [2]*raknet.Session = undefined,
        connected: std.atomic.Value(usize) = .init(0),
        fn onConnect(raw: *anyopaque, session: *raknet.Session) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.sessions[self.connected.load(.monotonic)] = session;
            _ = self.connected.fetchAdd(1, .release);
        }
        fn onMessage(_: *anyopaque, _: *raknet.Session, _: raknet.BorrowedPayload) !void {}
        fn run(self: *@This(), target: usize) !void {
            while (self.connected.load(.acquire) < target) {
                _ = try self.listener.poll(.none, .{ .context = self, .connected = onConnect, .message = onMessage });
            }
        }
    };
    const Received = struct {
        count: usize = 0,
        fn message(raw: *anyopaque, payload: raknet.BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (std.mem.eql(u8, payload.bytes, "\xfewake")) self.count += 1;
        }
        fn drain(self: *@This(), client: *Client) !void {
            while (!client.isClosed()) _ = client.poll(.{ .duration = .{ .raw = .zero, .clock = .awake } }, self, message) catch |err| switch (err) {
                error.Timeout => return,
                else => return err,
            };
        }
    };
    const Wait = struct {
        fn run(client: *const Client, timeout: std.Io.Timeout) !void {
            return client.waitReadable(timeout);
        }
    };

    var harness: Harness = .{ .listener = listener };
    var clients: [2]*Client = undefined;
    for (&clients, 1..) |*client, target| {
        var task = try io.concurrent(Harness.run, .{ &harness, target });
        defer task.cancel(io) catch {};
        client.* = try Client.connect(std.testing.allocator, io, listener.socket.value.address, .{ .handshake_retry_ms = 10 });
        try task.await(io);
    }
    defer for (clients) |client| client.destroy();
    var received: [2]Received = .{ .{}, .{} };
    for (clients, &received) |client, *state| try state.drain(client);

    const long: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(10_000), .clock = .awake } };
    try std.testing.expect(clients[0].pollTimeout(long).deadline.raw.nanoseconds <= std.Io.Clock.awake.now(io).nanoseconds + 10_000 * std.time.ns_per_ms);
    for (0..20) |round| {
        const target = round % 2;
        var waiters: [2]std.Io.Future(@typeInfo(@TypeOf(Wait.run)).@"fn".return_type.?) = undefined;
        for (clients, &waiters) |client, *waiter| waiter.* = try io.concurrent(Wait.run, .{ client, long });
        try harness.sessions[target].send("\xfewake", .reliable_ordered, 0);
        try waiters[target].await(io);
        try std.testing.expectError(error.Canceled, waiters[1 - target].cancel(io));
        try received[target].drain(clients[target]);
        try std.testing.expectEqual(round / 2 + 1, received[target].count);
    }

    var waiters: [2]std.Io.Future(@typeInfo(@TypeOf(Wait.run)).@"fn".return_type.?) = undefined;
    for (clients, &waiters) |client, *waiter| waiter.* = try io.concurrent(Wait.run, .{ client, .none });
    listener.close();
    for (clients, &waiters, &received) |client, *waiter, *state| {
        try waiter.await(io);
        try state.drain(client);
        try std.testing.expect(client.isClosed());
    }
}
