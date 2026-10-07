const std = @import("std");
const raknet = @import("raknet");
const protocol = raknet.advanced.protocol;

fn now(io: std.Io) u64 {
    return @intCast(@divFloor(std.Io.Clock.awake.now(io).nanoseconds, std.time.ns_per_ms));
}

const Harness = struct {
    listener: *raknet.Server,
    session: ?*raknet.Session = null,
    stopped: std.atomic.Value(bool) = .init(false),
    messages: usize = 0,

    fn connected(raw: *anyopaque, session: *raknet.Session) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.session = session;
    }
    fn message(_: *anyopaque, _: *raknet.Session, _: raknet.BorrowedPayload) !void {
        return error.ApplicationFailure;
    }
    fn received(raw: *anyopaque, payload: raknet.BorrowedPayload) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (!std.mem.eql(u8, "\xfeapplication", payload.bytes)) return error.ApplicationFailure;
        self.messages += 1;
    }
    fn accept(self: *@This()) !void {
        while (self.session == null) _ = try self.listener.poll(.none, .{ .context = self, .connected = connected, .message = message });
    }
    fn run(self: *@This()) !void {
        while (!self.stopped.load(.acquire)) {
            _ = self.listener.poll(.{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }, .{ .context = self, .connected = connected, .message = message }) catch |err| switch (err) {
                error.Timeout => continue,
                else => return err,
            };
        }
    }
    fn drain(self: *@This(), client: *raknet.Client) !void {
        for (0..16) |_| {
            _ = client.poll(.{ .duration = .{ .raw = .zero, .clock = .awake } }, self, received) catch |err| switch (err) {
                error.Timeout => return,
                else => return err,
            };
        }
    }
    fn connect(self: *@This(), allocator: std.mem.Allocator, io: std.Io, options: raknet.ClientOptions) !*raknet.Client {
        var task = try io.concurrent(accept, .{self});
        defer task.cancel(io) catch {};
        const client = try raknet.Client.connect(allocator, io, self.listener.localAddress(), options);
        errdefer client.destroy();
        try task.await(io);
        try self.drain(client);
        return client;
    }
};

test "many idle clients stay connected without per-client timer tasks or ping allocations" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer threaded.deinit();
    const io = threaded.io();
    var config: raknet.Config = .{};
    config.timing.idle_timeout_ms = 5_000;
    const listener = try raknet.Server.listen(std.testing.allocator, io, try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0"), .{ .advertisement = "test", .config = config, .offline_rate_per_second = 20_000, .offline_burst = 40_000 });
    defer listener.destroy();
    var harness: Harness = .{ .listener = listener };
    var task = try io.concurrent(Harness.run, .{&harness});
    defer task.cancel(io) catch {};
    var quota = raknet.advanced.QuotaAllocator.init(std.testing.allocator, 64 * 1024 * 1024);
    var clients: [128]*raknet.Client = undefined;
    var opened: usize = 0;
    defer for (clients[0..opened]) |client| client.destroy();
    for (&clients) |*client| {
        client.* = try raknet.Client.connect(quota.allocator(), io, listener.localAddress(), .{ .config = config });
        opened += 1;
        try harness.drain(client.*);
    }
    const allocations = quota.allocations;
    const started = now(io);
    while (now(io) - started < 5 * config.timing.idle_timeout_ms) {
        for (clients) |client| try harness.drain(client);
        try io.sleep(.fromMilliseconds(5), .awake);
    }
    harness.stopped.store(true, .release);
    try task.await(io);
    try std.testing.expectEqual(clients.len, listener.sessions.count());
    try std.testing.expectEqual(@as(usize, 0), harness.messages);
    try std.testing.expectEqual(allocations, quota.allocations);
    for (clients) |client| {
        try std.testing.expect(!client.isClosed());
        try std.testing.expect(client.last_keepalive_ms >= started);
        try std.testing.expect(client.last_seen_ms > started);
        try std.testing.expectEqual(@as(usize, 0), client.core.recovery_state.count());
    }
}

test "keepalive shares a one-packet timer budget and skips congestion without allocating" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer threaded.deinit();
    const io = threaded.io();
    const listener = try raknet.Server.listen(std.testing.allocator, io, try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0"), .{ .advertisement = "test" });
    defer listener.destroy();
    var harness: Harness = .{ .listener = listener };
    var quota = raknet.advanced.QuotaAllocator.init(std.testing.allocator, 1024 * 1024);
    const client = try harness.connect(quota.allocator(), io, .{});
    defer client.destroy();
    client.core.config.batching.maximum_packets_per_iteration = 1;
    const due = now(io);
    client.last_seen_ms = due -| client.keepalive_interval_ms;
    var wire: [128]u8 = undefined;
    const data = try protocol.datagram.encodeData(client.core.receiver_state.datagrams.expected, &.{.{ .reliability = .unreliable, .payload = "\xfeapplication" }}, &wire);
    const receipt = (try client.core.processIncoming(data, due, &harness, Harness.received)).data;
    try client.receipts.append(receipt);
    client.ack_deadline_ms = due;
    _ = try client.core.enqueueOutbound(.application, "\xfeapplication", .reliable_ordered, 0);
    client.outbound_deadline_ms = due;
    const sent = client.traffic().datagrams_sent;
    for (0..6) |_| try client.processTimers(due);
    try std.testing.expect(client.ack_deadline_ms == null);
    try std.testing.expectEqual(@as(usize, 0), client.core.outboundCount(.application));
    try std.testing.expectEqual(due, client.last_keepalive_ms);
    try std.testing.expectEqual(sent + 3, client.traffic().datagrams_sent);
    var ack_storage: [32]u8 = undefined;
    const sequence = client.core.newest_sent;
    const ack = try protocol.datagram.encodeControl(.ack, &.{.{ .first = sequence, .last = sequence }}, &ack_storage);
    _ = try client.core.processIncoming(ack, due, &harness, Harness.received);
    client.core.congestion_state.in_flight = std.math.maxInt(u64);
    defer client.core.congestion_state.in_flight = 0;
    const retry = due + client.keepalive_interval_ms;
    client.core.config.batching.maximum_packets_per_iteration = 256;
    const congested_allocations = quota.allocations;
    const before_skip = client.traffic().datagrams_sent;
    try client.processTimers(retry);
    try std.testing.expectEqual(retry, client.last_keepalive_ms);
    try std.testing.expectEqual(before_skip, client.traffic().datagrams_sent);
    try std.testing.expectEqual(@as(usize, 0), client.core.outboundCount(.control));
    try std.testing.expectEqual(congested_allocations, quota.allocations);
}

test "idle timers send connected pings and consume connected pongs internally" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer threaded.deinit();
    const io = threaded.io();
    var config: raknet.Config = .{};
    config.timing.idle_timeout_ms = 1_000;
    const listener = try raknet.Server.listen(std.testing.allocator, io, try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0"), .{ .advertisement = "test", .config = config });
    defer listener.destroy();
    var harness: Harness = .{ .listener = listener };
    const client = try harness.connect(std.testing.allocator, io, .{ .config = config });
    defer client.destroy();
    const due = client.last_seen_ms + client.keepalive_interval_ms;
    const before = client.traffic().datagrams_sent;
    try std.testing.expectEqual(due, client.nextDeadline().?);
    try client.processTimers(due - 1);
    try std.testing.expectEqual(before, client.traffic().datagrams_sent);
    while (client.last_keepalive_ms == 0)
        try std.testing.expectError(error.Timeout, client.poll(.none, &harness, Harness.received));
    try std.testing.expect(client.last_keepalive_ms >= due);
    try std.testing.expectEqual(before + 1, client.traffic().datagrams_sent);
    try std.testing.expectEqual(@as(usize, 0), client.core.recovery_state.count());
    var buffer: [2048]u8 = undefined;
    for (0..8) |_| {
        const message = try listener.socket.receive(&buffer, .{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } });
        var decoded = protocol.frame.decodeDatagram(message.data) catch continue;
        const value = try protocol.frame.decodeOne(&decoded.frames, 8192, 8192);
        const ping = try protocol.connected.decode(value.payload);
        try std.testing.expectEqual(client.last_keepalive_ms, ping.connected_ping);
        try std.testing.expectEqual(protocol.frame.Reliability.unreliable, value.reliability);
        var payload: [17]u8 = undefined;
        const pong = try protocol.connected.encodePong(due, due, &payload);
        var wire: [128]u8 = undefined;
        const data = try protocol.datagram.encodeData(client.core.receiver_state.datagrams.expected, &.{.{ .reliability = .unreliable, .payload = pong }}, &wire);
        try listener.socket.send(message.from, data);
        try harness.drain(client);
        try std.testing.expectEqual(@as(usize, 0), harness.messages);
        try std.testing.expect(client.traffic().datagrams_received != 0);
        return;
    }
    return error.PingNotReceived;
}

test "application and ACK traffic suppress keepalive but malformed input does not" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer threaded.deinit();
    const io = threaded.io();
    const listener = try raknet.Server.listen(std.testing.allocator, io, try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0"), .{ .advertisement = "test" });
    defer listener.destroy();
    var harness: Harness = .{ .listener = listener };
    const client = try harness.connect(std.testing.allocator, io, .{});
    defer client.destroy();
    const session = harness.session.?;
    client.last_seen_ms = now(io) -| client.keepalive_interval_ms;
    try session.send("\xfeapplication", .unreliable, 0);
    try harness.drain(client);
    try std.testing.expectEqual(@as(usize, 1), harness.messages);
    try std.testing.expectEqual(@as(u64, 0), client.last_keepalive_ms);
    try std.testing.expect(client.nextDeadline().? > now(io));
    client.last_seen_ms = now(io) -| client.keepalive_interval_ms;
    var storage: [32]u8 = undefined;
    const sequence = client.core.newest_sent;
    const ack = try protocol.datagram.encodeControl(.ack, &.{.{ .first = sequence, .last = sequence }}, &storage);
    try listener.socket.send(session.address, ack);
    try harness.drain(client);
    try std.testing.expectEqual(@as(u64, 0), client.last_keepalive_ms);
    try std.testing.expect(client.nextDeadline().? > now(io));
    client.last_seen_ms = now(io) -| client.keepalive_interval_ms;
    const last_seen = client.last_seen_ms;
    const rejected = client.rejected_datagrams;
    try listener.socket.send(session.address, &.{0x80});
    try harness.drain(client);
    try std.testing.expectEqual(last_seen, client.last_seen_ms);
    try std.testing.expectEqual(rejected + 1, client.rejected_datagrams);
    try std.testing.expect(client.last_keepalive_ms > last_seen);
}

test "unanswered keepalives time out and shutdown removes their deadline" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer threaded.deinit();
    const io = threaded.io();
    var config: raknet.Config = .{};
    config.timing.idle_timeout_ms = 1_000;
    const Mode = enum { timeout, malformed, closing };
    for ([_]Mode{ .timeout, .malformed, .closing }) |mode| {
        const listener = try raknet.Server.listen(std.testing.allocator, io, try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0"), .{ .advertisement = "test", .config = config });
        defer listener.destroy();
        var harness: Harness = .{ .listener = listener };
        const client = try harness.connect(std.testing.allocator, io, .{ .config = config });
        defer client.destroy();
        const seen = client.last_seen_ms;
        if (mode == .closing) {
            client.close();
            try client.processTimers(seen + client.keepalive_interval_ms);
            try std.testing.expectEqual(@as(u64, 0), client.last_keepalive_ms);
            client.closeNow();
        } else {
            for (1..4) |multiple| try client.processTimers(seen + multiple * client.keepalive_interval_ms);
            try std.testing.expectEqual(seen, client.last_seen_ms);
            try std.testing.expectEqual(seen + 3 * client.keepalive_interval_ms, client.last_keepalive_ms);
            if (mode == .malformed) {
                client.last_seen_ms = now(io) -| config.timing.idle_timeout_ms;
                var malformed = [_]u8{0x80};
                client.messages[0] = .{ .from = listener.localAddress(), .data = &malformed, .control = &.{}, .flags = @bitCast(@as(u8, 0)) };
                client.pending_message_count = 1;
                try std.testing.expectError(error.ConnectionTimedOut, client.poll(.none, &harness, Harness.received));
            } else {
                try std.testing.expectError(error.ConnectionTimedOut, client.processTimers(seen + config.timing.idle_timeout_ms));
            }
        }
        try std.testing.expect(client.isClosed());
        try std.testing.expect(client.nextDeadline() == null);
        try std.testing.expectError(error.ConnectionClosed, client.processTimers(seen));
    }
}
