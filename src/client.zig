const std = @import("std");
const Config = @import("config.zig").Config;
const backend = @import("net/backend.zig");
const ack = @import("protocol/ack.zig");
const connected = @import("protocol/connected.zig");
const datagram = @import("protocol/datagram.zig");
const frame = @import("protocol/frame.zig");
const offline = @import("protocol/offline.zig");
const recovery = @import("reliability/recovery.zig");
const core_mod = @import("session/core.zig");
const receiver = @import("session/receiver.zig");
const time = @import("util/time.zig");

pub const Options = struct {
    config: Config = .{},
    protocol_version: u8 = 11,
    mtu: u16 = 1492,
    client_guid: u64 = 0,
    handshake_timeout_ms: u32 = 5_000,
    handshake_retry_ms: u32 = 500,
};
pub const MessageFn = *const fn (context: *anyopaque, payload: receiver.BorrowedPayload) core_mod.ApplicationCallbackError!void;

pub const Client = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    socket: backend.Socket,
    server: std.Io.net.IpAddress,
    core: core_mod.Core,
    scratch: []u8,
    receive_buffer: []u8,
    frame_scratch: []frame.Frame,
    client_guid: u64,
    server_guid: u64,
    mtu: u16,
    last_seen_ms: u64,
    timer_cursor: u8 = 0,
    closed: bool = false,

    pub fn connect(allocator: std.mem.Allocator, io: std.Io, server: std.Io.net.IpAddress, options: Options) !*Client {
        try options.config.validate();
        if (options.mtu < options.config.minimum_mtu or options.mtu > options.config.maximum_mtu or options.handshake_timeout_ms == 0 or options.handshake_retry_ms == 0 or options.handshake_retry_ms > options.handshake_timeout_ms) return error.InvalidConfiguration;
        const self = try allocator.create(Client);
        errdefer allocator.destroy(self);
        const local: std.Io.net.IpAddress = switch (server) {
            .ip4 => .{ .ip4 = .unspecified(0) },
            .ip6 => .{ .ip6 = .unspecified(0) },
        };
        var socket = try backend.Socket.bind(io, local, options.config.maximum_datagram_size);
        errdefer socket.close();
        const scratch = try allocator.alloc(u8, options.config.maximum_datagram_size);
        errdefer allocator.free(scratch);
        const receive_buffer = try allocator.alloc(u8, options.config.maximum_datagram_size);
        errdefer allocator.free(receive_buffer);
        var random: [8]u8 = undefined;
        io.random(&random);
        const guid = if (options.client_guid != 0) options.client_guid else (std.mem.readInt(u64, &random, .little) | 0x8000_0000_0000_0000);
        const deadline = time.after(io, options.handshake_timeout_ms);

        const request1 = try offline.encodeOpenConnectionRequest1(options.protocol_version, options.mtu, scratch);
        const reply1_wire = try exchangeExpected(&socket, server, request1, receive_buffer, offline.Id.open_connection_reply_1, deadline, options.handshake_retry_ms, options.config.maximum_packets_per_iteration);
        const reply1 = try offline.decodeOpenConnectionReply1(reply1_wire, options.config.minimum_mtu, options.config.maximum_mtu);
        const request2 = try offline.encodeOpenConnectionRequest2(toRakAddress(server), reply1.cookie, reply1.mtu, guid, scratch);
        const reply2_wire = try exchangeExpected(&socket, server, request2, receive_buffer, offline.Id.open_connection_reply_2, deadline, options.handshake_retry_ms, options.config.maximum_packets_per_iteration);
        const reply2 = try offline.decodeOpenConnectionReply2(reply2_wire, options.config.minimum_mtu, options.config.maximum_mtu);
        if (reply2.server_guid != reply1.server_guid or reply2.mtu > reply1.mtu) return error.HandshakeMismatch;

        const frame_scratch = try allocator.alloc(frame.Frame, options.config.maximum_packets_per_iteration);
        errdefer allocator.free(frame_scratch);

        var core = try core_mod.Core.init(allocator, reply2.mtu, options.config);
        errdefer core.deinit();
        self.* = .{ .allocator = allocator, .io = io, .socket = socket, .server = server, .core = core, .scratch = scratch, .receive_buffer = receive_buffer, .frame_scratch = frame_scratch, .client_guid = guid, .server_guid = reply2.server_guid, .mtu = reply2.mtu, .last_seen_ms = time.nowMilliseconds(io) };
        try self.finishConnectedHandshake(deadline, options.handshake_retry_ms, options.config.maximum_packets_per_iteration);
        self.last_seen_ms = time.nowMilliseconds(io);
        return self;
    }

    pub fn close(self: *Client) void {
        if (self.closed) return;
        self.closed = true;
        var payload = [_]u8{@intFromEnum(offline.Id.disconnect_notification)};
        _ = self.sendWire(&payload, .reliable_ordered, 0, time.nowMilliseconds(self.io)) catch {};
        self.socket.close();
    }
    fn abort(self: *Client) void {
        if (self.closed) return;
        self.closed = true;
        self.socket.close();
    }
    pub fn destroy(self: *Client) void {
        self.close();
        self.core.deinit();
        self.allocator.free(self.frame_scratch);
        self.allocator.free(self.receive_buffer);
        self.allocator.free(self.scratch);
        self.allocator.destroy(self);
    }
    pub fn send(self: *Client, payload: []const u8, reliability: frame.Reliability, channel: u8) !void {
        if (self.closed) return error.ConnectionClosed;
        _ = self.sendWire(payload, reliability, channel, time.nowMilliseconds(self.io)) catch |err| {
            if (core_mod.classifyTransitionError(.application_send, err).disposition == .close_session) {
                self.abort();
            }
            return err;
        };
    }

    /// Returns the next absolute deadline in monotonic milliseconds.
    pub fn nextDeadline(self: *const Client) ?u64 {
        if (self.closed) return null;
        var deadline = time.deadline(self.last_seen_ms, self.core.config.idle_timeout_ms);
        if (self.core.nextRetransmissionDeadline()) |retransmission| deadline = @min(deadline, retransmission);
        if (self.core.nextSplitDeadline()) |split| deadline = @min(deadline, split);
        return deadline;
    }

    /// Processes due timers without waiting for socket traffic.
    pub fn processTimers(self: *Client, now_ms: u64) !void {
        if (self.closed) return error.ConnectionClosed;
        if (time.reached(now_ms, time.deadline(self.last_seen_ms, self.core.config.idle_timeout_ms))) {
            self.abort();
            return error.ConnectionTimedOut;
        }
        self.processDueTimers(now_ms) catch |err| {
            self.abort();
            return err;
        };
    }

    /// The callback payload expires when the callback returns.
    pub fn poll(self: *Client, timeout: std.Io.Timeout, context: *anyopaque, on_message: MessageFn) !usize {
        if (self.closed) return error.ConnectionClosed;
        const wait = if (self.nextDeadline()) |deadline| time.earliest(self.io, timeout, time.atMilliseconds(deadline)) else timeout;
        const message = receiveTimed(&self.socket.value, self.io, self.receive_buffer, wait) catch |err| switch (err) {
            error.Timeout => {
                try self.processTimers(time.nowMilliseconds(self.io));
                return error.Timeout;
            },
            else => return err,
        };
        if (!std.meta.eql(message.from, self.server) or message.flags.trunc) return 0;
        const now_ms = time.nowMilliseconds(self.io);
        self.last_seen_ms = now_ms;
        const Bridge = struct {
            client: *Client,
            context: *anyopaque,
            callback: MessageFn,
            now_ms: u64,
            remote_disconnect: bool = false,
            fn deliver(raw: *anyopaque, payload: receiver.BorrowedPayload) receiver.DeliveryError!void {
                const bridge: *@This() = @ptrCast(@alignCast(raw));
                const packet = connected.decode(payload.bytes) catch return error.PeerProtocolFailure;
                switch (packet) {
                    .connected_ping => |sent| {
                        var wire: [17]u8 = undefined;
                        const pong = connected.encodePong(sent, bridge.now_ms, &wire) catch return error.InternalFailure;
                        _ = bridge.client.sendWire(pong, .unreliable, 0, bridge.now_ms) catch |err| return core_mod.deliverySendFailure(err);
                    },
                    .disconnect => bridge.remote_disconnect = true,
                    .detect_lost_connections => {
                        var wire: [9]u8 = undefined;
                        const ping = connected.encodePing(bridge.now_ms, &wire) catch return error.InternalFailure;
                        _ = bridge.client.sendWire(ping, .reliable, 0, bridge.now_ms) catch |err| return core_mod.deliverySendFailure(err);
                    },
                    .user => |data| bridge.callback(bridge.context, .init(data)) catch return error.ApplicationFailure,
                    else => {},
                }
            }
        };
        var bridge: Bridge = .{ .client = self, .context = context, .callback = on_message, .now_ms = now_ms };
        const incoming = self.core.processIncomingWithScratch(message.data, now_ms, self.frame_scratch, &bridge, Bridge.deliver) catch |err| {
            const failure = core_mod.classifyIncomingError(err);
            if (failure.disposition == .close_session) self.abort();
            return err;
        };
        if (incoming == .data) {
            self.flushReceipt(incoming.data) catch |err| {
                if (core_mod.classifyTransitionError(.receipt, err).disposition == .close_session) self.abort();
                return err;
            };
        }
        if (bridge.remote_disconnect) {
            self.abort();
            return if (incoming == .data) incoming.data.delivered else 0;
        }
        self.processDueTimers(now_ms) catch |err| {
            if (core_mod.classifyTransitionError(.retransmission, err).disposition == .close_session) self.abort();
            return err;
        };
        return if (incoming == .data) incoming.data.delivered else 0;
    }

    fn finishConnectedHandshake(self: *Client, deadline: std.Io.Timeout, retry_ms: u32, maximum_work: usize) !void {
        var control: [18]u8 = undefined;
        _ = try self.sendWire(try connected.encodeConnectionRequest(self.client_guid, time.nowMilliseconds(self.io), &control), .reliable_ordered, 0, time.nowMilliseconds(self.io));
        var work: usize = 0;
        while (work < maximum_work) : (work += 1) {
            const attempt = time.earliest(self.io, deadline, time.after(self.io, retry_ms));
            const message = receiveTimed(&self.socket.value, self.io, self.receive_buffer, attempt) catch |err| switch (err) {
                error.Timeout => {
                    _ = try self.flushRetransmissions(time.nowMilliseconds(self.io), self.core.config.maximum_packets_per_iteration);
                    continue;
                },
                else => return err,
            };
            if (!std.meta.eql(message.from, self.server) or message.flags.trunc) continue;
            const Handshake = struct {
                client: *Client,
                accepted: bool = false,
                now_ms: u64,
                fn deliver(raw: *anyopaque, payload: receiver.BorrowedPayload) receiver.DeliveryError!void {
                    const value: *@This() = @ptrCast(@alignCast(raw));
                    const packet = connected.decode(payload.bytes) catch return error.PeerProtocolFailure;
                    if (packet != .connection_request_accepted) return;
                    var wire: [512]u8 = undefined;
                    const incoming = connected.encodeAddressList(.incoming, toRakAddress(value.client.server), 0, &.{}, value.now_ms, value.now_ms, &wire) catch return error.InternalFailure;
                    _ = value.client.sendWire(incoming, .reliable_ordered, 0, value.now_ms) catch |err| return core_mod.deliverySendFailure(err);
                    value.accepted = true;
                }
            };
            const now_ms = time.nowMilliseconds(self.io);
            var state: Handshake = .{ .client = self, .now_ms = now_ms };
            const incoming = try self.core.processIncomingWithScratch(message.data, now_ms, self.frame_scratch, &state, Handshake.deliver);
            if (incoming == .data) try self.flushReceipt(incoming.data);
            if (state.accepted) return;
        }
        return error.HandshakeWorkLimitExceeded;
    }

    fn sendWire(self: *Client, payload: []const u8, reliability: frame.Reliability, channel: u8, now_ms: u64) !usize {
        const Emitter = struct {
            client: *Client,
            count: usize = 0,
            fn emit(raw: *anyopaque, wire: []const u8) core_mod.SendError!void {
                const value: *@This() = @ptrCast(@alignCast(raw));
                value.client.socket.send(value.client.server, wire) catch return error.TransportFailure;
                value.count += 1;
            }
        };
        var emitter: Emitter = .{ .client = self };
        _ = try self.core.send(payload, reliability, channel, self.scratch, now_ms, &emitter, Emitter.emit);
        return emitter.count;
    }
    fn flushReceipt(self: *Client, receipt: @import("session/receiver.zig").Receipt) !void {
        if (receipt.acknowledge) |sequence| {
            const wire = datagram.encodeControl(.ack, &.{.{ .first = sequence, .last = sequence }}, self.scratch) catch return error.InternalFailure;
            self.socket.send(self.server, wire) catch return error.TransportFailure;
        }
        if (receipt.missing) |gap| {
            var ranges: [2]ack.Record = undefined;
            const count: usize = if (gap.first <= gap.last) blk: {
                ranges[0] = .{ .first = gap.first, .last = gap.last };
                break :blk 1;
            } else blk: {
                ranges[0] = .{ .first = 0, .last = gap.last };
                ranges[1] = .{ .first = gap.first, .last = 0xffffff };
                break :blk 2;
            };
            const wire = datagram.encodeControl(.nack, ranges[0..count], self.scratch) catch return error.InternalFailure;
            self.socket.send(self.server, wire) catch return error.TransportFailure;
        }
    }
    fn processDueTimers(self: *Client, now_ms: u64) !void {
        var remaining = self.core.config.maximum_packets_per_iteration;
        var active: usize = @intFromBool(if (self.core.nextSplitDeadline()) |deadline| deadline <= now_ms else false) +
            @intFromBool(if (self.core.nextRetransmissionDeadline()) |deadline| deadline <= now_ms else false);
        const start = self.timer_cursor;
        var visited: usize = 0;
        while (visited < 2 and remaining != 0 and active != 0) : (visited += 1) {
            const timer = (start + visited) % 2;
            const due = switch (timer) {
                0 => if (self.core.nextSplitDeadline()) |deadline| deadline <= now_ms else false,
                else => if (self.core.nextRetransmissionDeadline()) |deadline| deadline <= now_ms else false,
            };
            if (!due) continue;
            const quota = @max(@as(usize, 1), remaining / active);
            const used = switch (timer) {
                0 => self.core.expireSplits(now_ms, quota).inspected,
                else => try self.flushRetransmissions(now_ms, quota),
            };
            remaining -= @min(remaining, used);
            active -= 1;
            self.timer_cursor = @intCast((timer + 1) % 2);
        }
    }

    fn flushRetransmissions(self: *Client, now_ms: u64, maximum_work: usize) !usize {
        var due: [256]recovery.Due = undefined;
        const batch = self.core.collectRetransmissions(now_ms, due[0..@min(due.len, maximum_work)], maximum_work);
        if (batch.exhausted != 0) return error.RetransmissionLimitExceeded;
        for (batch.items) |item| self.socket.send(self.server, item.data) catch return error.TransportFailure;
        return batch.inspected;
    }
};

fn exchangeExpected(socket: *backend.Socket, server: std.Io.net.IpAddress, request: []const u8, buffer: []u8, id: offline.Id, overall: std.Io.Timeout, retry_ms: u32, maximum_work: usize) ![]const u8 {
    while (std.Io.Clock.awake.now(socket.io).nanoseconds < overall.deadline.raw.nanoseconds) {
        try socket.send(server, request);
        const attempt = time.earliest(socket.io, overall, time.after(socket.io, retry_ms));
        return receiveExpected(socket, server, buffer, id, attempt, maximum_work) catch |err| switch (err) {
            error.Timeout, error.HandshakeWorkLimitExceeded => continue,
            else => return err,
        };
    }
    return error.Timeout;
}

fn receiveTimed(socket: *const std.Io.net.Socket, io: std.Io, buffer: []u8, timeout: std.Io.Timeout) !std.Io.net.IncomingMessage {
    return socket.receiveTimeout(io, buffer, timeout) catch |err| switch (err) {
        error.ConcurrencyUnavailable => if (timeout == .none) try socket.receive(io, buffer) else return err,
        else => return err,
    };
}
fn receiveExpected(socket: *backend.Socket, server: std.Io.net.IpAddress, buffer: []u8, id: offline.Id, deadline: std.Io.Timeout, maximum_work: usize) ![]const u8 {
    for (0..maximum_work) |_| {
        const message = try receiveTimed(&socket.value, socket.io, buffer, deadline);
        if (!std.meta.eql(message.from, server) or message.flags.trunc or message.data.len == 0) continue;
        if (message.data[0] == @intFromEnum(offline.Id.incompatible_protocol_version)) return error.IncompatibleProtocol;
        if (message.data[0] == @intFromEnum(id)) return message.data;
    }
    return error.HandshakeWorkLimitExceeded;
}
fn toRakAddress(address: std.Io.net.IpAddress) offline.Address {
    return switch (address) {
        .ip4 => |v| .{ .ipv4 = .{ .octets = v.bytes, .port = v.port } },
        .ip6 => |v| .{ .ipv6 = .{ .octets = v.bytes, .port = v.port, .flow = v.flow, .scope = v.interface.index } },
    };
}
test "client and server complete a real loopback handshake" {
    const server_mod = @import("server.zig");
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var listener = try server_mod.Listener.listen(std.testing.allocator, io, address, .{ .advertisement = "MCPE;interop" });
    defer listener.destroy();
    const Harness = struct {
        listener: *server_mod.Listener,
        connected: std.atomic.Value(bool) = .init(false),
        message_data: [32]u8 = undefined,
        message_len: usize = 0,
        fail_messages: bool = false,
        disconnected: usize = 0,
        fn onConnect(raw: *anyopaque, _: *server_mod.Session) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.connected.store(true, .release);
        }
        fn onMessage(raw: *anyopaque, _: *server_mod.Session, payload: receiver.BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail_messages or payload.bytes.len > self.message_data.len) return error.ApplicationFailure;
            @memcpy(self.message_data[0..payload.bytes.len], payload.bytes);
            self.message_len = payload.bytes.len;
        }
        fn onDisconnect(raw: *anyopaque, _: *server_mod.Session) void {
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
    const client = Client.connect(std.testing.allocator, io, listener.socket.value.address, .{}) catch |err| {
        std.debug.print("client connect failed: {any}\n", .{err});
        return err;
    };
    defer client.destroy();
    try server_task.await(io);
    try std.testing.expect(harness.connected.load(.acquire));
    try std.testing.expectEqual(listener.handshake_handler.server_guid, client.server_guid);

    try client.send("\xfehello", .reliable_ordered, 0);
    for (0..4) |_| {
        _ = try listener.poll(.none, .{ .context = &harness, .connected = Harness.onConnect, .message = Harness.onMessage });
        if (harness.message_len != 0) break;
    }
    try std.testing.expectEqualStrings("\xfehello", harness.message_data[0..harness.message_len]);

    var session_iterator = listener.sessions.valueIterator();
    const session = session_iterator.next().?.*;
    try session.send("\xfeworld", .reliable_ordered, 0);
    const ClientCollector = struct {
        data: [32]u8 = undefined,
        len: usize = 0,
        fn collect(raw: *anyopaque, payload: receiver.BorrowedPayload) !void {
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
    try std.testing.expectError(error.Timeout, client.poll(.none, &collector, ClientCollector.collect));
    try std.testing.expect(client.core.nextRetransmissionDeadline().? > retransmission_deadline);
    try std.testing.expectError(error.ConnectionTimedOut, client.processTimers(std.math.maxInt(u64)));
    try std.testing.expect(client.nextDeadline() == null);
}
