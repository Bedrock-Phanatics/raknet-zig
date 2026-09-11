const std = @import("std");
const Config = @import("config.zig").Config;
const backend = @import("net/backend.zig");
const ack = @import("protocol/ack.zig");
const connected = @import("protocol/connected.zig");
const datagram = @import("protocol/datagram.zig");
const frame = @import("protocol/frame.zig");
const offline = @import("protocol/offline.zig");
const recovery = @import("reliability/recovery.zig");
const cookie = @import("security/cookie.zig");
const rate = @import("security/rate_limit.zig");
const core_mod = @import("session/core.zig");
const handshake = @import("session/offline_handshake.zig");
const QuotaAllocator = @import("util/quota_allocator.zig").QuotaAllocator;

const EndpointKey = [23]u8;
const State = enum { connecting, connected, closed };

pub const Options = struct {
    config: Config = .{},
    server_guid: u64 = 0,
    protocol_version: u8 = 11,
    advertisement: []const u8,
    receive_batch_size: usize = 32,
    offline_rate_per_second: u32 = 20,
    offline_burst: u32 = 40,
    global_offline_rate_per_second: u32 = 20_000,
    global_offline_burst: u32 = 40_000,
    /// Hard aggregate cap for the session table and all remotely-created
    /// per-session protocol state.
    maximum_session_memory_bytes: usize = 512 * 1024 * 1024,
    maintenance_interval_ms: u32 = 10,
};

pub const Callbacks = struct {
    context: *anyopaque,
    connected: *const fn (context: *anyopaque, session: *Session) anyerror!void,
    message: *const fn (context: *anyopaque, session: *Session, payload: []const u8) anyerror!void,
    disconnected: ?*const fn (context: *anyopaque, session: *Session) void = null,
};

pub const PollStats = struct {
    datagrams: usize = 0,
    malformed: usize = 0,
    rate_limited_or_dropped: usize = 0,
    sessions_expired: usize = 0,
    sessions_failed: usize = 0,
    resource_failures: usize = 0,
    transport_failures: usize = 0,
    application_failures: usize = 0,
    internal_failures: usize = 0,
};

pub const Session = struct {
    allocator: std.mem.Allocator,
    socket: *backend.Socket,
    address: std.Io.net.IpAddress,
    key: EndpointKey,
    core: core_mod.Core,
    scratch: []u8,
    client_guid: u64,
    mtu: u16,
    state: State = .connecting,
    last_seen_ms: u64,

    fn create(allocator: std.mem.Allocator, socket: *backend.Socket, address: std.Io.net.IpAddress, key: EndpointKey, client_guid: u64, mtu: u16, now_ms: u64, config: Config) !*Session {
        const self = try allocator.create(Session);
        errdefer allocator.destroy(self);
        var core = try core_mod.Core.init(allocator, mtu, config);
        errdefer core.deinit();
        const scratch = try allocator.alloc(u8, mtu);
        self.* = .{ .allocator = allocator, .socket = socket, .address = address, .key = key, .core = core, .scratch = scratch, .client_guid = client_guid, .mtu = mtu, .last_seen_ms = now_ms };
        return self;
    }
    fn destroy(self: *Session) void {
        self.core.deinit();
        self.allocator.free(self.scratch);
        self.allocator.destroy(self);
    }
    pub fn isConnected(self: Session) bool {
        return self.state == .connected;
    }
    pub fn rttMs(self: Session) ?u64 {
        return if (self.core.rtt_state.initialized) self.core.rtt_state.smoothed_ms else null;
    }
    pub fn send(self: *Session, payload: []const u8, reliability: frame.Reliability, channel: u8) !void {
        if (self.state != .connected) return error.NotConnected;
        _ = try self.sendAt(payload, reliability, channel, nowMilliseconds(self.socket.io));
    }
    fn sendAt(self: *Session, payload: []const u8, reliability: frame.Reliability, channel: u8, now_ms: u64) !usize {
        const Emitter = struct {
            session: *Session,
            count: usize = 0,
            fn emit(raw: *anyopaque, wire: []const u8) !void {
                const value: *@This() = @ptrCast(@alignCast(raw));
                value.session.socket.send(value.session.address, wire) catch return error.TransportFailure;
                value.count += 1;
            }
        };
        var emitter: Emitter = .{ .session = self };
        _ = try self.core.send(payload, reliability, channel, self.scratch, now_ms, &emitter, Emitter.emit);
        return emitter.count;
    }
    fn sendControl(self: *Session, payload: []const u8, reliability: frame.Reliability, now_ms: u64) !void {
        _ = try self.sendAt(payload, reliability, 0, now_ms);
    }
    fn flushReceipt(self: *Session, receipt: @import("session/receiver.zig").Receipt) !void {
        if (receipt.acknowledge) |sequence| {
            const wire = try datagram.encodeControl(.ack, &.{.{ .first = sequence, .last = sequence }}, self.scratch);
            try self.socket.send(self.address, wire);
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
            const wire = try datagram.encodeControl(.nack, ranges[0..count], self.scratch);
            try self.socket.send(self.address, wire);
        }
    }
    fn flushRetransmissions(self: *Session, now_ms: u64) !void {
        var due: [256]recovery.Due = undefined;
        const batch = self.core.collectRetransmissions(now_ms, due[0..@min(due.len, self.core.config.maximum_packets_per_iteration)]);
        if (batch.exhausted != 0) return error.RetransmissionLimitExceeded;
        for (batch.items) |item| try self.socket.send(self.address, item.data);
    }
};

pub const Listener = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    socket: backend.Socket,
    session_quota: QuotaAllocator,
    sessions: std.AutoHashMapUnmanaged(EndpointKey, *Session) = .empty,
    advertisement: []u8,
    rate_entries: []rate.Entry,
    limiter: rate.Limiter,
    handshake_handler: handshake.Handler,
    source_secret: u64,
    messages: []std.Io.net.IncomingMessage,
    receive_storage: []u8,
    frame_scratch: []frame.Frame,
    handshake_output: []u8,
    closed: bool = false,
    last_sweep_ms: u64 = 0,
    last_maintenance_ms: u64 = 0,
    maintenance_interval_ms: u32,

    pub fn listen(allocator: std.mem.Allocator, io: std.Io, address: std.Io.net.IpAddress, options: Options) !*Listener {
        try options.config.validate();
        if (options.receive_batch_size == 0 or options.receive_batch_size > 256 or options.advertisement.len > options.config.maximum_datagram_size -| 35 or options.maximum_session_memory_bytes == 0 or options.maintenance_interval_ms == 0) return error.InvalidConfiguration;
        const self = try allocator.create(Listener);
        errdefer allocator.destroy(self);
        var socket = try backend.Socket.bind(io, address, options.config.maximum_datagram_size);
        errdefer socket.close();
        const advertisement = try allocator.dupe(u8, options.advertisement);
        errdefer allocator.free(advertisement);
        const limiter_count = @min(options.config.maximum_pending_handshakes, 65_536);
        const rate_entries = try allocator.alloc(rate.Entry, limiter_count);
        errdefer allocator.free(rate_entries);
        const limiter = try rate.Limiter.init(rate_entries, .{ .tokens_per_second = options.offline_rate_per_second, .burst = options.offline_burst, .global_tokens_per_second = options.global_offline_rate_per_second, .global_burst = options.global_offline_burst }, nowMilliseconds(io));
        const messages = try allocator.alloc(std.Io.net.IncomingMessage, options.receive_batch_size);
        errdefer allocator.free(messages);
        const receive_size = try std.math.mul(usize, options.receive_batch_size, options.config.maximum_datagram_size);
        const receive_storage = try allocator.alloc(u8, receive_size);
        errdefer allocator.free(receive_storage);
        const handshake_output = try allocator.alloc(u8, options.config.maximum_datagram_size);
        errdefer allocator.free(handshake_output);
        const frame_scratch = try allocator.alloc(frame.Frame, options.config.maximum_packets_per_iteration);
        errdefer allocator.free(frame_scratch);
        var random: [80]u8 = undefined;
        io.random(&random);
        const guid = if (options.server_guid != 0) options.server_guid else std.mem.readInt(u64, random[0..8], .little);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .config = options.config,
            .socket = socket,
            .session_quota = QuotaAllocator.init(allocator, options.maximum_session_memory_bytes),
            .advertisement = advertisement,
            .rate_entries = rate_entries,
            .limiter = limiter,
            .handshake_handler = undefined,
            .source_secret = std.mem.readInt(u64, random[72..80], .little),
            .messages = messages,
            .receive_storage = receive_storage,
            .frame_scratch = frame_scratch,
            .handshake_output = handshake_output,
            .maintenance_interval_ms = options.maintenance_interval_ms,
        };
        self.handshake_handler = try handshake.Handler.init(guid, options.protocol_version, options.config.minimum_mtu, options.config.maximum_mtu, self.advertisement, .{ .current_key = random[8..40].*, .previous_key = random[40..72].* }, &self.limiter);
        return self;
    }

    pub fn close(self: *Listener) void {
        if (self.closed) return;
        self.closed = true;
        self.socket.close();
    }
    pub fn destroy(self: *Listener) void {
        var iterator = self.sessions.valueIterator();
        while (iterator.next()) |session| session.*.destroy();
        self.sessions.deinit(self.session_quota.allocator());
        std.debug.assert(self.session_quota.used_bytes == 0);
        self.close();
        self.allocator.free(self.frame_scratch);
        self.allocator.free(self.handshake_output);
        self.allocator.free(self.receive_storage);
        self.allocator.free(self.messages);
        self.allocator.free(self.rate_entries);
        self.allocator.free(self.advertisement);
        self.allocator.destroy(self);
    }

    pub fn poll(self: *Listener, timeout: std.Io.Timeout, callbacks: Callbacks) !PollStats {
        if (self.closed) return error.ConnectionClosed;
        var stats: PollStats = .{};
        const batch = self.socket.receiveMany(self.messages, self.receive_storage, timeout) catch |err| switch (err) {
            error.Timeout => {
                const now_ms = nowMilliseconds(self.io);
                self.maintain(now_ms, callbacks);
                stats.sessions_expired = self.expire(now_ms, callbacks);
                return stats;
            },
            else => return err,
        };
        stats.malformed += batch.dropped_oversize;
        for (batch.messages) |message| {
            stats.datagrams += 1;
            const key = endpointKey(message.from);
            const now_ms = nowMilliseconds(self.io);
            if (self.sessions.get(key)) |session| {
                if (message.data.len != 0 and switch (message.data[0]) {
                    @intFromEnum(offline.Id.unconnected_ping), @intFromEnum(offline.Id.unconnected_ping_open_connections), @intFromEnum(offline.Id.open_connection_request_1), @intFromEnum(offline.Id.open_connection_request_2) => true,
                    else => false,
                }) {
                    const repeated = self.handshake_handler.handle(message.data, &key, std.hash.Wyhash.hash(self.source_secret, &key), now_ms / 30_000, now_ms, self.handshake_output);
                    switch (repeated) {
                        .drop => stats.rate_limited_or_dropped += 1,
                        .response => |wire| try self.socket.send(message.from, wire),
                        .accepted => |accepted| try self.socket.send(message.from, accepted.response),
                    }
                    continue;
                }
                session.last_seen_ms = now_ms;
                const Bridge = struct {
                    callbacks: Callbacks,
                    session: *Session,
                    now_ms: u64,

                    fn deliver(raw: *anyopaque, payload: []const u8) !void {
                        const bridge: *@This() = @ptrCast(@alignCast(raw));
                        const packet = connected.decode(payload) catch return error.PeerProtocolFailure;
                        switch (packet) {
                            .connected_ping => |sent| {
                                var wire: [17]u8 = undefined;
                                const pong = connected.encodePong(sent, bridge.now_ms, &wire) catch return error.InternalFailure;
                                bridge.session.sendControl(pong, .unreliable, bridge.now_ms) catch |err| return core_mod.deliverySendFailure(err);
                            },
                            .connection_request => |request| {
                                var wire: [1024]u8 = undefined;
                                const remote = toRakAddress(bridge.session.address);
                                const local: offline.Address = switch (remote) {
                                    .ipv4 => .{ .ipv4 = .{ .octets = .{ 0, 0, 0, 0 }, .port = 0 } },
                                    .ipv6 => .{ .ipv6 = .{ .octets = @splat(0), .port = 0 } },
                                };
                                var systems: [20]offline.Address = @splat(local);
                                const accepted = connected.encodeAddressList(.accepted, remote, 0, &systems, request.request_time, bridge.now_ms, &wire) catch return error.InternalFailure;
                                bridge.session.sendControl(accepted, .reliable_ordered, bridge.now_ms) catch |err| return core_mod.deliverySendFailure(err);
                            },
                            .new_incoming_connection => {
                                if (bridge.session.state == .connecting) {
                                    bridge.session.state = .connected;
                                    bridge.callbacks.connected(bridge.callbacks.context, bridge.session) catch return error.ApplicationFailure;
                                }
                            },
                            .disconnect => bridge.session.state = .closed,
                            .detect_lost_connections => {
                                var wire: [9]u8 = undefined;
                                const ping = connected.encodePing(bridge.now_ms, &wire) catch return error.InternalFailure;
                                bridge.session.sendControl(ping, .reliable, bridge.now_ms) catch |err| return core_mod.deliverySendFailure(err);
                            },
                            .connected_pong => {},
                            .user => |user| {
                                if (bridge.session.state == .connected) {
                                    bridge.callbacks.message(bridge.callbacks.context, bridge.session, user) catch return error.ApplicationFailure;
                                }
                            },
                            .connection_request_accepted => {},
                        }
                    }
                };
                var bridge: Bridge = .{ .callbacks = callbacks, .session = session, .now_ms = now_ms };
                const incoming = session.core.processIncomingWithScratch(message.data, now_ms, self.frame_scratch, &bridge, Bridge.deliver) catch |err| {
                    const failure = core_mod.classifyIncomingError(err);
                    if (failure.disposition == .reject) {
                        stats.malformed += 1;
                        continue;
                    }

                    stats.sessions_failed += 1;
                    switch (failure.class) {
                        .protocol => stats.malformed += 1,
                        .resource => stats.resource_failures += 1,
                        .transport => stats.transport_failures += 1,
                        .application => stats.application_failures += 1,
                        .internal => stats.internal_failures += 1,
                    }
                    session.state = .closed;
                    self.removeSession(key, callbacks);
                    continue;
                };
                if (incoming == .data) session.flushReceipt(incoming.data) catch {};
                session.flushRetransmissions(now_ms) catch {
                    session.state = .closed;
                };
                if (session.state == .closed) self.removeSession(key, callbacks);
                continue;
            }

            const action = self.handshake_handler.handle(message.data, &key, std.hash.Wyhash.hash(self.source_secret, &key), now_ms / 30_000, now_ms, self.handshake_output);
            switch (action) {
                .drop => stats.rate_limited_or_dropped += 1,
                .response => |wire| try self.socket.send(message.from, wire),
                .accepted => |accepted| {
                    if (self.sessions.count() >= self.config.maximum_connections) {
                        stats.rate_limited_or_dropped += 1;
                        continue;
                    }
                    const session_allocator = self.session_quota.allocator();
                    const session = Session.create(session_allocator, &self.socket, message.from, key, accepted.client_guid, accepted.mtu, now_ms, self.config) catch {
                        stats.rate_limited_or_dropped += 1;
                        continue;
                    };
                    self.sessions.put(session_allocator, key, session) catch |err| {
                        session.destroy();
                        return err;
                    };
                    self.socket.send(message.from, accepted.response) catch |err| {
                        _ = self.sessions.remove(key);
                        session.destroy();
                        return err;
                    };
                },
            }
        }
        const maintenance_now_ms = nowMilliseconds(self.io);
        self.maintain(maintenance_now_ms, callbacks);
        stats.sessions_expired = self.expire(maintenance_now_ms, callbacks);
        return stats;
    }

    fn maintain(self: *Listener, now_ms: u64, callbacks: Callbacks) void {
        if (now_ms -| self.last_maintenance_ms < self.maintenance_interval_ms) return;
        self.last_maintenance_ms = now_ms;
        var failed: [256]EndpointKey = undefined;
        var count: usize = 0;
        var iterator = self.sessions.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.*.flushRetransmissions(now_ms) catch {
                if (count < failed.len) {
                    failed[count] = entry.key_ptr.*;
                    count += 1;
                }
            };
        }
        for (failed[0..count]) |key| self.removeSession(key, callbacks);
    }

    fn expire(self: *Listener, now_ms: u64, callbacks: Callbacks) usize {
        if (now_ms -| self.last_sweep_ms < 1000) return 0;
        self.last_sweep_ms = now_ms;
        var keys: [256]EndpointKey = undefined;
        var count: usize = 0;
        var iterator = self.sessions.iterator();
        while (iterator.next()) |entry| {
            if (count >= keys.len) break;
            if (now_ms -| entry.value_ptr.*.last_seen_ms < self.config.idle_timeout_ms) continue;
            keys[count] = entry.key_ptr.*;
            count += 1;
        }
        for (keys[0..count]) |key| self.removeSession(key, callbacks);
        return count;
    }
    fn removeSession(self: *Listener, key: EndpointKey, callbacks: Callbacks) void {
        const removed = self.sessions.fetchRemove(key) orelse return;
        if (callbacks.disconnected) |notify| notify(callbacks.context, removed.value);
        removed.value.destroy();
    }
};

fn endpointKey(address: std.Io.net.IpAddress) EndpointKey {
    var key: EndpointKey = @splat(0);
    switch (address) {
        .ip4 => |value| {
            key[0] = 4;
            @memcpy(key[1..5], &value.bytes);
            std.mem.writeInt(u16, key[17..19], value.port, .big);
        },
        .ip6 => |value| {
            key[0] = 6;
            @memcpy(key[1..17], &value.bytes);
            std.mem.writeInt(u16, key[17..19], value.port, .big);
            std.mem.writeInt(u32, key[19..23], value.interface.index, .big);
        },
    }
    return key;
}
fn toRakAddress(address: std.Io.net.IpAddress) offline.Address {
    return switch (address) {
        .ip4 => |value| .{ .ipv4 = .{ .octets = value.bytes, .port = value.port } },
        .ip6 => |value| .{ .ipv6 = .{ .octets = value.bytes, .port = value.port, .flow = value.flow, .scope = value.interface.index } },
    };
}
fn nowMilliseconds(io: std.Io) u64 {
    return @intCast(@max(@as(i96, 0), @divTrunc(std.Io.Clock.awake.now(io).nanoseconds, std.time.ns_per_ms)));
}

test "listener answers an offline ping over loopback" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var listener = try Listener.listen(std.testing.allocator, io, address, .{ .advertisement = "MCPE;zig-raknet" });
    defer listener.destroy();
    var client = try backend.Socket.bind(io, address, 2048);
    defer client.close();
    var ping: [33]u8 = undefined;
    var writer: @import("protocol/cursor.zig").Writer = .{ .data = &ping };
    try writer.byte(@intFromEnum(offline.Id.unconnected_ping));
    try writer.u64be(123);
    try writer.bytes(&offline.magic);
    try writer.u64be(456);
    try client.send(listener.socket.value.address, &ping);
    const TestContext = struct { connections: usize = 0 };
    const Noop = struct {
        fn connected(raw: *anyopaque, _: *Session) !void {
            const value: *TestContext = @ptrCast(@alignCast(raw));
            value.connections += 1;
        }
        fn message(_: *anyopaque, _: *Session, _: []const u8) !void {}
    };
    var context: TestContext = .{};
    const stats = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    try std.testing.expectEqual(@as(usize, 1), stats.datagrams);
    var response: [2048]u8 = undefined;
    const message = try client.value.receive(io, &response);
    try std.testing.expectEqual(@intFromEnum(offline.Id.unconnected_pong), message.data[0]);

    var request1: [548]u8 = @splat(0);
    request1[0] = @intFromEnum(offline.Id.open_connection_request_1);
    @memcpy(request1[1..17], &offline.magic);
    request1[17] = 11;
    try client.send(listener.socket.value.address, &request1);
    _ = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    const reply1 = try client.value.receive(io, &response);
    try std.testing.expectEqual(@intFromEnum(offline.Id.open_connection_reply_1), reply1.data[0]);
    const cookie_value = std.mem.readInt(u32, reply1.data[26..30], .big);
    const mtu = std.mem.readInt(u16, reply1.data[30..32], .big);

    var request2_buffer: [64]u8 = undefined;
    var request2: @import("protocol/cursor.zig").Writer = .{ .data = &request2_buffer };
    try request2.byte(@intFromEnum(offline.Id.open_connection_request_2));
    try request2.bytes(&offline.magic);
    try request2.u32be(cookie_value);
    try request2.byte(0);
    try offline.encodeAddress(toRakAddress(listener.socket.value.address), &request2);
    try request2.u16be(mtu);
    try request2.u64be(0x8000_0000_0000_0001);
    try client.send(listener.socket.value.address, request2.written());
    _ = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    const reply2 = try client.value.receive(io, &response);
    try std.testing.expectEqual(@intFromEnum(offline.Id.open_connection_reply_2), reply2.data[0]);
    try std.testing.expectEqual(@as(u32, 1), listener.sessions.count());

    try client.send(listener.socket.value.address, request2.written());
    _ = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    _ = try client.value.receive(io, &response);
    try std.testing.expectEqual(@as(u32, 1), listener.sessions.count());

    const Sender = struct {
        socket: *backend.Socket,
        destination: std.Io.net.IpAddress,
        fn emit(raw: *anyopaque, _: u32, _: bool, wire: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.socket.send(self.destination, wire);
        }
    };
    var sender: Sender = .{ .socket = &client, .destination = listener.socket.value.address };
    var transmitter = try @import("session/transmitter.zig").Transmitter.init(mtu, .{});
    var send_scratch: [1492]u8 = undefined;
    var control: [512]u8 = undefined;
    const connection_request = try connected.encodeConnectionRequest(0x8000_0000_0000_0001, 1000, &control);
    _ = try transmitter.send(connection_request, .reliable_ordered, 0, send_scratch[0..mtu], &sender, Sender.emit);
    _ = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    const accepted_datagram = try client.value.receive(io, &response);
    var decoded_datagram = try @import("protocol/frame.zig").decodeDatagram(accepted_datagram.data);
    const accepted_frame = try @import("protocol/frame.zig").decodeOne(&decoded_datagram.frames, 8192, 2048);
    try std.testing.expect((try connected.decode(accepted_frame.payload)) == .connection_request_accepted);

    const new_incoming = try connected.encodeAddressList(.incoming, toRakAddress(listener.socket.value.address), 0, &.{}, 1000, 1001, &control);
    _ = try transmitter.send(new_incoming, .reliable_ordered, 0, send_scratch[0..mtu], &sender, Sender.emit);
    _ = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    try std.testing.expectEqual(@as(usize, 1), context.connections);
}
