const std = @import("std");

const offline = @import("../protocol/offline.zig");

pub const Options = struct {
    protocol_version: u8,
    mtus: []const u16,
    attempts_per_mtu: u8 = 4,
    minimum_mtu: u16,
    retry_ms: u32,
    client_guid: u64,
    server_address: offline.Address,
    maximum_transient_errors: u16 = 10,
    maximum_datagrams: usize = 1024,
};

pub const Result = struct { server_guid: u64, mtu: u16 };

const Grant = struct { server_guid: u64, cookie: ?u32, mtu: u16 };
const Challenge = struct { cookie: ?u32, mtu: u16 };

const challenge_floor = 400;
const challenge_ceiling = 1500;

pub const Negotiator = struct {
    options: Options,
    request1_sent: usize = 0,
    next_request1_ms: u64 = 0,
    next_request2_ms: u64 = 0,
    grant: ?Grant = null,
    challenge: ?Challenge = null,
    transient_errors: u16 = 0,
    result: ?Result = null,

    pub fn init(options: Options) Negotiator {
        std.debug.assert(options.mtus.len != 0 and options.attempts_per_mtu != 0 and options.retry_ms != 0);
        return .{ .options = options };
    }

    pub fn poll(self: *Negotiator, now_ms: u64, output: []u8) !?[]const u8 {
        if (self.result != null) return null;
        if (self.challenge) |challenge| {
            self.challenge = null;
            return try offline.encodeOpenConnectionRequest2(self.options.server_address, challenge.cookie, challenge.mtu, self.options.client_guid, output);
        }
        if (self.grant) |grant| if (now_ms >= self.next_request2_ms) {
            self.next_request2_ms = now_ms +| self.options.retry_ms;
            return try offline.encodeOpenConnectionRequest2(self.options.server_address, grant.cookie, grant.mtu, self.options.client_guid, output);
        };
        if (self.probing() and now_ms >= self.next_request1_ms) {
            const rung = (self.request1_sent / self.options.attempts_per_mtu) % self.options.mtus.len;
            self.request1_sent += 1;
            self.next_request1_ms = now_ms +| self.options.retry_ms;
            return try offline.encodeOpenConnectionRequest1(self.options.protocol_version, self.options.mtus[rung], output);
        }
        return null;
    }

    pub fn nextDeadline(self: *const Negotiator) u64 {
        if (self.result != null) return std.math.maxInt(u64);
        var deadline: u64 = std.math.maxInt(u64);
        if (self.challenge != null) return 0;
        if (self.grant != null) deadline = self.next_request2_ms;
        if (self.probing()) deadline = @min(deadline, self.next_request1_ms);
        return deadline;
    }

    pub fn receive(self: *Negotiator, datagram: []const u8) !?Result {
        if (self.result != null or datagram.len == 0) return null;
        switch (datagram[0]) {
            @intFromEnum(offline.Id.incompatible_protocol_version) => return error.IncompatibleProtocol,
            @intFromEnum(offline.Id.no_free_incoming_connections) => return error.NoFreeIncomingConnections,
            @intFromEnum(offline.Id.open_connection_reply_1) => self.reply1(datagram),
            @intFromEnum(offline.Id.open_connection_reply_2) => return self.reply2(datagram),
            else => {},
        }
        return null;
    }

    pub fn transient(self: *Negotiator, err: anyerror) !void {
        if (!isTransient(err) or self.transient_errors >= self.options.maximum_transient_errors) return err;
        self.transient_errors += 1;
    }

    pub fn run(self: *Negotiator, transport: anytype, deadline_ms: u64, output: []u8) !Result {
        var work: usize = 0;
        while (true) {
            const now_ms = transport.now();
            if (now_ms >= deadline_ms) return error.Timeout;
            while (try self.poll(now_ms, output)) |wire| transport.send(wire) catch |err| try self.transient(err);
            const datagram = transport.receive(@min(deadline_ms, self.nextDeadline())) catch |err| {
                try self.transient(err);
                continue;
            } orelse continue;
            work += 1;
            if (work > self.options.maximum_datagrams) return error.HandshakeWorkLimitExceeded;
            if (try self.receive(datagram)) |result| return result;
        }
    }

    fn probing(self: *const Negotiator) bool {
        return self.grant == null or self.request1_sent < self.options.mtus.len * self.options.attempts_per_mtu;
    }

    fn reply1(self: *Negotiator, datagram: []const u8) void {
        const reply = offline.decodeOpenConnectionReply1(datagram) catch return;
        if (reply.server_guid == 0 or reply.mtu < challenge_floor or reply.mtu > challenge_ceiling) {
            if (self.grant == null) self.challenge = .{ .cookie = reply.cookie, .mtu = reply.mtu };
            return;
        }
        if (reply.mtu < self.options.minimum_mtu or reply.mtu > self.options.mtus[0]) return;
        const grant: Grant = .{ .server_guid = reply.server_guid, .cookie = reply.cookie, .mtu = reply.mtu };
        if (self.grant) |current| if (std.meta.eql(current, grant)) return;
        self.grant = grant;
        self.next_request2_ms = 0;
    }

    fn reply2(self: *Negotiator, datagram: []const u8) !?Result {
        const grant = self.grant orelse return null;
        const reply = offline.decodeOpenConnectionReply2(datagram, 0, std.math.maxInt(u16)) catch |err| switch (err) {
            error.UnsupportedSecurity => return err,
            else => return null,
        };
        const mtu = if (reply.mtu >= self.options.minimum_mtu) @min(grant.mtu, reply.mtu) else grant.mtu;
        self.result = .{ .server_guid = reply.server_guid, .mtu = mtu };
        return self.result;
    }
};

pub fn isTransient(err: anyerror) bool {
    return switch (err) {
        error.MessageOversize,
        error.ConnectionResetByPeer,
        error.ConnectionRefused,
        error.PortUnreachable,
        error.HostUnreachable,
        error.NetworkUnreachable,
        => true,
        else => false,
    };
}

const testing = std.testing;
const server_address: offline.Address = .{ .ipv4 = .{ .octets = .{ 127, 0, 0, 1 }, .port = 19132 } };
const ladder = [_]u16{ 1492, 1200, 576 };

fn testOptions() Options {
    return .{
        .protocol_version = 11,
        .mtus = &ladder,
        .minimum_mtu = 576,
        .retry_ms = 500,
        .client_guid = 99,
        .server_address = server_address,
    };
}

const FakeServer = struct {
    now_ms: u64 = 0,
    path_mtu: u16 = 1492,
    server_mtu: u16 = 1492,
    cookie: u32 = 0xc0ffee,
    reply2_mtu: ?u16 = null,
    challenge_mtu: ?u16 = null,
    challenge_answered: bool = false,
    require_probe_at_most: ?u16 = null,
    smallest_probe: u16 = std.math.maxInt(u16),
    rotate_cookie: bool = false,
    receive_errors: []const anyerror = &.{},
    send_errors: []const anyerror = &.{},

    queue: [8][64]u8 = undefined,
    queue_len: [8]usize = undefined,
    queued: usize = 0,
    delivered: [64]u8 = undefined,
    request1: usize = 0,
    request2: usize = 0,
    request2_mtus: [32]u16 = undefined,
    request2_cookies: [32]?u32 = undefined,

    fn now(self: *FakeServer) u64 {
        return self.now_ms;
    }

    fn send(self: *FakeServer, data: []const u8) !void {
        if (self.send_errors.len != 0) {
            const err = self.send_errors[0];
            self.send_errors = self.send_errors[1..];
            return err;
        }
        switch (data[0]) {
            @intFromEnum(offline.Id.open_connection_request_1) => {
                self.request1 += 1;
                const size: u16 = @intCast(data.len + 28);
                if (size > self.path_mtu) return;
                self.smallest_probe = @min(self.smallest_probe, size);
                if (self.challenge_mtu) |value| if (!self.challenge_answered) {
                    var out: [64]u8 = undefined;
                    return self.push(try offline.encodeOpenConnectionReply1(1, self.cookie, value, false, &out));
                };
                if (self.rotate_cookie and self.request1 > 1) self.cookie +%= 1;
                var out: [64]u8 = undefined;
                self.push(try offline.encodeOpenConnectionReply1(1, self.cookie, @min(size, self.server_mtu), false, &out));
            },
            @intFromEnum(offline.Id.open_connection_request_2) => {
                const request = try offline.decodeOpenConnectionRequest2(data, true, 0, std.math.maxInt(u16));
                self.request2_mtus[self.request2] = request.mtu;
                self.request2_cookies[self.request2] = request.cookie;
                self.request2 += 1;
                if (request.cookie != self.cookie) return;
                if (self.challenge_mtu) |value| if (!self.challenge_answered) {
                    if (request.mtu == value) self.challenge_answered = true;
                    return;
                };
                if (self.require_probe_at_most) |limit| if (self.smallest_probe > limit) return;
                var out: [64]u8 = undefined;
                self.push(try offline.encodeOpenConnectionReply2(1, server_address, self.reply2_mtu orelse request.mtu, &out));
            },
            else => {},
        }
    }

    fn receive(self: *FakeServer, deadline_ms: u64) !?[]const u8 {
        if (self.receive_errors.len != 0) {
            const err = self.receive_errors[0];
            self.receive_errors = self.receive_errors[1..];
            return err;
        }
        if (self.queued == 0) {
            self.now_ms = @max(self.now_ms, deadline_ms);
            return null;
        }
        const len = self.queue_len[0];
        @memcpy(self.delivered[0..len], self.queue[0][0..len]);
        self.queued -= 1;
        for (0..self.queued) |i| {
            self.queue[i] = self.queue[i + 1];
            self.queue_len[i] = self.queue_len[i + 1];
        }
        return self.delivered[0..len];
    }

    fn push(self: *FakeServer, data: []const u8) void {
        if (self.queued == self.queue.len) return;
        @memcpy(self.queue[self.queued][0..data.len], data);
        self.queue_len[self.queued] = data.len;
        self.queued += 1;
    }
};

fn negotiate(server: *FakeServer, options: Options) !Result {
    var negotiator: Negotiator = .init(options);
    var output: [1500]u8 = undefined;
    const result = try negotiator.run(server, 5_000, &output);
    try testing.expectEqual(@as(?[]const u8, null), try negotiator.poll(std.math.maxInt(u64), &output));
    try testing.expectEqual(@as(u64, std.math.maxInt(u64)), negotiator.nextDeadline());
    return result;
}

test "vanilla handshake completes at 1492" {
    var server: FakeServer = .{};
    const result = try negotiate(&server, testOptions());
    try testing.expectEqual(@as(u16, 1492), result.mtu);
    try testing.expectEqual(@as(usize, 1), server.request2);
    try testing.expectEqual(@as(u16, 1492), server.request2_mtus[0]);
    try testing.expectEqual(@as(?u32, 0xc0ffee), server.request2_cookies[0]);
}

test "MTU ladder falls back to 1200 and 576" {
    var server: FakeServer = .{ .path_mtu = 1300 };
    try testing.expectEqual(@as(u16, 1200), (try negotiate(&server, testOptions())).mtu);
    try testing.expectEqual(@as(usize, 5), server.request1);

    server = .{ .path_mtu = 1000 };
    try testing.expectEqual(@as(u16, 576), (try negotiate(&server, testOptions())).mtu);
    try testing.expectEqual(@as(usize, 9), server.request1);
}

test "Request 2 echoes the exact Reply 1 grant and Reply 2 may lower it" {
    var server: FakeServer = .{ .server_mtu = 1400 };
    try testing.expectEqual(@as(u16, 1400), (try negotiate(&server, testOptions())).mtu);
    try testing.expectEqual(@as(u16, 1400), server.request2_mtus[0]);

    server = .{ .reply2_mtu = 1300 };
    try testing.expectEqual(@as(u16, 1300), (try negotiate(&server, testOptions())).mtu);

    server = .{ .server_mtu = 1200, .reply2_mtu = 1492 };
    try testing.expectEqual(@as(u16, 1200), (try negotiate(&server, testOptions())).mtu);
    server = .{ .reply2_mtu = 300 };
    try testing.expectEqual(@as(u16, 1492), (try negotiate(&server, testOptions())).mtu);
}

test "Request 1 probing continues until a protected server accepts Request 2" {
    var server: FakeServer = .{ .require_probe_at_most = 1200, .rotate_cookie = true };
    const result = try negotiate(&server, testOptions());
    try testing.expectEqual(@as(u16, 1200), result.mtu);
    try testing.expectEqual(@as(u16, 1200), server.request2_mtus[server.request2 - 1]);
    try testing.expectEqual(@as(?u32, server.cookie), server.request2_cookies[server.request2 - 1]);
    try testing.expect(server.request1 >= 5);
}

test "repeated identical Reply 1 does not restart Request 2 early" {
    var negotiator: Negotiator = .init(testOptions());
    var output: [1500]u8 = undefined;
    _ = (try negotiator.poll(0, &output)).?;
    var reply: [64]u8 = undefined;
    const wire = try offline.encodeOpenConnectionReply1(1, 5, 1492, false, &reply);
    _ = try negotiator.receive(wire);
    try testing.expectEqual(offline.Id.open_connection_request_2, @as(offline.Id, @enumFromInt((try negotiator.poll(0, &output)).?[0])));
    try testing.expectEqual(@as(?[]const u8, null), try negotiator.poll(10, &output));
    _ = try negotiator.receive(wire);
    try testing.expectEqual(@as(?[]const u8, null), try negotiator.poll(10, &output));
    _ = try negotiator.receive(try offline.encodeOpenConnectionReply1(1, 6, 1492, false, &reply));
    const request = try offline.decodeOpenConnectionRequest2((try negotiator.poll(10, &output)).?, true, 0, 65535);
    try testing.expectEqual(@as(?u32, 6), request.cookie);
}

test "protected server challenge MTU is echoed but never used" {
    for ([_]u16{ 65535, 1, 399, 1501 }) |challenge| {
        var server: FakeServer = .{ .challenge_mtu = challenge };
        const result = try negotiate(&server, testOptions());
        try testing.expectEqual(challenge, server.request2_mtus[0]);
        try testing.expectEqual(@as(u16, 1492), result.mtu);
    }
    var negotiator: Negotiator = .init(testOptions());
    var reply: [64]u8 = undefined;
    _ = try negotiator.receive(try offline.encodeOpenConnectionReply1(1, null, 1496, false, &reply));
    _ = try negotiator.receive(try offline.encodeOpenConnectionReply1(1, null, 500, false, &reply));
    try testing.expect(negotiator.grant == null and negotiator.challenge == null);
}

test "transient UDP errors are retried within a bounded budget" {
    const errors = [_]anyerror{ error.ConnectionResetByPeer, error.PortUnreachable, error.MessageOversize };
    var server: FakeServer = .{ .receive_errors = &errors, .send_errors = &.{ error.MessageOversize, error.NetworkUnreachable } };
    try testing.expectEqual(@as(u16, 1492), (try negotiate(&server, testOptions())).mtu);

    var options = testOptions();
    options.maximum_transient_errors = 2;
    server = .{ .receive_errors = &errors };
    try testing.expectError(error.MessageOversize, negotiate(&server, options));
    server = .{ .receive_errors = &.{error.Canceled} };
    try testing.expectError(error.Canceled, negotiate(&server, testOptions()));
}

test "unexpected datagrams are ignored and terminal replies fail" {
    var negotiator: Negotiator = .init(testOptions());
    try testing.expectEqual(@as(?Result, null), try negotiator.receive(&.{}));
    try testing.expectEqual(@as(?Result, null), try negotiator.receive(&.{ 0x84, 0, 0, 0 }));
    try testing.expectEqual(@as(?Result, null), try negotiator.receive(&.{@intFromEnum(offline.Id.open_connection_reply_1)}));
    var reply: [64]u8 = undefined;
    try testing.expectEqual(@as(?Result, null), try negotiator.receive(try offline.encodeOpenConnectionReply2(1, server_address, 1492, &reply)));
    try testing.expectError(error.IncompatibleProtocol, negotiator.receive(&.{@intFromEnum(offline.Id.incompatible_protocol_version)}));
    try testing.expectError(error.NoFreeIncomingConnections, negotiator.receive(&.{@intFromEnum(offline.Id.no_free_incoming_connections)}));
}

test "silent server times out at the deadline with bounded sends" {
    var server: FakeServer = .{ .path_mtu = 0 };
    try testing.expectError(error.Timeout, negotiate(&server, testOptions()));
    try testing.expectEqual(@as(usize, 10), server.request1);
    try testing.expectEqual(@as(u64, 5_000), server.now_ms);
}
