const std = @import("std");

const offline = @import("../protocol/offline.zig");
const cookie = @import("../security/cookie.zig");
const rate = @import("../security/rate_limit.zig");

pub const Accepted = struct {
    response: []const u8,
    client_guid: u64,
    mtu: u16,
    server_address: offline.Address,
};
pub const Action = union(enum) { drop, response: []const u8, accepted: Accepted };

pub const Counters = struct {
    unconnected_pings: u64 = 0,
    open_connection_requests_1: u64 = 0,
    open_connection_requests_2: u64 = 0,
    rejected: u64 = 0,
};

pub const Handler = struct {
    server_guid: u64,
    protocol_version: u8,
    minimum_mtu: u16,
    maximum_mtu: u16,
    advertisement: []const u8,
    cookies: cookie.Jar,
    limiter: *rate.Limiter,
    counters: Counters = .{},

    pub fn init(server_guid: u64, protocol_version: u8, minimum_mtu: u16, maximum_mtu: u16, advertisement: []const u8, cookies: cookie.Jar, limiter: *rate.Limiter) !Handler {
        if (minimum_mtu < 400 or minimum_mtu > maximum_mtu or advertisement.len > 65_535) return error.InvalidConfiguration;
        return .{
            .server_guid = server_guid,
            .protocol_version = protocol_version,
            .minimum_mtu = minimum_mtu,
            .maximum_mtu = maximum_mtu,
            .advertisement = advertisement,
            .cookies = cookies,
            .limiter = limiter,
        };
    }

    pub fn handle(self: *Handler, datagram: []const u8, endpoint: []const u8, source_key: u64, epoch: u64, now_ms: u64, output: []u8) Action {
        if (datagram.len == 0 or datagram.len > 65_507) return .drop;
        return switch (datagram[0]) {
            @intFromEnum(offline.Id.unconnected_ping), @intFromEnum(offline.Id.unconnected_ping_open_connections) => self.ping(datagram, source_key, now_ms, output),
            @intFromEnum(offline.Id.open_connection_request_1) => self.request1(datagram, endpoint, source_key, epoch, now_ms, output),
            @intFromEnum(offline.Id.open_connection_request_2) => self.request2(datagram, endpoint, source_key, epoch, now_ms, output),
            else => .drop,
        };
    }

    fn ping(self: *Handler, datagram: []const u8, source_key: u64, now_ms: u64, output: []u8) Action {
        self.counters.unconnected_pings += 1;
        const request = offline.decodeUnconnectedPing(datagram) catch return .drop;
        const response_size = 35 + self.advertisement.len;
        const cost: u32 = @intCast(@min(@as(usize, std.math.maxInt(u32)), (response_size + datagram.len - 1) / datagram.len));
        if (!self.limiter.allow(source_key, @max(cost, 1), now_ms)) return .drop;
        const response = offline.encodeUnconnectedPong(request.time, self.server_guid, self.advertisement, output) catch return .drop;
        return .{ .response = response };
    }

    fn request1(self: *Handler, datagram: []const u8, endpoint: []const u8, source_key: u64, epoch: u64, now_ms: u64, output: []u8) Action {
        self.counters.open_connection_requests_1 += 1;
        if (!self.limiter.allow(source_key, 1, now_ms)) return self.reject();
        const request = offline.decodeOpenConnectionRequest1(datagram, self.minimum_mtu, self.maximum_mtu) catch return self.reject();
        if (request.protocol_version != self.protocol_version) {
            const response = offline.encodeIncompatibleProtocol(self.protocol_version, self.server_guid, output) catch return .drop;
            return .{ .response = response };
        }
        const value = self.cookies.create(endpoint, epoch);
        const response = offline.encodeOpenConnectionReply1(self.server_guid, value, request.mtu, true, output) catch return .drop;
        if (response.len > datagram.len) return .drop;
        return .{ .response = response };
    }

    fn request2(self: *Handler, datagram: []const u8, endpoint: []const u8, source_key: u64, epoch: u64, now_ms: u64, output: []u8) Action {
        self.counters.open_connection_requests_2 += 1;
        if (!self.limiter.allow(source_key, 1, now_ms)) return self.reject();
        const request = offline.decodeOpenConnectionRequest2(datagram, true, self.minimum_mtu, self.maximum_mtu) catch return self.reject();
        if (!self.cookies.verify(request.cookie.?, endpoint, epoch)) return self.reject();
        const response = offline.encodeOpenConnectionReply2(self.server_guid, request.server_address, request.mtu, output) catch return .drop;
        return .{ .accepted = .{ .response = response, .client_guid = request.client_guid, .mtu = request.mtu, .server_address = request.server_address } };
    }

    fn reject(self: *Handler) Action {
        self.counters.rejected += 1;
        return .drop;
    }
};

test "request one is stateless, cookie bound, and non-amplifying" {
    var entries: [8]rate.Entry = undefined;
    var limiter = try rate.Limiter.init(&entries, .{ .tokens_per_second = 100, .burst = 100, .global_tokens_per_second = 100, .global_burst = 100 }, 0);
    const jar: cookie.Jar = .{ .current_key = [_]u8{3} ** 32, .previous_key = [_]u8{4} ** 32 };
    var handler = try Handler.init(7, 11, 576, 1492, "MCPE;server", jar, &limiter);
    var request: [548]u8 = @splat(0);
    request[0] = @intFromEnum(offline.Id.open_connection_request_1);
    @memcpy(request[1..17], &offline.magic);
    request[17] = 11;
    var output: [1492]u8 = undefined;
    const action = handler.handle(&request, "192.0.2.1:19132", 1, 10, 0, &output);
    try std.testing.expect(action == .response);
    try std.testing.expect(action.response.len <= request.len);
}

test "handshake floods stay inside rate limits" {
    var entries: [4]rate.Entry = undefined;
    var limiter = try rate.Limiter.init(&entries, .{
        .tokens_per_second = 1,
        .burst = 4,
        .global_tokens_per_second = 1,
        .global_burst = 4,
    }, 0);
    const jar: cookie.Jar = .{ .current_key = [_]u8{3} ** 32, .previous_key = [_]u8{4} ** 32 };
    var handler = try Handler.init(7, 11, 576, 1492, "MCPE;server", jar, &limiter);
    var request: [548]u8 = @splat(0);
    request[0] = @intFromEnum(offline.Id.open_connection_request_1);
    @memcpy(request[1..17], &offline.magic);
    request[17] = 11;
    var output: [1492]u8 = undefined;
    var responses: usize = 0;
    for (0..10_000) |_| if (handler.handle(&request, "192.0.2.1:19132", 1, 10, 0, &output) == .response) {
        responses += 1;
    };
    try std.testing.expectEqual(@as(usize, 4), responses);
}
