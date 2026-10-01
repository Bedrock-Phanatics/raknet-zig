const std = @import("std");
const offline = @import("../protocol/offline.zig");
const backend = @import("socket.zig");
const time = @import("../util/time.zig");

pub const Pong = offline.UnconnectedPong;

pub fn ping(io: std.Io, server: std.Io.net.IpAddress, buffer: []u8, timeout_ms: u32) !Pong {
    if (buffer.len < 35 or buffer.len > 65_507 or timeout_ms == 0) return error.InvalidConfiguration;
    const local: std.Io.net.IpAddress = switch (server) {
        .ip4 => .{ .ip4 = .unspecified(0) },
        .ip6 => .{ .ip6 = .unspecified(0) },
    };
    var socket = try backend.Socket.bind(io, local, buffer.len);
    defer socket.close();
    var identity: [16]u8 = undefined;
    io.random(&identity);
    const timestamp = std.mem.readInt(u64, identity[0..8], .big);
    const guid = std.mem.readInt(u64, identity[8..16], .big);
    const request = try offline.encodeUnconnectedPing(timestamp, guid, buffer);
    const deadline = time.after(io, timeout_ms);
    try socket.send(server, request);
    while (true) {
        try io.checkCancel();
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.deadline.raw.nanoseconds) return error.Timeout;
        const message = try socket.receive(buffer, deadline);
        if (!std.meta.eql(message.from, server)) continue;
        if (message.flags.trunc) return error.ResponseTooLarge;
        const pong = offline.decodeUnconnectedPong(message.data) catch continue;
        if (pong.time == timestamp) return pong;
    }
}
