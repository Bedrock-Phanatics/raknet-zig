const std = @import("std");
const offline = @import("../protocol/offline.zig");

pub fn toRakNet(address: std.Io.net.IpAddress) offline.Address {
    return switch (address) {
        .ip4 => |value| .{ .ipv4 = .{ .octets = value.bytes, .port = value.port } },
        .ip6 => |value| .{ .ipv6 = .{
            .octets = value.bytes,
            .port = value.port,
            .flow = value.flow,
            .scope = value.interface.index,
        } },
    };
}

test "IP addresses preserve their RakNet wire fields" {
    const ipv4 = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:19132");
    try std.testing.expectEqual(offline.Address{ .ipv4 = .{
        .octets = .{ 127, 0, 0, 1 },
        .port = 19132,
    } }, toRakNet(ipv4));
}
