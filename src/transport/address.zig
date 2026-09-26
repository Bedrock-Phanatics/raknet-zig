const std = @import("std");

const offline = @import("../protocol/offline.zig");

pub fn toRakNet(address: std.Io.net.IpAddress) offline.Address {
    return switch (address) {
        .ip4 => |value| .{ .ipv4 = .{ .octets = value.bytes, .port = value.port } },
        .ip6 => |value| if (std.mem.eql(u8, value.bytes[0..12], &mapped_prefix))
            .{ .ipv4 = .{ .octets = value.bytes[12..16].*, .port = value.port } }
        else
            .{ .ipv6 = .{
                .octets = value.bytes,
                .port = value.port,
                .flow = value.flow,
                .scope = value.interface.index,
            } },
    };
}

pub fn unspecified(address: offline.Address) offline.Address {
    return switch (address) {
        .ipv4 => .{ .ipv4 = .{ .octets = .{ 0, 0, 0, 0 }, .port = 0 } },
        .ipv6 => .{ .ipv6 = .{ .octets = @splat(0), .port = 0 } },
    };
}

const mapped_prefix = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff };

test "IP addresses preserve their RakNet wire fields" {
    const ipv4 = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:19132");
    try std.testing.expectEqual(offline.Address{ .ipv4 = .{
        .octets = .{ 127, 0, 0, 1 },
        .port = 19132,
    } }, toRakNet(ipv4));

    const ipv6 = try std.Io.net.IpAddress.parseLiteral("[2001:db8::1]:19133");
    const wire = toRakNet(ipv6);
    try std.testing.expectEqual(@as(u16, 19133), wire.ipv6.port);
    try std.testing.expectEqual(@as(u8, 0x20), wire.ipv6.octets[0]);
    try std.testing.expectEqual(@as(u8, 1), wire.ipv6.octets[15]);

    const mapped = try std.Io.net.IpAddress.parseLiteral("[::ffff:192.0.2.7]:19132");
    try std.testing.expectEqual(offline.Address{ .ipv4 = .{
        .octets = .{ 192, 0, 2, 7 },
        .port = 19132,
    } }, toRakNet(mapped));

    try std.testing.expect(unspecified(wire) == .ipv6);
    try std.testing.expect(unspecified(toRakNet(ipv4)) == .ipv4);
}
