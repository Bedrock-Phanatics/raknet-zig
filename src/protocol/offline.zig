const std = @import("std");
const cursor = @import("cursor.zig");

pub const magic = [16]u8{ 0x00, 0xff, 0xff, 0x00, 0xfe, 0xfe, 0xfe, 0xfe, 0xfd, 0xfd, 0xfd, 0xfd, 0x12, 0x34, 0x56, 0x78 };

pub const Id = enum(u8) {
    connected_ping = 0x00,
    unconnected_ping = 0x01,
    unconnected_ping_open_connections = 0x02,
    connected_pong = 0x03,
    detect_lost_connections = 0x04,
    open_connection_request_1 = 0x05,
    open_connection_reply_1 = 0x06,
    open_connection_request_2 = 0x07,
    open_connection_reply_2 = 0x08,
    connection_request = 0x09,
    connection_request_accepted = 0x10,
    new_incoming_connection = 0x13,
    no_free_incoming_connections = 0x14,
    disconnect_notification = 0x15,
    incompatible_protocol_version = 0x19,
    unconnected_pong = 0x1c,
};

pub const Address = union(enum) {
    ipv4: struct { octets: [4]u8, port: u16 },
    ipv6: struct { octets: [16]u8, port: u16, flow: u32 = 0, scope: u32 = 0 },
};

pub fn decodeAddress(reader: *cursor.Reader) !Address {
    return switch (try reader.byte()) {
        4, 0 => blk: {
            const encoded = try reader.take(4);
            const port = try reader.u16be();
            break :blk .{ .ipv4 = .{ .octets = .{ ~encoded[0], ~encoded[1], ~encoded[2], ~encoded[3] }, .port = port } };
        },
        6 => blk: {
            _ = try reader.u16le(); // RakNet uses Windows AF_INET6 (23); peers vary, so don't require it.
            const port = try reader.u16be();
            const flow = try reader.u32be();
            const octets_slice = try reader.take(16);
            var octets: [16]u8 = undefined;
            @memcpy(&octets, octets_slice);
            const scope = try reader.u32be();
            break :blk .{ .ipv6 = .{ .octets = octets, .port = port, .flow = flow, .scope = scope } };
        },
        else => error.InvalidAddressFamily,
    };
}

pub fn encodeAddress(address: Address, writer: *cursor.Writer) !void {
    switch (address) {
        .ipv4 => |v| {
            try writer.byte(4);
            try writer.bytes(&.{ ~v.octets[0], ~v.octets[1], ~v.octets[2], ~v.octets[3] });
            try writer.u16be(v.port);
        },
        .ipv6 => |v| {
            try writer.byte(6);
            try writer.u16le(23);
            try writer.u16be(v.port);
            try writer.u32be(v.flow);
            try writer.bytes(&v.octets);
            try writer.u32be(v.scope);
        },
    }
}

fn expectMagic(reader: *cursor.Reader) !void {
    if (!std.mem.eql(u8, try reader.take(magic.len), &magic)) return error.InvalidMagic;
}

pub const UnconnectedPing = struct { time: u64, client_guid: u64, open_connections_only: bool };
pub fn decodeUnconnectedPing(data: []const u8) !UnconnectedPing {
    var r: cursor.Reader = .{ .data = data };
    const id = try r.byte();
    if (id != @intFromEnum(Id.unconnected_ping) and id != @intFromEnum(Id.unconnected_ping_open_connections)) return error.WrongPacket;
    const time = try r.u64be();
    try expectMagic(&r);
    const guid = try r.u64be();
    if (r.remaining() != 0) return error.TrailingData;
    return .{ .time = time, .client_guid = guid, .open_connections_only = id == @intFromEnum(Id.unconnected_ping_open_connections) };
}

pub const OpenConnectionRequest1 = struct { protocol_version: u8, mtu: u16 };
pub fn decodeOpenConnectionRequest1(data: []const u8, minimum_mtu: u16, maximum_mtu: u16) !OpenConnectionRequest1 {
    if (data.len > 65_507 or data.len > maximum_mtu -| 28) return error.MtuTooLarge;
    var r: cursor.Reader = .{ .data = data };
    if (try r.byte() != @intFromEnum(Id.open_connection_request_1)) return error.WrongPacket;
    try expectMagic(&r);
    const version = try r.byte();
    const mtu_usize = try std.math.add(usize, data.len, 28);
    if (mtu_usize < minimum_mtu or mtu_usize > maximum_mtu) return error.InvalidMtu;
    return .{ .protocol_version = version, .mtu = @intCast(mtu_usize) };
}

pub const OpenConnectionRequest2 = struct { cookie: ?u32, server_address: Address, mtu: u16, client_guid: u64 };
pub fn decodeOpenConnectionRequest2(data: []const u8, cookie_required: bool, minimum_mtu: u16, maximum_mtu: u16) !OpenConnectionRequest2 {
    var r: cursor.Reader = .{ .data = data };
    if (try r.byte() != @intFromEnum(Id.open_connection_request_2)) return error.WrongPacket;
    try expectMagic(&r);
    const cookie = if (cookie_required) try r.u32be() else null;
    if (cookie_required and try r.byte() != 0) return error.UnsupportedSecurity;
    const address = try decodeAddress(&r);
    const mtu = try r.u16be();
    const guid = try r.u64be();
    if (r.remaining() != 0) return error.TrailingData;
    if (mtu < minimum_mtu or mtu > maximum_mtu) return error.InvalidMtu;
    return .{ .cookie = cookie, .server_address = address, .mtu = mtu, .client_guid = guid };
}

pub fn encodeUnconnectedPong(time: u64, server_guid: u64, advertisement: []const u8, output: []u8) ![]u8 {
    if (advertisement.len > 65_535) return error.AdvertisementTooLarge;
    var w: cursor.Writer = .{ .data = output };
    try w.byte(@intFromEnum(Id.unconnected_pong));
    try w.u64be(time);
    try w.u64be(server_guid);
    try w.bytes(&magic);
    try w.u16be(@intCast(advertisement.len));
    try w.bytes(advertisement);
    return w.written();
}

pub fn encodeOpenConnectionRequest1(protocol_version: u8, mtu: u16, output: []u8) ![]u8 {
    const target_size = @as(usize, mtu) -| 28;
    if (target_size < 18 or output.len < target_size) return error.InvalidMtu;
    var writer: cursor.Writer = .{ .data = output[0..target_size] };
    try writer.byte(@intFromEnum(Id.open_connection_request_1));
    try writer.bytes(&magic);
    try writer.byte(protocol_version);
    @memset(output[writer.offset..target_size], 0);
    return output[0..target_size];
}

pub const OpenConnectionReply1 = struct { server_guid: u64, cookie: ?u32, mtu: u16 };
pub fn decodeOpenConnectionReply1(data: []const u8, minimum_mtu: u16, maximum_mtu: u16) !OpenConnectionReply1 {
    var reader: cursor.Reader = .{ .data = data };
    if (try reader.byte() != @intFromEnum(Id.open_connection_reply_1)) return error.WrongPacket;
    try expectMagic(&reader);
    const guid = try reader.u64be();
    const security = try reader.byte();
    if (security > 1) return error.InvalidBoolean;
    const value = if (security != 0) try reader.u32be() else null;
    const mtu = try reader.u16be();
    if (mtu < minimum_mtu or mtu > maximum_mtu) return error.InvalidMtu;
    return .{ .server_guid = guid, .cookie = value, .mtu = mtu };
}

pub fn encodeOpenConnectionRequest2(server_address: Address, cookie: ?u32, mtu: u16, client_guid: u64, output: []u8) ![]u8 {
    var writer: cursor.Writer = .{ .data = output };
    try writer.byte(@intFromEnum(Id.open_connection_request_2));
    try writer.bytes(&magic);
    if (cookie) |value| {
        try writer.u32be(value);
        try writer.byte(0);
    }
    try encodeAddress(server_address, &writer);
    try writer.u16be(mtu);
    try writer.u64be(client_guid);
    return writer.written();
}

pub const OpenConnectionReply2 = struct { server_guid: u64, client_address: Address, mtu: u16 };
pub fn decodeOpenConnectionReply2(data: []const u8, minimum_mtu: u16, maximum_mtu: u16) !OpenConnectionReply2 {
    var reader: cursor.Reader = .{ .data = data };
    if (try reader.byte() != @intFromEnum(Id.open_connection_reply_2)) return error.WrongPacket;
    try expectMagic(&reader);
    const guid = try reader.u64be();
    const address = try decodeAddress(&reader);
    const mtu = try reader.u16be();
    if (try reader.byte() != 0) return error.UnsupportedSecurity;
    if (reader.remaining() != 0) return error.TrailingData;
    if (mtu < minimum_mtu or mtu > maximum_mtu) return error.InvalidMtu;
    return .{ .server_guid = guid, .client_address = address, .mtu = mtu };
}
pub fn encodeOpenConnectionReply1(server_guid: u64, cookie: ?u32, mtu: u16, pad_to_mtu: bool, output: []u8) ![]u8 {
    const base_size: usize = 1 + magic.len + 8 + 1 + (if (cookie != null) @as(usize, 4) else 0) + 2;
    const target_size = if (pad_to_mtu) @max(base_size, @as(usize, mtu) -| 28) else base_size;
    if (output.len < target_size) return error.NoSpaceLeft;
    var w: cursor.Writer = .{ .data = output[0..target_size] };
    try w.byte(@intFromEnum(Id.open_connection_reply_1));
    try w.bytes(&magic);
    try w.u64be(server_guid);
    try w.byte(@intFromBool(cookie != null));
    if (cookie) |value| try w.u32be(value);
    try w.u16be(mtu);
    @memset(output[w.offset..target_size], 0);
    return output[0..target_size];
}

pub fn encodeOpenConnectionReply2(server_guid: u64, client_address: Address, mtu: u16, output: []u8) ![]u8 {
    var w: cursor.Writer = .{ .data = output };
    try w.byte(@intFromEnum(Id.open_connection_reply_2));
    try w.bytes(&magic);
    try w.u64be(server_guid);
    try encodeAddress(client_address, &w);
    try w.u16be(mtu);
    try w.byte(0);
    return w.written();
}

pub fn encodeIncompatibleProtocol(version: u8, server_guid: u64, output: []u8) ![]u8 {
    var w: cursor.Writer = .{ .data = output };
    try w.byte(@intFromEnum(Id.incompatible_protocol_version));
    try w.byte(version);
    try w.bytes(&magic);
    try w.u64be(server_guid);
    return w.written();
}
pub fn encodeNoFreeIncomingConnections(server_guid: u64, output: []u8) ![]u8 {
    var w: cursor.Writer = .{ .data = output };
    try w.byte(@intFromEnum(Id.no_free_incoming_connections));
    try w.bytes(&magic);
    try w.u64be(server_guid);
    return w.written();
}
test "offline ping validates magic and exact structure" {
    var bytes: [64]u8 = undefined;
    var w: cursor.Writer = .{ .data = &bytes };
    try w.byte(1);
    try w.u64be(5);
    try w.bytes(&magic);
    try w.u64be(7);
    const ping = try decodeUnconnectedPing(w.written());
    try std.testing.expectEqual(@as(u64, 5), ping.time);
    bytes[9] ^= 1;
    try std.testing.expectError(error.InvalidMagic, decodeUnconnectedPing(w.written()));
}

test "addresses round trip and truncate safely" {
    const original: Address = .{ .ipv4 = .{ .octets = .{ 127, 0, 0, 1 }, .port = 19132 } };
    var bytes: [32]u8 = undefined;
    var w: cursor.Writer = .{ .data = &bytes };
    try encodeAddress(original, &w);
    var r: cursor.Reader = .{ .data = w.written() };
    try std.testing.expectEqual(original, try decodeAddress(&r));
    r = .{ .data = w.written()[0..3] };
    try std.testing.expectError(error.Truncated, decodeAddress(&r));
}

test "IPv6 addresses preserve flow, scope, port, and MTU" {
    const original: Address = .{ .ipv6 = .{
        .octets = .{ 0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 },
        .port = 19132,
        .flow = 7,
        .scope = 3,
    } };
    var bytes: [128]u8 = undefined;
    const wire = try encodeOpenConnectionRequest2(original, 0x12345678, 1200, 9, &bytes);
    const decoded = try decodeOpenConnectionRequest2(wire, true, 576, 1492);
    try std.testing.expectEqual(original, decoded.server_address);
    try std.testing.expectEqual(@as(u16, 1200), decoded.mtu);
}

test "no-free response has the canonical offline shape" {
    var bytes: [32]u8 = undefined;
    const wire = try encodeNoFreeIncomingConnections(0x0102030405060708, &bytes);
    try std.testing.expectEqual(@as(usize, 25), wire.len);
    try std.testing.expectEqual(@intFromEnum(Id.no_free_incoming_connections), wire[0]);
    try std.testing.expectEqualSlices(u8, &magic, wire[1..17]);
    try std.testing.expectEqual(@as(u64, 0x0102030405060708), std.mem.readInt(u64, wire[17..25], .big));
}
