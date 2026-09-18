const std = @import("std");
const cursor = @import("cursor.zig");
const offline = @import("offline.zig");

pub const ConnectionRequest = struct { client_guid: u64, request_time: u64, secure: bool };
pub const ConnectedPong = struct { ping_time: u64, pong_time: u64 };
pub const AddressList = struct {
    primary: offline.Address,
    system_index: ?u16,
    system_addresses: [20]offline.Address,
    system_address_count: usize,
    ping_time: u64,
    pong_time: u64,
};
pub const Message = union(enum) {
    connected_ping: u64,
    connected_pong: ConnectedPong,
    detect_lost_connections,
    connection_request: ConnectionRequest,
    connection_request_accepted: AddressList,
    new_incoming_connection: AddressList,
    disconnect,
    user: []const u8,
};

pub fn decode(data: []const u8) !Message {
    var reader: cursor.Reader = .{ .data = data };
    const id = try reader.byte();
    return switch (id) {
        @intFromEnum(offline.Id.connected_ping) => .{ .connected_ping = try exactU64(&reader) },
        @intFromEnum(offline.Id.connected_pong) => blk: {
            const ping = try reader.u64be();
            const pong = try reader.u64be();
            try exactEnd(&reader);
            break :blk .{ .connected_pong = .{ .ping_time = ping, .pong_time = pong } };
        },
        @intFromEnum(offline.Id.detect_lost_connections) => blk: {
            try exactEnd(&reader);
            break :blk .detect_lost_connections;
        },
        @intFromEnum(offline.Id.connection_request) => blk: {
            const guid = try reader.u64be();
            const time = try reader.u64be();
            const secure_byte = try reader.byte();
            if (secure_byte > 1) return error.InvalidBoolean;
            try exactEnd(&reader);
            break :blk .{ .connection_request = .{ .client_guid = guid, .request_time = time, .secure = secure_byte != 0 } };
        },
        @intFromEnum(offline.Id.connection_request_accepted) => .{ .connection_request_accepted = try decodeAddressList(&reader, true) },
        @intFromEnum(offline.Id.new_incoming_connection) => .{ .new_incoming_connection = try decodeAddressList(&reader, false) },
        @intFromEnum(offline.Id.disconnect_notification) => blk: {
            try exactEnd(&reader);
            break :blk .disconnect;
        },
        else => .{ .user = data },
    };
}

fn exactU64(reader: *cursor.Reader) !u64 {
    const value = try reader.u64be();
    try exactEnd(reader);
    return value;
}
fn exactEnd(reader: *cursor.Reader) !void {
    if (reader.remaining() != 0) return error.TrailingData;
}

fn decodeAddressList(reader: *cursor.Reader, has_system_index: bool) !AddressList {
    var result: AddressList = .{
        .primary = try offline.decodeAddress(reader),
        .system_index = if (has_system_index) try reader.u16be() else null,
        .system_addresses = undefined,
        .system_address_count = 0,
        .ping_time = 0,
        .pong_time = 0,
    };
    while (reader.remaining() > 16 and result.system_address_count < result.system_addresses.len) {
        result.system_addresses[result.system_address_count] = try offline.decodeAddress(reader);
        result.system_address_count += 1;
    }
    if (reader.remaining() != 16) return error.InvalidSystemAddresses;
    result.ping_time = try reader.u64be();
    result.pong_time = try reader.u64be();
    return result;
}

pub fn encodePing(time: u64, output: []u8) ![]u8 {
    var writer: cursor.Writer = .{ .data = output };
    try writer.byte(@intFromEnum(offline.Id.connected_ping));
    try writer.u64be(time);
    return writer.written();
}
pub fn encodePong(ping_time: u64, pong_time: u64, output: []u8) ![]u8 {
    var writer: cursor.Writer = .{ .data = output };
    try writer.byte(@intFromEnum(offline.Id.connected_pong));
    try writer.u64be(ping_time);
    try writer.u64be(pong_time);
    return writer.written();
}
pub fn encodeConnectionRequest(client_guid: u64, request_time: u64, output: []u8) ![]u8 {
    var writer: cursor.Writer = .{ .data = output };
    try writer.byte(@intFromEnum(offline.Id.connection_request));
    try writer.u64be(client_guid);
    try writer.u64be(request_time);
    try writer.byte(0);
    return writer.written();
}
pub fn encodeAddressList(
    id: enum { accepted, incoming },
    primary: offline.Address,
    system_index: u16,
    addresses: []const offline.Address,
    ping_time: u64,
    pong_time: u64,
    output: []u8,
) ![]u8 {
    if (addresses.len > 20) return error.TooManySystemAddresses;
    var writer: cursor.Writer = .{ .data = output };
    try writer.byte(if (id == .accepted) @intFromEnum(offline.Id.connection_request_accepted) else @intFromEnum(offline.Id.new_incoming_connection));
    try offline.encodeAddress(primary, &writer);
    if (id == .accepted) try writer.u16be(system_index);
    for (addresses) |address| try offline.encodeAddress(address, &writer);
    try writer.u64be(ping_time);
    try writer.u64be(pong_time);
    return writer.written();
}

test "connected control messages round trip and reject every truncation" {
    var bytes: [64]u8 = undefined;
    const wire = try encodePong(3, 7, &bytes);
    try std.testing.expectEqual(@as(u64, 3), (try decode(wire)).connected_pong.ping_time);
    for (0..wire.len) |end| try std.testing.expectError(error.Truncated, decode(wire[0..end]));
}

test "variable system-address lists preserve timestamps" {
    const address: offline.Address = .{ .ipv4 = .{ .octets = .{ 127, 0, 0, 1 }, .port = 19132 } };
    var bytes: [256]u8 = undefined;
    const wire = try encodeAddressList(.accepted, address, 0, &.{address}, 11, 12, &bytes);
    const value = (try decode(wire)).connection_request_accepted;
    try std.testing.expectEqual(@as(usize, 1), value.system_address_count);
    try std.testing.expectEqual(@as(u64, 12), value.pong_time);
}
