const std = @import("std");
const ack = @import("ack.zig");
const cursor = @import("cursor.zig");
const frame = @import("frame.zig");

pub const Kind = enum { data, ack, nack };
pub const Packet = union(Kind) {
    data: frame.Datagram,
    ack: ack.Decoded,
    nack: ack.Decoded,
};

pub fn decode(data: []const u8, records: []ack.Record, maximum_records: usize, maximum_acknowledged: usize) !Packet {
    if (data.len == 0) return error.Truncated;
    const flags = data[0];
    if (flags & 0x80 == 0) return error.NotDatagram;
    const is_ack = flags & 0x40 != 0;
    const is_nack = flags & 0x20 != 0;
    if (is_ack and is_nack) return error.InvalidDatagramFlags;
    if (is_ack or is_nack) {
        if (flags & 0x1f != 0) return error.InvalidDatagramFlags;
        const decoded = try ack.decode(data[1..], records, maximum_records, maximum_acknowledged);
        return if (is_ack) .{ .ack = decoded } else .{ .nack = decoded };
    }
    return .{ .data = try frame.decodeDatagram(data) };
}

pub fn encodeControl(kind: enum { ack, nack }, records: []const ack.Record, output: []u8) ![]u8 {
    if (output.len == 0) return error.NoSpaceLeft;
    output[0] = if (kind == .ack) 0xc0 else 0xa0;
    const payload = try ack.encode(records, output[1..]);
    return output[0 .. payload.len + 1];
}

pub fn encodeData(sequence: u32, frames: []const frame.Frame, output: []u8) ![]u8 {
    var writer: cursor.Writer = .{ .data = output };
    try writer.byte(0x84);
    try writer.u24le(sequence);
    if (frames.len == 0) return error.EmptyDatagram;
    for (frames) |value| try frame.encode(value, &writer);
    return writer.written();
}

test "outer control codec rejects ambiguous flags and bounded ranges" {
    var output: [64]u8 = undefined;
    const source = [_]ack.Record{.{ .first = 5, .last = 8 }};
    const wire = try encodeControl(.nack, &source, &output);
    var storage: [2]ack.Record = undefined;
    const decoded = try decode(wire, &storage, 2, 8);
    try std.testing.expectEqual(@as(usize, 4), decoded.nack.acknowledged_count);
    output[0] = 0xe0;
    try std.testing.expectError(error.InvalidDatagramFlags, decode(wire, &storage, 2, 8));
}

test "data datagram round trips borrowed frames" {
    const frames = [_]frame.Frame{.{ .reliability = .unreliable, .payload = "x" }};
    var output: [64]u8 = undefined;
    const wire = try encodeData(0xffffff, &frames, &output);
    var storage: [1]ack.Record = undefined;
    var decoded = (try decode(wire, &storage, 1, 1)).data;
    try std.testing.expectEqual(@as(u32, 0xffffff), decoded.sequence);
    try std.testing.expectEqualStrings("x", (try frame.decodeOne(&decoded.frames, 16, 4)).payload);
}
