const std = @import("std");

const cursor = @import("cursor.zig");

pub const Reliability = enum(u3) {
    unreliable = 0,
    unreliable_sequenced = 1,
    reliable = 2,
    reliable_ordered = 3,
    reliable_sequenced = 4,
    unreliable_with_ack_receipt = 5,
    reliable_with_ack_receipt = 6,
    reliable_ordered_with_ack_receipt = 7,

    pub fn hasReliableIndex(self: Reliability) bool {
        return switch (self) {
            .reliable, .reliable_ordered, .reliable_sequenced, .reliable_with_ack_receipt, .reliable_ordered_with_ack_receipt => true,
            else => false,
        };
    }
    pub fn hasSequenceIndex(self: Reliability) bool {
        return self == .unreliable_sequenced or self == .reliable_sequenced;
    }
    pub fn hasOrderIndex(self: Reliability) bool {
        return self == .unreliable_sequenced or self == .reliable_ordered or self == .reliable_sequenced or self == .reliable_ordered_with_ack_receipt;
    }
    pub fn supportedForSend(self: Reliability) bool {
        return switch (self) {
            .unreliable, .unreliable_sequenced, .reliable, .reliable_ordered, .reliable_sequenced => true,
            .unreliable_with_ack_receipt, .reliable_with_ack_receipt, .reliable_ordered_with_ack_receipt => false,
        };
    }
};

pub const Split = struct { count: u32, id: u16, index: u32 };

pub const Frame = struct {
    reliability: Reliability,
    reliable_index: ?u32 = null,
    sequence_index: ?u32 = null,
    order_index: ?u32 = null,
    order_channel: ?u8 = null,
    split: ?Split = null,
    payload: []const u8,
};

pub fn decodeOne(reader: *cursor.Reader, maximum_payload: usize, maximum_split_parts: usize) !Frame {
    const flags = try reader.byte();
    const reliability: Reliability = @enumFromInt(@as(u3, @truncate(flags >> 5)));
    const split_flag = flags & 0x10 != 0;
    if (flags & 0x0f != 0) return error.InvalidFrameFlags;
    const bit_length = try reader.u16be();
    const payload_len: usize = (@as(usize, bit_length) + 7) / 8;
    if (payload_len > maximum_payload) return error.PayloadTooLarge;

    var result: Frame = .{ .reliability = reliability, .payload = undefined };
    if (reliability.hasReliableIndex()) result.reliable_index = try reader.u24le();
    if (reliability.hasSequenceIndex()) result.sequence_index = try reader.u24le();
    if (reliability.hasOrderIndex()) {
        result.order_index = try reader.u24le();
        result.order_channel = try reader.byte();
    }
    if (split_flag) {
        const split: Split = .{ .count = try reader.u32be(), .id = try reader.u16be(), .index = try reader.u32be() };
        if (split.count < 2 or split.count > maximum_split_parts or split.index >= split.count) return error.InvalidSplit;
        result.split = split;
    }
    result.payload = try reader.take(payload_len);
    return result;
}

pub fn encodedSize(value: Frame) !usize {
    if (value.payload.len == 0 or value.payload.len > 8191) return error.InvalidPayloadSize;
    var size: usize = 3 + value.payload.len;
    if (value.reliability.hasReliableIndex()) size += 3;
    if (value.reliability.hasSequenceIndex()) size += 3;
    if (value.reliability.hasOrderIndex()) size += 4;
    if (value.split != null) size += 10;
    return size;
}

pub fn encode(value: Frame, writer: *cursor.Writer) !void {
    _ = try encodedSize(value);
    if (value.reliability.hasReliableIndex() != (value.reliable_index != null)) return error.MissingIndex;
    if (value.reliability.hasSequenceIndex() != (value.sequence_index != null)) return error.MissingIndex;
    if (value.reliability.hasOrderIndex() != (value.order_index != null and value.order_channel != null)) return error.MissingIndex;
    var flags: u8 = @as(u8, @intFromEnum(value.reliability)) << 5;
    if (value.split != null) flags |= 0x10;
    try writer.byte(flags);
    const bits = try std.math.mul(usize, value.payload.len, 8);
    try writer.u16be(@intCast(bits));
    if (value.reliable_index) |index| try writer.u24le(index);
    if (value.sequence_index) |index| try writer.u24le(index);
    if (value.order_index) |index| try writer.u24le(index);
    if (value.order_channel) |channel| try writer.byte(channel);
    if (value.split) |split| {
        if (split.count < 2 or split.index >= split.count) return error.InvalidSplit;
        try writer.u32be(split.count);
        try writer.u16be(split.id);
        try writer.u32be(split.index);
    }
    try writer.bytes(value.payload);
}

pub const Datagram = struct {
    sequence: u32,
    frames: cursor.Reader,
};

pub fn decodeDatagram(data: []const u8) !Datagram {
    var reader: cursor.Reader = .{ .data = data };
    const flags = try reader.byte();
    if (flags & 0x80 == 0 or flags & 0x60 != 0) return error.InvalidDatagramFlags;
    const sequence = try reader.u24le();
    if (reader.remaining() == 0) return error.EmptyDatagram;
    return .{ .sequence = sequence, .frames = reader };
}

test "borrowed reliable ordered frame round trip" {
    const original: Frame = .{ .reliability = .reliable_ordered, .reliable_index = 7, .order_index = 9, .order_channel = 2, .payload = "hello" };
    var bytes: [64]u8 = undefined;
    var writer: cursor.Writer = .{ .data = &bytes };
    try encode(original, &writer);
    var reader: cursor.Reader = .{ .data = writer.written() };
    const decoded = try decodeOne(&reader, 1024, 128);
    try std.testing.expectEqual(original.reliability, decoded.reliability);
    try std.testing.expectEqualStrings("hello", decoded.payload);
    try std.testing.expectEqual(@as(?u8, 2), decoded.order_channel);
}

test "split and truncation validation precede payload use" {
    var reader: cursor.Reader = .{ .data = &.{ 0x30, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } };
    try std.testing.expectError(error.Truncated, decodeOne(&reader, 10, 128));
    reader = .{ .data = &.{ 0x00, 0xff, 0xff } };
    try std.testing.expectError(error.PayloadTooLarge, decodeOne(&reader, 100, 128));
}
