const std = @import("std");

pub const Reader = struct {
    data: []const u8,
    offset: usize = 0,

    pub fn remaining(self: Reader) usize {
        return self.data.len - self.offset;
    }
    pub fn take(self: *Reader, n: usize) ![]const u8 {
        if (n > self.remaining()) return error.Truncated;
        const result = self.data[self.offset..][0..n];
        self.offset += n;
        return result;
    }
    pub fn byte(self: *Reader) !u8 {
        return (try self.take(1))[0];
    }
    pub fn u16be(self: *Reader) !u16 {
        var a: [2]u8 = undefined;
        @memcpy(&a, try self.take(2));
        return std.mem.readInt(u16, &a, .big);
    }
    pub fn u16le(self: *Reader) !u16 {
        var a: [2]u8 = undefined;
        @memcpy(&a, try self.take(2));
        return std.mem.readInt(u16, &a, .little);
    }
    pub fn u24le(self: *Reader) !u32 {
        const b = try self.take(3);
        return @as(u32, b[0]) | (@as(u32, b[1]) << 8) | (@as(u32, b[2]) << 16);
    }
    pub fn u32be(self: *Reader) !u32 {
        var a: [4]u8 = undefined;
        @memcpy(&a, try self.take(4));
        return std.mem.readInt(u32, &a, .big);
    }
    pub fn u64be(self: *Reader) !u64 {
        var a: [8]u8 = undefined;
        @memcpy(&a, try self.take(8));
        return std.mem.readInt(u64, &a, .big);
    }
};

pub const Writer = struct {
    data: []u8,
    offset: usize = 0,

    pub fn remaining(self: Writer) usize {
        return self.data.len - self.offset;
    }
    pub fn reserve(self: *Writer, n: usize) ![]u8 {
        if (n > self.remaining()) return error.NoSpaceLeft;
        const result = self.data[self.offset..][0..n];
        self.offset += n;
        return result;
    }
    pub fn byte(self: *Writer, value: u8) !void {
        (try self.reserve(1))[0] = value;
    }
    pub fn bytes(self: *Writer, value: []const u8) !void {
        @memcpy(try self.reserve(value.len), value);
    }
    pub fn u16be(self: *Writer, value: u16) !void {
        var a: [2]u8 = undefined;
        std.mem.writeInt(u16, &a, value, .big);
        try self.bytes(&a);
    }
    pub fn u16le(self: *Writer, value: u16) !void {
        var a: [2]u8 = undefined;
        std.mem.writeInt(u16, &a, value, .little);
        try self.bytes(&a);
    }
    pub fn u24le(self: *Writer, value: u32) !void {
        if (value > 0xffffff) return error.IntegerOutOfRange;
        const b = try self.reserve(3);
        b[0] = @truncate(value);
        b[1] = @truncate(value >> 8);
        b[2] = @truncate(value >> 16);
    }
    pub fn u32be(self: *Writer, value: u32) !void {
        var a: [4]u8 = undefined;
        std.mem.writeInt(u32, &a, value, .big);
        try self.bytes(&a);
    }
    pub fn u64be(self: *Writer, value: u64) !void {
        var a: [8]u8 = undefined;
        std.mem.writeInt(u64, &a, value, .big);
        try self.bytes(&a);
    }
    pub fn written(self: Writer) []u8 {
        return self.data[0..self.offset];
    }
};

test "cursor never advances on exhaustion" {
    var r: Reader = .{ .data = &.{1} };
    try std.testing.expectError(error.Truncated, r.take(2));
    try std.testing.expectEqual(@as(usize, 0), r.offset);
    var out: [1]u8 = undefined;
    var w: Writer = .{ .data = &out };
    try std.testing.expectError(error.NoSpaceLeft, w.reserve(2));
    try std.testing.expectEqual(@as(usize, 0), w.offset);
}
