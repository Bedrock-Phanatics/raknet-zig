const std = @import("std");

pub const BorrowedPayload = struct {
    bytes: []const u8,

    pub inline fn init(bytes: []const u8) BorrowedPayload {
        return .{ .bytes = bytes };
    }
};

pub const OwnedPayload = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,

    pub inline fn borrowed(self: OwnedPayload) BorrowedPayload {
        return .init(self.bytes);
    }

    pub fn deinit(self: OwnedPayload) void {
        self.allocator.free(self.bytes);
    }
};

test "payload wrappers preserve slice storage" {
    try std.testing.expectEqual(@sizeOf([]const u8), @sizeOf(BorrowedPayload));
}
