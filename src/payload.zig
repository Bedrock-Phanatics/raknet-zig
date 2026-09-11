const std = @import("std");

/// A view that expires when its synchronous callback returns.
pub const BorrowedPayload = struct {
    bytes: []const u8,

    pub inline fn init(bytes: []const u8) BorrowedPayload {
        return .{ .bytes = bytes };
    }

    /// Copies exactly bytes.len bytes.
    pub fn toOwned(self: BorrowedPayload, allocator: std.mem.Allocator) !OwnedPayload {
        return .{ .allocator = allocator, .bytes = try allocator.dupe(u8, self.bytes) };
    }
};

/// Allocator-backed bytes. Call deinit exactly once.
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

test "owned copies allocate exactly the payload length" {
    const QuotaAllocator = @import("util/quota_allocator.zig").QuotaAllocator;
    const bytes = "exact";

    var quota = QuotaAllocator.init(std.testing.allocator, bytes.len);
    const owned = try BorrowedPayload.init(bytes).toOwned(quota.allocator());
    try std.testing.expectEqual(bytes.len, quota.used_bytes);
    try std.testing.expectEqualStrings(bytes, owned.bytes);
    try std.testing.expect(@intFromPtr(owned.bytes.ptr) != @intFromPtr(bytes.ptr));
    owned.deinit();
    try std.testing.expectEqual(@as(usize, 0), quota.used_bytes);

    var short = QuotaAllocator.init(std.testing.allocator, bytes.len - 1);
    try std.testing.expectError(error.OutOfMemory, BorrowedPayload.init(bytes).toOwned(short.allocator()));
}
