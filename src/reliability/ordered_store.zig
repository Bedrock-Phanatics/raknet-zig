const std = @import("std");
const uint24 = @import("../util/uint24.zig");
const OwnedPayload = @import("../payload.zig").OwnedPayload;

/// Shared storage for every order channel, so many channels cannot multiply configured limits.
pub const Store = struct {
    allocator: std.mem.Allocator,
    packets: std.AutoHashMapUnmanaged(u32, []u8) = .empty,
    expected: []u32,
    maximum_entries: usize,
    maximum_bytes: usize,
    maximum_window: usize,
    total_bytes: usize = 0,

    pub fn init(allocator: std.mem.Allocator, channels: usize, maximum_entries: usize, maximum_bytes: usize, maximum_window: usize) !Store {
        if (channels == 0 or channels > 256 or maximum_entries == 0 or maximum_entries > std.math.maxInt(u32) or maximum_bytes == 0 or maximum_window == 0 or maximum_window >= uint24.half_range) return error.InvalidConfiguration;
        const expected = try allocator.alloc(u32, channels);
        @memset(expected, 0);
        errdefer allocator.free(expected);
        const self: Store = .{ .allocator = allocator, .expected = expected, .maximum_entries = maximum_entries, .maximum_bytes = maximum_bytes, .maximum_window = maximum_window };
        return self;
    }
    pub fn deinit(self: *Store) void {
        var iterator = self.packets.valueIterator();
        while (iterator.next()) |data| self.allocator.free(data.*);
        self.packets.deinit(self.allocator);
        self.allocator.free(self.expected);
        self.* = undefined;
    }
    pub fn expectedIndex(self: Store, channel: u8) !u32 {
        if (channel >= self.expected.len) return error.InvalidOrderChannel;
        return self.expected[channel];
    }
    pub fn advanceBorrowed(self: *Store, channel: u8, index: u32) !void {
        if (try self.expectedIndex(channel) != uint24.normalize(index)) return error.UnexpectedOrderIndex;
        self.expected[channel] = uint24.add(index, 1);
    }
    pub fn push(self: *Store, channel: u8, raw_index: u32, payload: []const u8) !bool {
        if (channel >= self.expected.len) return error.InvalidOrderChannel;
        const index = uint24.normalize(raw_index);
        const forward = uint24.distance(self.expected[channel], index);
        if (forward >= uint24.half_range) return false;
        if (forward >= self.maximum_window) return error.OrderWindowExceeded;
        const key = makeKey(channel, index);
        if (self.packets.contains(key)) return false;
        if (self.packets.count() >= self.maximum_entries) return error.OrderQueueFull;
        if (payload.len > self.maximum_bytes -| self.total_bytes) return error.OrderBytesExceeded;
        const copy = try self.allocator.dupe(u8, payload);
        errdefer self.allocator.free(copy);
        try self.packets.put(self.allocator, key, copy);
        self.total_bytes += copy.len;
        return true;
    }
    pub fn pop(self: *Store, channel: u8) !?OwnedPayload {
        if (channel >= self.expected.len) return error.InvalidOrderChannel;
        const removed = self.packets.fetchRemove(makeKey(channel, self.expected[channel])) orelse return null;
        self.expected[channel] = uint24.add(self.expected[channel], 1);
        self.total_bytes -= removed.value.len;
        return .{ .allocator = self.allocator, .bytes = removed.value };
    }
    fn makeKey(channel: u8, index: u32) u32 {
        return (@as(u32, channel) << 24) | uint24.normalize(index);
    }
};

test "global quotas span channels and in-order fast path advances" {
    var store = try Store.init(std.testing.allocator, 2, 2, 8, 8);
    defer store.deinit();
    try store.advanceBorrowed(0, 0);
    try std.testing.expectEqual(@as(u32, 1), try store.expectedIndex(0));
    try std.testing.expect(try store.push(0, 2, "a"));
    try std.testing.expect(try store.push(1, 1, "b"));
    try std.testing.expectError(error.OrderQueueFull, store.push(1, 2, "c"));
}
fn checkOrderedAllocationFailures(allocator: std.mem.Allocator) !void {
    var store = try Store.init(allocator, 2, 4, 64, 8);
    defer store.deinit();
    try std.testing.expect(try store.push(0, 1, "retained"));
    try std.testing.expectEqual(@as(usize, 1), store.packets.count());
    try std.testing.expectEqual(@as(usize, 8), store.total_bytes);
}

test "ordered retention handles every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkOrderedAllocationFailures, .{});
}
