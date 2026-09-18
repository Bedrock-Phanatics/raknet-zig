const std = @import("std");

const BorrowedPayload = @import("../payload.zig").BorrowedPayload;
pub const OwnedPayload = @import("../payload.zig").OwnedPayload;
const uint24 = @import("../util/uint24.zig");

pub const OrderedQueue = struct {
    allocator: std.mem.Allocator,
    packets: std.AutoHashMapUnmanaged(u32, []u8) = .empty,
    expected: u32,
    maximum_entries: usize,
    maximum_bytes: usize,
    maximum_window: usize,
    total_bytes: usize = 0,

    pub fn init(allocator: std.mem.Allocator, expected: u32, maximum_entries: usize, maximum_bytes: usize, maximum_window: usize) !OrderedQueue {
        if (maximum_entries == 0 or
            maximum_bytes == 0 or
            maximum_window == 0 or
            maximum_window >= uint24.half_range or
            maximum_entries > std.math.maxInt(u32)) return error.InvalidConfiguration;
        return .{
            .allocator = allocator,
            .expected = uint24.normalize(expected),
            .maximum_entries = maximum_entries,
            .maximum_bytes = maximum_bytes,
            .maximum_window = maximum_window,
        };
    }
    pub fn deinit(self: *OrderedQueue) void {
        var iterator = self.packets.valueIterator();
        while (iterator.next()) |data| self.allocator.free(data.*);
        self.packets.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn push(self: *OrderedQueue, raw_index: u32, payload: []const u8) !bool {
        const index = uint24.normalize(raw_index);
        const forward = uint24.distance(self.expected, index);
        if (forward >= uint24.half_range) return false;
        if (forward >= self.maximum_window) return error.OrderWindowExceeded;
        if (self.packets.contains(index)) return false;
        if (self.packets.count() >= self.maximum_entries) return error.OrderQueueFull;
        if (payload.len > self.maximum_bytes -| self.total_bytes) return error.OrderBytesExceeded;
        const copy = try BorrowedPayload.init(payload).toOwned(self.allocator);
        errdefer copy.deinit();
        try self.packets.put(self.allocator, index, copy.bytes);
        self.total_bytes += copy.bytes.len;
        return true;
    }

    /// Transfers ownership of the next contiguous packet to the caller.
    pub fn pop(self: *OrderedQueue) ?OwnedPayload {
        const removed = self.packets.fetchRemove(self.expected) orelse return null;
        self.expected = uint24.add(self.expected, 1);
        self.total_bytes -= removed.value.len;
        return .{ .allocator = self.allocator, .bytes = removed.value };
    }
};

pub const Sequenced = struct {
    latest: u32 = 0,
    initialized: bool = false,
    pub fn accept(self: *Sequenced, raw_index: u32) bool {
        const index = uint24.normalize(raw_index);
        if (!self.initialized or uint24.isNewer(index, self.latest)) {
            self.latest = index;
            self.initialized = true;
            return true;
        }
        return false;
    }
};

test "ordered delivery wraps, deduplicates, and transfers ownership" {
    var queue = try OrderedQueue.init(std.testing.allocator, 0xffffff, 3, 16, 8);
    defer queue.deinit();
    try std.testing.expect(try queue.push(0, "zero"));
    try std.testing.expect(!(try queue.push(0, "replacement")));
    try std.testing.expect(try queue.push(0xffffff, "last"));
    var packet = queue.pop().?;
    try std.testing.expectEqualStrings("last", packet.bytes);
    packet.deinit();
    packet = queue.pop().?;
    try std.testing.expectEqualStrings("zero", packet.bytes);
    packet.deinit();
    try std.testing.expect(queue.pop() == null);
    try std.testing.expectError(error.OrderWindowExceeded, queue.push(100, "far"));
}

test "sequenced packets use modular ordering" {
    var sequence: Sequenced = .{};
    try std.testing.expect(sequence.accept(0xffffff));
    try std.testing.expect(sequence.accept(0));
    try std.testing.expect(!sequence.accept(0xffffff));
    try std.testing.expect(!sequence.accept(0));
}
