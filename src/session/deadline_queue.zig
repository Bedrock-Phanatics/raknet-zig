const std = @import("std");

pub const Key = [23]u8;

pub const Entry = struct {
    key: Key,
    deadline_ms: u64,
};

pub const Queue = struct {
    allocator: std.mem.Allocator,
    items: []Entry,
    len: usize = 0,
    indices: std.AutoHashMapUnmanaged(Key, usize) = .empty,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !Queue {
        if (capacity == 0) return error.InvalidCapacity;
        const items = try allocator.alloc(Entry, capacity);
        errdefer allocator.free(items);
        var indices: std.AutoHashMapUnmanaged(Key, usize) = .empty;
        errdefer indices.deinit(allocator);
        try indices.ensureTotalCapacity(allocator, @intCast(capacity));
        return .{ .allocator = allocator, .items = items, .indices = indices };
    }

    pub fn deinit(self: *Queue) void {
        self.indices.deinit(self.allocator);
        self.allocator.free(self.items);
        self.* = undefined;
    }

    pub fn count(self: Queue) usize {
        return self.len;
    }

    pub fn upsert(self: *Queue, key: Key, deadline_ms: u64) !void {
        if (self.indices.get(key)) |index| {
            const previous = self.items[index].deadline_ms;
            self.items[index].deadline_ms = deadline_ms;
            if (deadline_ms < previous) {
                self.siftUp(index);
            } else if (deadline_ms > previous) {
                self.siftDown(index);
            }
            return;
        }
        if (self.len == self.items.len) return error.DeadlineQueueFull;
        const index = self.len;
        self.len += 1;
        self.items[index] = .{ .key = key, .deadline_ms = deadline_ms };
        self.indices.putAssumeCapacityNoClobber(key, index);
        self.siftUp(index);
    }

    pub fn remove(self: *Queue, key: Key) bool {
        const index = self.indices.get(key) orelse return false;
        _ = self.indices.remove(key);
        _ = self.removeIndex(index);
        return true;
    }

    pub fn peek(self: Queue) ?Entry {
        return if (self.len == 0) null else self.items[0];
    }

    pub fn popDue(self: *Queue, now_ms: u64) ?Entry {
        const entry = self.peek() orelse return null;
        if (entry.deadline_ms > now_ms) return null;
        _ = self.indices.remove(entry.key);
        return self.removeIndex(0);
    }

    fn removeIndex(self: *Queue, index: usize) Entry {
        const removed = self.items[index];
        self.len -= 1;
        if (index == self.len) return removed;

        self.items[index] = self.items[self.len];
        self.indices.getPtr(self.items[index].key).?.* = index;
        if (index != 0 and less(self.items[index], self.items[(index - 1) / 2])) {
            self.siftUp(index);
        } else {
            self.siftDown(index);
        }
        return removed;
    }

    fn siftUp(self: *Queue, start: usize) void {
        var index = start;
        while (index != 0) {
            const parent = (index - 1) / 2;
            if (!less(self.items[index], self.items[parent])) break;
            self.swap(index, parent);
            index = parent;
        }
    }

    fn siftDown(self: *Queue, start: usize) void {
        var index = start;
        while (true) {
            const left = index * 2 + 1;
            if (left >= self.len) return;
            const right = left + 1;
            const child = if (right < self.len and less(self.items[right], self.items[left])) right else left;
            if (!less(self.items[child], self.items[index])) return;
            self.swap(index, child);
            index = child;
        }
    }

    fn swap(self: *Queue, a: usize, b: usize) void {
        std.mem.swap(Entry, &self.items[a], &self.items[b]);
        self.indices.getPtr(self.items[a].key).?.* = a;
        self.indices.getPtr(self.items[b].key).?.* = b;
    }

    fn less(a: Entry, b: Entry) bool {
        if (a.deadline_ms != b.deadline_ms) return a.deadline_ms < b.deadline_ms;
        return std.mem.order(u8, &a.key, &b.key) == .lt;
    }
};

test "indexed deadlines update and remove without stale entries" {
    var queue = try Queue.init(std.testing.allocator, 3);
    defer queue.deinit();

    var first: Key = @splat(0);
    var second: Key = @splat(0);
    var third: Key = @splat(0);
    first[0] = 1;
    second[0] = 2;
    third[0] = 3;

    try queue.upsert(first, 100);
    try queue.upsert(second, 50);
    try queue.upsert(third, 75);
    try std.testing.expectEqual(second, queue.peek().?.key);

    var fourth: Key = @splat(0);
    fourth[0] = 4;
    try std.testing.expectError(error.DeadlineQueueFull, queue.upsert(fourth, 1));

    try queue.upsert(first, 25);
    try std.testing.expectEqual(@as(usize, 3), queue.count());
    try std.testing.expectEqual(first, queue.peek().?.key);
    try std.testing.expect(queue.remove(first));
    try std.testing.expect(!queue.remove(first));

    try queue.upsert(second, 200);
    try std.testing.expectEqual(third, queue.peek().?.key);
    try queue.upsert(second, 50);
    try std.testing.expectEqual(second, queue.popDue(60).?.key);
    try std.testing.expect(queue.popDue(60) == null);
    try std.testing.expectEqual(third, queue.popDue(100).?.key);
    try std.testing.expectEqual(@as(usize, 0), queue.count());
}
