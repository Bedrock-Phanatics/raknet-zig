const std = @import("std");

pub const Key = [23]u8;

pub const Entry = struct {
    key: Key,
    deadline_ms: u64,
    order: u64,
};

// Preallocation keeps these map pointers stable.
const HeapEntry = struct {
    deadline_ms: u64,
    order: u64,
    key_ptr: *Key,
    index_ptr: *usize,

    fn entry(self: HeapEntry) Entry {
        return .{ .key = self.key_ptr.*, .deadline_ms = self.deadline_ms, .order = self.order };
    }
};

pub const Queue = struct {
    allocator: std.mem.Allocator,
    items: []HeapEntry,
    len: usize = 0,
    next_order: u64 = 0,
    indices: std.AutoHashMapUnmanaged(Key, usize) = .empty,
    diagnostics: struct {
        upserts: u64 = 0,
        unchanged: u64 = 0,
        sifts: u64 = 0,
        swaps: u64 = 0,
    } = .{},

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !Queue {
        if (capacity == 0) return error.InvalidCapacity;
        const items = try allocator.alloc(HeapEntry, capacity);
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
        self.diagnostics.upserts += 1;
        if (self.indices.get(key)) |index| {
            const previous = self.items[index].deadline_ms;
            self.diagnostics.unchanged += @intFromBool(previous == deadline_ms);
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
        const inserted = self.indices.getOrPutAssumeCapacity(key);
        std.debug.assert(!inserted.found_existing);
        inserted.value_ptr.* = index;
        self.items[index] = .{ .deadline_ms = deadline_ms, .order = self.takeOrder(), .key_ptr = inserted.key_ptr, .index_ptr = inserted.value_ptr };
        self.siftUp(index);
    }

    pub fn remove(self: *Queue, key: Key) bool {
        const index = self.indices.get(key) orelse return false;
        self.removeIndex(index);
        return true;
    }

    pub fn peek(self: Queue) ?Entry {
        return if (self.len == 0) null else self.items[0].entry();
    }

    pub fn popDue(self: *Queue, now_ms: u64) ?Entry {
        if (self.len == 0 or self.items[0].deadline_ms > now_ms) return null;
        const removed = self.items[0].entry();
        self.removeIndex(0);
        return removed;
    }

    fn removeIndex(self: *Queue, index: usize) void {
        self.indices.removeByPtr(self.items[index].key_ptr);
        self.len -= 1;
        if (index == self.len) return;

        self.items[index] = self.items[self.len];
        self.items[index].index_ptr.* = index;
        if (index != 0 and less(self.items[index], self.items[(index - 1) / 2])) {
            self.siftUp(index);
        } else {
            self.siftDown(index);
        }
    }

    fn siftUp(self: *Queue, start: usize) void {
        self.diagnostics.sifts += 1;
        var index = start;
        while (index != 0) {
            const parent = (index - 1) / 2;
            if (!less(self.items[index], self.items[parent])) break;
            self.swap(index, parent);
            index = parent;
        }
    }

    fn siftDown(self: *Queue, start: usize) void {
        self.diagnostics.sifts += 1;
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
        self.diagnostics.swaps += 1;
        std.mem.swap(HeapEntry, &self.items[a], &self.items[b]);
        self.items[a].index_ptr.* = a;
        self.items[b].index_ptr.* = b;
    }

    fn less(a: HeapEntry, b: HeapEntry) bool {
        if (a.deadline_ms != b.deadline_ms) return a.deadline_ms < b.deadline_ms;
        return a.order < b.order;
    }

    fn takeOrder(self: *Queue) u64 {
        const order = self.next_order;
        self.next_order +%= 1;
        return order;
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

test "rescheduling never accumulates stale entries" {
    const capacity = 64;
    var queue = try Queue.init(std.testing.allocator, capacity);
    defer queue.deinit();

    var keys: [capacity]Key = @splat(@splat(0));
    for (&keys, 0..) |*key, index| {
        key[0] = @intCast(index);
        try queue.upsert(key.*, index);
    }

    for (0..100_000) |iteration| {
        const index = iteration & (capacity - 1);
        const deadline = (iteration *% 2_654_435_761) % 100_003;
        try queue.upsert(keys[index], deadline);
    }
    try std.testing.expectEqual(@as(usize, capacity), queue.count());
    try std.testing.expectEqual(@as(u32, capacity), queue.indices.count());

    var seen: [capacity]bool = @splat(false);
    var previous: u64 = 0;
    while (queue.popDue(std.math.maxInt(u64))) |entry| {
        try std.testing.expect(entry.deadline_ms >= previous);
        previous = entry.deadline_ms;
        const index = entry.key[0];
        try std.testing.expect(!seen[index]);
        seen[index] = true;
    }
    try std.testing.expectEqual(@as(usize, 0), queue.count());
    try std.testing.expectEqual(@as(u32, 0), queue.indices.count());
    for (seen) |present| try std.testing.expect(present);
}

test "equal due deadlines rotate after rescheduling" {
    var queue = try Queue.init(std.testing.allocator, 3);
    defer queue.deinit();

    var first: Key = @splat(0);
    var second: Key = @splat(0);
    var third: Key = @splat(0);
    first[0] = 1;
    second[0] = 2;
    third[0] = 3;
    try queue.upsert(first, 10);
    try queue.upsert(second, 10);
    try queue.upsert(third, 10);

    const popped = queue.popDue(10).?;
    try std.testing.expectEqual(first, popped.key);
    try queue.upsert(popped.key, popped.deadline_ms);
    try std.testing.expectEqual(second, queue.popDue(10).?.key);
    try std.testing.expectEqual(third, queue.popDue(10).?.key);
    try std.testing.expectEqual(first, queue.popDue(10).?.key);
}

test "heap index pointers survive full capacity churn and table slot reuse" {
    const capacity = 4096;
    var queue = try Queue.init(std.testing.allocator, capacity);
    defer queue.deinit();
    for (0..capacity) |index| {
        var key: Key = @splat(0);
        std.mem.writeInt(u64, key[0..8], index, .little);
        try queue.upsert(key, index);
    }
    for (0..20_000) |iteration| {
        const popped = queue.popDue(std.math.maxInt(u64)).?;
        var key: Key = @splat(0);
        std.mem.writeInt(u64, key[0..8], capacity + iteration, .little);
        try queue.upsert(key, iteration *% 7919 % 10_007);
        try std.testing.expect(!queue.remove(popped.key));
        if (iteration % 256 == 0) for (queue.items[0..queue.len], 0..) |item, index| {
            try std.testing.expectEqual(index, item.index_ptr.*);
            try std.testing.expectEqual(index, queue.indices.get(item.key_ptr.*).?);
        };
    }
    var previous: u64 = 0;
    while (queue.popDue(std.math.maxInt(u64))) |entry| {
        try std.testing.expect(entry.deadline_ms >= previous);
        previous = entry.deadline_ms;
    }
    try std.testing.expectEqual(@as(u32, 0), queue.indices.count());
}
