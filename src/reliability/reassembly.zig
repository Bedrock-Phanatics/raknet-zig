const std = @import("std");

const OwnedPayload = @import("../payload.zig").OwnedPayload;
const time = @import("../util/time.zig");

const none = std.math.maxInt(u32);
const class_sizes = [_]usize{ 64, 256, 576, 1200 };

const Fragment = struct {
    data: ?[]u8 = null,
    storage: []u8 = &.{},
};

const Assembly = struct {
    active: bool = false,
    id: u16 = 0,
    count: u32 = 0,
    received: u32 = 0,
    bytes: usize = 0,
    deadline_ms: u64 = 0,
    heap_index: u32 = none,
};

pub const ScatterPayload = struct {
    owner: *const Reassembler,
    slot: usize,
    fragment_count: usize,
    total_bytes: usize,

    pub fn count(self: ScatterPayload) usize {
        return self.fragment_count;
    }

    pub fn get(self: ScatterPayload, index: usize) []const u8 {
        return self.owner.fragmentSlice(self.slot)[index].data.?;
    }
};

pub const ScatterFn = *const fn (*anyopaque, ScatterPayload) error{ApplicationFailure}!void;

pub const Limits = struct {
    maximum_parts: usize,
    maximum_bytes: usize,
    maximum_concurrent: usize,
    maximum_total_bytes: usize,
    timeout_ms: u32,

    pub fn validate(self: Limits) !void {
        if (self.maximum_parts < 2 or self.maximum_parts > std.math.maxInt(u32) or self.maximum_bytes == 0 or self.maximum_concurrent == 0 or
            self.maximum_concurrent > std.math.maxInt(u32) or self.maximum_total_bytes < self.maximum_bytes or self.timeout_ms == 0) return error.InvalidConfiguration;
        if (self.maximum_parts > std.math.maxInt(usize) / self.maximum_concurrent) return error.InvalidConfiguration;
    }
};

pub const ExpiryBatch = struct { expired: usize, inspected: usize };

pub const Reassembler = struct {
    allocator: std.mem.Allocator,
    limits: Limits,
    assemblies: []Assembly,
    fragment_blocks: [][]Fragment,
    heap: []u32,
    assembly_count: usize = 0,
    heap_len: usize = 0,
    total_bytes: usize = 0,
    retained_bytes: usize = 0,

    pub fn init(allocator: std.mem.Allocator, limits: Limits) !Reassembler {
        try limits.validate();
        const assemblies = try allocator.alloc(Assembly, limits.maximum_concurrent);
        @memset(assemblies, .{});
        errdefer allocator.free(assemblies);
        const fragment_blocks = try allocator.alloc([]Fragment, limits.maximum_concurrent);
        @memset(fragment_blocks, &.{});
        errdefer allocator.free(fragment_blocks);
        const heap = try allocator.alloc(u32, limits.maximum_concurrent);
        errdefer allocator.free(heap);
        return .{ .allocator = allocator, .limits = limits, .assemblies = assemblies, .fragment_blocks = fragment_blocks, .heap = heap };
    }

    pub fn deinit(self: *Reassembler) void {
        for (0..self.assemblies.len) |slot| if (self.assemblies[slot].active) self.freeFragments(slot);
        for (self.fragment_blocks) |block| {
            for (block) |fragment| if (fragment.storage.len != 0) self.allocator.free(fragment.storage);
            if (block.len != 0) self.allocator.free(block);
        }
        self.allocator.free(self.heap);
        self.allocator.free(self.fragment_blocks);
        self.allocator.free(self.assemblies);
        self.* = undefined;
    }

    pub fn count(self: Reassembler) usize {
        return self.assembly_count;
    }

    pub fn retainedCapacity(self: Reassembler) usize {
        return self.retained_bytes;
    }

    /// Returns one contiguous owned payload.
    pub fn push(self: *Reassembler, id: u16, count_value: u32, index: u32, payload: []const u8, now_ms: u64) !?OwnedPayload {
        const slot = try self.retain(id, count_value, index, payload, now_ms);
        if (slot == null or self.assemblies[slot.?].received != self.assemblies[slot.?].count) return null;
        return try self.finish(slot.?);
    }

    /// Borrows fragments and skips the final copy.
    pub fn pushScatter(self: *Reassembler, id: u16, count_value: u32, index: u32, payload: []const u8, now_ms: u64, context: *anyopaque, consume: ScatterFn) !bool {
        const slot = try self.retain(id, count_value, index, payload, now_ms);
        if (slot == null or self.assemblies[slot.?].received != self.assemblies[slot.?].count) return false;
        const assembly = self.assemblies[slot.?];
        try consume(context, .{ .owner = self, .slot = slot.?, .fragment_count = assembly.count, .total_bytes = assembly.bytes });
        self.remove(slot.?);
        return true;
    }

    fn retain(self: *Reassembler, id: u16, count_value: u32, index: u32, payload: []const u8, now_ms: u64) !?usize {
        if (count_value < 2 or count_value > self.limits.maximum_parts or index >= count_value) return error.InvalidSplit;
        if (payload.len == 0 or payload.len > self.limits.maximum_bytes) return error.InvalidSplit;

        var slot = self.find(id);
        var created = false;
        if (slot == null) {
            if (self.assembly_count == self.assemblies.len) return error.TooManyAssemblies;
            slot = self.freeSlot() orelse return error.InternalInvariant;
            try self.ensureFragmentBlock(slot.?, count_value);
            const deadline_ms = time.deadline(now_ms, self.limits.timeout_ms);
            self.assemblies[slot.?] = .{ .active = true, .id = id, .count = count_value, .deadline_ms = deadline_ms };
            self.assembly_count += 1;
            self.heapInsert(slot.?);
            created = true;
        }
        errdefer if (created) self.remove(slot.?);

        const assembly = &self.assemblies[slot.?];
        if (assembly.count != count_value) {
            self.remove(slot.?);
            return error.SplitIdCollision;
        }
        const fragment = &self.fragmentSlice(slot.?)[index];
        if (fragment.data) |existing| {
            if (!std.mem.eql(u8, existing, payload)) {
                self.remove(slot.?);
                return error.ConflictingFragment;
            }
            self.updateDeadline(slot.?, now_ms);
            return slot;
        }
        if (payload.len > self.limits.maximum_bytes -| assembly.bytes or payload.len > self.limits.maximum_total_bytes -| self.total_bytes) {
            self.remove(slot.?);
            return error.ReassemblyLimitExceeded;
        }
        try self.ensureFragmentStorage(fragment, payload.len);
        @memcpy(fragment.storage[0..payload.len], payload);
        fragment.data = fragment.storage[0..payload.len];
        assembly.received += 1;
        assembly.bytes += payload.len;
        self.total_bytes += payload.len;
        self.updateDeadline(slot.?, now_ms);
        return slot;
    }

    fn finish(self: *Reassembler, slot: usize) !OwnedPayload {
        const assembly = self.assemblies[slot];
        const output = try self.allocator.alloc(u8, assembly.bytes);
        errdefer self.allocator.free(output);
        var offset: usize = 0;
        for (self.fragmentSlice(slot)[0..assembly.count]) |fragment| {
            const bytes = fragment.data orelse return error.InternalInvariant;
            @memcpy(output[offset..][0..bytes.len], bytes);
            offset += bytes.len;
        }
        self.remove(slot);
        return .{ .allocator = self.allocator, .bytes = output };
    }

    pub fn expire(self: *Reassembler, now_ms: u64, maximum_work: usize) ExpiryBatch {
        var expired: usize = 0;
        while (expired < maximum_work and self.heap_len != 0) {
            const slot = self.heap[0];
            if (!time.reached(now_ms, self.assemblies[slot].deadline_ms)) break;
            self.remove(slot);
            expired += 1;
        }
        return .{ .expired = expired, .inspected = expired };
    }

    pub fn nextDeadline(self: Reassembler) ?u64 {
        return if (self.heap_len == 0) null else self.assemblies[self.heap[0]].deadline_ms;
    }

    fn find(self: Reassembler, id: u16) ?usize {
        for (self.assemblies, 0..) |assembly, slot| if (assembly.active and assembly.id == id) return slot;
        return null;
    }

    fn freeSlot(self: Reassembler) ?usize {
        for (self.assemblies, 0..) |assembly, slot| if (!assembly.active) return slot;
        return null;
    }

    fn fragmentSlice(self: Reassembler, slot: usize) []Fragment {
        return self.fragment_blocks[slot];
    }

    fn ensureFragmentBlock(self: *Reassembler, slot: usize, count_value: u32) !void {
        const needed: usize = count_value;
        if (self.fragment_blocks[slot].len >= needed) return;
        const capacity = @min(self.limits.maximum_parts, std.math.ceilPowerOfTwo(usize, needed) catch self.limits.maximum_parts);
        const replacement = try self.allocator.alloc(Fragment, capacity);
        @memset(replacement, .{});
        if (self.fragment_blocks[slot].len != 0) {
            for (self.fragment_blocks[slot]) |fragment| {
                if (fragment.storage.len != 0) {
                    self.retained_bytes -= fragment.storage.len;
                    self.allocator.free(fragment.storage);
                }
            }
            self.allocator.free(self.fragment_blocks[slot]);
        }
        self.fragment_blocks[slot] = replacement;
    }

    fn ensureFragmentStorage(self: *Reassembler, fragment: *Fragment, len: usize) !void {
        const capacity = storageCapacity(len);
        if (fragment.storage.len == capacity) return;
        const replacement = try self.allocator.alloc(u8, capacity);
        if (fragment.storage.len != 0) {
            self.retained_bytes -= fragment.storage.len;
            self.allocator.free(fragment.storage);
        }
        fragment.storage = replacement;
        self.retained_bytes += replacement.len;
    }

    fn updateDeadline(self: *Reassembler, slot: usize, now_ms: u64) void {
        self.assemblies[slot].deadline_ms = time.deadline(now_ms, self.limits.timeout_ms);
        const position = self.assemblies[slot].heap_index;
        self.siftDown(position);
        self.siftUp(self.assemblies[slot].heap_index);
    }

    fn remove(self: *Reassembler, slot: usize) void {
        if (!self.assemblies[slot].active) return;
        self.heapRemove(self.assemblies[slot].heap_index);
        self.freeFragments(slot);
        self.assemblies[slot] = .{};
        self.assembly_count -= 1;
    }

    fn freeFragments(self: *Reassembler, slot: usize) void {
        const bytes = self.assemblies[slot].bytes;
        for (self.fragmentSlice(slot)[0..self.assemblies[slot].count]) |*fragment| {
            fragment.data = null;
            if (fragment.storage.len > class_sizes[class_sizes.len - 1]) {
                self.retained_bytes -= fragment.storage.len;
                self.allocator.free(fragment.storage);
                fragment.storage = &.{};
            }
        }
        self.total_bytes -= bytes;
    }

    fn heapInsert(self: *Reassembler, slot: usize) void {
        const position = self.heap_len;
        self.heap[position] = @intCast(slot);
        self.heap_len += 1;
        self.assemblies[slot].heap_index = @intCast(position);
        self.siftUp(@intCast(position));
    }

    fn heapRemove(self: *Reassembler, raw_position: u32) void {
        const position: usize = raw_position;
        self.heap_len -= 1;
        if (position == self.heap_len) return;
        const moved = self.heap[self.heap_len];
        self.heap[position] = moved;
        self.assemblies[moved].heap_index = @intCast(position);
        self.siftDown(@intCast(position));
        self.siftUp(self.assemblies[moved].heap_index);
    }

    fn less(self: Reassembler, left: usize, right: usize) bool {
        const lhs = self.assemblies[self.heap[left]];
        const rhs = self.assemblies[self.heap[right]];
        return lhs.deadline_ms < rhs.deadline_ms or (lhs.deadline_ms == rhs.deadline_ms and lhs.id < rhs.id);
    }

    fn swapHeap(self: *Reassembler, left: usize, right: usize) void {
        const temporary = self.heap[left];
        self.heap[left] = self.heap[right];
        self.heap[right] = temporary;
        self.assemblies[self.heap[left]].heap_index = @intCast(left);
        self.assemblies[self.heap[right]].heap_index = @intCast(right);
    }

    fn siftUp(self: *Reassembler, raw_position: u32) void {
        var position: usize = raw_position;
        while (position != 0) {
            const parent = (position - 1) / 2;
            if (!self.less(position, parent)) break;
            self.swapHeap(position, parent);
            position = parent;
        }
    }

    fn siftDown(self: *Reassembler, raw_position: u32) void {
        var position: usize = raw_position;
        while (true) {
            const left = position * 2 + 1;
            if (left >= self.heap_len) break;
            const right = left + 1;
            const child = if (right < self.heap_len and self.less(right, left)) right else left;
            if (!self.less(child, position)) break;
            self.swapHeap(position, child);
            position = child;
        }
    }
};

fn storageCapacity(len: usize) usize {
    for (class_sizes) |size| if (len <= size) return size;
    return len;
}

test "split assembly handles duplicates conflicts collisions and expiry" {
    var value = try Reassembler.init(std.testing.allocator, .{ .maximum_parts = 4, .maximum_bytes = 16, .maximum_concurrent = 2, .maximum_total_bytes = 24, .timeout_ms = 10 });
    defer value.deinit();
    try std.testing.expect((try value.push(1, 2, 1, "world", 0)) == null);
    try std.testing.expect((try value.push(1, 2, 1, "world", 1)) == null);
    try std.testing.expectError(error.ConflictingFragment, value.push(1, 2, 1, "evil", 2));
    try std.testing.expect((try value.push(2, 2, 0, "hello ", 3)) == null);
    const complete = (try value.push(2, 2, 1, "world", 4)).?;
    defer complete.deinit();
    try std.testing.expectEqualStrings("hello world", complete.bytes);
    try std.testing.expect((try value.push(3, 2, 0, "x", 5)) == null);
    try std.testing.expectError(error.SplitIdCollision, value.push(3, 3, 1, "y", 6));
    try std.testing.expect((try value.push(4, 2, 0, "z", 7)) == null);
    try std.testing.expectEqual(@as(?u64, 17), value.nextDeadline());
    try std.testing.expectEqual(@as(usize, 1), value.expire(100, 2).expired);
    try std.testing.expectEqual(@as(?u64, null), value.nextDeadline());
}

test "scatter completion avoids a final allocation" {
    var value = try Reassembler.init(std.testing.allocator, .{ .maximum_parts = 2, .maximum_bytes = 16, .maximum_concurrent = 1, .maximum_total_bytes = 16, .timeout_ms = 10 });
    defer value.deinit();
    try std.testing.expect((try value.push(7, 2, 0, "tiny", 0)) == null);
    const Consumer = struct {
        value: [8]u8 = undefined,
        length: usize = 0,
        fn consume(raw: *anyopaque, payload: ScatterPayload) error{ApplicationFailure}!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (0..payload.count()) |index| {
                const bytes = payload.get(index);
                @memcpy(self.value[self.length..][0..bytes.len], bytes);
                self.length += bytes.len;
            }
        }
    };
    var consumer: Consumer = .{};
    try std.testing.expect(try value.pushScatter(7, 2, 1, "!", 1, &consumer, Consumer.consume));
    try std.testing.expectEqualStrings("tiny!", consumer.value[0..consumer.length]);
    try std.testing.expectEqual(@as(usize, 0), value.count());
}

test "small fragments detach from large receive buffers" {
    var value = try Reassembler.init(std.testing.allocator, .{ .maximum_parts = 2, .maximum_bytes = 16, .maximum_concurrent = 1, .maximum_total_bytes = 16, .timeout_ms = 10 });
    defer value.deinit();
    var source: [4096]u8 = @splat(0);
    @memcpy(source[100..104], "tiny");
    try std.testing.expect((try value.push(7, 2, 0, source[100..104], 0)) == null);
    @memset(&source, 0xaa);
    const complete = (try value.push(7, 2, 1, "!", 1)).?;
    defer complete.deinit();
    try std.testing.expectEqualStrings("tiny!", complete.bytes);
}

test "fragment classes are reused and oversized storage is evicted" {
    const QuotaAllocator = @import("../util/quota_allocator.zig").QuotaAllocator;
    const Consumer = struct {
        fn consume(_: *anyopaque, _: ScatterPayload) error{ApplicationFailure}!void {}
    };
    var quota = QuotaAllocator.init(std.testing.allocator, std.math.maxInt(usize));
    var value = try Reassembler.init(quota.allocator(), .{ .maximum_parts = 2, .maximum_bytes = 4096, .maximum_concurrent = 1, .maximum_total_bytes = 4096, .timeout_ms = 10 });
    defer value.deinit();
    var unused: u8 = 0;
    try std.testing.expect((try value.push(1, 2, 0, "a", 0)) == null);
    try std.testing.expect(try value.pushScatter(1, 2, 1, "b", 1, &unused, Consumer.consume));
    const retained = quota.used_bytes;
    quota.maximum_bytes = retained;
    try std.testing.expect((try value.push(2, 2, 0, "c", 2)) == null);
    try std.testing.expect(try value.pushScatter(2, 2, 1, "d", 3, &unused, Consumer.consume));
    try std.testing.expectEqual(retained, quota.used_bytes);

    quota.maximum_bytes = std.math.maxInt(usize);
    var large: [1300]u8 = @splat(1);
    try std.testing.expect((try value.push(3, 2, 0, &large, 4)) == null);
    try std.testing.expect(try value.pushScatter(3, 2, 1, &large, 5, &unused, Consumer.consume));
    try std.testing.expectEqual(@as(usize, 0), value.retainedCapacity());
}

test "split limits reject before payload allocation" {
    var value = try Reassembler.init(std.testing.allocator, .{ .maximum_parts = 4, .maximum_bytes = 8, .maximum_concurrent = 1, .maximum_total_bytes = 8, .timeout_ms = 10 });
    defer value.deinit();
    try std.testing.expectError(error.InvalidSplit, value.push(1, 1, 0, "x", 0));
    try std.testing.expectError(error.InvalidSplit, value.push(1, 5, 0, "x", 0));
    try std.testing.expect((try value.push(1, 2, 0, "12345678", 0)) == null);
    try std.testing.expectError(error.TooManyAssemblies, value.push(2, 2, 0, "x", 0));
    try std.testing.expectError(error.ReassemblyLimitExceeded, value.push(1, 2, 1, "x", 0));
}

test "advertised split size does not reserve final payload" {
    var value = try Reassembler.init(std.testing.allocator, .{
        .maximum_parts = 2048,
        .maximum_bytes = 4 * 1024 * 1024,
        .maximum_concurrent = 1,
        .maximum_total_bytes = 4 * 1024 * 1024,
        .timeout_ms = 10,
    });
    defer value.deinit();
    try std.testing.expect((try value.push(1, 2048, 0, "x", 0)) == null);
    try std.testing.expectEqual(@as(usize, 1), value.total_bytes);
    try std.testing.expectEqual(@as(usize, 64), value.retainedCapacity());
}

fn checkReassemblyAllocationFailures(allocator: std.mem.Allocator) !void {
    var value = try Reassembler.init(allocator, .{ .maximum_parts = 4, .maximum_bytes = 32, .maximum_concurrent = 2, .maximum_total_bytes = 64, .timeout_ms = 10 });
    defer value.deinit();
    try std.testing.expect((try value.push(1, 2, 0, "hello ", 0)) == null);
    const complete = (try value.push(1, 2, 1, "world", 1)).?;
    defer complete.deinit();
    try std.testing.expectEqualStrings("hello world", complete.bytes);
    try std.testing.expectEqual(@as(usize, 0), value.count());
}

test "split reassembly handles every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkReassemblyAllocationFailures, .{});
}

test "expiry inspects only due assemblies" {
    const count = 8;
    var value = try Reassembler.init(std.testing.allocator, .{
        .maximum_parts = 2,
        .maximum_bytes = count,
        .maximum_concurrent = count,
        .maximum_total_bytes = count,
        .timeout_ms = 10,
    });
    defer value.deinit();
    try std.testing.expect((try value.push(1, 2, 0, "x", 0)) == null);
    for (2..count + 1) |id| try std.testing.expect((try value.push(@intCast(id), 2, 0, "x", 100)) == null);
    const batch = value.expire(11, 1);
    try std.testing.expectEqual(@as(usize, 1), batch.inspected);
    try std.testing.expectEqual(@as(usize, 1), batch.expired);
    try std.testing.expectEqual(@as(usize, count - 1), value.count());
    try std.testing.expectEqual(@as(?u64, 110), value.nextDeadline());
}

test "split expiry honors a saturated deadline" {
    var value = try Reassembler.init(std.testing.allocator, .{ .maximum_parts = 2, .maximum_bytes = 1, .maximum_concurrent = 1, .maximum_total_bytes = 1, .timeout_ms = 10 });
    defer value.deinit();
    try std.testing.expect((try value.push(1, 2, 0, "x", std.math.maxInt(u64) - 5)) == null);
    try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), value.nextDeadline());
    try std.testing.expectEqual(@as(usize, 1), value.expire(std.math.maxInt(u64), 1).expired);
    try std.testing.expectEqual(@as(?u64, null), value.nextDeadline());
}
