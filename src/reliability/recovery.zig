const std = @import("std");

const ack = @import("../protocol/ack.zig");
const time = @import("../util/time.zig");
const uint24 = @import("../util/uint24.zig");

const none = std.math.maxInt(u32);
const class_count = 5;

pub const StoragePolicy = enum { exact, size_classes, mtu_slabs };

const Slot = struct {
    occupied: bool = false,
    sequence: u32 = 0,
    block_index: u32 = none,
    data_len: u16 = 0,
    sent_ms: u64 = 0,
    deadline_ms: u64 = 0,
    in_flight_bytes: u16 = 0,
    transmissions: u8 = 1,
    timeouts: u8 = 0,
    nacked: bool = false,
    heap_index: u32 = none,
    active_previous: u32 = none,
    active_next: u32 = none,
};

const Block = struct {
    data: []u8 = undefined,
    next: u32 = none,
    allocated: bool = false,
};

pub const Acknowledged = struct { packets: usize = 0, bytes: usize = 0, rtt_sample_ms: ?u64 = null };
pub const Due = struct { sequence: u32, data: []const u8, in_flight_bytes: usize, timed_out: bool };
pub const DueBatch = struct { items: []Due, exhausted: usize, inspected: usize };

pub const Recovery = struct {
    allocator: std.mem.Allocator,
    slots: []Slot,
    heap: []u32,
    blocks: []Block,
    heap_len: usize = 0,
    count_value: usize = 0,
    maximum_bytes: usize,
    maximum_transmissions: u8,
    maximum_delay_ms: u32 = 5_000,
    minimum_abandon_ms: u32 = 0,
    mtu: u16,
    storage_policy: StoragePolicy,
    total_bytes: usize = 0,
    retained_bytes: usize = 0,
    allocated_blocks: usize = 0,
    unused_head: u32 = none,
    active_head: u32 = none,
    free_heads: [class_count]u32 = @splat(none),

    pub fn init(allocator: std.mem.Allocator, maximum_entries: usize, maximum_bytes: usize, maximum_transmissions: u8, mtu: usize) !Recovery {
        const slab_bytes = std.math.mul(usize, maximum_entries, mtu) catch std.math.maxInt(usize);
        const policy: StoragePolicy = if (slab_bytes <= maximum_bytes) .size_classes else .exact;
        return initWithPolicy(allocator, maximum_entries, maximum_bytes, maximum_transmissions, mtu, policy);
    }

    pub fn initWithPolicy(allocator: std.mem.Allocator, maximum_entries: usize, maximum_bytes: usize, maximum_transmissions: u8, mtu: usize, policy: StoragePolicy) !Recovery {
        if (maximum_entries == 0 or
            maximum_entries > std.math.maxInt(u32) or
            maximum_bytes == 0 or
            maximum_transmissions < 2 or
            mtu == 0 or
            mtu > std.math.maxInt(u16)) return error.InvalidConfiguration;
        if (policy != .exact and (std.math.mul(usize, maximum_entries, mtu) catch return error.InvalidConfiguration) > maximum_bytes) return error.InvalidConfiguration;
        const slots = try allocator.alloc(Slot, maximum_entries);
        errdefer allocator.free(slots);
        const heap = try allocator.alloc(u32, maximum_entries);
        errdefer allocator.free(heap);
        const blocks = try allocator.alloc(Block, maximum_entries);
        errdefer allocator.free(blocks);
        for (slots) |*slot| slot.* = .{};
        for (blocks, 0..) |*block, index| block.* = .{ .next = if (index + 1 < blocks.len) @intCast(index + 1) else none };
        return .{
            .allocator = allocator,
            .slots = slots,
            .heap = heap,
            .blocks = blocks,
            .maximum_bytes = maximum_bytes,
            .maximum_transmissions = maximum_transmissions,
            .mtu = @intCast(mtu),
            .storage_policy = policy,
            .unused_head = 0,
        };
    }

    pub fn deinit(self: *Recovery) void {
        for (self.blocks) |block| if (block.allocated) self.allocator.free(block.data);
        self.allocator.free(self.blocks);
        self.allocator.free(self.heap);
        self.allocator.free(self.slots);
        self.* = undefined;
    }

    pub fn track(self: *Recovery, raw_sequence: u32, data: []const u8, in_flight_bytes: usize, now_ms: u64, rto_ms: u32) !void {
        const sequence = uint24.normalize(raw_sequence);
        if (data.len == 0 or data.len > self.mtu or in_flight_bytes > std.math.maxInt(u16)) return error.InvalidDatagram;
        const slot_index: u32 = @intCast(sequence % self.slots.len);
        const slot = &self.slots[slot_index];
        if (slot.occupied) return if (slot.sequence == sequence) error.InvalidDatagram else error.RecoveryFull;
        if (self.count_value == self.slots.len) return error.RecoveryFull;
        if (data.len > self.maximum_bytes -| self.total_bytes) return error.RecoveryBytesExceeded;
        const block_index = try self.acquireBlock(data.len);
        errdefer self.releaseBlock(block_index);
        @memcpy(self.blocks[block_index].data[0..data.len], data);
        slot.* = .{
            .occupied = true,
            .sequence = sequence,
            .block_index = block_index,
            .data_len = @intCast(data.len),
            .sent_ms = now_ms,
            .deadline_ms = time.deadline(now_ms, rto_ms),
            .in_flight_bytes = @intCast(in_flight_bytes),
        };
        self.total_bytes += data.len;
        self.count_value += 1;
        slot.active_next = self.active_head;
        if (self.active_head != none) self.slots[self.active_head].active_previous = slot_index;
        self.active_head = slot_index;
        self.heapInsert(slot_index);
    }

    pub fn untrack(self: *Recovery, raw_sequence: u32) ?usize {
        const slot = self.find(raw_sequence) orelse return null;
        const in_flight_bytes = slot.in_flight_bytes;
        self.removeSlot(@intCast(slot.sequence % self.slots.len));
        return in_flight_bytes;
    }

    pub fn acknowledge(self: *Recovery, ranges: []const ack.Record, now_ms: u64, maximum_work: usize) !Acknowledged {
        const range_count = try validateRanges(ranges, maximum_work);
        var result: Acknowledged = .{};
        if (range_count <= self.count_value) {
            var iterator = ack.SequenceIterator.init(ranges, maximum_work);
            while (try iterator.next()) |sequence| {
                if (self.find(sequence)) |slot| self.ackSlot(@intCast(slot.sequence % self.slots.len), now_ms, &result);
            }
        } else {
            var current = self.active_head;
            while (current != none) {
                const slot = &self.slots[current];
                const next = slot.active_next;
                if (contains(ranges, slot.sequence)) self.ackSlot(current, now_ms, &result);
                current = next;
            }
        }
        return result;
    }

    pub fn markNack(self: *Recovery, ranges: []const ack.Record, now_ms: u64, maximum_work: usize) !usize {
        const range_count = try validateRanges(ranges, maximum_work);
        var marked: usize = 0;
        if (range_count <= self.count_value) {
            var iterator = ack.SequenceIterator.init(ranges, maximum_work);
            while (try iterator.next()) |sequence| {
                if (self.find(sequence)) |slot| marked += @intFromBool(self.nackSlot(slot, now_ms));
            }
        } else {
            var current = self.active_head;
            while (current != none) : (current = self.slots[current].active_next) {
                const slot = &self.slots[current];
                if (contains(ranges, slot.sequence)) marked += @intFromBool(self.nackSlot(slot, now_ms));
            }
        }
        return marked;
    }

    pub fn collectDue(self: *Recovery, now_ms: u64, rto_ms: u32, output: []Due, maximum_work: usize) DueBatch {
        var due_count: usize = 0;
        var inspected: usize = 0;
        var exhausted: usize = 0;
        while (inspected < maximum_work and due_count < output.len and self.heap_len != 0) {
            const slot = &self.slots[self.heap[0]];
            if (slot.deadline_ms > now_ms) break;
            inspected += 1;
            if (slot.transmissions >= self.maximum_transmissions and time.elapsed(now_ms, slot.sent_ms) >= self.minimum_abandon_ms) {
                exhausted = 1;
                break;
            }
            const block = &self.blocks[slot.block_index];
            const timed_out = !slot.nacked;
            output[due_count] = .{
                .sequence = slot.sequence,
                .data = block.data[0..slot.data_len],
                .in_flight_bytes = slot.in_flight_bytes,
                .timed_out = timed_out,
            };
            due_count += 1;
            slot.transmissions +|= 1;
            slot.nacked = false;
            if (timed_out) slot.timeouts += 1;
            const backoff = @as(u64, rto_ms) << @intCast(@min(slot.timeouts, 6));
            slot.deadline_ms = time.deadline(now_ms, @max(rto_ms, @min(backoff, self.maximum_delay_ms)));
            self.siftDown(0);
        }
        return .{ .items = output[0..due_count], .exhausted = exhausted, .inspected = inspected };
    }

    pub fn freeSlots(self: *const Recovery, raw_first: u32, limit: usize) usize {
        const bound = @min(limit, self.slots.len - self.count_value);
        var run: usize = 0;
        while (run < bound and !self.slots[uint24.add(raw_first, @intCast(run)) % self.slots.len].occupied) run += 1;
        return run;
    }

    pub fn expedite(self: *Recovery, raw_first: u32, free: usize, now_ms: u64) bool {
        const slot = &self.slots[uint24.add(raw_first, @intCast(free)) % self.slots.len];
        return slot.occupied and slot.transmissions == 1 and self.nackSlot(slot, now_ms);
    }

    pub fn nextDeadline(self: Recovery) ?u64 {
        return if (self.heap_len == 0) null else self.slots[self.heap[0]].deadline_ms;
    }
    pub fn count(self: Recovery) usize {
        return self.count_value;
    }
    pub fn payloadBytes(self: Recovery) usize {
        return self.total_bytes;
    }
    pub fn retainedCapacity(self: Recovery) usize {
        return self.retained_bytes;
    }
    pub fn allocatedBlockCount(self: Recovery) usize {
        return self.allocated_blocks;
    }

    fn find(self: *Recovery, raw_sequence: u32) ?*Slot {
        const sequence = uint24.normalize(raw_sequence);
        const slot = &self.slots[sequence % self.slots.len];
        return if (slot.occupied and slot.sequence == sequence) slot else null;
    }

    fn removeSlot(self: *Recovery, slot_index: u32) void {
        const slot = &self.slots[slot_index];
        if (slot.active_previous == none) self.active_head = slot.active_next else self.slots[slot.active_previous].active_next = slot.active_next;
        if (slot.active_next != none) self.slots[slot.active_next].active_previous = slot.active_previous;
        self.heapRemove(slot.heap_index);
        self.releaseBlock(slot.block_index);
        self.total_bytes -= @as(usize, slot.data_len);
        self.count_value -= 1;
        slot.* = .{};
    }

    fn ackSlot(self: *Recovery, slot_index: u32, now_ms: u64, result: *Acknowledged) void {
        const slot = &self.slots[slot_index];
        result.packets += 1;
        result.bytes +|= slot.in_flight_bytes;
        if (slot.transmissions == 1) result.rtt_sample_ms = time.elapsed(now_ms, slot.sent_ms);
        self.removeSlot(slot_index);
    }

    fn nackSlot(self: *Recovery, slot: *Slot, now_ms: u64) bool {
        if (slot.deadline_ms <= now_ms) return false;
        slot.deadline_ms = now_ms;
        slot.nacked = true;
        self.siftUp(slot.heap_index);
        return true;
    }

    fn desiredCapacity(self: Recovery, len: usize) struct { capacity: usize, class: usize } {
        if (self.storage_policy == .exact) return .{ .capacity = len, .class = 0 };
        if (self.storage_policy == .mtu_slabs) return .{ .capacity = self.mtu, .class = 0 };
        const capacities = [_]usize{ 64, 256, 576, 1200, self.mtu };
        for (capacities, 0..) |capacity, class| if (capacity >= len and capacity <= self.mtu) return .{ .capacity = capacity, .class = class };
        return .{ .capacity = self.mtu, .class = class_count - 1 };
    }

    fn acquireBlock(self: *Recovery, len: usize) !u32 {
        const desired = self.desiredCapacity(len);
        if (self.takeFree(desired.class, desired.capacity)) |index| return index;
        var index = self.popUnused();
        while (self.retained_bytes > self.maximum_bytes -| desired.capacity) {
            const evicted = self.evictFree() orelse return error.RecoveryBytesExceeded;
            if (index == null) index = evicted else self.pushUnused(evicted);
        }
        if (index == null) index = self.evictFree();
        const block_index = index orelse return error.RecoveryFull;
        const data = self.allocator.alloc(u8, desired.capacity) catch |err| {
            self.pushUnused(block_index);
            return err;
        };
        self.blocks[block_index] = .{ .data = data, .allocated = true };
        self.retained_bytes += desired.capacity;
        self.allocated_blocks += 1;
        return block_index;
    }

    fn releaseBlock(self: *Recovery, block_index: u32) void {
        const block = &self.blocks[block_index];
        const class = self.desiredCapacity(block.data.len).class;
        block.next = self.free_heads[class];
        self.free_heads[class] = block_index;
    }

    fn takeFree(self: *Recovery, class: usize, capacity: usize) ?u32 {
        var previous: u32 = none;
        var current = self.free_heads[class];
        while (current != none) {
            const block = &self.blocks[current];
            if (block.data.len == capacity) {
                if (previous == none) self.free_heads[class] = block.next else self.blocks[previous].next = block.next;
                block.next = none;
                return current;
            }
            previous = current;
            current = block.next;
        }
        return null;
    }

    fn evictFree(self: *Recovery) ?u32 {
        var class: usize = class_count;
        while (class != 0) {
            class -= 1;
            const index = self.free_heads[class];
            if (index == none) continue;
            const block = &self.blocks[index];
            self.free_heads[class] = block.next;
            self.retained_bytes -= block.data.len;
            self.allocated_blocks -= 1;
            self.allocator.free(block.data);
            block.* = .{};
            return index;
        }
        return null;
    }

    fn popUnused(self: *Recovery) ?u32 {
        if (self.unused_head == none) return null;
        const index = self.unused_head;
        self.unused_head = self.blocks[index].next;
        self.blocks[index].next = none;
        return index;
    }
    fn pushUnused(self: *Recovery, index: u32) void {
        self.blocks[index] = .{ .next = self.unused_head };
        self.unused_head = index;
    }

    fn heapInsert(self: *Recovery, slot_index: u32) void {
        const index: u32 = @intCast(self.heap_len);
        self.heap[self.heap_len] = slot_index;
        self.heap_len += 1;
        self.slots[slot_index].heap_index = index;
        self.siftUp(index);
    }
    fn heapRemove(self: *Recovery, raw_index: u32) void {
        const index: usize = raw_index;
        self.heap_len -= 1;
        if (index == self.heap_len) return;
        const moved = self.heap[self.heap_len];
        self.heap[index] = moved;
        self.slots[moved].heap_index = @intCast(index);
        if (index != 0 and self.less(moved, self.heap[(index - 1) / 2])) self.siftUp(@intCast(index)) else self.siftDown(@intCast(index));
    }
    fn siftUp(self: *Recovery, raw_index: u32) void {
        var index: usize = raw_index;
        while (index != 0) {
            const parent = (index - 1) / 2;
            if (!self.less(self.heap[index], self.heap[parent])) break;
            self.swapHeap(index, parent);
            index = parent;
        }
    }
    fn siftDown(self: *Recovery, raw_index: u32) void {
        var index: usize = raw_index;
        while (true) {
            const left = index * 2 + 1;
            if (left >= self.heap_len) break;
            const right = left + 1;
            const child = if (right < self.heap_len and self.less(self.heap[right], self.heap[left])) right else left;
            if (!self.less(self.heap[child], self.heap[index])) break;
            self.swapHeap(index, child);
            index = child;
        }
    }
    fn swapHeap(self: *Recovery, a: usize, b: usize) void {
        std.mem.swap(u32, &self.heap[a], &self.heap[b]);
        self.slots[self.heap[a]].heap_index = @intCast(a);
        self.slots[self.heap[b]].heap_index = @intCast(b);
    }
    fn less(self: Recovery, a: u32, b: u32) bool {
        const left = self.slots[a];
        const right = self.slots[b];
        return left.deadline_ms < right.deadline_ms or (left.deadline_ms == right.deadline_ms and left.sequence < right.sequence);
    }
};

fn validateRanges(ranges: []const ack.Record, maximum_work: usize) !usize {
    var total: usize = 0;
    var previous: ?ack.Record = null;
    for (ranges) |record| {
        if (record.first > record.last or record.last > uint24.mask) return error.InvalidRange;
        if (previous) |prior| if (record.first <= prior.last) return error.InvalidRange;
        const amount = @as(usize, record.last - record.first) + 1;
        if (amount > maximum_work -| total) return error.WorkLimitExceeded;
        total += amount;
        previous = record;
    }
    return total;
}

fn contains(ranges: []const ack.Record, sequence: u32) bool {
    var low: usize = 0;
    var high = ranges.len;
    while (low < high) {
        const middle = low + (high - low) / 2;
        const record = ranges[middle];
        if (sequence < record.first) high = middle else if (sequence > record.last) low = middle + 1 else return true;
    }
    return false;
}

test "recovery ring, duplicate ACKs, and deadlines" {
    var recovery = try Recovery.init(std.testing.allocator, 2, 1152, 3, 576);
    defer recovery.deinit();
    try recovery.track(0xffffff, "one", 7, 10, 50);
    try recovery.track(0, "two", 8, 10, 50);
    try std.testing.expectError(error.RecoveryFull, recovery.track(1, "x", 1, 10, 50));
    const records = [_]ack.Record{.{ .first = 0xffffff, .last = 0xffffff }};
    try std.testing.expectEqual(@as(usize, 1), (try recovery.acknowledge(&records, 30, 4)).packets);
    try std.testing.expectEqual(@as(usize, 0), (try recovery.acknowledge(&records, 40, 4)).packets);
    var due: [2]Due = undefined;
    try std.testing.expectEqual(@as(usize, 1), recovery.collectDue(61, 50, &due, 2).items.len);
    try std.testing.expectEqualStrings("two", due[0].data);
    try std.testing.expectEqual(@as(?u64, 161), recovery.nextDeadline());
}

test "timeouts back off while NACK retransmits do not" {
    var recovery = try Recovery.init(std.testing.allocator, 4, 2304, 8, 576);
    defer recovery.deinit();
    recovery.maximum_delay_ms = 300;
    try recovery.track(0, "data", 4, 0, 50);
    var due: [1]Due = undefined;
    for ([_]u64{ 50, 150, 350, 650, 950 }) |deadline| {
        try std.testing.expectEqual(deadline, recovery.nextDeadline().?);
        try std.testing.expect(recovery.collectDue(deadline, 50, &due, 1).items[0].timed_out);
    }
    const now = recovery.nextDeadline().? - 1;
    try std.testing.expectEqual(@as(usize, 1), try recovery.markNack(&.{.{ .first = 0, .last = 0 }}, now, 1));
    try std.testing.expectEqual(@as(usize, 0), try recovery.markNack(&.{.{ .first = 0, .last = 0 }}, now, 1));
    const nacked = recovery.collectDue(now, 50, &due, 1);
    try std.testing.expect(!nacked.items[0].timed_out);
    try std.testing.expectEqual(now + 300, recovery.nextDeadline().?);
}

test "free slots stop at a pinned wrap alias" {
    var recovery = try Recovery.init(std.testing.allocator, 4, 2304, 3, 576);
    defer recovery.deinit();
    try std.testing.expectEqual(@as(usize, 4), recovery.freeSlots(0, 8));
    try recovery.track(2, "pinned", 6, 0, 10);
    try std.testing.expectEqual(@as(usize, 2), recovery.freeSlots(0, 8));
    try std.testing.expectEqual(@as(usize, 0), recovery.freeSlots(6, 8));
    try std.testing.expectEqual(@as(usize, 1), recovery.freeSlots(3, 1));
    try std.testing.expectEqual(@as(usize, 3), recovery.freeSlots(0xffffff, 8));
}

test "a pinned slot is expedited once" {
    var recovery = try Recovery.init(std.testing.allocator, 4, 2304, 8, 576);
    defer recovery.deinit();
    try recovery.track(2, "pinned", 6, 0, 50);
    try std.testing.expect(recovery.expedite(0, 2, 1));
    var due: [1]Due = undefined;
    try std.testing.expectEqual(@as(usize, 1), recovery.collectDue(1, 50, &due, 1).items.len);
    try std.testing.expect(!recovery.expedite(0, 2, 1));
    try std.testing.expect(!recovery.expedite(0, 0, 1));
    try std.testing.expectEqual(@as(?u64, 51), recovery.nextDeadline());
}

test "ring rejects delayed wrap aliases" {
    var recovery = try Recovery.init(std.testing.allocator, 2, 1152, 3, 576);
    defer recovery.deinit();
    try recovery.track(0xffffff, "old", 3, 0, 10);
    try recovery.track(0, "new", 3, 0, 20);
    try std.testing.expectError(error.RecoveryFull, recovery.track(1, "collision", 9, 0, 10));
    const old = [_]ack.Record{.{ .first = 0xffffff, .last = 0xffffff }};
    _ = try recovery.acknowledge(&old, 1, 1);
    try recovery.track(1, "replacement", 9, 1, 10);
    try std.testing.expectEqual(@as(usize, 0), (try recovery.acknowledge(&old, 2, 1)).packets);
    try std.testing.expectEqual(@as(usize, 2), recovery.count());
}

test "wire buffers are lazy, bounded, and reused" {
    var recovery = try Recovery.init(std.testing.allocator, 4, 2304, 3, 576);
    defer recovery.deinit();
    try std.testing.expectEqual(@as(usize, 0), recovery.retainedCapacity());
    try recovery.track(1, "first", 5, 0, 10);
    const retained = recovery.retainedCapacity();
    const record = [_]ack.Record{.{ .first = 1, .last = 1 }};
    _ = try recovery.acknowledge(&record, 1, 1);
    try recovery.track(2, "again", 5, 0, 10);
    try std.testing.expectEqual(@as(usize, 1), recovery.allocatedBlockCount());
    try std.testing.expectEqual(retained, recovery.retainedCapacity());
}

test "duplicate NACKs are idempotent" {
    var recovery = try Recovery.init(std.testing.allocator, 2, 1152, 3, 576);
    defer recovery.deinit();
    try recovery.track(7, "wire", 4, 0, 100);
    const record = [_]ack.Record{.{ .first = 7, .last = 7 }};
    try std.testing.expectEqual(@as(usize, 1), try recovery.markNack(&record, 10, 1));
    try std.testing.expectEqual(@as(usize, 0), try recovery.markNack(&record, 10, 1));
    try std.testing.expectEqual(@as(?u64, 10), recovery.nextDeadline());
}

test "deadline heap inspects only due records" {
    var recovery = try Recovery.init(std.testing.allocator, 8, 4608, 3, 576);
    defer recovery.deinit();
    for (0..8) |sequence| try recovery.track(@intCast(sequence), "x", 1, 0, @intCast(10 + sequence));
    var due: [2]Due = undefined;
    const first = recovery.collectDue(10, 100, &due, 2);
    try std.testing.expectEqual(@as(usize, 1), first.inspected);
    try std.testing.expectEqual(@as(u32, 0), first.items[0].sequence);
    try std.testing.expectEqual(@as(?u64, 11), recovery.nextDeadline());
}

test "exhaustion waits for the minimum abandon age" {
    var recovery = try Recovery.init(std.testing.allocator, 1, 576, 2, 576);
    defer recovery.deinit();
    recovery.minimum_abandon_ms = 1000;
    try recovery.track(1, "x", 1, 0, 50);
    var due: [1]Due = undefined;
    var now: u64 = 0;
    for (0..6) |_| {
        now = recovery.nextDeadline().?;
        const batch = recovery.collectDue(now, 50, &due, 1);
        if (now >= 1000) break;
        try std.testing.expectEqual(@as(usize, 0), batch.exhausted);
        try std.testing.expectEqual(@as(usize, 1), batch.items.len);
    }
    try std.testing.expect(now >= 1000);
    try std.testing.expectEqual(@as(usize, 1), recovery.collectDue(now, 50, &due, 1).exhausted);
}

test "retransmission exhaustion and clock saturation" {
    var recovery = try Recovery.init(std.testing.allocator, 1, 576, 2, 576);
    defer recovery.deinit();
    try recovery.track(1, "x", 1, std.math.maxInt(u64) - 1, 50);
    var due: [1]Due = undefined;
    const timed_out = recovery.collectDue(std.math.maxInt(u64), 50, &due, 1);
    try std.testing.expectEqual(@as(usize, 1), timed_out.items.len);
    try std.testing.expectEqual(@as(usize, 1), recovery.collectDue(std.math.maxInt(u64), 50, &due, 1).exhausted);
    try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), recovery.nextDeadline());

    var timeout = try Recovery.init(std.testing.allocator, 1, 576, 3, 576);
    defer timeout.deinit();
    try timeout.track(2, "y", 1, 0, 1);
    try std.testing.expect(timeout.collectDue(2, 1, &due, 1).items[0].timed_out);
}

test "range work rejection is atomic" {
    var recovery = try Recovery.init(std.testing.allocator, 4, 2304, 3, 576);
    defer recovery.deinit();
    for (0..3) |sequence| try recovery.track(@intCast(sequence), "x", 1, 0, 10);
    const range = [_]ack.Record{.{ .first = 0, .last = 2 }};
    try std.testing.expectError(error.WorkLimitExceeded, recovery.acknowledge(&range, 1, 2));
    try std.testing.expectEqual(@as(usize, 3), recovery.count());
}

fn checkAllocationFailures(allocator: std.mem.Allocator) !void {
    var recovery = try Recovery.init(allocator, 4, 2304, 3, 576);
    defer recovery.deinit();
    try recovery.track(1, "wire", 4, 0, 10);
    const record = [_]ack.Record{.{ .first = 1, .last = 1 }};
    _ = try recovery.acknowledge(&record, 1, 1);
}

test "recovery cleans up after every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkAllocationFailures, .{});
}

test "untrack repairs deadlines and active teardown releases storage" {
    var recovery = try Recovery.init(std.testing.allocator, 2, 1152, 3, 576);
    try recovery.track(1, "one", 4, 0, 10);
    try recovery.track(2, "two", 5, 0, 20);
    try std.testing.expectEqual(@as(?usize, 4), recovery.untrack(1));
    try std.testing.expectEqual(@as(?u64, 20), recovery.nextDeadline());
    try std.testing.expectEqual(@as(?usize, null), recovery.untrack(1));
    recovery.deinit();
}
