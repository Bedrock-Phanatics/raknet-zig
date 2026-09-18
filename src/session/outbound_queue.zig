const std = @import("std");

const payload_mod = @import("../payload.zig");
const frame = @import("../protocol/frame.zig");

const none = std.math.maxInt(u32);

pub const Lane = enum(u1) { control, application };
pub const Id = u64;

pub const Message = struct {
    id: Id,
    payload: payload_mod.OwnedPayload,
    reliability: frame.Reliability,
    channel: u8,
};

pub const Iterator = struct {
    queue: *const Queue,
    next_index: u32,

    pub fn next(self: *Iterator) ?*const Message {
        if (self.next_index == none) return null;
        const slot = &self.queue.slots[self.next_index];
        self.next_index = slot.next;
        return &slot.message;
    }
};

const Slot = struct {
    message: Message = undefined,
    next: u32 = none,
    occupied: bool = false,
};

pub const Queue = struct {
    allocator: std.mem.Allocator,
    slots: []Slot,
    maximum_bytes: usize,
    reserved_control_messages: usize,
    reserved_control_bytes: usize,
    total_bytes: usize = 0,
    bytes: [2]usize = @splat(0),
    free_head: u32,
    heads: [2]u32 = @splat(none),
    tails: [2]u32 = @splat(none),
    counts: [2]usize = @splat(0),
    next_id: Id = 1,

    pub fn init(
        allocator: std.mem.Allocator,
        maximum_messages: usize,
        maximum_bytes: usize,
        reserved_control_messages: usize,
        reserved_control_bytes: usize,
    ) !Queue {
        if (maximum_messages == 0 or maximum_messages >= none or maximum_bytes == 0) return error.InvalidConfiguration;
        if (reserved_control_messages == 0 or reserved_control_messages >= maximum_messages) return error.InvalidConfiguration;
        if (reserved_control_bytes == 0 or reserved_control_bytes >= maximum_bytes) return error.InvalidConfiguration;
        const slots = try allocator.alloc(Slot, maximum_messages);
        for (slots, 0..) |*slot, index| {
            slot.* = .{ .next = if (index + 1 < slots.len) @intCast(index + 1) else none };
        }
        return .{
            .allocator = allocator,
            .slots = slots,
            .maximum_bytes = maximum_bytes,
            .reserved_control_messages = reserved_control_messages,
            .reserved_control_bytes = reserved_control_bytes,
            .free_head = 0,
        };
    }

    pub fn deinit(self: *Queue) void {
        for (self.slots) |slot| if (slot.occupied) slot.message.payload.deinit();
        self.allocator.free(self.slots);
        self.* = undefined;
    }

    pub fn enqueue(self: *Queue, lane: Lane, payload: []const u8, reliability: frame.Reliability, channel: u8) !Id {
        if (payload.len == 0) return error.EmptyPayload;
        if (self.next_id == std.math.maxInt(Id)) return error.OutboundQueueIdExhausted;
        if (payload.len > self.maximum_bytes -| self.total_bytes) return error.OutboundQueueBytesExceeded;
        if (self.free_head == none) return error.OutboundQueueFull;
        if (lane == .application) {
            const control_index = @intFromEnum(Lane.control);
            const reserved_messages = self.reserved_control_messages -| self.counts[control_index];
            if (self.slots.len - self.countAll() <= reserved_messages) return error.OutboundQueueFull;
            const reserved_bytes = self.reserved_control_bytes -| self.bytes[control_index];
            const available_bytes = (self.maximum_bytes -| self.total_bytes) -| reserved_bytes;
            if (payload.len > available_bytes) return error.OutboundQueueBytesExceeded;
        }

        const owned = try payload_mod.BorrowedPayload.init(payload).toOwned(self.allocator);
        const index = self.free_head;
        const id = self.next_id;
        const slot = &self.slots[index];
        self.free_head = slot.next;
        slot.* = .{
            .message = .{ .id = id, .payload = owned, .reliability = reliability, .channel = channel },
            .occupied = true,
        };

        const lane_index = @intFromEnum(lane);
        if (self.tails[lane_index] == none) {
            self.heads[lane_index] = index;
        } else {
            self.slots[self.tails[lane_index]].next = index;
        }
        self.tails[lane_index] = index;
        self.counts[lane_index] += 1;
        self.bytes[lane_index] += payload.len;
        self.total_bytes += payload.len;
        self.next_id += 1;
        return id;
    }

    /// Transfers payload ownership to the caller.
    pub fn pop(self: *Queue, lane: Lane) ?Message {
        const lane_index = @intFromEnum(lane);
        const index = self.heads[lane_index];
        if (index == none) return null;
        const slot = &self.slots[index];
        const message = slot.message;
        self.heads[lane_index] = slot.next;
        if (self.heads[lane_index] == none) self.tails[lane_index] = none;
        self.counts[lane_index] -= 1;
        self.bytes[lane_index] -= message.payload.bytes.len;
        self.total_bytes -= message.payload.bytes.len;
        slot.occupied = false;
        slot.next = self.free_head;
        self.free_head = index;
        return message;
    }

    pub fn peek(self: *const Queue, lane: Lane) ?*const Message {
        const index = self.heads[@intFromEnum(lane)];
        return if (index == none) null else &self.slots[index].message;
    }

    pub fn iterator(self: *const Queue, lane: Lane) Iterator {
        return .{ .queue = self, .next_index = self.heads[@intFromEnum(lane)] };
    }

    pub fn cancel(self: *Queue, lane: Lane, id: Id) ?Message {
        const lane_index = @intFromEnum(lane);
        var previous: u32 = none;
        var index = self.heads[lane_index];
        while (index != none) {
            const slot = &self.slots[index];
            if (slot.message.id == id) {
                const next = slot.next;
                if (previous == none) self.heads[lane_index] = next else self.slots[previous].next = next;
                if (self.tails[lane_index] == index) self.tails[lane_index] = previous;
                const message = slot.message;
                self.counts[lane_index] -= 1;
                self.bytes[lane_index] -= message.payload.bytes.len;
                self.total_bytes -= message.payload.bytes.len;
                slot.occupied = false;
                slot.next = self.free_head;
                self.free_head = index;
                return message;
            }
            previous = index;
            index = slot.next;
        }
        return null;
    }

    pub fn count(self: Queue, lane: Lane) usize {
        return self.counts[@intFromEnum(lane)];
    }

    pub fn countAll(self: Queue) usize {
        return self.counts[0] + self.counts[1];
    }
};

test "control and application lanes are bounded FIFOs" {
    var queue = try Queue.init(std.testing.allocator, 3, 5, 1, 1);
    defer queue.deinit();

    var source = [_]u8{ 'a', 'b' };
    _ = try queue.enqueue(.application, &source, .reliable_ordered, 2);
    _ = try queue.enqueue(.control, "c", .reliable, 0);
    _ = try queue.enqueue(.application, "de", .unreliable, 3);
    source = @splat('x');

    try std.testing.expectEqual(@as(usize, 2), queue.count(.application));
    try std.testing.expectEqual(@as(usize, 1), queue.count(.control));
    try std.testing.expectEqual(@as(usize, 5), queue.total_bytes);

    const first = queue.pop(.application).?;
    defer first.payload.deinit();
    try std.testing.expectEqualStrings("ab", first.payload.bytes);
    try std.testing.expectEqual(frame.Reliability.reliable_ordered, first.reliability);
    try std.testing.expectEqual(@as(u8, 2), first.channel);
    _ = try queue.enqueue(.application, "f", .reliable, 0);

    const second = queue.pop(.application).?;
    defer second.payload.deinit();
    try std.testing.expectEqualStrings("de", second.payload.bytes);

    const third = queue.pop(.application).?;
    defer third.payload.deinit();
    try std.testing.expectEqualStrings("f", third.payload.bytes);

    const control = queue.pop(.control).?;
    defer control.payload.deinit();
    try std.testing.expectEqualStrings("c", control.payload.bytes);
    try std.testing.expectEqual(@as(usize, 0), queue.countAll());
    try std.testing.expectEqual(@as(usize, 0), queue.total_bytes);
}

test "queue rejection leaves ownership and accounting unchanged" {
    var queue = try Queue.init(std.testing.allocator, 2, 3, 1, 1);
    defer queue.deinit();

    _ = try queue.enqueue(.application, "ab", .reliable, 0);
    try std.testing.expectError(error.OutboundQueueBytesExceeded, queue.enqueue(.control, "xx", .reliable, 0));
    try std.testing.expectEqual(@as(usize, 1), queue.countAll());
    try std.testing.expectEqual(@as(usize, 2), queue.total_bytes);

    _ = try queue.enqueue(.control, "c", .reliable, 0);
    try std.testing.expectError(error.OutboundQueueBytesExceeded, queue.enqueue(.control, "x", .reliable, 0));
    try std.testing.expectEqual(@as(usize, 2), queue.countAll());
    try std.testing.expectEqual(@as(usize, 3), queue.total_bytes);
}

fn checkAllocationFailures(allocator: std.mem.Allocator) !void {
    var queue = try Queue.init(allocator, 2, 8, 1, 1);
    defer queue.deinit();
    _ = try queue.enqueue(.application, "first", .reliable_ordered, 0);
    _ = try queue.enqueue(.control, "x", .reliable, 0);
}

test "queue handles every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkAllocationFailures, .{});
}

test "application traffic cannot consume reserved control slots" {
    var queue = try Queue.init(std.testing.allocator, 3, 32, 1, 1);
    defer queue.deinit();

    _ = try queue.enqueue(.application, "a", .reliable, 0);
    _ = try queue.enqueue(.application, "b", .reliable, 0);
    try std.testing.expectError(error.OutboundQueueFull, queue.enqueue(.application, "c", .reliable, 0));
    _ = try queue.enqueue(.control, "c", .reliable, 0);

    const control = queue.pop(.control).?;
    control.payload.deinit();
    try std.testing.expectError(error.OutboundQueueFull, queue.enqueue(.application, "d", .reliable, 0));
}

test "application traffic cannot consume reserved control bytes" {
    var queue = try Queue.init(std.testing.allocator, 4, 6, 1, 2);
    defer queue.deinit();

    _ = try queue.enqueue(.application, "abcd", .reliable, 0);
    try std.testing.expectError(error.OutboundQueueBytesExceeded, queue.enqueue(.application, "x", .reliable, 0));
    _ = try queue.enqueue(.control, "yz", .reliable, 0);
    try std.testing.expectEqual(@as(usize, 4), queue.bytes[@intFromEnum(Lane.application)]);
    try std.testing.expectEqual(@as(usize, 2), queue.bytes[@intFromEnum(Lane.control)]);
}

test "cancellation preserves FIFO order and accounting" {
    var queue = try Queue.init(std.testing.allocator, 4, 16, 1, 1);
    defer queue.deinit();
    _ = try queue.enqueue(.application, "a", .reliable, 0);
    const canceled_id = try queue.enqueue(.application, "bb", .reliable, 0);
    _ = try queue.enqueue(.application, "c", .reliable, 0);

    const canceled = queue.cancel(.application, canceled_id).?;
    defer canceled.payload.deinit();
    try std.testing.expectEqualStrings("bb", canceled.payload.bytes);
    try std.testing.expectEqual(@as(usize, 2), queue.count(.application));
    try std.testing.expectEqual(@as(usize, 2), queue.total_bytes);
    const first = queue.pop(.application).?;
    defer first.payload.deinit();
    const second = queue.pop(.application).?;
    defer second.payload.deinit();
    try std.testing.expectEqualStrings("a", first.payload.bytes);
    try std.testing.expectEqualStrings("c", second.payload.bytes);
}
