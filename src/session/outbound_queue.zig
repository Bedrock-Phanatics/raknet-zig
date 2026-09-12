const std = @import("std");
const frame = @import("../protocol/frame.zig");
const payload_mod = @import("../payload.zig");

const none = std.math.maxInt(u32);

pub const Lane = enum(u1) { control, application };

pub const Message = struct {
    payload: payload_mod.OwnedPayload,
    reliability: frame.Reliability,
    channel: u8,
};

const Slot = struct {
    message: Message = undefined,
    next: u32 = none,
    occupied: bool = false,
};

/// Two FIFO lanes backed by one bounded slot and byte pool.
pub const Queue = struct {
    allocator: std.mem.Allocator,
    slots: []Slot,
    maximum_bytes: usize,
    total_bytes: usize = 0,
    free_head: u32,
    heads: [2]u32 = @splat(none),
    tails: [2]u32 = @splat(none),
    counts: [2]usize = @splat(0),

    pub fn init(allocator: std.mem.Allocator, maximum_messages: usize, maximum_bytes: usize) !Queue {
        if (maximum_messages == 0 or maximum_messages >= none or maximum_bytes == 0) return error.InvalidConfiguration;
        const slots = try allocator.alloc(Slot, maximum_messages);
        for (slots, 0..) |*slot, index| {
            slot.* = .{ .next = if (index + 1 < slots.len) @intCast(index + 1) else none };
        }
        return .{
            .allocator = allocator,
            .slots = slots,
            .maximum_bytes = maximum_bytes,
            .free_head = 0,
        };
    }

    pub fn deinit(self: *Queue) void {
        for (self.slots) |slot| if (slot.occupied) slot.message.payload.deinit();
        self.allocator.free(self.slots);
        self.* = undefined;
    }

    pub fn enqueue(self: *Queue, lane: Lane, payload: []const u8, reliability: frame.Reliability, channel: u8) !void {
        if (payload.len == 0) return error.EmptyPayload;
        if (payload.len > self.maximum_bytes -| self.total_bytes) return error.OutboundQueueBytesExceeded;
        if (self.free_head == none) return error.OutboundQueueFull;

        const owned = try payload_mod.BorrowedPayload.init(payload).toOwned(self.allocator);
        const index = self.free_head;
        const slot = &self.slots[index];
        self.free_head = slot.next;
        slot.* = .{
            .message = .{ .payload = owned, .reliability = reliability, .channel = channel },
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
        self.total_bytes += payload.len;
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

    pub fn count(self: Queue, lane: Lane) usize {
        return self.counts[@intFromEnum(lane)];
    }

    pub fn countAll(self: Queue) usize {
        return self.counts[0] + self.counts[1];
    }
};

test "control and application lanes are bounded FIFOs" {
    var queue = try Queue.init(std.testing.allocator, 3, 5);
    defer queue.deinit();

    var source = [_]u8{ 'a', 'b' };
    try queue.enqueue(.application, &source, .reliable_ordered, 2);
    try queue.enqueue(.control, "c", .reliable, 0);
    try queue.enqueue(.application, "de", .unreliable, 3);
    source = @splat('x');

    try std.testing.expectEqual(@as(usize, 2), queue.count(.application));
    try std.testing.expectEqual(@as(usize, 1), queue.count(.control));
    try std.testing.expectEqual(@as(usize, 5), queue.total_bytes);

    const first = queue.pop(.application).?;
    defer first.payload.deinit();
    try std.testing.expectEqualStrings("ab", first.payload.bytes);
    try std.testing.expectEqual(frame.Reliability.reliable_ordered, first.reliability);
    try std.testing.expectEqual(@as(u8, 2), first.channel);
    try queue.enqueue(.application, "f", .reliable, 0);

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
    var queue = try Queue.init(std.testing.allocator, 2, 3);
    defer queue.deinit();

    try queue.enqueue(.application, "ab", .reliable, 0);
    try std.testing.expectError(error.OutboundQueueBytesExceeded, queue.enqueue(.control, "xx", .reliable, 0));
    try std.testing.expectEqual(@as(usize, 1), queue.countAll());
    try std.testing.expectEqual(@as(usize, 2), queue.total_bytes);

    try queue.enqueue(.control, "c", .reliable, 0);
    try std.testing.expectError(error.OutboundQueueFull, queue.enqueue(.control, "x", .reliable, 0));
    try std.testing.expectEqual(@as(usize, 2), queue.countAll());
    try std.testing.expectEqual(@as(usize, 3), queue.total_bytes);
}

fn checkAllocationFailures(allocator: std.mem.Allocator) !void {
    var queue = try Queue.init(allocator, 2, 8);
    defer queue.deinit();
    try queue.enqueue(.application, "first", .reliable_ordered, 0);
    try queue.enqueue(.control, "x", .reliable, 0);
}

test "queue handles every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkAllocationFailures, .{});
}
