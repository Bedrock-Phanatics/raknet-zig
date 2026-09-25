const std = @import("std");

const uint24 = @import("../util/uint24.zig");

pub const Result = union(enum) {
    accepted: ?Gap,
    duplicate,
    stale,
    too_far_ahead,
    ambiguous,
};

pub const Gap = struct { first: u32, last: u32, count: usize };

pub const Window = struct {
    present: []bool,
    expected: u32 = 0,
    next: u32 = 0,

    pub fn init(storage: []bool, initial_expected: u32) !Window {
        if (storage.len == 0 or storage.len >= uint24.half_range) return error.InvalidWindowSize;
        @memset(storage, false);
        const expected = uint24.normalize(initial_expected);
        return .{ .present = storage, .expected = expected, .next = expected };
    }

    pub fn inspect(self: *const Window, raw_index: u32, maximum_gap_report: usize) Result {
        const index = uint24.normalize(raw_index);
        const forward = uint24.distance(self.expected, index);
        if (forward == uint24.half_range) return .ambiguous;
        if (forward >= uint24.half_range) return .stale;
        if (forward >= self.present.len) return .too_far_ahead;

        const slot = index % self.present.len;
        if (forward != 0 and self.present[slot]) return .duplicate;
        const skipped = uint24.distance(self.next, index);
        if (skipped == 0 or skipped >= uint24.half_range) return .{ .accepted = null };
        const count = @min(skipped, maximum_gap_report);
        return .{ .accepted = .{
            .first = uint24.sub(index, @intCast(count)),
            .last = uint24.sub(index, 1),
            .count = count,
        } };
    }

    pub fn add(self: *Window, raw_index: u32, maximum_gap_report: usize) Result {
        const result = self.inspect(raw_index, maximum_gap_report);
        const index = uint24.normalize(raw_index);
        const forward = uint24.distance(self.expected, index);
        switch (result) {
            .accepted => {
                if (uint24.distance(self.next, index) < uint24.half_range) self.next = uint24.add(index, 1);
                if (forward != 0) {
                    self.present[index % self.present.len] = true;
                    return result;
                }
            },
            else => return result,
        }

        self.advanceOne();
        while (self.present[self.expected % self.present.len]) self.advanceOne();
        return result;
    }

    fn advanceOne(self: *Window) void {
        self.present[self.expected % self.present.len] = false;
        self.expected = uint24.add(self.expected, 1);
    }
};

test "receive window wraps and bounds malicious jumps" {
    var slots: [8]bool = undefined;
    var window = try Window.init(&slots, 0xfffffe);
    try std.testing.expect(window.add(0, 4) == .accepted);
    try std.testing.expectEqual(@as(u32, 0xfffffe), window.expected);
    try std.testing.expect(window.add(0, 4) == .duplicate);
    try std.testing.expect(window.add(0xfffffe, 4) == .accepted);
    try std.testing.expectEqual(@as(u32, 0xffffff), window.expected);
    try std.testing.expect(window.add(0xffffff, 4) == .accepted);
    try std.testing.expectEqual(@as(u32, 1), window.expected);
    try std.testing.expect(window.add(1000, 4) == .too_far_ahead);
    try std.testing.expect(window.add(0xffffff, 4) == .stale);
}

test "gap reports have a strict work cap" {
    var slots: [64]bool = undefined;
    var window = try Window.init(&slots, 10);
    const result = window.add(50, 5);
    try std.testing.expectEqual(@as(usize, 5), result.accepted.?.count);
    try std.testing.expectEqual(@as(u32, 45), result.accepted.?.first);
}
test "gaps are reported once as they appear" {
    var slots: [64]bool = undefined;
    var window = try Window.init(&slots, 0);
    try std.testing.expectEqual(@as(?Gap, null), window.add(0, 64).accepted);
    try std.testing.expectEqual(Gap{ .first = 1, .last = 2, .count = 2 }, window.add(3, 64).accepted.?);
    try std.testing.expectEqual(@as(?Gap, null), window.add(4, 64).accepted);
    try std.testing.expectEqual(Gap{ .first = 5, .last = 5, .count = 1 }, window.add(6, 64).accepted.?);
    try std.testing.expectEqual(@as(?Gap, null), window.add(1, 64).accepted);
    try std.testing.expectEqual(@as(?Gap, null), window.add(2, 64).accepted);
    try std.testing.expectEqual(@as(u32, 5), window.expected);
    try std.testing.expectEqual(@as(?Gap, null), window.add(5, 64).accepted);
    try std.testing.expectEqual(@as(u32, 7), window.expected);

    window = try Window.init(&slots, 0xfffffe);
    try std.testing.expectEqual(Gap{ .first = 0xfffffe, .last = 0xffffff, .count = 2 }, window.add(0, 64).accepted.?);
    try std.testing.expectEqual(@as(?Gap, null), window.add(1, 64).accepted);
}

test "inspection is non-mutating" {
    var slots: [8]bool = undefined;
    var window = try Window.init(&slots, 4);
    const result = window.inspect(6, 2);
    try std.testing.expect(result == .accepted);
    try std.testing.expectEqual(@as(u32, 4), window.expected);
    try std.testing.expect(!slots[6]);
}
