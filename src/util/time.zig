const std = @import("std");

pub fn nowMilliseconds(io: std.Io) u64 {
    const nanoseconds = std.Io.Clock.awake.now(io).nanoseconds;
    return @intCast(@max(@as(i96, 0), @divTrunc(nanoseconds, std.time.ns_per_ms)));
}

pub fn after(io: std.Io, milliseconds: u32) std.Io.Timeout {
    return .{ .deadline = .{
        .raw = std.Io.Clock.awake.now(io).addDuration(.fromMilliseconds(milliseconds)),
        .clock = .awake,
    } };
}

pub fn atMilliseconds(milliseconds: u64) std.Io.Timeout {
    return .{ .deadline = .{
        .raw = .{ .nanoseconds = @as(i96, milliseconds) * std.time.ns_per_ms },
        .clock = .awake,
    } };
}

pub fn earliest(io: std.Io, a: std.Io.Timeout, b: std.Io.Timeout) std.Io.Timeout {
    if (a == .none) return b;
    if (b == .none) return a;
    const a_deadline = a.toDeadline(io);
    const b_deadline = b.toDeadline(io);
    return if (a_deadline.deadline.raw.nanoseconds <= b_deadline.deadline.raw.nanoseconds) a_deadline else b_deadline;
}

test "earliest timeout preserves absolute protocol deadlines" {
    const early = atMilliseconds(10);
    const late = atMilliseconds(20);
    try std.testing.expectEqual(early.deadline.raw.nanoseconds, earliest(std.testing.io, late, early).deadline.raw.nanoseconds);
    try std.testing.expectEqual(early.deadline.raw.nanoseconds, earliest(std.testing.io, .none, early).deadline.raw.nanoseconds);
}
