const std = @import("std");

pub fn nowMilliseconds(io: std.Io) u64 {
    return millisecondsFromNanoseconds(std.Io.Clock.awake.now(io).nanoseconds);
}

pub fn after(io: std.Io, milliseconds: u32) std.Io.Timeout {
    const now = std.Io.Clock.awake.now(io);
    const delta = std.Io.Duration.fromMilliseconds(milliseconds);
    return .{ .deadline = .{
        .raw = .{ .nanoseconds = addNanoseconds(now.nanoseconds, delta.nanoseconds) },
        .clock = .awake,
    } };
}

pub fn deadline(start_ms: u64, duration_ms: u64) u64 {
    return start_ms +| duration_ms;
}

pub fn elapsed(now_ms: u64, start_ms: u64) u64 {
    return now_ms -| start_ms;
}

pub fn reached(now_ms: u64, deadline_ms: u64) bool {
    return now_ms >= deadline_ms;
}

pub fn atMilliseconds(milliseconds: u64) std.Io.Timeout {
    return .{ .deadline = .{
        .raw = .{ .nanoseconds = @as(i96, milliseconds) * std.time.ns_per_ms },
        .clock = .awake,
    } };
}

pub fn earliest(io: std.Io, a: std.Io.Timeout, b: std.Io.Timeout) std.Io.Timeout {
    const a_deadline = normalize(io, a);
    const b_deadline = normalize(io, b);
    if (a_deadline == .none) return b_deadline;
    if (b_deadline == .none) return a_deadline;
    const a_remaining = a_deadline.deadline.raw.nanoseconds -| a_deadline.deadline.clock.now(io).nanoseconds;
    const b_remaining = b_deadline.deadline.raw.nanoseconds -| b_deadline.deadline.clock.now(io).nanoseconds;
    return if (a_remaining <= b_remaining) a_deadline else b_deadline;
}

fn normalize(io: std.Io, timeout: std.Io.Timeout) std.Io.Timeout {
    return switch (timeout) {
        .none, .deadline => timeout,
        .duration => |duration| .{ .deadline = .{
            .raw = .{ .nanoseconds = addNanoseconds(duration.clock.now(io).nanoseconds, duration.raw.nanoseconds) },
            .clock = duration.clock,
        } },
    };
}

fn addNanoseconds(timestamp: i96, duration: i96) i96 {
    return timestamp +| duration;
}

fn millisecondsFromNanoseconds(nanoseconds: i96) u64 {
    const milliseconds = @divTrunc(@max(@as(i96, 0), nanoseconds), std.time.ns_per_ms);
    return @intCast(@min(milliseconds, @as(i96, std.math.maxInt(u64))));
}

test "earliest timeout preserves absolute protocol deadlines" {
    const early = atMilliseconds(10);
    const late = atMilliseconds(20);
    try std.testing.expectEqual(early.deadline.raw.nanoseconds, earliest(std.testing.io, late, early).deadline.raw.nanoseconds);
    try std.testing.expectEqual(early.deadline.raw.nanoseconds, earliest(std.testing.io, .none, early).deadline.raw.nanoseconds);
}

test "millisecond arithmetic saturates at both bounds" {
    try std.testing.expectEqual(std.math.maxInt(u64), deadline(std.math.maxInt(u64) - 5, 10));
    try std.testing.expectEqual(@as(u64, 0), elapsed(5, 10));
    try std.testing.expect(reached(std.math.maxInt(u64), deadline(std.math.maxInt(u64) - 5, 10)));
    try std.testing.expectEqual(@as(u64, 0), millisecondsFromNanoseconds(-1));
    try std.testing.expectEqual(std.math.maxInt(u64), millisecondsFromNanoseconds(std.math.maxInt(i96)));
    try std.testing.expectEqual(std.math.maxInt(i96), addNanoseconds(std.math.maxInt(i96) - 1, 10));
}

test "timeout normalization saturates without mixing clocks" {
    const huge: std.Io.Timeout = .{ .duration = .{ .raw = .max, .clock = .awake } };
    const normalized = earliest(std.testing.io, .none, huge);
    try std.testing.expectEqual(std.math.maxInt(i96), normalized.deadline.raw.nanoseconds);

    const protocol = atMilliseconds(std.math.maxInt(u64));
    const selected = earliest(std.testing.io, huge, protocol);
    try std.testing.expectEqual(protocol.deadline.raw.nanoseconds, selected.deadline.raw.nanoseconds);
}
