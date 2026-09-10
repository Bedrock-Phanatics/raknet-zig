const std = @import("std");

pub const Value = u32;
pub const mask: Value = 0x00ff_ffff;
pub const modulus: Value = 0x0100_0000;
pub const half_range: Value = 0x0080_0000;

pub fn normalize(value: anytype) Value {
    return @as(Value, @intCast(value)) & mask;
}

pub fn add(value: Value, delta: Value) Value {
    return (value +% delta) & mask;
}

pub fn sub(value: Value, delta: Value) Value {
    return (value -% delta) & mask;
}

/// Forward modular distance from `from` to `to`.
pub fn distance(from: Value, to: Value) Value {
    return (to -% from) & mask;
}

/// Half-range modular comparison. Exactly half a cycle is deliberately unordered.
pub fn isNewer(candidate: Value, reference: Value) bool {
    const d = distance(reference, candidate);
    return d != 0 and d < half_range;
}

pub fn isOlder(candidate: Value, reference: Value) bool {
    return isNewer(reference, candidate);
}

test "wrap-safe comparisons" {
    try std.testing.expect(isNewer(0, mask));
    try std.testing.expect(isOlder(mask, 0));
    try std.testing.expectEqual(@as(Value, 2), distance(mask, 1));
    try std.testing.expect(!isNewer(half_range, 0));
    try std.testing.expect(!isOlder(half_range, 0));
}
