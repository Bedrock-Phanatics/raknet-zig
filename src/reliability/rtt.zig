const std = @import("std");

/// Integer RFC 6298-style estimator. Durations are monotonic milliseconds.
pub const Estimator = struct {
    smoothed_ms: u64 = 0,
    variation_ms: u64 = 0,
    initialized: bool = false,
    minimum_rto_ms: u32,
    maximum_rto_ms: u32,

    pub fn init(minimum_rto_ms: u32, maximum_rto_ms: u32) !Estimator {
        if (minimum_rto_ms == 0 or minimum_rto_ms > maximum_rto_ms) return error.InvalidConfiguration;
        return .{ .minimum_rto_ms = minimum_rto_ms, .maximum_rto_ms = maximum_rto_ms };
    }

    pub fn observe(self: *Estimator, raw_sample_ms: u64) void {
        const sample = @min(raw_sample_ms, self.maximum_rto_ms);
        if (!self.initialized) {
            self.smoothed_ms = sample;
            self.variation_ms = @max(@as(u64, 1), sample / 2);
            self.initialized = true;
            return;
        }
        const difference = if (self.smoothed_ms > sample) self.smoothed_ms - sample else sample - self.smoothed_ms;
        self.variation_ms = (3 * self.variation_ms + difference) / 4;
        self.smoothed_ms = (7 * self.smoothed_ms + sample) / 8;
    }

    pub fn rto(self: Estimator) u32 {
        if (!self.initialized) return @min(@max(@as(u32, 500), self.minimum_rto_ms), self.maximum_rto_ms);
        const calculated = self.smoothed_ms +| (4 *| self.variation_ms);
        return @intCast(@min(@max(calculated, self.minimum_rto_ms), self.maximum_rto_ms));
    }
};

test "RTO is smoothed and clamped" {
    var value = try Estimator.init(50, 5000);
    try std.testing.expectEqual(@as(u32, 500), value.rto());
    value.observe(100);
    try std.testing.expectEqual(@as(u32, 300), value.rto());
    for (0..20) |_| value.observe(100);
    try std.testing.expect(value.rto() >= 100 and value.rto() < 300);
    value.observe(100_000);
    try std.testing.expect(value.rto() <= 5000);
}
