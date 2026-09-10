const std = @import("std");
const uint24 = @import("../util/uint24.zig");

/// Byte-counting AIMD window with one loss response per recovery block.
pub const Controller = struct {
    mtu: u32,
    window: u64,
    threshold: u64 = std.math.maxInt(u32),
    in_flight: u64 = 0,
    recovery_until: u32 = 0,
    recovering: bool = false,

    pub fn init(mtu: u16) !Controller {
        if (mtu < 400) return error.InvalidMtu;
        const initial = @min(@as(u64, 10) * mtu, @max(@as(u64, 2) * mtu, 14_600));
        return .{ .mtu = mtu, .window = initial };
    }
    pub fn available(self: Controller) u64 {
        return self.window -| self.in_flight;
    }
    pub fn sent(self: *Controller, bytes: usize) !void {
        if (bytes > self.available()) return error.CongestionWindowFull;
        self.in_flight = try std.math.add(u64, self.in_flight, bytes);
    }
    pub fn cancel(self: *Controller, bytes: usize) void {
        self.in_flight -|= bytes;
    }
    pub fn acknowledged(self: *Controller, sequence: u32, bytes: usize) void {
        self.in_flight -|= bytes;
        if (self.recovering and uint24.isNewer(sequence, self.recovery_until)) self.recovering = false;
        if (self.window < self.threshold) self.window +|= @min(@as(u64, bytes), self.mtu) else {
            const increase = @max(@as(u64, 1), (@as(u64, self.mtu) * self.mtu) / @max(self.window, 1));
            self.window +|= increase;
        }
    }
    pub fn lost(self: *Controller, newest_sent: u32) void {
        if (self.recovering) return;
        self.threshold = @max(@as(u64, self.mtu), self.window / 2);
        self.window = self.threshold;
        self.recovery_until = newest_sent;
        self.recovering = true;
    }
    pub fn timeout(self: *Controller, newest_sent: u32) void {
        if (self.recovering) return;
        self.threshold = @max(@as(u64, self.mtu), self.window / 2);
        self.window = self.mtu;
        self.recovery_until = newest_sent;
        self.recovering = true;
    }
};

test "congestion accounting cannot underflow and backs off once" {
    var c = try Controller.init(1200);
    try c.sent(1000);
    c.acknowledged(0, 1000);
    try std.testing.expectEqual(@as(u64, 13_000), c.window);
    c.lost(10);
    const reduced = c.window;
    c.lost(11);
    try std.testing.expectEqual(reduced, c.window);
    c.acknowledged(11, 999999);
    try std.testing.expectEqual(@as(u64, 0), c.in_flight);
}
