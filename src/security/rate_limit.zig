const std = @import("std");

const time = @import("../util/time.zig");

pub const Entry = struct { key: u64 = 0, tokens: u64 = 0, updated_ms: u64 = 0, occupied: bool = false };
pub const Options = struct {
    tokens_per_second: u32,
    burst: u32,
    global_tokens_per_second: u32,
    global_burst: u32,
    pub fn validate(self: Options) !void {
        if (self.tokens_per_second == 0 or self.burst == 0 or self.global_tokens_per_second == 0 or self.global_burst == 0) return error.InvalidConfiguration;
    }
};

pub const Limiter = struct {
    entries: []Entry,
    options: Options,
    global_tokens: u64,
    global_updated_ms: u64,

    pub fn init(entries: []Entry, options: Options, now_ms: u64) !Limiter {
        if (entries.len == 0) return error.InvalidConfiguration;
        try options.validate();
        @memset(entries, .{});
        return .{ .entries = entries, .options = options, .global_tokens = scaled(options.global_burst), .global_updated_ms = now_ms };
    }
    pub fn allow(self: *Limiter, key: u64, cost: u32, now_ms: u64) bool {
        if (cost == 0 or cost > self.options.burst or cost > self.options.global_burst) return false;
        refill(&self.global_tokens, &self.global_updated_ms, now_ms, self.options.global_tokens_per_second, self.options.global_burst);
        const slot = &self.entries[key % self.entries.len];
        if (!slot.occupied or slot.key != key) slot.* = .{ .key = key, .tokens = scaled(self.options.burst), .updated_ms = now_ms, .occupied = true };
        refill(&slot.tokens, &slot.updated_ms, now_ms, self.options.tokens_per_second, self.options.burst);
        const needed = scaled(cost);
        if (slot.tokens < needed or self.global_tokens < needed) return false;
        slot.tokens -= needed;
        self.global_tokens -= needed;
        return true;
    }
};

const scale: u64 = 1000;
fn scaled(value: u32) u64 {
    return @as(u64, value) * scale;
}
fn refill(tokens: *u64, updated_ms: *u64, now_ms: u64, per_second: u32, burst: u32) void {
    const elapsed_ms = time.elapsed(now_ms, updated_ms.*);
    if (elapsed_ms == 0) return;
    tokens.* = @min(scaled(burst), tokens.* +| (elapsed_ms *| per_second));
    updated_ms.* = now_ms;
}

test "per-source and global buckets are bounded" {
    var entries: [4]Entry = undefined;
    var limiter = try Limiter.init(&entries, .{ .tokens_per_second = 2, .burst = 2, .global_tokens_per_second = 3, .global_burst = 3 }, 0);
    try std.testing.expect(limiter.allow(1, 1, 0));
    try std.testing.expect(limiter.allow(1, 1, 0));
    try std.testing.expect(!limiter.allow(1, 1, 0));
    try std.testing.expect(limiter.allow(2, 1, 0));
    try std.testing.expect(!limiter.allow(3, 1, 0));
    try std.testing.expect(limiter.allow(1, 1, 500));
}

test "long clock advances saturate token refill" {
    var entries: [1]Entry = undefined;
    var limiter = try Limiter.init(&entries, .{ .tokens_per_second = 1, .burst = 1, .global_tokens_per_second = 1, .global_burst = 1 }, 0);
    try std.testing.expect(limiter.allow(1, 1, 0));
    try std.testing.expect(limiter.allow(1, 1, std.math.maxInt(u64)));
}
