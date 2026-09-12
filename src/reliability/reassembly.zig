const std = @import("std");
const BorrowedPayload = @import("../payload.zig").BorrowedPayload;
const OwnedPayload = @import("../payload.zig").OwnedPayload;

const Fragment = struct { data: ?[]u8 = null };
const Assembly = struct {
    count: u32,
    fragments: []Fragment,
    received: usize = 0,
    bytes: usize = 0,
    updated_ms: u64,
};

pub const Limits = struct {
    maximum_parts: usize,
    maximum_bytes: usize,
    maximum_concurrent: usize,
    maximum_total_bytes: usize,
    timeout_ms: u32,

    pub fn validate(self: Limits) !void {
        if (self.maximum_parts < 2 or self.maximum_bytes == 0 or self.maximum_concurrent == 0 or
            self.maximum_total_bytes < self.maximum_bytes or self.timeout_ms == 0) return error.InvalidConfiguration;
    }
};

pub const ExpiryBatch = struct { expired: usize, inspected: usize };

/// Reassembles fragments with bounded count, payload bytes, concurrency, metadata, and lifetime.
pub const Reassembler = struct {
    allocator: std.mem.Allocator,
    limits: Limits,
    assemblies: std.AutoHashMapUnmanaged(u16, Assembly) = .empty,
    total_bytes: usize = 0,
    next_deadline_ms: ?u64 = null,
    scan_index: u32 = 0,
    deadline_rebuild_remaining: usize = 0,
    deadline_rebuild_min: ?u64 = null,

    pub fn init(allocator: std.mem.Allocator, limits: Limits) !Reassembler {
        try limits.validate();
        const self: Reassembler = .{ .allocator = allocator, .limits = limits };
        return self;
    }

    pub fn deinit(self: *Reassembler) void {
        var iterator = self.assemblies.valueIterator();
        while (iterator.next()) |assembly| self.freeAssembly(assembly);
        self.assemblies.deinit(self.allocator);
        self.* = undefined;
    }

    /// Returns a newly allocated complete payload. The caller owns it.
    pub fn push(self: *Reassembler, id: u16, count: u32, index: u32, payload: []const u8, now_ms: u64) !?OwnedPayload {
        if (count < 2 or count > self.limits.maximum_parts or index >= count) return error.InvalidSplit;
        if (payload.len == 0 or payload.len > self.limits.maximum_bytes) return error.InvalidSplit;

        var entry = self.assemblies.getPtr(id);
        var created = false;
        if (entry == null) {
            if (self.assemblies.count() >= self.limits.maximum_concurrent) return error.TooManyAssemblies;
            const fragments = try self.allocator.alloc(Fragment, count);
            @memset(fragments, .{});
            errdefer self.allocator.free(fragments);
            try self.assemblies.put(self.allocator, id, .{ .count = count, .fragments = fragments, .updated_ms = now_ms });
            created = true;
            entry = self.assemblies.getPtr(id).?;
        }
        errdefer if (created) self.remove(id);

        const assembly = entry.?;
        if (assembly.count != count) {
            self.remove(id);
            return error.SplitIdCollision;
        }
        const slot = &assembly.fragments[index];
        if (slot.data) |existing| {
            if (std.mem.eql(u8, existing, payload)) {
                assembly.updated_ms = now_ms;
                self.recomputeNextDeadline();
                if (assembly.received == assembly.fragments.len) return try self.finish(id, assembly);
                return null;
            }
            self.remove(id);
            return error.ConflictingFragment;
        }
        if (payload.len > self.limits.maximum_bytes -| assembly.bytes or
            payload.len > self.limits.maximum_total_bytes -| self.total_bytes)
        {
            self.remove(id);
            return error.ReassemblyLimitExceeded;
        }
        const copy = try BorrowedPayload.init(payload).toOwned(self.allocator);
        slot.data = copy.bytes;
        assembly.received += 1;
        assembly.bytes += copy.bytes.len;
        self.total_bytes += copy.bytes.len;
        assembly.updated_ms = now_ms;
        if (assembly.received != assembly.fragments.len) {
            self.recomputeNextDeadline();
            return null;
        }
        return try self.finish(id, assembly);
    }

    fn finish(self: *Reassembler, id: u16, assembly: *Assembly) !OwnedPayload {
        const output = try self.allocator.alloc(u8, assembly.bytes);
        errdefer self.allocator.free(output);
        var offset: usize = 0;
        for (assembly.fragments) |fragment| {
            const bytes = fragment.data orelse return error.InternalInvariant;
            @memcpy(output[offset..][0..bytes.len], bytes);
            offset += bytes.len;
        }
        self.remove(id);
        return .{ .allocator = self.allocator, .bytes = output };
    }

    pub fn expire(self: *Reassembler, now_ms: u64, maximum_work: usize) ExpiryBatch {
        if (self.deadline_rebuild_remaining == 0) {
            self.deadline_rebuild_remaining = self.assemblies.count();
            self.deadline_rebuild_min = null;
        }
        var expired: usize = 0;
        var inspected: usize = 0;
        const visit_limit = @min(maximum_work, self.deadline_rebuild_remaining);
        const capacity = self.assemblies.capacity();
        const start_index = if (self.scan_index < capacity) self.scan_index else 0;
        var iterator = self.assemblies.iterator();
        iterator.index = start_index;
        var wrapped = false;
        while (inspected < visit_limit) {
            const entry = iterator.next() orelse {
                if (wrapped or start_index == 0) break;
                iterator = self.assemblies.iterator();
                wrapped = true;
                continue;
            };
            self.scan_index = if (iterator.index == capacity) 0 else iterator.index;
            inspected += 1;
            if (now_ms -| entry.value_ptr.updated_ms < self.limits.timeout_ms) {
                self.includeRebuiltDeadline(entry.value_ptr.updated_ms +| self.limits.timeout_ms);
                continue;
            }
            self.freeAssembly(entry.value_ptr);
            _ = self.assemblies.remove(entry.key_ptr.*);
            expired += 1;
        }
        self.deadline_rebuild_remaining -= inspected;
        if (self.deadline_rebuild_remaining == 0) self.next_deadline_ms = self.deadline_rebuild_min;
        return .{ .expired = expired, .inspected = inspected };
    }

    pub fn nextDeadline(self: Reassembler) ?u64 {
        return self.next_deadline_ms;
    }

    fn remove(self: *Reassembler, id: u16) void {
        const removed = self.assemblies.fetchRemove(id) orelse return;
        var assembly = removed.value;
        self.freeAssembly(&assembly);
        self.recomputeNextDeadline();
    }

    fn recomputeNextDeadline(self: *Reassembler) void {
        self.deadline_rebuild_remaining = 0;
        self.deadline_rebuild_min = null;
        self.next_deadline_ms = null;
        var iterator = self.assemblies.valueIterator();
        while (iterator.next()) |assembly| {
            const deadline = assembly.updated_ms +| self.limits.timeout_ms;
            self.next_deadline_ms = if (self.next_deadline_ms) |current| @min(current, deadline) else deadline;
        }
    }

    fn includeRebuiltDeadline(self: *Reassembler, deadline_ms: u64) void {
        self.deadline_rebuild_min = if (self.deadline_rebuild_min) |current| @min(current, deadline_ms) else deadline_ms;
    }

    fn freeAssembly(self: *Reassembler, assembly: *Assembly) void {
        for (assembly.fragments) |fragment| if (fragment.data) |data| self.allocator.free(data);
        self.total_bytes -= assembly.bytes;
        self.allocator.free(assembly.fragments);
    }
};

test "split assembly handles duplicates, conflicts, collision, and expiry" {
    const limits: Limits = .{ .maximum_parts = 4, .maximum_bytes = 16, .maximum_concurrent = 2, .maximum_total_bytes = 24, .timeout_ms = 10 };
    var value = try Reassembler.init(std.testing.allocator, limits);
    defer value.deinit();
    try std.testing.expect((try value.push(1, 2, 1, "world", 0)) == null);
    try std.testing.expect((try value.push(1, 2, 1, "world", 1)) == null);
    try std.testing.expectError(error.ConflictingFragment, value.push(1, 2, 1, "evil", 2));
    try std.testing.expect((try value.push(2, 2, 0, "hello ", 3)) == null);
    const complete = (try value.push(2, 2, 1, "world", 4)).?;
    defer complete.deinit();
    try std.testing.expectEqualStrings("hello world", complete.bytes);
    try std.testing.expect((try value.push(3, 2, 0, "x", 5)) == null);
    try std.testing.expectError(error.SplitIdCollision, value.push(3, 3, 1, "y", 6));
    try std.testing.expect((try value.push(4, 2, 0, "z", 7)) == null);
    try std.testing.expectEqual(@as(?u64, 17), value.nextDeadline());
    try std.testing.expectEqual(@as(usize, 1), value.expire(100, 2).expired);
    try std.testing.expectEqual(@as(?u64, null), value.nextDeadline());
}

test "small fragments detach from large receive buffers" {
    const limits: Limits = .{ .maximum_parts = 2, .maximum_bytes = 16, .maximum_concurrent = 1, .maximum_total_bytes = 16, .timeout_ms = 10 };
    var value = try Reassembler.init(std.testing.allocator, limits);
    defer value.deinit();

    var source: [4096]u8 = @splat(0);
    @memcpy(source[100..104], "tiny");
    try std.testing.expect((try value.push(7, 2, 0, source[100..104], 0)) == null);

    const retained = value.assemblies.getPtr(7).?.fragments[0].data.?;
    const retained_start = @intFromPtr(retained.ptr);
    const source_start = @intFromPtr(&source);
    try std.testing.expectEqual(@as(usize, 4), retained.len);
    try std.testing.expect(retained_start + retained.len <= source_start or retained_start >= source_start + source.len);

    @memset(&source, 0xaa);
    const complete = (try value.push(7, 2, 1, "!", 1)).?;
    defer complete.deinit();
    try std.testing.expectEqualStrings("tiny!", complete.bytes);
}

test "split limits reject before allocation" {
    var value = try Reassembler.init(std.testing.allocator, .{ .maximum_parts = 4, .maximum_bytes = 8, .maximum_concurrent = 1, .maximum_total_bytes = 8, .timeout_ms = 10 });
    defer value.deinit();
    try std.testing.expectError(error.InvalidSplit, value.push(1, 1, 0, "x", 0));
    try std.testing.expectError(error.InvalidSplit, value.push(1, 5, 0, "x", 0));
    try std.testing.expect((try value.push(1, 2, 0, "12345678", 0)) == null);
    try std.testing.expectError(error.TooManyAssemblies, value.push(2, 2, 0, "x", 0));
    try std.testing.expectError(error.ReassemblyLimitExceeded, value.push(1, 2, 1, "x", 0));
}
test "new assembly allocation failure leaves no retained state" {
    const QuotaAllocator = @import("../util/quota_allocator.zig").QuotaAllocator;
    var quota = QuotaAllocator.init(std.testing.allocator, 0);
    var value = try Reassembler.init(quota.allocator(), .{
        .maximum_parts = 4,
        .maximum_bytes = 16,
        .maximum_concurrent = 2,
        .maximum_total_bytes = 32,
        .timeout_ms = 10,
    });
    defer value.deinit();

    try std.testing.expectError(error.OutOfMemory, value.push(1, 2, 0, "first", 0));
    try std.testing.expectEqual(@as(usize, 0), value.assemblies.count());
    try std.testing.expectEqual(@as(usize, 0), value.total_bytes);
    try std.testing.expectEqual(@as(usize, 0), quota.used_bytes);

    quota.maximum_bytes = std.math.maxInt(usize);
    try std.testing.expect((try value.push(1, 2, 0, "first", 1)) == null);
    try std.testing.expectEqual(@as(usize, 1), value.assemblies.count());
    try std.testing.expectEqual(@as(usize, 5), value.total_bytes);
}
fn checkReassemblyAllocationFailures(allocator: std.mem.Allocator) !void {
    var value = try Reassembler.init(allocator, .{
        .maximum_parts = 4,
        .maximum_bytes = 32,
        .maximum_concurrent = 2,
        .maximum_total_bytes = 64,
        .timeout_ms = 10,
    });
    defer value.deinit();

    try std.testing.expect((try value.push(1, 2, 0, "hello ", 0)) == null);
    const complete = (try value.push(1, 2, 1, "world", 1)).?;
    defer complete.deinit();
    try std.testing.expectEqualStrings("hello world", complete.bytes);
    try std.testing.expectEqual(@as(usize, 0), value.assemblies.count());
    try std.testing.expectEqual(@as(usize, 0), value.total_bytes);
}

test "split reassembly handles every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkReassemblyAllocationFailures, .{});
}

test "bounded split expiry resumes fairly" {
    const count = 8;
    var value = try Reassembler.init(std.testing.allocator, .{
        .maximum_parts = 2,
        .maximum_bytes = count,
        .maximum_concurrent = count,
        .maximum_total_bytes = count,
        .timeout_ms = 10,
    });
    defer value.deinit();
    for (0..count) |id| try std.testing.expect((try value.push(@intCast(id), 2, 0, "x", 100)) == null);

    var iterator = value.assemblies.iterator();
    var last: ?u16 = null;
    while (iterator.next()) |entry| last = entry.key_ptr.*;
    value.assemblies.getPtr(last.?).?.updated_ms = 0;
    value.recomputeNextDeadline();

    var expired: usize = 0;
    for (0..count) |_| {
        const batch = value.expire(11, 1);
        try std.testing.expect(batch.inspected <= 1);
        expired += batch.expired;
    }
    try std.testing.expectEqual(@as(usize, 1), expired);
    try std.testing.expectEqual(@as(usize, count - 1), value.assemblies.count());
    try std.testing.expectEqual(@as(?u64, 110), value.nextDeadline());
}
