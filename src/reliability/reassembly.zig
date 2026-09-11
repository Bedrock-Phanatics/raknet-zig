const std = @import("std");

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

/// Reassembles fragments with bounded count, payload bytes, concurrency, metadata, and lifetime.
pub const Reassembler = struct {
    allocator: std.mem.Allocator,
    limits: Limits,
    assemblies: std.AutoHashMapUnmanaged(u16, Assembly) = .empty,
    total_bytes: usize = 0,

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
    pub fn push(self: *Reassembler, id: u16, count: u32, index: u32, payload: []const u8, now_ms: u64) !?[]u8 {
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
        const copy = try self.allocator.dupe(u8, payload);
        slot.data = copy;
        assembly.received += 1;
        assembly.bytes += payload.len;
        self.total_bytes += payload.len;
        assembly.updated_ms = now_ms;
        if (assembly.received != assembly.fragments.len) return null;
        return try self.finish(id, assembly);
    }

    fn finish(self: *Reassembler, id: u16, assembly: *Assembly) ![]u8 {
        const output = try self.allocator.alloc(u8, assembly.bytes);
        errdefer self.allocator.free(output);
        var offset: usize = 0;
        for (assembly.fragments) |fragment| {
            const bytes = fragment.data orelse return error.InternalInvariant;
            @memcpy(output[offset..][0..bytes.len], bytes);
            offset += bytes.len;
        }
        self.remove(id);
        return output;
    }

    pub fn expire(self: *Reassembler, now_ms: u64, maximum_work: usize) usize {
        var expired: usize = 0;
        var inspected: usize = 0;
        var iterator = self.assemblies.iterator();
        while (iterator.next()) |entry| {
            if (inspected >= maximum_work) break;
            inspected += 1;
            if (now_ms -| entry.value_ptr.updated_ms < self.limits.timeout_ms) continue;
            self.freeAssembly(entry.value_ptr);
            _ = self.assemblies.remove(entry.key_ptr.*);
            expired += 1;
        }
        return expired;
    }

    fn remove(self: *Reassembler, id: u16) void {
        const removed = self.assemblies.fetchRemove(id) orelse return;
        var assembly = removed.value;
        self.freeAssembly(&assembly);
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
    defer std.testing.allocator.free(complete);
    try std.testing.expectEqualStrings("hello world", complete);
    try std.testing.expect((try value.push(3, 2, 0, "x", 5)) == null);
    try std.testing.expectError(error.SplitIdCollision, value.push(3, 3, 1, "y", 6));
    try std.testing.expect((try value.push(4, 2, 0, "z", 7)) == null);
    try std.testing.expectEqual(@as(usize, 1), value.expire(100, 2));
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
    defer allocator.free(complete);
    try std.testing.expectEqualStrings("hello world", complete);
    try std.testing.expectEqual(@as(usize, 0), value.assemblies.count());
    try std.testing.expectEqual(@as(usize, 0), value.total_bytes);
}

test "split reassembly handles every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkReassemblyAllocationFailures, .{});
}
