const std = @import("std");

/// A hard byte quota layered over another allocator.
///
/// This allocator is intentionally single-owner. RakNet listeners and their
/// sessions follow the same rule, avoiding synchronization in the packet path.
pub const QuotaAllocator = struct {
    backing: std.mem.Allocator,
    maximum_bytes: usize,
    used_bytes: usize = 0,

    pub fn init(backing: std.mem.Allocator, maximum_bytes: usize) QuotaAllocator {
        return .{ .backing = backing, .maximum_bytes = maximum_bytes };
    }

    pub fn allocator(self: *QuotaAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn reserve(self: *QuotaAllocator, amount: usize) bool {
        if (amount > self.maximum_bytes -| self.used_bytes) return false;
        self.used_bytes += amount;
        return true;
    }

    fn release(self: *QuotaAllocator, amount: usize) void {
        std.debug.assert(amount <= self.used_bytes);
        self.used_bytes -= amount;
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *QuotaAllocator = @ptrCast(@alignCast(raw));
        if (!self.reserve(len)) return null;
        return self.backing.rawAlloc(len, alignment, ret_addr) orelse {
            self.release(len);
            return null;
        };
    }

    fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *QuotaAllocator = @ptrCast(@alignCast(raw));
        const growth = new_len -| memory.len;
        if (growth != 0 and !self.reserve(growth)) return false;
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) {
            if (growth != 0) self.release(growth);
            return false;
        }
        if (new_len < memory.len) self.release(memory.len - new_len);
        return true;
    }

    fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *QuotaAllocator = @ptrCast(@alignCast(raw));
        const growth = new_len -| memory.len;
        if (growth != 0 and !self.reserve(growth)) return null;
        const result = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse {
            if (growth != 0) self.release(growth);
            return null;
        };
        if (new_len < memory.len) self.release(memory.len - new_len);
        return result;
    }

    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *QuotaAllocator = @ptrCast(@alignCast(raw));
        self.backing.rawFree(memory, alignment, ret_addr);
        self.release(memory.len);
    }

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };
};

test "quota rejects growth and returns capacity after free" {
    var quota = QuotaAllocator.init(std.testing.allocator, 32);
    const allocator = quota.allocator();
    const first = try allocator.alloc(u8, 24);
    try std.testing.expectEqual(@as(usize, 24), quota.used_bytes);
    try std.testing.expectError(error.OutOfMemory, allocator.alloc(u8, 9));
    try std.testing.expect(allocator.resize(first, 32));
    try std.testing.expectEqual(@as(usize, 32), quota.used_bytes);
    const grown: []u8 = first.ptr[0..32];
    try std.testing.expect(!allocator.resize(grown, 33));
    allocator.free(grown);
    try std.testing.expectEqual(@as(usize, 0), quota.used_bytes);
}
