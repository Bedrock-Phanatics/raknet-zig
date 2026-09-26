const std = @import("std");

const uint24 = @import("../util/uint24.zig");

const empty_ref = std.math.maxInt(u32);
const class_count = 4;
const class_sizes = [class_count]usize{ 64, 256, 576, 1200 };

const Channel = struct {
    expected: u32 = 0,
    ring: []u32 = &.{},
};

const Packet = struct {
    occupied: bool = false,
    index: u32 = 0,
    channel: u8 = 0,
    class_index: u8 = class_count,
    data_len: usize = 0,
    storage: []u8 = &.{},
    next: u32 = empty_ref,
};

pub const RetainedPayload = struct {
    owner: *Store,
    packet_ref: u32,
    bytes: []const u8,

    pub fn deinit(self: RetainedPayload) void {
        self.owner.releasePacket(self.packet_ref);
    }
};

pub const Sequenced = struct {
    latest: u32 = 0,
    initialized: bool = false,

    pub fn accept(self: *Sequenced, raw_index: u32) bool {
        const index = uint24.normalize(raw_index);
        if (self.initialized and !uint24.isNewer(index, self.latest)) return false;
        self.latest = index;
        self.initialized = true;
        return true;
    }
};

pub const Store = struct {
    allocator: std.mem.Allocator,
    channels: []Channel,
    packets: []Packet = &.{},
    maximum_entries: usize,
    maximum_bytes: usize,
    maximum_window: usize,
    total_bytes: usize = 0,
    packet_count: usize = 0,
    free_head: u32 = empty_ref,
    class_heads: [class_count]u32 = @splat(empty_ref),
    retained_bytes: usize = 0,

    pub fn init(allocator: std.mem.Allocator, channel_count: usize, maximum_entries: usize, maximum_bytes: usize, maximum_window: usize) !Store {
        if (channel_count == 0 or
            channel_count > 256 or
            maximum_entries == 0 or
            maximum_entries >= empty_ref or
            maximum_bytes == 0 or
            maximum_window == 0 or
            maximum_window >= uint24.half_range) return error.InvalidConfiguration;
        const channels = try allocator.alloc(Channel, channel_count);
        @memset(channels, .{});
        return .{
            .allocator = allocator,
            .channels = channels,
            .maximum_entries = maximum_entries,
            .maximum_bytes = maximum_bytes,
            .maximum_window = maximum_window,
        };
    }

    pub fn deinit(self: *Store) void {
        for (self.packets) |packet| if (packet.storage.len != 0) self.allocator.free(packet.storage);
        for (self.channels) |channel| if (channel.ring.len != 0) self.allocator.free(channel.ring);
        self.allocator.free(self.packets);
        self.allocator.free(self.channels);
        self.* = undefined;
    }

    pub fn count(self: Store) usize {
        return self.packet_count;
    }

    pub fn payloadBytes(self: Store) usize {
        return self.total_bytes;
    }

    pub fn metadataCapacity(self: Store) usize {
        var bytes = self.channels.len * @sizeOf(Channel) + self.packets.len * @sizeOf(Packet);
        for (self.channels) |channel| bytes += channel.ring.len * @sizeOf(u32);
        return bytes;
    }

    pub fn retainedCapacity(self: Store) usize {
        return self.retained_bytes;
    }

    pub fn expectedIndex(self: Store, channel: u8) !u32 {
        if (channel >= self.channels.len) return error.InvalidOrderChannel;
        return self.channels[channel].expected;
    }

    pub fn peek(self: Store, channel: u8, raw_index: u32) ?[]const u8 {
        if (channel >= self.channels.len) return null;
        const state = self.channels[channel];
        if (state.ring.len == 0) return null;
        const index = uint24.normalize(raw_index);
        const packet_ref = state.ring[index % self.maximum_window];
        if (packet_ref == empty_ref) return null;
        const packet = self.packets[packet_ref];
        if (packet.channel != channel or packet.index != index) return null;
        if (!packet.occupied) return null;
        return packet.storage[0..packet.data_len];
    }

    pub fn advanceBorrowed(self: *Store, channel: u8, index: u32) !void {
        if (try self.expectedIndex(channel) != uint24.normalize(index)) return error.UnexpectedOrderIndex;
        self.channels[channel].expected = uint24.add(index, 1);
    }

    pub fn push(self: *Store, channel: u8, raw_index: u32, payload: []const u8) !bool {
        if (channel >= self.channels.len) return error.InvalidOrderChannel;
        const index = uint24.normalize(raw_index);
        const forward = uint24.distance(self.channels[channel].expected, index);
        if (forward >= uint24.half_range) return false;
        if (forward >= self.maximum_window) return error.OrderWindowExceeded;
        if (self.peek(channel, index) != null) return false;
        if (self.packet_count == self.maximum_entries) return error.OrderQueueFull;
        if (payload.len > self.maximum_bytes -| self.total_bytes) return error.OrderBytesExceeded;

        if (self.channels[channel].ring.len == 0) {
            const ring = try self.allocator.alloc(u32, self.maximum_window);
            @memset(ring, empty_ref);
            self.channels[channel].ring = ring;
        }
        const packet_ref = try self.acquirePacket(payload.len);
        errdefer self.releasePacket(packet_ref);
        const ring_slot = index % self.maximum_window;
        const previous_ref = self.channels[channel].ring[ring_slot];
        if (previous_ref != empty_ref) {
            const previous = self.packets[previous_ref];
            if (previous.occupied and previous.channel == channel and previous.index != index) return error.InternalInvariant;
        }
        const packet = &self.packets[packet_ref];
        @memcpy(packet.storage[0..payload.len], payload);
        packet.occupied = true;
        packet.index = index;
        packet.channel = channel;
        packet.data_len = payload.len;
        self.channels[channel].ring[ring_slot] = packet_ref;
        self.packet_count += 1;
        self.total_bytes += payload.len;
        return true;
    }

    pub fn pop(self: *Store, channel: u8) !?RetainedPayload {
        if (channel >= self.channels.len) return error.InvalidOrderChannel;
        const state = &self.channels[channel];
        if (state.ring.len == 0) return null;
        const index = state.expected;
        const ring_slot = index % self.maximum_window;
        const packet_ref = state.ring[ring_slot];
        if (packet_ref == empty_ref) return null;
        const packet = &self.packets[packet_ref];
        if (!packet.occupied or packet.channel != channel or packet.index != index) return null;
        const data = packet.storage[0..packet.data_len];
        state.ring[ring_slot] = empty_ref;
        packet.occupied = false;
        state.expected = uint24.add(index, 1);
        self.packet_count -= 1;
        self.total_bytes -= data.len;
        return .{ .owner = self, .packet_ref = packet_ref, .bytes = data };
    }

    fn acquirePacket(self: *Store, len: usize) !u32 {
        const class = classIndex(len);
        var packet_ref: u32 = undefined;
        if (class) |class_index| {
            if (self.class_heads[class_index] != empty_ref) {
                packet_ref = self.class_heads[class_index];
                self.class_heads[class_index] = self.packets[packet_ref].next;
                return packet_ref;
            }
            packet_ref = try self.takeUnused() orelse return error.OrderQueueFull;
            const storage = self.allocator.alloc(u8, class_sizes[class_index]) catch |err| {
                self.returnUnused(packet_ref);
                return err;
            };
            self.packets[packet_ref].storage = storage;
            self.packets[packet_ref].class_index = @intCast(class_index);
            self.retained_bytes += storage.len;
            return packet_ref;
        }
        packet_ref = try self.takeUnused() orelse return error.OrderQueueFull;
        const storage = self.allocator.alloc(u8, len) catch |err| {
            self.returnUnused(packet_ref);
            return err;
        };
        self.packets[packet_ref].storage = storage;
        self.packets[packet_ref].class_index = class_count;
        self.retained_bytes += storage.len;
        return packet_ref;
    }

    fn releasePacket(self: *Store, packet_ref: u32) void {
        const packet = &self.packets[packet_ref];
        packet.occupied = false;
        packet.data_len = 0;
        if (packet.class_index < class_count) {
            packet.next = self.class_heads[packet.class_index];
            self.class_heads[packet.class_index] = packet_ref;
            return;
        }
        if (packet.storage.len != 0) {
            self.retained_bytes -= packet.storage.len;
            self.allocator.free(packet.storage);
            packet.storage = &.{};
        }
        self.returnUnused(packet_ref);
    }

    fn takeUnused(self: *Store) !?u32 {
        if (self.free_head == empty_ref) try self.grow();
        if (self.free_head == empty_ref) return null;
        const packet_ref = self.free_head;
        self.free_head = self.packets[packet_ref].next;
        return packet_ref;
    }

    fn grow(self: *Store) !void {
        const old_len = self.packets.len;
        if (old_len == self.maximum_entries) return;
        const new_len = @min(self.maximum_entries, @max(16, old_len * 2));
        self.packets = if (old_len == 0) try self.allocator.alloc(Packet, new_len) else try self.allocator.realloc(self.packets, new_len);
        for (self.packets[old_len..], old_len..) |*packet, index| packet.* = .{ .next = if (index + 1 < new_len) @intCast(index + 1) else self.free_head };
        self.free_head = @intCast(old_len);
    }

    fn returnUnused(self: *Store, packet_ref: u32) void {
        self.packets[packet_ref] = .{ .next = self.free_head };
        self.free_head = packet_ref;
    }
};

fn classIndex(len: usize) ?usize {
    for (class_sizes, 0..) |size, index| if (len <= size) return index;
    return null;
}

test "sequenced packets use modular ordering" {
    var sequence: Sequenced = .{};
    try std.testing.expect(sequence.accept(0xffffff));
    try std.testing.expect(sequence.accept(0));
    try std.testing.expect(!sequence.accept(0xffffff));
    try std.testing.expect(!sequence.accept(0));
}

test "packet metadata grows on demand up to the limit" {
    var store = try Store.init(std.testing.allocator, 1, 40, 4096, 64);
    defer store.deinit();
    try std.testing.expectEqual(@as(usize, 0), store.packets.len);
    for (1..41) |index| try std.testing.expect(try store.push(0, @intCast(index), "x"));
    try std.testing.expectEqual(@as(usize, 40), store.packets.len);
    try std.testing.expectError(error.OrderQueueFull, store.push(0, 41, "x"));
    try store.advanceBorrowed(0, 0);
    var delivered: usize = 0;
    while (try store.pop(0)) |owned| : (delivered += 1) owned.deinit();
    try std.testing.expectEqual(@as(usize, 40), delivered);
    try std.testing.expect(try store.push(0, 42, "y"));
}

test "global quotas span channels and in-order fast path advances" {
    var store = try Store.init(std.testing.allocator, 2, 2, 8, 8);
    defer store.deinit();
    try store.advanceBorrowed(0, 0);
    try std.testing.expectEqual(@as(u32, 1), try store.expectedIndex(0));
    try std.testing.expect(try store.push(0, 2, "a"));
    try std.testing.expect(try store.push(1, 1, "b"));
    try std.testing.expectError(error.OrderQueueFull, store.push(1, 2, "c"));
}

test "circular slots reject stale wrap aliases" {
    var store = try Store.init(std.testing.allocator, 1, 4, 16, 4);
    defer store.deinit();
    try std.testing.expect(try store.push(0, 3, "old"));
    try std.testing.expectError(error.OrderWindowExceeded, store.push(0, 7, "new"));
    try std.testing.expectEqualStrings("old", store.peek(0, 3).?);
    try std.testing.expect(store.peek(0, 7) == null);
}

test "size-class payload storage is reused" {
    const QuotaAllocator = @import("../util/quota_allocator.zig").QuotaAllocator;
    var quota = QuotaAllocator.init(std.testing.allocator, std.math.maxInt(usize));
    var store = try Store.init(quota.allocator(), 1, 2, 64, 4);
    defer store.deinit();
    try std.testing.expect(try store.push(0, 1, "first"));
    try store.advanceBorrowed(0, 0);
    const first = (try store.pop(0)).?;
    first.deinit();
    const retained = quota.used_bytes;
    quota.maximum_bytes = retained;
    try std.testing.expect(try store.push(0, 2, "second"));
    try std.testing.expectEqual(retained, quota.used_bytes);
}

fn checkOrderedAllocationFailures(allocator: std.mem.Allocator) !void {
    var store = try Store.init(allocator, 2, 4, 64, 8);
    defer store.deinit();
    try std.testing.expect(try store.push(0, 1, "retained"));
    try std.testing.expectEqual(@as(usize, 1), store.count());
    try std.testing.expectEqual(@as(usize, 8), store.total_bytes);
}

test "ordered retention handles every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkOrderedAllocationFailures, .{});
}
