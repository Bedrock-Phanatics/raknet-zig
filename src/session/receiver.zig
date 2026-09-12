const std = @import("std");
const Config = @import("../config.zig").Config;
const frame = @import("../protocol/frame.zig");
const receive_window = @import("../reliability/receive_window.zig");
const reassembly = @import("../reliability/reassembly.zig");
const ordering = @import("../reliability/ordering.zig");
const ordered_store = @import("../reliability/ordered_store.zig");

pub const BorrowedPayload = @import("../payload.zig").BorrowedPayload;
pub const OwnedPayload = @import("../payload.zig").OwnedPayload;

pub const Receipt = struct { acknowledge: ?u32 = null, missing: ?receive_window.Gap = null, delivered: usize = 0 };
pub const DeliveryError = error{
    PeerProtocolFailure,
    ResourceLimitFailure,
    TransportFailure,
    ApplicationFailure,
    InternalFailure,
};
/// The payload expires when this function returns.
pub const DeliverFn = *const fn (context: *anyopaque, payload: BorrowedPayload) DeliveryError!void;

/// Single-owner connected receive state. It retains no slice into the datagram after `process` returns.
pub const Receiver = struct {
    allocator: std.mem.Allocator,
    config: Config,
    datagrams: receive_window.Window,
    reliable: receive_window.Window,
    datagram_storage: []bool,
    reliable_storage: []bool,
    splits: reassembly.Reassembler,
    ordered: ordered_store.Store,
    sequenced: []ordering.Sequenced,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Receiver {
        try config.validate();
        const datagram_storage = try allocator.alloc(bool, config.receive_window);
        errdefer allocator.free(datagram_storage);
        const reliable_storage = try allocator.alloc(bool, config.reliable_window);
        errdefer allocator.free(reliable_storage);
        var splits = try reassembly.Reassembler.init(allocator, .{ .maximum_parts = config.maximum_split_parts, .maximum_bytes = config.maximum_split_bytes, .maximum_concurrent = config.maximum_concurrent_splits, .maximum_total_bytes = config.maximum_split_bytes_per_connection, .timeout_ms = config.split_timeout_ms });
        errdefer splits.deinit();
        var ordered = try ordered_store.Store.init(allocator, config.maximum_order_channels, config.maximum_ordered_packets, config.maximum_ordered_bytes, config.reliable_window);
        errdefer ordered.deinit();
        const sequenced = try allocator.alloc(ordering.Sequenced, config.maximum_order_channels);
        @memset(sequenced, .{});
        return .{
            .allocator = allocator,
            .config = config,
            .datagrams = try receive_window.Window.init(datagram_storage, 0),
            .reliable = try receive_window.Window.init(reliable_storage, 0),
            .datagram_storage = datagram_storage,
            .reliable_storage = reliable_storage,
            .splits = splits,
            .ordered = ordered,
            .sequenced = sequenced,
        };
    }
    pub fn deinit(self: *Receiver) void {
        self.splits.deinit();
        self.ordered.deinit();
        self.allocator.free(self.sequenced);
        self.allocator.free(self.reliable_storage);
        self.allocator.free(self.datagram_storage);
        self.* = undefined;
    }

    pub fn nextSplitDeadline(self: Receiver) ?u64 {
        return self.splits.nextDeadline();
    }

    pub fn expireSplits(self: *Receiver, now_ms: u64, maximum_work: usize) reassembly.ExpiryBatch {
        return self.splits.expire(now_ms, maximum_work);
    }

    pub fn process(self: *Receiver, data: []const u8, now_ms: u64, context: *anyopaque, deliver: DeliverFn) !Receipt {
        if (data.len > self.config.maximum_datagram_size) return error.DatagramTooLarge;

        // Validate the whole datagram before changing state.
        try self.validateDatagram(data);
        var datagram = try frame.decodeDatagram(data);
        if (try self.beginDatagram(datagram.sequence)) |receipt| return receipt;

        var receipt: Receipt = .{};
        var work: usize = 0;
        while (datagram.frames.remaining() > 0) {
            if (work >= self.config.maximum_packets_per_iteration) return error.PacketWorkLimitExceeded;
            work += 1;
            const value = try frame.decodeOne(&datagram.frames, self.config.maximum_frame_payload, self.config.maximum_split_parts);
            receipt.delivered += try self.processFrame(value, now_ms, context, deliver);
        }
        return self.commitDatagram(datagram.sequence, receipt);
    }

    pub fn processWithScratch(self: *Receiver, data: []const u8, now_ms: u64, scratch: []frame.Frame, context: *anyopaque, deliver: DeliverFn) !Receipt {
        if (data.len > self.config.maximum_datagram_size) return error.DatagramTooLarge;

        const datagram = try self.decodeDatagramInto(data, scratch);
        if (try self.beginDatagram(datagram.sequence)) |receipt| return receipt;

        var receipt: Receipt = .{};
        for (datagram.frames) |value| {
            receipt.delivered += try self.processFrame(value, now_ms, context, deliver);
        }
        return self.commitDatagram(datagram.sequence, receipt);
    }

    const ParsedDatagram = struct {
        sequence: u32,
        frames: []const frame.Frame,
    };

    fn decodeDatagramInto(self: *const Receiver, data: []const u8, scratch: []frame.Frame) !ParsedDatagram {
        var datagram = try frame.decodeDatagram(data);
        var count: usize = 0;
        while (datagram.frames.remaining() > 0) {
            if (count >= self.config.maximum_packets_per_iteration) return error.PacketWorkLimitExceeded;
            if (count >= scratch.len) return error.FrameScratchTooSmall;
            const value = try frame.decodeOne(&datagram.frames, self.config.maximum_frame_payload, self.config.maximum_split_parts);
            if (value.order_channel) |channel| {
                if (channel >= self.config.maximum_order_channels) return error.InvalidOrderChannel;
            }
            scratch[count] = value;
            count += 1;
        }
        return .{ .sequence = datagram.sequence, .frames = scratch[0..count] };
    }

    fn validateDatagram(self: *const Receiver, data: []const u8) !void {
        var datagram = try frame.decodeDatagram(data);
        var work: usize = 0;
        while (datagram.frames.remaining() > 0) {
            if (work >= self.config.maximum_packets_per_iteration) return error.PacketWorkLimitExceeded;
            work += 1;
            const value = try frame.decodeOne(&datagram.frames, self.config.maximum_frame_payload, self.config.maximum_split_parts);
            if (value.order_channel) |channel| {
                if (channel >= self.config.maximum_order_channels) return error.InvalidOrderChannel;
            }
        }
    }

    fn beginDatagram(self: *const Receiver, sequence: u32) !?Receipt {
        return switch (self.datagrams.inspect(sequence, self.config.maximum_acknowledged_datagrams)) {
            .accepted => null,
            .duplicate, .stale => .{ .acknowledge = sequence },
            .too_far_ahead, .ambiguous => error.DatagramWindowExceeded,
        };
    }

    fn commitDatagram(self: *Receiver, sequence: u32, receipt: Receipt) !Receipt {
        var committed = receipt;
        switch (self.datagrams.add(sequence, self.config.maximum_acknowledged_datagrams)) {
            .accepted => |gap| {
                committed.acknowledge = sequence;
                committed.missing = gap;
            },
            else => return error.InternalInvariant,
        }
        return committed;
    }
    fn processFrame(self: *Receiver, value: frame.Frame, now_ms: u64, context: *anyopaque, deliver: DeliverFn) !usize {
        if (!try self.previewReliable(value.reliable_index)) return 0;

        var payload = value.payload;
        var complete: ?OwnedPayload = null;
        defer if (complete) |owned| owned.deinit();
        if (value.split) |split| {
            complete = try self.splits.push(split.id, split.count, split.index, payload, now_ms);
            if (complete == null) {
                try self.commitReliable(value.reliable_index);
                return 0;
            }
            payload = complete.?.bytes;
        }

        if (value.reliability.hasSequenceIndex()) {
            const channel = value.order_channel.?;
            if (channel >= self.sequenced.len) return error.InvalidOrderChannel;
            try self.commitReliable(value.reliable_index);
            if (!self.sequenced[channel].accept(value.sequence_index.?)) return 0;
            try deliverPayload(context, payload, deliver);
            return 1;
        }
        if (value.reliability.hasOrderIndex()) {
            const channel = value.order_channel.?;
            const index = value.order_index.?;
            if (index == try self.ordered.expectedIndex(channel)) {
                try self.commitReliable(value.reliable_index);
                try deliverPayload(context, payload, deliver);
                try self.ordered.advanceBorrowed(channel, index);
                var delivered: usize = 1;
                while (try self.ordered.pop(channel)) |owned| {
                    defer owned.deinit();
                    try deliverPayload(context, owned.bytes, deliver);
                    delivered += 1;
                    if (delivered >= self.config.maximum_packets_per_iteration) break;
                }
                return delivered;
            }
            _ = try self.ordered.push(channel, index, payload);
            try self.commitReliable(value.reliable_index);
            return 0;
        }

        try self.commitReliable(value.reliable_index);
        try deliverPayload(context, payload, deliver);
        return 1;
    }

    fn deliverPayload(context: *anyopaque, payload: []const u8, deliver: DeliverFn) !void {
        try deliver(context, .init(payload));
    }
    fn previewReliable(self: *const Receiver, reliable_index: ?u32) !bool {
        const index = reliable_index orelse return true;
        return switch (self.reliable.inspect(index, 0)) {
            .accepted => true,
            .duplicate, .stale => false,
            .too_far_ahead, .ambiguous => error.ReliableWindowExceeded,
        };
    }

    fn commitReliable(self: *Receiver, reliable_index: ?u32) !void {
        const index = reliable_index orelse return;
        switch (self.reliable.add(index, 0)) {
            .accepted => {},
            else => return error.InternalInvariant,
        }
    }
};

test "receiver delivers in order with a zero-copy fast path" {
    const Collector = struct {
        values: [2][8]u8 = undefined,
        lengths: [2]usize = @splat(0),
        count: usize = 0,
        fn add(raw: *anyopaque, payload: BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.count >= self.values.len or payload.bytes.len > self.values[0].len) return error.ApplicationFailure;
            @memcpy(self.values[self.count][0..payload.bytes.len], payload.bytes);
            self.lengths[self.count] = payload.bytes.len;
            self.count += 1;
        }
    };
    var receiver = try Receiver.init(std.testing.allocator, .{});
    defer receiver.deinit();
    var collector: Collector = .{};
    var descriptors: [2]frame.Frame = undefined;
    var wire: [4096]u8 = undefined;
    const second = [_]frame.Frame{.{ .reliability = .reliable_ordered, .reliable_index = 0, .order_index = 1, .order_channel = 0, .payload = "second" }};
    _ = try receiver.processWithScratch(try @import("../protocol/datagram.zig").encodeData(0, &second, &wire), 0, &descriptors, &collector, Collector.add);
    const retained = receiver.ordered.packets.get(1).?;
    const retained_start = @intFromPtr(retained.ptr);
    const wire_start = @intFromPtr(&wire);
    try std.testing.expectEqual(second[0].payload.len, retained.len);
    try std.testing.expect(retained_start + retained.len <= wire_start or retained_start >= wire_start + wire.len);

    const first = [_]frame.Frame{.{ .reliability = .reliable_ordered, .reliable_index = 1, .order_index = 0, .order_channel = 0, .payload = "first" }};
    const receipt = try receiver.processWithScratch(try @import("../protocol/datagram.zig").encodeData(1, &first, &wire), 1, &descriptors, &collector, Collector.add);
    try std.testing.expectEqual(@as(usize, 2), receipt.delivered);
    try std.testing.expectEqual(@as(usize, 2), collector.count);
    try std.testing.expectEqualStrings("first", collector.values[0][0..collector.lengths[0]]);
    try std.testing.expectEqualStrings("second", collector.values[1][0..collector.lengths[1]]);
}

test "malformed suffix cannot consume sequence state or invoke callbacks" {
    const Counter = struct {
        count: usize = 0,
        fn deliver(raw: *anyopaque, _: BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };

    var config: Config = .{};
    config.receive_window = 8;
    config.reliable_window = 8;
    config.maximum_packets_per_iteration = 8;
    var receiver = try Receiver.init(std.testing.allocator, config);
    defer receiver.deinit();

    var counter: Counter = .{};
    var wire_storage: [128]u8 = undefined;
    const valid = [_]frame.Frame{.{
        .reliability = .reliable_ordered,
        .reliable_index = 0,
        .order_index = 0,
        .order_channel = 0,
        .payload = "valid",
    }};
    const prefix = try @import("../protocol/datagram.zig").encodeData(0, &valid, &wire_storage);
    const malformed_len = prefix.len + 2;
    wire_storage[prefix.len] = @as(u8, @intFromEnum(frame.Reliability.reliable_ordered)) << 5;
    wire_storage[prefix.len + 1] = 0;

    var datagrams_before: [8]bool = undefined;
    var reliable_before: [8]bool = undefined;
    @memcpy(&datagrams_before, receiver.datagram_storage);
    @memcpy(&reliable_before, receiver.reliable_storage);
    try std.testing.expectError(error.Truncated, receiver.process(wire_storage[0..malformed_len], 100, &counter, Counter.deliver));
    try std.testing.expectEqual(@as(usize, 0), counter.count);
    try std.testing.expectEqual(@as(u32, 0), receiver.datagrams.expected);
    try std.testing.expectEqual(@as(u32, 0), receiver.reliable.expected);
    try std.testing.expectEqualSlices(bool, &datagrams_before, receiver.datagram_storage);
    try std.testing.expectEqualSlices(bool, &reliable_before, receiver.reliable_storage);

    const corrected = try @import("../protocol/datagram.zig").encodeData(0, &valid, &wire_storage);
    const receipt = try receiver.process(corrected, 101, &counter, Counter.deliver);
    try std.testing.expectEqual(@as(usize, 1), receipt.delivered);
    try std.testing.expectEqual(@as(usize, 1), counter.count);
    try std.testing.expectEqual(@as(u32, 1), receiver.datagrams.expected);
    try std.testing.expectEqual(@as(u32, 1), receiver.reliable.expected);
}

test "invalid later frame metadata is rejected before earlier delivery" {
    const Counter = struct {
        count: usize = 0,
        fn deliver(raw: *anyopaque, _: BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };

    var config: Config = .{};
    config.receive_window = 8;
    config.reliable_window = 8;
    config.maximum_packets_per_iteration = 8;
    var receiver = try Receiver.init(std.testing.allocator, config);
    defer receiver.deinit();

    var counter: Counter = .{};
    var wire_storage: [128]u8 = undefined;
    const frames = [_]frame.Frame{
        .{ .reliability = .reliable_ordered, .reliable_index = 0, .order_index = 0, .order_channel = 0, .payload = "first" },
        .{ .reliability = .reliable_ordered, .reliable_index = 1, .order_index = 0, .order_channel = 32, .payload = "invalid" },
    };
    const wire = try @import("../protocol/datagram.zig").encodeData(0, &frames, &wire_storage);
    var descriptors: [2]frame.Frame = undefined;
    try std.testing.expectError(error.InvalidOrderChannel, receiver.processWithScratch(wire, 0, &descriptors, &counter, Counter.deliver));
    try std.testing.expectEqual(@as(usize, 0), counter.count);
    try std.testing.expectEqual(@as(u32, 0), receiver.datagrams.expected);
    try std.testing.expectEqual(@as(u32, 0), receiver.reliable.expected);
}
test "retained allocation failure does not consume datagram or reliable indices" {
    const Counter = struct {
        count: usize = 0,
        fn deliver(raw: *anyopaque, _: BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };

    var config: Config = .{};
    config.receive_window = 8;
    config.reliable_window = 8;
    config.maximum_ordered_packets = 8;
    config.maximum_ordered_bytes = 128;
    config.maximum_packets_per_iteration = 8;

    const QuotaAllocator = @import("../util/quota_allocator.zig").QuotaAllocator;
    var quota = QuotaAllocator.init(std.testing.allocator, std.math.maxInt(usize));
    var receiver = try Receiver.init(quota.allocator(), config);
    defer receiver.deinit();
    quota.maximum_bytes = quota.used_bytes;

    var counter: Counter = .{};
    var wire_storage: [128]u8 = undefined;
    const second = [_]frame.Frame{.{
        .reliability = .reliable_ordered,
        .reliable_index = 0,
        .order_index = 1,
        .order_channel = 0,
        .payload = "second",
    }};
    const blocked_wire = try @import("../protocol/datagram.zig").encodeData(0, &second, &wire_storage);
    try std.testing.expectError(error.OutOfMemory, receiver.process(blocked_wire, 0, &counter, Counter.deliver));
    try std.testing.expectEqual(@as(usize, 0), counter.count);
    try std.testing.expectEqual(@as(u32, 0), receiver.datagrams.expected);
    try std.testing.expectEqual(@as(u32, 0), receiver.reliable.expected);
    try std.testing.expectEqual(@as(usize, 0), receiver.ordered.packets.count());

    quota.maximum_bytes = std.math.maxInt(usize);
    const retry_receipt = try receiver.process(blocked_wire, 1, &counter, Counter.deliver);
    try std.testing.expectEqual(@as(usize, 0), retry_receipt.delivered);
    try std.testing.expectEqual(@as(u32, 1), receiver.datagrams.expected);
    try std.testing.expectEqual(@as(u32, 1), receiver.reliable.expected);

    const first = [_]frame.Frame{.{
        .reliability = .reliable_ordered,
        .reliable_index = 1,
        .order_index = 0,
        .order_channel = 0,
        .payload = "first",
    }};
    const first_wire = try @import("../protocol/datagram.zig").encodeData(1, &first, &wire_storage);
    const final_receipt = try receiver.process(first_wire, 2, &counter, Counter.deliver);
    try std.testing.expectEqual(@as(usize, 2), final_receipt.delivered);
    try std.testing.expectEqual(@as(usize, 2), counter.count);
}
test "every truncation inside a later frame is atomic" {
    const Counter = struct {
        count: usize = 0,
        fn deliver(raw: *anyopaque, _: BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };

    var config: Config = .{};
    config.receive_window = 8;
    config.reliable_window = 8;
    config.maximum_order_channels = 2;
    config.maximum_packets_per_iteration = 8;

    const frames = [_]frame.Frame{
        .{ .reliability = .reliable_ordered, .reliable_index = 0, .order_index = 0, .order_channel = 0, .payload = "first" },
        .{ .reliability = .reliable_ordered, .reliable_index = 1, .order_index = 1, .order_channel = 0, .payload = "second" },
    };
    var full_storage: [128]u8 = undefined;
    const full = try @import("../protocol/datagram.zig").encodeData(0, &frames, &full_storage);

    var prefix_storage: [128]u8 = undefined;
    const prefix_frames = frames[0..1];
    const first_frame_end = (try @import("../protocol/datagram.zig").encodeData(0, prefix_frames, &prefix_storage)).len;

    for (0..full.len) |cut| {
        // A clean frame boundary is still a valid datagram.
        if (cut == first_frame_end) continue;
        var receiver = try Receiver.init(std.testing.allocator, config);
        defer receiver.deinit();
        var descriptors: [2]frame.Frame = undefined;
        var counter: Counter = .{};

        if (receiver.processWithScratch(full[0..cut], 0, &descriptors, &counter, Counter.deliver)) |_| {
            return error.ExpectedTruncationRejection;
        } else |_| {}
        try std.testing.expectEqual(@as(usize, 0), counter.count);
        try std.testing.expectEqual(@as(u32, 0), receiver.datagrams.expected);
        try std.testing.expectEqual(@as(u32, 0), receiver.reliable.expected);
        try std.testing.expectEqual(@as(usize, 0), receiver.splits.assemblies.count());
        try std.testing.expectEqual(@as(usize, 0), receiver.splits.total_bytes);
        try std.testing.expectEqual(@as(usize, 0), receiver.ordered.packets.count());
        try std.testing.expectEqual(@as(usize, 0), receiver.ordered.total_bytes);
        try std.testing.expectEqual(@as(u32, 0), receiver.ordered.expected[0]);
        try std.testing.expect(!receiver.sequenced[0].initialized);
    }
}
test "completed split can retry after final allocation failure" {
    const Collector = struct {
        value: [16]u8 = undefined,
        length: usize = 0,
        count: usize = 0,
        fn deliver(raw: *anyopaque, payload: BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            @memcpy(self.value[0..payload.bytes.len], payload.bytes);
            self.length = payload.bytes.len;
            self.count += 1;
        }
    };

    var config: Config = .{};
    config.receive_window = 8;
    config.reliable_window = 8;
    config.maximum_order_channels = 1;
    config.maximum_frame_payload = 64;
    config.maximum_split_parts = 4;
    config.maximum_split_bytes = 64;
    config.maximum_split_bytes_per_connection = 64;
    config.maximum_concurrent_splits = 2;
    config.maximum_packets_per_iteration = 8;

    const QuotaAllocator = @import("../util/quota_allocator.zig").QuotaAllocator;
    var quota = QuotaAllocator.init(std.testing.allocator, std.math.maxInt(usize));
    var receiver = try Receiver.init(quota.allocator(), config);
    defer receiver.deinit();
    var collector: Collector = .{};
    var wire_storage: [128]u8 = undefined;

    const first = [_]frame.Frame{.{
        .reliability = .reliable_ordered,
        .reliable_index = 0,
        .order_index = 0,
        .order_channel = 0,
        .split = .{ .count = 2, .id = 7, .index = 0 },
        .payload = "hello ",
    }};
    const first_wire = try @import("../protocol/datagram.zig").encodeData(0, &first, &wire_storage);
    _ = try receiver.process(first_wire, 0, &collector, Collector.deliver);
    try std.testing.expectEqual(@as(usize, 6), receiver.splits.total_bytes);

    const second = [_]frame.Frame{.{
        .reliability = .reliable_ordered,
        .reliable_index = 1,
        .order_index = 0,
        .order_channel = 0,
        .split = .{ .count = 2, .id = 7, .index = 1 },
        .payload = "world",
    }};
    const second_wire = try @import("../protocol/datagram.zig").encodeData(1, &second, &wire_storage);
    quota.maximum_bytes = quota.used_bytes + second[0].payload.len;
    try std.testing.expectError(error.OutOfMemory, receiver.process(second_wire, 1, &collector, Collector.deliver));
    try std.testing.expectEqual(@as(usize, 0), collector.count);
    try std.testing.expectEqual(@as(u32, 1), receiver.datagrams.expected);
    try std.testing.expectEqual(@as(u32, 1), receiver.reliable.expected);
    try std.testing.expectEqual(@as(usize, 1), receiver.splits.assemblies.count());
    try std.testing.expectEqual(@as(usize, 11), receiver.splits.total_bytes);

    quota.maximum_bytes = std.math.maxInt(usize);
    const receipt = try receiver.process(second_wire, 2, &collector, Collector.deliver);
    try std.testing.expectEqual(@as(usize, 1), receipt.delivered);
    try std.testing.expectEqual(@as(usize, 1), collector.count);
    try std.testing.expectEqualStrings("hello world", collector.value[0..collector.length]);
    try std.testing.expectEqual(@as(u32, 2), receiver.datagrams.expected);
    try std.testing.expectEqual(@as(u32, 2), receiver.reliable.expected);
    try std.testing.expectEqual(@as(usize, 0), receiver.splits.assemblies.count());
    try std.testing.expectEqual(@as(usize, 0), receiver.splits.total_bytes);
}
test "small descriptor scratch is rejected before state changes" {
    const Counter = struct {
        count: usize = 0,
        fn deliver(raw: *anyopaque, _: BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };

    var config: Config = .{};
    config.receive_window = 8;
    config.reliable_window = 8;
    config.maximum_packets_per_iteration = 2;
    var receiver = try Receiver.init(std.testing.allocator, config);
    defer receiver.deinit();

    const frames = [_]frame.Frame{
        .{ .reliability = .reliable_ordered, .reliable_index = 0, .order_index = 0, .order_channel = 0, .payload = "first" },
        .{ .reliability = .reliable_ordered, .reliable_index = 1, .order_index = 1, .order_channel = 0, .payload = "second" },
    };
    var wire_storage: [128]u8 = undefined;
    const wire = try @import("../protocol/datagram.zig").encodeData(0, &frames, &wire_storage);
    var too_small: [1]frame.Frame = undefined;
    var counter: Counter = .{};

    try std.testing.expectError(error.FrameScratchTooSmall, receiver.processWithScratch(wire, 0, &too_small, &counter, Counter.deliver));
    try std.testing.expectEqual(@as(usize, 0), counter.count);
    try std.testing.expectEqual(@as(u32, 0), receiver.datagrams.expected);
    try std.testing.expectEqual(@as(u32, 0), receiver.reliable.expected);

    var enough: [2]frame.Frame = undefined;
    const receipt = try receiver.processWithScratch(wire, 1, &enough, &counter, Counter.deliver);
    try std.testing.expectEqual(@as(usize, 2), receipt.delivered);
    try std.testing.expectEqual(@as(usize, 2), counter.count);
}
test "callback failure before prepared state keeps stores empty" {
    const Failing = struct {
        calls: usize = 0,
        fn deliver(raw: *anyopaque, _: BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.ApplicationFailure;
        }
    };

    var config: Config = .{};
    config.receive_window = 8;
    config.reliable_window = 8;
    var receiver = try Receiver.init(std.testing.allocator, config);
    defer receiver.deinit();

    const frames = [_]frame.Frame{.{
        .reliability = .reliable,
        .reliable_index = 0,
        .payload = "payload",
    }};
    var wire_storage: [64]u8 = undefined;
    const wire = try @import("../protocol/datagram.zig").encodeData(0, &frames, &wire_storage);
    var descriptors: [1]frame.Frame = undefined;
    var failing: Failing = .{};

    try std.testing.expectError(error.ApplicationFailure, receiver.processWithScratch(wire, 0, &descriptors, &failing, Failing.deliver));
    try std.testing.expectEqual(@as(usize, 1), failing.calls);
    try std.testing.expectEqual(@as(u32, 0), receiver.datagrams.expected);
    try std.testing.expectEqual(@as(u32, 1), receiver.reliable.expected);
    try std.testing.expectEqual(@as(usize, 0), receiver.ordered.packets.count());
    try std.testing.expectEqual(@as(usize, 0), receiver.ordered.total_bytes);
    try std.testing.expectEqual(@as(usize, 0), receiver.splits.assemblies.count());
}

test "callback failure after prepared ordered state releases it" {
    const FailingSecond = struct {
        calls: usize = 0,
        first: [8]u8 = undefined,
        first_len: usize = 0,
        failed: [8]u8 = undefined,
        failed_len: usize = 0,

        fn deliver(raw: *anyopaque, payload: BorrowedPayload) DeliveryError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (payload.bytes.len > self.first.len) return error.ApplicationFailure;
            self.calls += 1;
            if (self.calls == 1) {
                @memcpy(self.first[0..payload.bytes.len], payload.bytes);
                self.first_len = payload.bytes.len;
                return;
            }
            @memcpy(self.failed[0..payload.bytes.len], payload.bytes);
            self.failed_len = payload.bytes.len;
            return error.ApplicationFailure;
        }
    };

    var config: Config = .{};
    config.receive_window = 8;
    config.reliable_window = 8;
    config.maximum_order_channels = 1;
    config.maximum_ordered_packets = 2;
    config.maximum_ordered_bytes = 16;
    config.maximum_packets_per_iteration = 2;
    var receiver = try Receiver.init(std.testing.allocator, config);
    defer receiver.deinit();

    var callback: FailingSecond = .{};
    var descriptors: [1]frame.Frame = undefined;
    var wire_storage: [64]u8 = undefined;
    const queued = [_]frame.Frame{.{
        .reliability = .reliable_ordered,
        .reliable_index = 0,
        .order_index = 1,
        .order_channel = 0,
        .payload = "second",
    }};
    const queued_wire = try @import("../protocol/datagram.zig").encodeData(0, &queued, &wire_storage);
    const queued_receipt = try receiver.processWithScratch(queued_wire, 0, &descriptors, &callback, FailingSecond.deliver);
    try std.testing.expectEqual(@as(usize, 0), queued_receipt.delivered);
    try std.testing.expectEqual(@as(usize, 1), receiver.ordered.packets.count());
    try std.testing.expectEqual(@as(usize, 6), receiver.ordered.total_bytes);

    const immediate = [_]frame.Frame{.{
        .reliability = .reliable_ordered,
        .reliable_index = 1,
        .order_index = 0,
        .order_channel = 0,
        .payload = "first",
    }};
    const immediate_wire = try @import("../protocol/datagram.zig").encodeData(1, &immediate, &wire_storage);
    try std.testing.expectError(error.ApplicationFailure, receiver.processWithScratch(immediate_wire, 1, &descriptors, &callback, FailingSecond.deliver));

    try std.testing.expectEqual(@as(usize, 2), callback.calls);
    try std.testing.expectEqualStrings("first", callback.first[0..callback.first_len]);
    try std.testing.expectEqualStrings("second", callback.failed[0..callback.failed_len]);
    try std.testing.expectEqual(@as(u32, 1), receiver.datagrams.expected);
    try std.testing.expectEqual(@as(u32, 2), receiver.reliable.expected);
    try std.testing.expectEqual(@as(u32, 2), receiver.ordered.expected[0]);
    try std.testing.expectEqual(@as(usize, 0), receiver.ordered.packets.count());
    try std.testing.expectEqual(@as(usize, 0), receiver.ordered.total_bytes);
}

fn checkOrderedReceiveAllocationFailures(allocator: std.mem.Allocator) !void {
    const Discard = struct {
        fn deliver(_: *anyopaque, _: BorrowedPayload) !void {}
    };

    var config: Config = .{};
    config.receive_window = 8;
    config.reliable_window = 8;
    config.maximum_order_channels = 1;
    config.maximum_ordered_packets = 4;
    config.maximum_ordered_bytes = 64;
    config.maximum_packets_per_iteration = 2;
    var receiver = try Receiver.init(allocator, config);
    defer receiver.deinit();

    const frames = [_]frame.Frame{.{
        .reliability = .reliable_ordered,
        .reliable_index = 0,
        .order_index = 1,
        .order_channel = 0,
        .payload = "retained",
    }};
    var wire_storage: [64]u8 = undefined;
    const wire = try @import("../protocol/datagram.zig").encodeData(0, &frames, &wire_storage);
    var descriptors: [1]frame.Frame = undefined;
    var unused: u8 = 0;

    _ = receiver.processWithScratch(wire, 0, &descriptors, &unused, Discard.deliver) catch |err| {
        if (err != error.OutOfMemory) return err;
        try std.testing.expectEqual(@as(u32, 0), receiver.datagrams.expected);
        try std.testing.expectEqual(@as(u32, 0), receiver.reliable.expected);
        try std.testing.expectEqual(@as(usize, 0), receiver.ordered.packets.count());
        try std.testing.expectEqual(@as(usize, 0), receiver.ordered.total_bytes);
        return err;
    };

    try std.testing.expectEqual(@as(u32, 1), receiver.datagrams.expected);
    try std.testing.expectEqual(@as(u32, 1), receiver.reliable.expected);
    try std.testing.expectEqual(@as(usize, 1), receiver.ordered.packets.count());
    try std.testing.expectEqual(@as(usize, 8), receiver.ordered.total_bytes);
}

test "ordered receive state survives every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkOrderedReceiveAllocationFailures, .{});
}

fn checkSplitReceiveAllocationFailures(allocator: std.mem.Allocator) !void {
    const Counter = struct {
        count: usize = 0,
        fn deliver(raw: *anyopaque, _: BorrowedPayload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };

    var config: Config = .{};
    config.receive_window = 8;
    config.reliable_window = 8;
    config.maximum_order_channels = 1;
    config.maximum_frame_payload = 64;
    config.maximum_split_parts = 4;
    config.maximum_split_bytes = 64;
    config.maximum_split_bytes_per_connection = 64;
    config.maximum_concurrent_splits = 2;
    config.maximum_packets_per_iteration = 2;
    var receiver = try Receiver.init(allocator, config);
    defer receiver.deinit();

    var descriptors: [1]frame.Frame = undefined;
    var wire_storage: [64]u8 = undefined;
    var counter: Counter = .{};
    const first = [_]frame.Frame{.{
        .reliability = .reliable_ordered,
        .reliable_index = 0,
        .order_index = 0,
        .order_channel = 0,
        .split = .{ .count = 2, .id = 9, .index = 0 },
        .payload = "hello ",
    }};
    const first_wire = try @import("../protocol/datagram.zig").encodeData(0, &first, &wire_storage);
    _ = receiver.processWithScratch(first_wire, 0, &descriptors, &counter, Counter.deliver) catch |err| {
        if (err != error.OutOfMemory) return err;
        try std.testing.expectEqual(@as(u32, 0), receiver.datagrams.expected);
        try std.testing.expectEqual(@as(u32, 0), receiver.reliable.expected);
        try std.testing.expectEqual(@as(usize, 0), receiver.splits.assemblies.count());
        try std.testing.expectEqual(@as(usize, 0), receiver.splits.total_bytes);
        return err;
    };

    const second = [_]frame.Frame{.{
        .reliability = .reliable_ordered,
        .reliable_index = 1,
        .order_index = 0,
        .order_channel = 0,
        .split = .{ .count = 2, .id = 9, .index = 1 },
        .payload = "world",
    }};
    const second_wire = try @import("../protocol/datagram.zig").encodeData(1, &second, &wire_storage);
    _ = receiver.processWithScratch(second_wire, 1, &descriptors, &counter, Counter.deliver) catch |err| {
        if (err != error.OutOfMemory) return err;
        try std.testing.expectEqual(@as(usize, 0), counter.count);
        try std.testing.expectEqual(@as(u32, 1), receiver.datagrams.expected);
        try std.testing.expectEqual(@as(u32, 1), receiver.reliable.expected);
        try std.testing.expectEqual(@as(usize, 1), receiver.splits.assemblies.count());
        try std.testing.expect(receiver.splits.total_bytes == 6 or receiver.splits.total_bytes == 11);
        return err;
    };

    try std.testing.expectEqual(@as(usize, 1), counter.count);
    try std.testing.expectEqual(@as(u32, 2), receiver.datagrams.expected);
    try std.testing.expectEqual(@as(u32, 2), receiver.reliable.expected);
    try std.testing.expectEqual(@as(usize, 0), receiver.splits.assemblies.count());
    try std.testing.expectEqual(@as(usize, 0), receiver.splits.total_bytes);
}

test "split receive state survives every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkSplitReceiveAllocationFailures, .{});
}
