const std = @import("std");
const Config = @import("../config.zig").Config;
const frame = @import("../protocol/frame.zig");
const receive_window = @import("../reliability/receive_window.zig");
const reassembly = @import("../reliability/reassembly.zig");
const ordering = @import("../reliability/ordering.zig");
const ordered_store = @import("../reliability/ordered_store.zig");

pub const Receipt = struct { acknowledge: ?u32 = null, missing: ?receive_window.Gap = null, delivered: usize = 0 };
pub const DeliverFn = *const fn (context: *anyopaque, payload: []const u8) anyerror!void;

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

    pub fn process(self: *Receiver, data: []const u8, now_ms: u64, context: *anyopaque, deliver: DeliverFn) !Receipt {
        if (data.len > self.config.maximum_datagram_size) return error.DatagramTooLarge;
        _ = self.splits.expire(now_ms, self.config.maximum_packets_per_iteration);
        var datagram = try frame.decodeDatagram(data);
        var receipt: Receipt = .{};
        switch (self.datagrams.add(datagram.sequence, self.config.maximum_acknowledged_datagrams)) {
            .accepted => |gap| {
                receipt.acknowledge = datagram.sequence;
                receipt.missing = gap;
            },
            .duplicate, .stale => {
                receipt.acknowledge = datagram.sequence;
                return receipt;
            },
            .too_far_ahead, .ambiguous => return error.DatagramWindowExceeded,
        }
        var work: usize = 0;
        while (datagram.frames.remaining() > 0) {
            if (work >= self.config.maximum_packets_per_iteration) return error.PacketWorkLimitExceeded;
            work += 1;
            const value = try frame.decodeOne(&datagram.frames, self.config.maximum_frame_payload, self.config.maximum_split_parts);
            receipt.delivered += try self.processFrame(value, now_ms, context, deliver);
        }
        return receipt;
    }

    fn processFrame(self: *Receiver, value: frame.Frame, now_ms: u64, context: *anyopaque, deliver: DeliverFn) !usize {
        if (value.reliable_index) |index| switch (self.reliable.add(index, 0)) {
            .accepted => {},
            .duplicate, .stale => return 0,
            .too_far_ahead, .ambiguous => return error.ReliableWindowExceeded,
        };

        var payload = value.payload;
        var complete: ?[]u8 = null;
        defer if (complete) |owned| self.allocator.free(owned);
        if (value.split) |split| {
            complete = try self.splits.push(split.id, split.count, split.index, payload, now_ms);
            payload = complete orelse return 0;
        }

        if (value.reliability.hasSequenceIndex()) {
            const channel = value.order_channel.?;
            if (channel >= self.sequenced.len) return error.InvalidOrderChannel;
            if (!self.sequenced[channel].accept(value.sequence_index.?)) return 0;
        }
        if (value.reliability.hasOrderIndex() and !value.reliability.hasSequenceIndex()) {
            const channel = value.order_channel.?;
            const index = value.order_index.?;
            if (index == try self.ordered.expectedIndex(channel)) {
                try deliver(context, payload);
                try self.ordered.advanceBorrowed(channel, index);
                var delivered: usize = 1;
                while (try self.ordered.pop(channel)) |owned| {
                    defer owned.deinit();
                    try deliver(context, owned.data);
                    delivered += 1;
                    if (delivered >= self.config.maximum_packets_per_iteration) break;
                }
                return delivered;
            }
            _ = try self.ordered.push(channel, index, payload);
            return 0;
        }
        try deliver(context, payload);
        return 1;
    }
};

test "receiver delivers in order with a zero-copy fast path" {
    const Collector = struct {
        values: [2][8]u8 = undefined,
        lengths: [2]usize = @splat(0),
        count: usize = 0,
        fn add(raw: *anyopaque, payload: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.count >= self.values.len or payload.len > self.values[0].len) return error.Full;
            @memcpy(self.values[self.count][0..payload.len], payload);
            self.lengths[self.count] = payload.len;
            self.count += 1;
        }
    };
    var receiver = try Receiver.init(std.testing.allocator, .{});
    defer receiver.deinit();
    var collector: Collector = .{};
    var wire: [128]u8 = undefined;
    const second = [_]frame.Frame{.{ .reliability = .reliable_ordered, .reliable_index = 0, .order_index = 1, .order_channel = 0, .payload = "second" }};
    _ = try receiver.process(try @import("../protocol/datagram.zig").encodeData(0, &second, &wire), 0, &collector, Collector.add);
    const first = [_]frame.Frame{.{ .reliability = .reliable_ordered, .reliable_index = 1, .order_index = 0, .order_channel = 0, .payload = "first" }};
    const receipt = try receiver.process(try @import("../protocol/datagram.zig").encodeData(1, &first, &wire), 1, &collector, Collector.add);
    try std.testing.expectEqual(@as(usize, 2), receipt.delivered);
    try std.testing.expectEqual(@as(usize, 2), collector.count);
    try std.testing.expectEqualStrings("first", collector.values[0][0..collector.lengths[0]]);
    try std.testing.expectEqualStrings("second", collector.values[1][0..collector.lengths[1]]);
}
