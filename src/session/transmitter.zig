const std = @import("std");
const Config = @import("../config.zig").Config;
const datagram = @import("../protocol/datagram.zig");
const frame = @import("../protocol/frame.zig");
const uint24 = @import("../util/uint24.zig");

pub const EmitFn = *const fn (context: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) anyerror!void;
pub const Sent = struct { datagrams: usize, wire_bytes: usize };

/// Packetizes synchronously into caller scratch. The emit callback must consume/copy before returning.
pub const Transmitter = struct {
    config: Config,
    mtu: u16,
    datagram_sequence: u32 = 0,
    reliable_index: u32 = 0,
    sequence_indices: [256]u32 = @splat(0),
    order_indices: [256]u32 = @splat(0),
    split_id: u16 = 0,

    pub fn init(mtu: u16, config: Config) !Transmitter {
        try config.validate();
        if (mtu < config.minimum_mtu or mtu > config.maximum_mtu) return error.InvalidMtu;
        return .{ .config = config, .mtu = mtu };
    }

    /// A callback error after the first emitted fragment requires closing the connection: the peer may
    /// otherwise wait forever for the incomplete reliable-ordered message.
    pub fn send(self: *Transmitter, payload: []const u8, reliability: frame.Reliability, channel: u8, scratch: []u8, context: *anyopaque, emit: EmitFn) !Sent {
        if (payload.len == 0) return error.EmptyPayload;
        if (payload.len > self.config.maximum_split_bytes) return error.MessageTooLarge;
        if (channel >= self.config.maximum_order_channels and reliability.hasOrderIndex()) return error.InvalidOrderChannel;
        if (scratch.len < self.mtu) return error.NoSpaceLeft;

        const unsplit_capacity = try payloadCapacity(self.mtu, reliability, false);
        const needs_split = payload.len > unsplit_capacity;
        if (needs_split and !reliability.hasReliableIndex()) return error.UnreliableMessageTooLarge;
        const capacity = if (needs_split) try payloadCapacity(self.mtu, reliability, true) else unsplit_capacity;
        const split_count = (payload.len + capacity - 1) / capacity;
        if (split_count > self.config.maximum_split_parts or payload.len > self.config.maximum_split_bytes) return error.MessageTooLarge;

        const order_index = if (reliability.hasOrderIndex()) self.reserveOrder(channel) else null;
        const sequence_index = if (reliability.hasSequenceIndex()) self.reserveSequence(channel) else null;
        const selected_split_id = if (needs_split) self.reserveSplit() else null;
        var offset: usize = 0;
        var count: usize = 0;
        var bytes: usize = 0;
        while (offset < payload.len) {
            const amount = @min(capacity, payload.len - offset);
            const reliable_index = if (reliability.hasReliableIndex()) self.reserveReliable() else null;
            const value: frame.Frame = .{
                .reliability = reliability,
                .reliable_index = reliable_index,
                .sequence_index = sequence_index,
                .order_index = order_index,
                .order_channel = if (reliability.hasOrderIndex()) channel else null,
                .split = if (needs_split) .{ .count = @intCast(split_count), .id = selected_split_id.?, .index = @intCast(count) } else null,
                .payload = payload[offset..][0..amount],
            };
            const sequence = self.datagram_sequence;
            const wire = try datagram.encodeData(sequence, &.{value}, scratch[0..self.mtu]);
            try emit(context, sequence, reliability.hasReliableIndex(), wire);
            self.datagram_sequence = uint24.add(sequence, 1);
            offset += amount;
            count += 1;
            bytes += wire.len;
        }
        return .{ .datagrams = count, .wire_bytes = bytes };
    }

    pub fn estimateWireBytes(self: *const Transmitter, payload_len: usize, reliability: frame.Reliability, channel: u8) !usize {
        if (payload_len == 0) return error.EmptyPayload;
        if (payload_len > self.config.maximum_split_bytes) return error.MessageTooLarge;
        if (channel >= self.config.maximum_order_channels and reliability.hasOrderIndex()) return error.InvalidOrderChannel;
        const unsplit_capacity = try payloadCapacity(self.mtu, reliability, false);
        const split = payload_len > unsplit_capacity;
        if (split and !reliability.hasReliableIndex()) return error.UnreliableMessageTooLarge;
        const capacity = if (split) try payloadCapacity(self.mtu, reliability, true) else unsplit_capacity;
        const count = (payload_len + capacity - 1) / capacity;
        if (count > self.config.maximum_split_parts) return error.MessageTooLarge;
        const overhead = @as(usize, self.mtu) - capacity;
        return try std.math.add(usize, payload_len, try std.math.mul(usize, count, overhead));
    }
    fn reserveReliable(self: *Transmitter) u32 {
        const value = self.reliable_index;
        self.reliable_index = uint24.add(value, 1);
        return value;
    }
    fn reserveOrder(self: *Transmitter, channel: u8) u32 {
        const value = self.order_indices[channel];
        self.order_indices[channel] = uint24.add(value, 1);
        return value;
    }
    fn reserveSequence(self: *Transmitter, channel: u8) u32 {
        const value = self.sequence_indices[channel];
        self.sequence_indices[channel] = uint24.add(value, 1);
        return value;
    }
    fn reserveSplit(self: *Transmitter) u16 {
        const value = self.split_id;
        self.split_id +%= 1;
        return value;
    }
};

fn payloadCapacity(mtu: u16, reliability: frame.Reliability, split: bool) !usize {
    const probe: frame.Frame = .{
        .reliability = reliability,
        .reliable_index = if (reliability.hasReliableIndex()) 0 else null,
        .sequence_index = if (reliability.hasSequenceIndex()) 0 else null,
        .order_index = if (reliability.hasOrderIndex()) 0 else null,
        .order_channel = if (reliability.hasOrderIndex()) 0 else null,
        .split = if (split) .{ .count = 2, .id = 0, .index = 0 } else null,
        .payload = "x",
    };
    const overhead = 4 + (try frame.encodedSize(probe)) - 1;
    if (mtu <= overhead) return error.InvalidMtu;
    return @min(@as(usize, mtu) - overhead, 8191);
}

test "transmitter packetizes a split message within MTU" {
    const Collector = struct {
        count: usize = 0,
        fn emit(raw: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!reliable or wire.len > 576 or sequence != self.count) return error.InvalidEmission;
            self.count += 1;
        }
    };
    var transmitter = try Transmitter.init(576, .{});
    var scratch: [576]u8 = undefined;
    var payload: [1200]u8 = @splat(1);
    var collector: Collector = .{};
    const sent = try transmitter.send(&payload, .reliable_ordered, 0, &scratch, &collector, Collector.emit);
    try std.testing.expect(sent.datagrams > 1);
    try std.testing.expectEqual(sent.datagrams, collector.count);
}
