const std = @import("std");
const Config = @import("../config.zig").Config;
const datagram = @import("../protocol/datagram.zig");
const frame = @import("../protocol/frame.zig");
const uint24 = @import("../util/uint24.zig");

pub const EmitError = error{
    OutOfMemory,
    CongestionWindowFull,
    RecoveryFull,
    RecoveryBytesExceeded,
    Overflow,
    InvalidDatagram,
    TransportFailure,
};
pub const EmitFn = *const fn (context: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) EmitError!void;
pub const Sent = struct { datagrams: usize, wire_bytes: usize };

pub const Packetization = struct {
    payload_len: usize,
    reliability: frame.Reliability,
    channel: u8,
    capacity: usize,
    fragment_count: usize,
    order_index: ?u32,
    sequence_index: ?u32,
    split_id: ?u16,
    offset: usize = 0,
    next_fragment: usize = 0,

    pub fn complete(self: Packetization) bool {
        return self.offset == self.payload_len;
    }
};

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
        var packetization = try self.beginPacketization(payload.len, reliability, channel);
        return self.sendAvailable(&packetization, payload, scratch, std.math.maxInt(usize), context, emit);
    }

    pub fn beginPacketization(self: *Transmitter, payload_len: usize, reliability: frame.Reliability, channel: u8) !Packetization {
        if (payload_len == 0) return error.EmptyPayload;
        if (payload_len > self.config.maximum_split_bytes) return error.MessageTooLarge;
        if (channel >= self.config.maximum_order_channels and reliability.hasOrderIndex()) return error.InvalidOrderChannel;
        const unsplit_capacity = try payloadCapacity(self.mtu, reliability, false);
        const needs_split = payload_len > unsplit_capacity;
        if (needs_split and !reliability.hasReliableIndex()) return error.UnreliableMessageTooLarge;
        const capacity = if (needs_split) try payloadCapacity(self.mtu, reliability, true) else unsplit_capacity;
        const split_count = (payload_len + capacity - 1) / capacity;
        if (split_count > self.config.maximum_split_parts or split_count > std.math.maxInt(u32)) return error.MessageTooLarge;
        return .{
            .payload_len = payload_len,
            .reliability = reliability,
            .channel = channel,
            .capacity = capacity,
            .fragment_count = split_count,
            .order_index = if (reliability.hasOrderIndex()) self.reserveOrder(channel) else null,
            .sequence_index = if (reliability.hasSequenceIndex()) self.reserveSequence(channel) else null,
            .split_id = if (needs_split) self.reserveSplit() else null,
        };
    }

    /// Emits only complete datagrams that fit the supplied wire-byte budget.
    pub fn sendAvailable(self: *Transmitter, packetization: *Packetization, payload: []const u8, scratch: []u8, maximum_wire_bytes: usize, context: *anyopaque, emit: EmitFn) !Sent {
        if (payload.len != packetization.payload_len or packetization.offset > payload.len) return error.InvalidPacketizationState;
        if (scratch.len < self.mtu) return error.NoSpaceLeft;
        var sent: Sent = .{ .datagrams = 0, .wire_bytes = 0 };
        while (!packetization.complete()) {
            const amount = @min(packetization.capacity, payload.len - packetization.offset);
            const reliable_index = if (packetization.reliability.hasReliableIndex()) self.reliable_index else null;
            const value: frame.Frame = .{
                .reliability = packetization.reliability,
                .reliable_index = reliable_index,
                .sequence_index = packetization.sequence_index,
                .order_index = packetization.order_index,
                .order_channel = if (packetization.reliability.hasOrderIndex()) packetization.channel else null,
                .split = if (packetization.split_id) |split_id| .{
                    .count = @intCast(packetization.fragment_count),
                    .id = split_id,
                    .index = @intCast(packetization.next_fragment),
                } else null,
                .payload = payload[packetization.offset..][0..amount],
            };
            const wire_size = try std.math.add(usize, 4, try frame.encodedSize(value));
            if (wire_size > maximum_wire_bytes -| sent.wire_bytes) break;
            if (packetization.reliability.hasReliableIndex()) _ = self.reserveReliable();
            const sequence = self.datagram_sequence;
            const wire = try datagram.encodeData(sequence, &.{value}, scratch[0..self.mtu]);
            try emit(context, sequence, packetization.reliability.hasReliableIndex(), wire);
            self.datagram_sequence = uint24.add(sequence, 1);
            packetization.offset += amount;
            packetization.next_fragment += 1;
            sent.datagrams += 1;
            sent.wire_bytes += wire.len;
        }
        return sent;
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
            if (!reliable or wire.len > 576 or sequence != self.count) return error.TransportFailure;
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

test "packetization resumes at datagram boundaries within a wire budget" {
    const Collector = struct {
        count: usize = 0,
        fn emit(raw: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!reliable or sequence != self.count) return error.TransportFailure;
            var decoded = try frame.decodeDatagram(wire);
            const value = try frame.decodeOne(&decoded.frames, 8192, 2048);
            if (decoded.frames.remaining() != 0 or value.order_index != 0) return error.TransportFailure;
            const split = value.split orelse return error.TransportFailure;
            if (split.id != 0 or split.index != self.count) return error.TransportFailure;
            self.count += 1;
        }
    };
    var transmitter = try Transmitter.init(576, .{});
    var scratch: [576]u8 = undefined;
    var payload: [1200]u8 = @splat(1);
    var collector: Collector = .{};
    var packetization = try transmitter.beginPacketization(payload.len, .reliable_ordered, 0);

    const blocked = try transmitter.sendAvailable(&packetization, &payload, &scratch, 575, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 0), blocked.datagrams);
    try std.testing.expectEqual(@as(usize, 0), packetization.offset);

    const first = try transmitter.sendAvailable(&packetization, &payload, &scratch, 576, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 1), first.datagrams);
    try std.testing.expectEqual(@as(usize, 576), first.wire_bytes);
    try std.testing.expect(!packetization.complete());

    const rest = try transmitter.sendAvailable(&packetization, &payload, &scratch, std.math.maxInt(usize), &collector, Collector.emit);
    try std.testing.expect(rest.datagrams > 0);
    try std.testing.expect(packetization.complete());
    try std.testing.expectEqual(packetization.fragment_count, collector.count);
}
