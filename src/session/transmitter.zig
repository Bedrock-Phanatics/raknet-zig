const std = @import("std");

const Config = @import("../config.zig").Config;
const cursor = @import("../protocol/cursor.zig");
const datagram = @import("../protocol/datagram.zig");
const frame = @import("../protocol/frame.zig");
const uint24 = @import("../util/uint24.zig");

pub const EmitError = error{
    OutOfMemory,
    CongestionWindowFull,
    RecoveryFull,
    RecoveryBytesExceeded,
    Overflow,
    ConnectionClosed,
    InvalidDatagram,
    TransportFailure,
};
pub const EmitFn = *const fn (context: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) EmitError!void;
pub const Sent = struct { datagrams: usize, wire_bytes: usize };
pub const PackedMessage = struct { payload: []const u8, reliability: frame.Reliability, channel: u8 };
pub const PackResult = struct { messages: usize = 0, sent: Sent = .{ .datagrams = 0, .wire_bytes = 0 } };

const Layout = struct {
    capacity: usize,
    fragment_count: usize,
    split: bool,
};

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
        if (mtu < config.protocol.minimum_mtu or mtu > config.protocol.maximum_mtu) return error.InvalidMtu;
        return .{ .config = config, .mtu = mtu };
    }

    pub fn send(self: *Transmitter, payload: []const u8, reliability: frame.Reliability, channel: u8, scratch: []u8, context: *anyopaque, emit: EmitFn) !Sent {
        var packetization = try self.beginPacketization(payload.len, reliability, channel);
        return self.sendAvailable(&packetization, payload, scratch, std.math.maxInt(usize), std.math.maxInt(usize), context, emit);
    }

    pub fn beginPacketization(self: *Transmitter, payload_len: usize, reliability: frame.Reliability, channel: u8) !Packetization {
        const packet_layout = try self.layout(payload_len, reliability, channel);
        return .{
            .payload_len = payload_len,
            .reliability = reliability,
            .channel = channel,
            .capacity = packet_layout.capacity,
            .fragment_count = packet_layout.fragment_count,
            .order_index = if (reliability.hasOrderIndex()) self.reserveOrder(channel) else null,
            .sequence_index = if (reliability.hasSequenceIndex()) self.reserveSequence(channel) else null,
            .split_id = if (packet_layout.split) self.reserveSplit() else null,
        };
    }

    pub fn sendAvailable(
        self: *Transmitter,
        packetization: *Packetization,
        payload: []const u8,
        scratch: []u8,
        maximum_wire_bytes: usize,
        maximum_datagrams: usize,
        context: *anyopaque,
        emit: EmitFn,
    ) !Sent {
        if (payload.len != packetization.payload_len or packetization.offset > payload.len) return error.InvalidPacketizationState;
        if (scratch.len < self.mtu) return error.NoSpaceLeft;
        const had_progress = packetization.offset != 0;
        var sent: Sent = .{ .datagrams = 0, .wire_bytes = 0 };
        while (!packetization.complete() and sent.datagrams < maximum_datagrams) {
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
            const sequence = self.datagram_sequence;
            const wire = try datagram.encodeData(sequence, &.{value}, scratch[0..self.mtu]);
            emit(context, sequence, packetization.reliability.hasReliableIndex(), wire) catch |err| {
                if (had_progress or sent.datagrams != 0) return error.PartialSendFailure;
                return err;
            };
            if (packetization.reliability.hasReliableIndex()) _ = self.reserveReliable();
            self.datagram_sequence = uint24.add(sequence, 1);
            packetization.offset += amount;
            packetization.next_fragment += 1;
            sent.datagrams += 1;
            sent.wire_bytes += wire.len;
        }
        return sent;
    }

    pub fn pack(self: *Transmitter, source: anytype, scratch: []u8, maximum_wire_bytes: usize, context: *anyopaque, emit: EmitFn) !PackResult {
        if (maximum_wire_bytes < 4) return .{};
        if (scratch.len < self.mtu) return error.NoSpaceLeft;
        var messages = source;
        const first = messages.next() orelse return .{};
        try self.validateMessage(first);
        const first_capacity = try payloadCapacity(self.mtu, first.reliability, false);
        if (first.payload.len > first_capacity) return .{};

        const limit = @min(@as(usize, self.mtu), maximum_wire_bytes);
        var writer: cursor.Writer = .{ .data = scratch[0..limit] };
        try writer.byte(0x84);
        try writer.u24le(self.datagram_sequence);
        var next_reliable = self.reliable_index;
        var next_order = if (first.reliability.hasOrderIndex()) self.order_indices[first.channel] else 0;
        var next_sequence = if (first.reliability.hasSequenceIndex()) self.sequence_indices[first.channel] else 0;
        var count: usize = 0;

        var pending: ?PackedMessage = first;
        while (pending) |message| : (pending = messages.next()) {
            if (!compatible(first, message)) break;
            try self.validateMessage(message);
            if (message.payload.len > first_capacity) break;
            const value: frame.Frame = .{
                .reliability = message.reliability,
                .reliable_index = if (message.reliability.hasReliableIndex()) next_reliable else null,
                .sequence_index = if (message.reliability.hasSequenceIndex()) next_sequence else null,
                .order_index = if (message.reliability.hasOrderIndex()) next_order else null,
                .order_channel = if (message.reliability.hasOrderIndex()) message.channel else null,
                .payload = message.payload,
            };
            const encoded_size = try frame.encodedSize(value);
            if (encoded_size > writer.remaining()) break;
            try frame.encode(value, &writer);
            if (message.reliability.hasReliableIndex()) next_reliable = uint24.add(next_reliable, 1);
            if (message.reliability.hasOrderIndex()) next_order = uint24.add(next_order, 1);
            if (message.reliability.hasSequenceIndex()) next_sequence = uint24.add(next_sequence, 1);
            count += 1;
        }
        if (count == 0) return .{};

        const wire = writer.written();
        try emit(context, self.datagram_sequence, first.reliability.hasReliableIndex(), wire);
        self.datagram_sequence = uint24.add(self.datagram_sequence, 1);
        self.reliable_index = next_reliable;
        if (first.reliability.hasOrderIndex()) self.order_indices[first.channel] = next_order;
        if (first.reliability.hasSequenceIndex()) self.sequence_indices[first.channel] = next_sequence;
        return .{ .messages = count, .sent = .{ .datagrams = 1, .wire_bytes = wire.len } };
    }

    pub fn packReady(self: *const Transmitter, source: anytype) !bool {
        var messages = source;
        const first = messages.next() orelse return false;
        try self.validateMessage(first);
        const capacity = try payloadCapacity(self.mtu, first.reliability, false);
        if (first.payload.len > capacity) return true;
        var remaining = @as(usize, self.mtu) - 4;

        var pending: ?PackedMessage = first;
        while (pending) |message| : (pending = messages.next()) {
            if (!compatible(first, message)) return true;
            try self.validateMessage(message);
            if (message.payload.len > capacity) return true;
            const value: frame.Frame = .{
                .reliability = message.reliability,
                .reliable_index = if (message.reliability.hasReliableIndex()) 0 else null,
                .sequence_index = if (message.reliability.hasSequenceIndex()) 0 else null,
                .order_index = if (message.reliability.hasOrderIndex()) 0 else null,
                .order_channel = if (message.reliability.hasOrderIndex()) message.channel else null,
                .payload = message.payload,
            };
            const encoded_size = try frame.encodedSize(value);
            if (encoded_size > remaining) return true;
            remaining -= encoded_size;
            if (remaining == 0) return true;
        }
        return false;
    }

    fn validateMessage(self: *const Transmitter, message: PackedMessage) !void {
        _ = try self.layout(message.payload.len, message.reliability, message.channel);
    }

    pub fn estimateWireBytes(self: *const Transmitter, payload_len: usize, reliability: frame.Reliability, channel: u8) !usize {
        const packet_layout = try self.layout(payload_len, reliability, channel);
        const overhead = @as(usize, self.mtu) - packet_layout.capacity;
        return try std.math.add(usize, payload_len, try std.math.mul(usize, packet_layout.fragment_count, overhead));
    }

    pub fn fragmentCount(self: *const Transmitter, payload_len: usize, reliability: frame.Reliability, channel: u8) !usize {
        return (try self.layout(payload_len, reliability, channel)).fragment_count;
    }

    fn layout(self: *const Transmitter, payload_len: usize, reliability: frame.Reliability, channel: u8) !Layout {
        if (!reliability.supportedForSend()) return error.UnsupportedReliability;
        if (payload_len == 0) return error.EmptyPayload;
        if (payload_len > self.config.protocol.maximum_split_bytes) return error.MessageTooLarge;
        if (channel >= self.config.protocol.maximum_order_channels and reliability.hasOrderIndex()) return error.InvalidOrderChannel;

        const unsplit_capacity = try payloadCapacity(self.mtu, reliability, false);
        const split = payload_len > unsplit_capacity;
        if (split and !reliability.hasReliableIndex()) return error.UnreliableMessageTooLarge;
        const capacity = if (split) try payloadCapacity(self.mtu, reliability, true) else unsplit_capacity;
        const count = (payload_len + capacity - 1) / capacity;
        if (count > self.config.protocol.maximum_split_parts or count > std.math.maxInt(u32)) return error.MessageTooLarge;
        return .{ .capacity = capacity, .fragment_count = count, .split = split };
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

fn compatible(first: PackedMessage, next: PackedMessage) bool {
    if (first.reliability != next.reliability) return false;
    return !first.reliability.hasOrderIndex() or first.channel == next.channel;
}

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
        fn emit(raw: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) EmitError!void {
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
        fn emit(raw: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) EmitError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!reliable or sequence != self.count) return error.TransportFailure;
            var decoded = frame.decodeDatagram(wire) catch return error.TransportFailure;
            const value = frame.decodeOne(&decoded.frames, 8192, 2048) catch return error.TransportFailure;
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

    const blocked = try transmitter.sendAvailable(&packetization, &payload, &scratch, 575, 1, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 0), blocked.datagrams);
    try std.testing.expectEqual(@as(usize, 0), packetization.offset);

    const first = try transmitter.sendAvailable(&packetization, &payload, &scratch, 576, 1, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 1), first.datagrams);
    try std.testing.expectEqual(@as(usize, 576), first.wire_bytes);
    try std.testing.expect(!packetization.complete());

    const rest = try transmitter.sendAvailable(&packetization, &payload, &scratch, std.math.maxInt(usize), std.math.maxInt(usize), &collector, Collector.emit);
    try std.testing.expect(rest.datagrams > 0);
    try std.testing.expect(packetization.complete());
    try std.testing.expectEqual(packetization.fragment_count, collector.count);
}

test "packer fills one datagram with exact frame accounting" {
    const MessageIterator = struct {
        messages: []const PackedMessage,
        index: usize = 0,
        fn next(self: *@This()) ?PackedMessage {
            if (self.index == self.messages.len) return null;
            defer self.index += 1;
            return self.messages[self.index];
        }
    };
    const Collector = struct {
        frames: usize = 0,
        fn emit(raw: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) EmitError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!reliable or sequence != 0 or wire.len != 576) return error.TransportFailure;
            var decoded = frame.decodeDatagram(wire) catch return error.TransportFailure;
            while (decoded.frames.remaining() != 0) {
                const value = frame.decodeOne(&decoded.frames, 8192, 2048) catch return error.TransportFailure;
                const expected: u32 = @intCast(self.frames);
                if (value.reliable_index != expected or value.order_index != expected or value.order_channel != 3) return error.TransportFailure;
                self.frames += 1;
            }
        }
    };
    var transmitter = try Transmitter.init(576, .{});
    var scratch: [576]u8 = undefined;
    var first: [276]u8 = @splat(1);
    var second: [276]u8 = @splat(2);
    const messages = [_]PackedMessage{
        .{ .payload = &first, .reliability = .reliable_ordered, .channel = 3 },
        .{ .payload = &second, .reliability = .reliable_ordered, .channel = 3 },
        .{ .payload = "x", .reliability = .reliable_ordered, .channel = 3 },
    };
    var collector: Collector = .{};
    const result = try transmitter.pack(MessageIterator{ .messages = &messages }, &scratch, scratch.len, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 2), result.messages);
    try std.testing.expectEqual(@as(usize, 1), result.sent.datagrams);
    try std.testing.expectEqual(@as(usize, 576), result.sent.wire_bytes);
    try std.testing.expectEqual(@as(usize, 2), collector.frames);
    try std.testing.expectEqual(@as(u32, 2), transmitter.reliable_index);
    try std.testing.expectEqual(@as(u32, 2), transmitter.order_indices[3]);
}

test "packer stops before incompatible traffic without mutating it" {
    const MessageIterator = struct {
        messages: []const PackedMessage,
        index: usize = 0,
        fn next(self: *@This()) ?PackedMessage {
            if (self.index == self.messages.len) return null;
            defer self.index += 1;
            return self.messages[self.index];
        }
    };
    const Collector = struct {
        fn emit(_: *anyopaque, _: u32, _: bool, _: []const u8) EmitError!void {}
    };
    var transmitter = try Transmitter.init(576, .{});
    var scratch: [576]u8 = undefined;
    const messages = [_]PackedMessage{
        .{ .payload = "a", .reliability = .reliable_ordered, .channel = 0 },
        .{ .payload = "b", .reliability = .reliable_ordered, .channel = 1 },
    };
    var unused: u8 = 0;
    const result = try transmitter.pack(MessageIterator{ .messages = &messages }, &scratch, scratch.len, &unused, Collector.emit);
    try std.testing.expectEqual(@as(usize, 1), result.messages);
    try std.testing.expectEqual(@as(u32, 1), transmitter.order_indices[0]);
    try std.testing.expectEqual(@as(u32, 0), transmitter.order_indices[1]);
}

test "packer keeps indices when emit fails" {
    const MessageIterator = struct {
        messages: []const PackedMessage,
        index: usize = 0,
        fn next(self: *@This()) ?PackedMessage {
            if (self.index == self.messages.len) return null;
            defer self.index += 1;
            return self.messages[self.index];
        }
    };
    const Failing = struct {
        fn emit(_: *anyopaque, _: u32, _: bool, _: []const u8) EmitError!void {
            return error.TransportFailure;
        }
    };
    var transmitter = try Transmitter.init(576, .{});
    var scratch: [576]u8 = undefined;
    const messages = [_]PackedMessage{
        .{ .payload = "a", .reliability = .reliable_ordered, .channel = 2 },
        .{ .payload = "b", .reliability = .reliable_ordered, .channel = 2 },
    };
    var unused: u8 = 0;
    try std.testing.expectError(error.TransportFailure, transmitter.pack(MessageIterator{ .messages = &messages }, &scratch, scratch.len, &unused, Failing.emit));
    try std.testing.expectEqual(@as(u32, 0), transmitter.datagram_sequence);
    try std.testing.expectEqual(@as(u32, 0), transmitter.reliable_index);
    try std.testing.expectEqual(@as(u32, 0), transmitter.order_indices[2]);
}

test "packer honors MTU boundaries for every reliability mode" {
    const MessageIterator = struct {
        messages: []const PackedMessage,
        index: usize = 0,
        fn next(self: *@This()) ?PackedMessage {
            if (self.index == self.messages.len) return null;
            defer self.index += 1;
            return self.messages[self.index];
        }
    };
    const Collector = struct {
        expected: frame.Reliability,
        calls: usize = 0,
        fn emit(raw: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) EmitError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (sequence != 0 or reliable != self.expected.hasReliableIndex() or wire.len != 576) return error.TransportFailure;
            var decoded = frame.decodeDatagram(wire) catch return error.TransportFailure;
            const value = frame.decodeOne(&decoded.frames, 8192, 2048) catch return error.TransportFailure;
            if (decoded.frames.remaining() != 0 or value.reliability != self.expected) return error.TransportFailure;
            if ((value.reliable_index != null) != self.expected.hasReliableIndex()) return error.TransportFailure;
            if ((value.sequence_index != null) != self.expected.hasSequenceIndex()) return error.TransportFailure;
            if ((value.order_index != null) != self.expected.hasOrderIndex()) return error.TransportFailure;
            if (self.expected.hasOrderIndex() and value.order_channel != 5) return error.TransportFailure;
            self.calls += 1;
        }
    };

    for (std.enums.values(frame.Reliability)) |reliability| {
        if (!reliability.supportedForSend()) {
            const unsupported = [_]PackedMessage{.{ .payload = "x", .reliability = reliability, .channel = 5 }};
            const unsupported_transmitter = try Transmitter.init(576, .{});
            try std.testing.expectError(error.UnsupportedReliability, unsupported_transmitter.packReady(MessageIterator{ .messages = &unsupported }));
            continue;
        }
        const capacity = try payloadCapacity(576, reliability, false);
        var payload: [576]u8 = @splat(0xa5);
        const messages = [_]PackedMessage{.{ .payload = payload[0..capacity], .reliability = reliability, .channel = 5 }};
        var transmitter = try Transmitter.init(576, .{});
        var scratch: [576]u8 = undefined;
        var collector: Collector = .{ .expected = reliability };

        try std.testing.expect(try transmitter.packReady(MessageIterator{ .messages = &messages }));
        const short = try transmitter.pack(MessageIterator{ .messages = &messages }, &scratch, 575, &collector, Collector.emit);
        try std.testing.expectEqual(@as(usize, 0), short.messages);
        try std.testing.expectEqual(@as(usize, 0), collector.calls);

        const exact = try transmitter.pack(MessageIterator{ .messages = &messages }, &scratch, 576, &collector, Collector.emit);
        try std.testing.expectEqual(@as(usize, 1), exact.messages);
        try std.testing.expectEqual(@as(usize, 576), exact.sent.wire_bytes);
        try std.testing.expectEqual(@as(usize, 1), collector.calls);
        try std.testing.expectEqual(@as(u32, @intFromBool(reliability.hasReliableIndex())), transmitter.reliable_index);
        try std.testing.expectEqual(@as(u32, @intFromBool(reliability.hasOrderIndex())), transmitter.order_indices[5]);
        try std.testing.expectEqual(@as(u32, @intFromBool(reliability.hasSequenceIndex())), transmitter.sequence_indices[5]);

        var oversized = [_]PackedMessage{.{ .payload = payload[0 .. capacity + 1], .reliability = reliability, .channel = 5 }};
        var oversized_transmitter = try Transmitter.init(576, .{});
        if (reliability.hasReliableIndex()) {
            const result = try oversized_transmitter.pack(MessageIterator{ .messages = &oversized }, &scratch, 576, &collector, Collector.emit);
            try std.testing.expectEqual(@as(usize, 0), result.messages);
        } else {
            try std.testing.expectError(
                error.UnreliableMessageTooLarge,
                oversized_transmitter.pack(MessageIterator{ .messages = &oversized }, &scratch, 576, &collector, Collector.emit),
            );
        }
    }
}

test "packing readiness detects full and incompatible prefixes" {
    const MessageIterator = struct {
        messages: []const PackedMessage,
        index: usize = 0,
        fn next(self: *@This()) ?PackedMessage {
            if (self.index == self.messages.len) return null;
            defer self.index += 1;
            return self.messages[self.index];
        }
    };
    const transmitter = try Transmitter.init(576, .{});
    const small = [_]PackedMessage{
        .{ .payload = "a", .reliability = .reliable_ordered, .channel = 0 },
        .{ .payload = "b", .reliability = .reliable_ordered, .channel = 0 },
    };
    try std.testing.expect(!try transmitter.packReady(MessageIterator{ .messages = &small }));

    var exact_payload: [562]u8 = @splat(1);
    const exact = [_]PackedMessage{.{ .payload = &exact_payload, .reliability = .reliable_ordered, .channel = 0 }};
    try std.testing.expect(try transmitter.packReady(MessageIterator{ .messages = &exact }));

    const channel_change = [_]PackedMessage{
        .{ .payload = "a", .reliability = .reliable_ordered, .channel = 0 },
        .{ .payload = "b", .reliability = .reliable_ordered, .channel = 1 },
    };
    try std.testing.expect(try transmitter.packReady(MessageIterator{ .messages = &channel_change }));

    const reliability_change = [_]PackedMessage{
        .{ .payload = "a", .reliability = .reliable, .channel = 0 },
        .{ .payload = "b", .reliability = .unreliable, .channel = 0 },
    };
    try std.testing.expect(try transmitter.packReady(MessageIterator{ .messages = &reliability_change }));
}

test "packed and split messages preserve protocol indices" {
    const MessageIterator = struct {
        messages: []const PackedMessage,
        index: usize = 0,
        fn next(self: *@This()) ?PackedMessage {
            if (self.index == self.messages.len) return null;
            defer self.index += 1;
            return self.messages[self.index];
        }
    };
    const Collector = struct {
        frames: usize = 0,
        split_frames: usize = 0,
        fn emit(raw: *anyopaque, _: u32, reliable: bool, wire: []const u8) EmitError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!reliable) return error.TransportFailure;
            var decoded = frame.decodeDatagram(wire) catch return error.TransportFailure;
            while (decoded.frames.remaining() != 0) {
                const value = frame.decodeOne(&decoded.frames, 8192, 2048) catch return error.TransportFailure;
                const expected_reliable: u32 = @intCast(self.frames);
                if (value.reliable_index != expected_reliable or value.order_channel != 2) return error.TransportFailure;
                const expected_order: u32 = if (self.frames < 2) @intCast(self.frames) else if (self.frames < 5) 2 else 3;
                if (value.order_index != expected_order) return error.TransportFailure;
                if (self.frames >= 2 and self.frames < 5) {
                    const split = value.split orelse return error.TransportFailure;
                    if (split.id != 0 or split.count != 3 or split.index != self.split_frames) return error.TransportFailure;
                    self.split_frames += 1;
                } else if (value.split != null) return error.TransportFailure;
                self.frames += 1;
            }
        }
    };
    var transmitter = try Transmitter.init(576, .{});
    var scratch: [576]u8 = undefined;
    const prefix = [_]PackedMessage{
        .{ .payload = "a", .reliability = .reliable_ordered, .channel = 2 },
        .{ .payload = "b", .reliability = .reliable_ordered, .channel = 2 },
    };
    var collector: Collector = .{};
    _ = try transmitter.pack(MessageIterator{ .messages = &prefix }, &scratch, 576, &collector, Collector.emit);

    var split_payload: [1200]u8 = @splat(3);
    var packetization = try transmitter.beginPacketization(split_payload.len, .reliable_ordered, 2);
    _ = try transmitter.sendAvailable(&packetization, &split_payload, &scratch, std.math.maxInt(usize), std.math.maxInt(usize), &collector, Collector.emit);

    const suffix = [_]PackedMessage{.{ .payload = "c", .reliability = .reliable_ordered, .channel = 2 }};
    _ = try transmitter.pack(MessageIterator{ .messages = &suffix }, &scratch, 576, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 6), collector.frames);
    try std.testing.expectEqual(@as(usize, 3), collector.split_frames);
    try std.testing.expectEqual(@as(u32, 6), transmitter.reliable_index);
    try std.testing.expectEqual(@as(u32, 4), transmitter.order_indices[2]);
    try std.testing.expectEqual(@as(u16, 1), transmitter.split_id);
}
