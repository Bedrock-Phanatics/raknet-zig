const std = @import("std");
const raknet = @import("raknet");

const Core = raknet.advanced.session.Core;
const datagram = raknet.advanced.protocol.datagram;
const ack = raknet.advanced.protocol.ack;

const Direction = enum { to_receiver, to_sender };

const Packet = struct {
    direction: Direction,
    due: u64,
    len: usize,
    bytes: [576]u8,
};

const Network = struct {
    packets: [512]Packet = undefined,
    count: usize = 0,
    ordinal: usize = 0,
    now: u64 = 0,

    fn enqueue(self: *Network, direction: Direction, wire: []const u8) error{TransportFailure}!void {
        self.ordinal += 1;
        const number = self.ordinal;
        const in_burst = number % 29 == 8 or number % 29 == 9;
        const periodic_loss = number % 11 == 0;
        if (in_burst or periodic_loss) return;
        const delayed_ack: u64 = if (direction == .to_sender) 5 else 0;
        try self.append(direction, self.now + 1 + number % 4 + delayed_ack, wire);
        if (number % 7 == 0) try self.append(direction, self.now + 2 + delayed_ack, wire);
    }

    fn append(self: *Network, direction: Direction, due: u64, wire: []const u8) error{TransportFailure}!void {
        if (self.count == self.packets.len or wire.len > self.packets[0].bytes.len) return error.TransportFailure;
        var packet = &self.packets[self.count];
        packet.* = .{ .direction = direction, .due = due, .len = wire.len, .bytes = undefined };
        @memcpy(packet.bytes[0..wire.len], wire);
        self.count += 1;
    }

    fn popReady(self: *Network) ?Packet {
        var index = self.count;
        while (index != 0) {
            index -= 1;
            if (self.packets[index].due > self.now) continue;
            const packet = self.packets[index];
            self.count -= 1;
            self.packets[index] = self.packets[self.count];
            return packet;
        }
        return null;
    }
};

const Emitter = struct {
    network: *Network,
    direction: Direction,

    fn send(raw: *anyopaque, wire: []const u8) error{TransportFailure}!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.network.enqueue(self.direction, wire);
    }
};

const Delivery = struct {
    next: u8 = 0,
    count: usize = 0,
    valid: bool = true,

    fn accept(raw: *anyopaque, payload: raknet.BorrowedPayload) error{ PeerProtocolFailure, ResourceLimitFailure, TransportFailure, ApplicationFailure, InternalFailure }!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (payload.bytes.len != 1 or payload.bytes[0] != self.next) {
            self.valid = false;
            return error.ApplicationFailure;
        }
        self.next += 1;
        self.count += 1;
    }
};

const Discard = struct {
    fn payload(_: *anyopaque, _: raknet.BorrowedPayload) !void {}
};

fn queueReceipt(network: *Network, receipt: anytype) !void {
    var wire: [64]u8 = undefined;
    if (receipt.acknowledge) |sequence| {
        const encoded = try datagram.encodeControl(.ack, &.{.{ .first = sequence, .last = sequence }}, &wire);
        try network.enqueue(.to_sender, encoded);
    }
    if (receipt.missing) |gap| {
        const encoded = try datagram.encodeControl(.nack, &.{ack.Record{ .first = gap.first, .last = gap.last }}, &wire);
        try network.enqueue(.to_sender, encoded);
    }
}

test "reliable ordered traffic survives deterministic network impairments" {
    var sender = try Core.init(std.testing.allocator, 576, .{});
    defer sender.deinit();
    var receiver = try Core.init(std.testing.allocator, 576, .{});
    defer receiver.deinit();
    var network: Network = .{};
    var outbound: Emitter = .{ .network = &network, .direction = .to_receiver };
    var delivery: Delivery = .{};
    var scratch: [576]u8 = undefined;

    for (0..12) |index| {
        const payload = [1]u8{@intCast(index)};
        _ = try sender.send(&payload, .reliable_ordered, 0, &scratch, 0, &outbound, Emitter.send);
    }

    var frame_scratch: [16]raknet.advanced.protocol.frame.Frame = undefined;
    var retransmissions: [32]raknet.advanced.reliability.recovery.Due = undefined;
    var unused: u8 = 0;
    for (0..5000) |tick| {
        network.now = tick;
        while (network.popReady()) |packet| {
            switch (packet.direction) {
                .to_receiver => {
                    const incoming = receiver.processIncomingWithScratch(packet.bytes[0..packet.len], tick, &frame_scratch, &delivery, Delivery.accept) catch continue;
                    if (incoming == .data) try queueReceipt(&network, incoming.data);
                },
                .to_sender => _ = sender.processIncomingWithScratch(packet.bytes[0..packet.len], tick, &frame_scratch, &unused, Discard.payload) catch continue,
            }
        }
        const due = sender.collectRetransmissions(tick, &retransmissions, retransmissions.len);
        for (due.items) |item| try network.enqueue(.to_receiver, item.data);
        if (delivery.count == 12 and sender.statistics().recovery_packets == 0) break;
    }

    try std.testing.expectEqual(@as(usize, 12), delivery.count);
    try std.testing.expect(delivery.valid);
    try std.testing.expectEqual(@as(usize, 0), sender.statistics().recovery_packets);
    try std.testing.expect(sender.statistics().retransmitted_datagrams != 0);
}

const Impairment = struct {
    name: []const u8,
    seed: u64,
    loss_percent: u8 = 0,
    reverse_loss_percent: u8 = 0,
    burst_percent: u8 = 0,
    burst_length: u64 = 0,
    duplicate_percent: u8 = 0,
    delay: u64 = 5,
    jitter: u64 = 0,
    ack_delay: u64 = 0,
    wrap: bool = false,
    messages: usize = 300,
    maximum_size: usize = 1500,
};

const Link = struct {
    const capacity = 4096;
    packets: [capacity]Packet = undefined,
    count: usize = 0,
    now: u64 = 0,
    random: std.Random,
    impairment: Impairment,
    burst_until: u64 = 0,
    dropped: usize = 0,
    duplicated: usize = 0,

    fn send(self: *Link, direction: Direction, wire: []const u8) error{TransportFailure}!void {
        const loss = if (direction == .to_receiver) self.impairment.loss_percent else self.impairment.reverse_loss_percent;
        if (self.now < self.burst_until) {
            self.dropped += 1;
            return;
        }
        if (direction == .to_receiver and self.random.uintLessThan(u8, 100) < self.impairment.burst_percent) {
            self.burst_until = self.now + self.impairment.burst_length;
            self.dropped += 1;
            return;
        }
        if (self.random.uintLessThan(u8, 100) < loss) {
            self.dropped += 1;
            return;
        }
        try self.push(direction, wire);
        if (self.random.uintLessThan(u8, 100) < self.impairment.duplicate_percent) {
            self.duplicated += 1;
            try self.push(direction, wire);
        }
    }

    fn push(self: *Link, direction: Direction, wire: []const u8) error{TransportFailure}!void {
        if (self.count == capacity or wire.len > 576) return error.TransportFailure;
        const jitter = if (self.impairment.jitter == 0) 0 else self.random.uintAtMost(u64, self.impairment.jitter);
        const packet = &self.packets[self.count];
        packet.* = .{ .direction = direction, .due = self.now + self.impairment.delay + jitter, .len = wire.len, .bytes = undefined };
        @memcpy(packet.bytes[0..wire.len], wire);
        self.count += 1;
    }

    fn pop(self: *Link) ?Packet {
        var index: usize = 0;
        while (index < self.count) : (index += 1) {
            if (self.packets[index].due > self.now) continue;
            const packet = self.packets[index];
            self.count -= 1;
            self.packets[index] = self.packets[self.count];
            return packet;
        }
        return null;
    }
};

const LinkEmitter = struct {
    link: *Link,
    fn toReceiver(raw: *anyopaque, wire: []const u8) error{TransportFailure}!void {
        const self: *LinkEmitter = @ptrCast(@alignCast(raw));
        try self.link.send(.to_receiver, wire);
    }
};

const Stream = struct {
    expected: u32 = 0,
    failed: bool = false,

    fn deliver(raw: *anyopaque, payload: raknet.BorrowedPayload) error{ PeerProtocolFailure, ResourceLimitFailure, TransportFailure, ApplicationFailure, InternalFailure }!void {
        const self: *Stream = @ptrCast(@alignCast(raw));
        const bytes = payload.bytes;
        if (bytes.len < 4 or std.mem.readInt(u32, bytes[0..4], .little) != self.expected or !patternValid(bytes)) {
            self.failed = true;
            return error.ApplicationFailure;
        }
        self.expected += 1;
    }
};

fn messageSize(index: usize, maximum: usize) usize {
    return 4 + (index * 7919) % (maximum - 3);
}

fn fillMessage(index: usize, output: []u8) void {
    std.mem.writeInt(u32, output[0..4], @intCast(index), .little);
    for (output[4..], 4..) |*byte, offset| byte.* = @truncate(index +% offset *% 31);
}

fn patternValid(bytes: []const u8) bool {
    const index = std.mem.readInt(u32, bytes[0..4], .little);
    for (bytes[4..], 4..) |byte, offset| if (byte != @as(u8, @truncate(index +% offset *% 31))) return false;
    return true;
}

fn startAtWrap(core: *Core) !void {
    const start: u32 = 0xffff80;
    core.transmitter_state.datagram_sequence = start;
    core.transmitter_state.reliable_index = start;
    core.transmitter_state.order_indices[0] = start;
    const window = raknet.advanced.reliability.receive_window.Window;
    core.receiver_state.datagrams = try window.init(core.receiver_state.datagram_storage, start);
    core.receiver_state.reliable = try window.init(core.receiver_state.reliable_storage, start);
    core.receiver_state.ordered.channels[0].expected = start;
}

fn simulate(impairment: Impairment) !void {
    var config: raknet.Config = .{};
    config.session.maximum_queued_outbound_packets = 1024;
    var sender = try Core.init(std.testing.allocator, 576, config);
    defer sender.deinit();
    var receiver = try Core.init(std.testing.allocator, 576, config);
    defer receiver.deinit();
    if (impairment.wrap) {
        try startAtWrap(&sender);
        try startAtWrap(&receiver);
    }
    var prng = std.Random.DefaultPrng.init(impairment.seed);
    var link: Link = .{ .random = prng.random(), .impairment = impairment };
    var emitter: LinkEmitter = .{ .link = &link };
    var stream: Stream = .{};
    var scratch: [576]u8 = undefined;
    var frames: [64]raknet.advanced.protocol.frame.Frame = undefined;
    var due: [64]raknet.advanced.reliability.recovery.Due = undefined;
    var message: [4096]u8 = undefined;
    var pending_acks: [512]u32 = undefined;
    var pending_count: usize = 0;
    var ack_deadline: ?u64 = null;
    var records: [512]ack.Record = undefined;
    var wire: [576]u8 = undefined;
    var unused: u8 = 0;
    var queued: usize = 0;

    var tick: u64 = 0;
    while (tick < 120_000) : (tick += 1) {
        link.now = tick;
        while (queued < impairment.messages) {
            const size = messageSize(queued, impairment.maximum_size);
            fillMessage(queued, message[0..size]);
            _ = sender.enqueueOutbound(.application, message[0..size], .reliable_ordered, 0) catch break;
            queued += 1;
        }
        _ = try sender.flushAllOutbound(&scratch, 64, tick, &emitter, LinkEmitter.toReceiver);
        const batch = sender.collectRetransmissions(tick, &due, due.len);
        try std.testing.expectEqual(@as(usize, 0), batch.exhausted);
        for (batch.items) |item| try link.send(.to_receiver, item.data);

        while (link.pop()) |packet| switch (packet.direction) {
            .to_receiver => {
                const incoming = receiver.processIncomingWithScratch(packet.bytes[0..packet.len], tick, &frames, &stream, Stream.deliver) catch {
                    try std.testing.expect(!stream.failed);
                    continue;
                };
                if (incoming != .data) continue;
                if (incoming.data.acknowledge) |sequence| {
                    if (pending_count == pending_acks.len) return error.TestUnexpectedResult;
                    pending_acks[pending_count] = sequence;
                    pending_count += 1;
                    if (ack_deadline == null) ack_deadline = tick + impairment.ack_delay;
                }
                if (incoming.data.missing) |gap| {
                    if (gap.first <= gap.last) {
                        try link.send(.to_sender, try datagram.encodeControl(.nack, &.{.{ .first = gap.first, .last = gap.last }}, &wire));
                    } else {
                        try link.send(.to_sender, try datagram.encodeControl(.nack, &.{ .{ .first = 0, .last = gap.last }, .{ .first = gap.first, .last = 0xffffff } }, &wire));
                    }
                }
            },
            .to_sender => _ = sender.processIncomingWithScratch(packet.bytes[0..packet.len], tick, &frames, &unused, Discard.payload) catch {},
        };
        if (ack_deadline) |deadline| if (tick >= deadline) {
            const canonical = raknet.advanced.session.receipt_batch.canonicalizeValues(pending_acks[0..pending_count], &records);
            var offset: usize = 0;
            while (offset < canonical.len) : (offset += 64) {
                try link.send(.to_sender, try datagram.encodeControl(.ack, canonical[offset..@min(canonical.len, offset + 64)], &wire));
            }
            pending_count = 0;
            ack_deadline = null;
        };
        if (stream.expected == impairment.messages and sender.statistics().recovery_packets == 0 and sender.outbound_state.countAll() == 0) break;
    }
    if (stream.expected != impairment.messages) {
        std.debug.print("{s}: delivered {d}/{d} after {d} ticks\n", .{ impairment.name, stream.expected, impairment.messages, tick });
        return error.TestUnexpectedResult;
    }
    try std.testing.expect(!stream.failed);
    const stats = sender.statistics();
    try std.testing.expectEqual(@as(usize, 0), stats.recovery_packets);
    try std.testing.expectEqual(@as(usize, 0), stats.queued_packets);
    try std.testing.expectEqual(@as(?u64, null), sender.nextRetransmissionDeadline());
    try std.testing.expectEqual(@as(usize, 0), receiver.statistics().split_assemblies);
    try std.testing.expectEqual(@as(usize, 0), receiver.statistics().ordered_packets);
}

test "reliable ordered streams survive seeded impairment scenarios" {
    const scenarios = [_]Impairment{
        .{ .name = "clean", .seed = 1 },
        .{ .name = "random loss", .seed = 2, .loss_percent = 10, .reverse_loss_percent = 10 },
        .{ .name = "burst loss", .seed = 3, .burst_percent = 2, .burst_length = 12 },
        .{ .name = "duplication", .seed = 4, .duplicate_percent = 25 },
        .{ .name = "reordering jitter", .seed = 5, .jitter = 40 },
        .{ .name = "delayed acks", .seed = 6, .ack_delay = 10, .loss_percent = 3 },
        .{ .name = "asymmetric", .seed = 7, .loss_percent = 1, .reverse_loss_percent = 30 },
        .{ .name = "everything", .seed = 8, .loss_percent = 5, .reverse_loss_percent = 5, .burst_percent = 1, .burst_length = 6, .duplicate_percent = 10, .jitter = 25, .ack_delay = 5 },
        .{ .name = "sequence wrap", .seed = 9, .wrap = true, .loss_percent = 5, .jitter = 10, .messages = 600 },
        .{ .name = "outages", .seed = 11, .burst_percent = 1, .burst_length = 300, .loss_percent = 2 },
        .{ .name = "fragments", .seed = 10, .maximum_size = 4000, .loss_percent = 5, .duplicate_percent = 5, .jitter = 10, .messages = 150 },
    };
    for (scenarios) |scenario| simulate(scenario) catch |err| {
        std.debug.print("scenario {s} failed: {s}\n", .{ scenario.name, @errorName(err) });
        return err;
    };
}
