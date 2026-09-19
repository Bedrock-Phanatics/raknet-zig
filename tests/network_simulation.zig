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
