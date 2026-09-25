const std = @import("std");
const raknet = @import("raknet");
const Core = raknet.advanced.session.Core;
const frame = raknet.advanced.protocol.frame;
const datagram = raknet.advanced.protocol.datagram;
const ack = raknet.advanced.protocol.ack;

const Delivery = struct {
    count: usize = 0,
    next: u8 = 0,
    now: u64 = 0,
    sent_at: u64 = 0,
    latencies: [120]u64 = undefined,

    fn accept(raw: *anyopaque, payload: raknet.BorrowedPayload) error{ PeerProtocolFailure, ResourceLimitFailure, TransportFailure, ApplicationFailure, InternalFailure }!void {
        const self: *Delivery = @ptrCast(@alignCast(raw));
        if (payload.bytes.len != 1 or payload.bytes[0] != self.next) return error.ApplicationFailure;
        self.latencies[self.count] = self.now - self.sent_at;
        self.next += 1;
        self.count += 1;
    }
};

const Counter = struct {
    sum: usize = 0,
    fn accept(raw: *anyopaque, payload: raknet.BorrowedPayload) error{ PeerProtocolFailure, ResourceLimitFailure, TransportFailure, ApplicationFailure, InternalFailure }!void {
        const self: *Counter = @ptrCast(@alignCast(raw));
        for (payload.bytes) |byte| self.sum +%= byte;
    }
};

fn sessionConfig() raknet.Config {
    var config: raknet.Config = .{};
    config.protocol.receive_window = 64;
    config.protocol.reliable_window = 64;
    config.protocol.maximum_order_channels = 1;
    config.protocol.maximum_split_parts = 16;
    config.protocol.maximum_frame_payload = 1024;
    config.protocol.maximum_split_bytes = 1024;
    config.session.maximum_retransmissions = 64;
    config.session.maximum_recovery_bytes = 64 * 576;
    config.session.maximum_ordered_packets = 64;
    config.session.maximum_ordered_bytes = 1024;
    config.session.maximum_concurrent_splits = 1;
    config.session.maximum_split_bytes_per_connection = 1024;
    config.session.maximum_queued_outbound_packets = 16;
    config.session.maximum_queued_outbound_bytes = 2048;
    config.session.reserved_control_queue_packets = 1;
    config.session.reserved_control_queue_bytes = 64;
    config.batching.maximum_ack_records = 16;
    return config;
}

fn sessions(io: std.Io, count: usize) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const cores = try allocator.alloc(Core, count);
    var initialized: usize = 0;
    defer for (cores[0..initialized]) |*core| core.deinit();
    const config = sessionConfig();
    for (cores) |*core| {
        core.* = try Core.init(allocator, 576, config);
        initialized += 1;
    }
    const arena_bytes = arena.queryCapacity();
    const latencies = try std.heap.page_allocator.alloc(u64, count * 4);
    defer std.heap.page_allocator.free(latencies);
    var wires: [4][64]u8 = undefined;
    var lengths: [4]usize = undefined;
    for (0..4) |index| {
        const payload = [1]u8{@intCast(index)};
        lengths[index] = (try datagram.encodeData(@intCast(index), &.{frame.Frame{ .reliability = .unreliable, .payload = &payload }}, &wires[index])).len;
    }
    std.mem.doNotOptimizeAway(&wires);
    var scratch: [1]frame.Frame = undefined;
    var counter: Counter = .{};
    const started = std.Io.Clock.awake.now(io);
    for (0..4) |round| for (cores, 0..) |*core, index| {
        const before = std.Io.Clock.awake.now(io);
        _ = try core.processIncomingWithScratch(wires[round][0..lengths[round]], round, &scratch, &counter, Counter.accept);
        latencies[round * count + index] = @intCast(before.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    };
    const elapsed_ns: u64 = @intCast(started.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    if (counter.sum != count * 6) return error.BenchmarkDeliveryMismatch;
    std.mem.sort(u64, latencies, {}, std.sort.asc(u64));
    std.debug.print("sessions_{d}: {d:.2} ns/packet, p50 {d} ns, p95 {d} ns, p99 {d} ns, {d} arena bytes/session\n", .{
        count,
        @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(count * 4)),
        latencies[latencies.len / 2],
        latencies[latencies.len * 95 / 100],
        latencies[latencies.len * 99 / 100],
        arena_bytes / count,
    });
}

const Direction = enum { receiver, sender };
const Packet = struct { direction: Direction, due: u64, len: usize, bytes: [576]u8 };
const Network = struct {
    packets: [512]Packet = undefined,
    count: usize = 0,
    ordinal: usize = 0,
    dropped: usize = 0,
    now: u64 = 0,
    loss: u8,
    rtt: u64,
    jitter: u64,

    fn enqueue(self: *Network, direction: Direction, wire: []const u8) error{TransportFailure}!void {
        self.ordinal += 1;
        if (self.ordinal *% 37 % 100 < self.loss) {
            self.dropped += 1;
            return;
        }
        const delay = self.rtt / 2 + (self.ordinal *% 17 % (self.jitter + 1));
        if (self.count == self.packets.len or wire.len > 576) return error.TransportFailure;
        self.packets[self.count] = .{ .direction = direction, .due = self.now + delay, .len = wire.len, .bytes = undefined };
        @memcpy(self.packets[self.count].bytes[0..wire.len], wire);
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
        const self: *Emitter = @ptrCast(@alignCast(raw));
        try self.network.enqueue(self.direction, wire);
    }
};

const Discard = struct {
    fn payload(_: *anyopaque, _: raknet.BorrowedPayload) !void {}
};

fn receipt(network: *Network, value: anytype) !void {
    var wire: [64]u8 = undefined;
    if (value.acknowledge) |sequence| {
        const encoded = try datagram.encodeControl(.ack, &.{.{ .first = sequence, .last = sequence }}, &wire);
        try network.enqueue(.sender, encoded);
    }
    if (value.missing) |gap| {
        const encoded = try datagram.encodeControl(.nack, &.{ack.Record{ .first = gap.first, .last = gap.last }}, &wire);
        try network.enqueue(.sender, encoded);
    }
}

fn lossCase(io: std.Io, loss: u8, rtt: u64, jitter: u64) !void {
    var sender = try Core.init(std.heap.page_allocator, 576, .{});
    defer sender.deinit();
    var receiver = try Core.init(std.heap.page_allocator, 576, .{});
    defer receiver.deinit();
    var network: Network = .{ .loss = loss, .rtt = rtt, .jitter = jitter };
    var outbound: Emitter = .{ .network = &network, .direction = .receiver };
    var delivery: Delivery = .{};
    var wire: [576]u8 = undefined;
    var scratch: [16]frame.Frame = undefined;
    var retransmissions: [32]raknet.advanced.reliability.recovery.Due = undefined;
    var unused: u8 = 0;
    const started = std.Io.Clock.awake.now(io);
    var tick: u64 = 0;
    for (0..10) |batch_index| {
        const target = (batch_index + 1) * 12;
        network.now = tick;
        delivery.sent_at = tick;
        for (0..12) |index| {
            const payload = [1]u8{@intCast(batch_index * 12 + index)};
            _ = try sender.send(&payload, .reliable_ordered, 0, &wire, tick, &outbound, Emitter.send);
        }
        var completed = false;
        for (0..10_000) |_| {
            network.now = tick;
            delivery.now = tick;
            while (network.popReady()) |packet| {
                switch (packet.direction) {
                    .receiver => {
                        const incoming = receiver.processIncomingWithScratch(packet.bytes[0..packet.len], tick, &scratch, &delivery, Delivery.accept) catch continue;
                        if (incoming == .data) try receipt(&network, incoming.data);
                    },
                    .sender => _ = sender.processIncomingWithScratch(packet.bytes[0..packet.len], tick, &scratch, &unused, Discard.payload) catch continue,
                }
            }
            const due = sender.collectRetransmissions(tick, &retransmissions, retransmissions.len);
            for (due.items) |item| try network.enqueue(.receiver, item.data);
            if (delivery.count == target and sender.statistics().recovery_packets == 0) {
                completed = true;
                break;
            }
            tick += 1;
        }
        if (!completed) return error.BenchmarkDeliveryMismatch;
        tick += 1;
    }
    const elapsed_us: u64 = @intCast(@divTrunc(started.durationTo(std.Io.Clock.awake.now(io)).nanoseconds, 1000));
    std.mem.sort(u64, &delivery.latencies, {}, std.sort.asc(u64));
    std.debug.print("loss_{d}_rtt_{d}_jitter_{d}: actual {d}/{d} dropped, cpu {d} us, retransmits {d}, delivery p50 {d} ms, p95 {d} ms, p99 {d} ms\n", .{
        loss,                   rtt,                     jitter,                  network.dropped, network.ordinal, elapsed_us, sender.statistics().retransmitted_datagrams,
        delivery.latencies[60], delivery.latencies[114], delivery.latencies[118],
    });
}

const ReassemblySum = struct {
    fn consume(raw: *anyopaque, payload: raknet.advanced.reliability.reassembly.ScatterPayload) error{ApplicationFailure}!void {
        const checksum: *usize = @ptrCast(@alignCast(raw));
        for (0..payload.count()) |index| for (payload.get(index)) |byte| {
            checksum.* +%= byte;
        };
    }
};

fn reassembly(io: std.Io, scatter: bool) !usize {
    var state = try raknet.advanced.reliability.reassembly.Reassembler.init(std.heap.page_allocator, .{
        .maximum_parts = 2,
        .maximum_bytes = 512,
        .maximum_concurrent = 1,
        .maximum_total_bytes = 512,
        .timeout_ms = 1000,
    });
    defer state.deinit();
    var parts: [2][256]u8 = undefined;
    for (&parts, 0..) |*part, index| for (part, 0..) |*byte, offset| {
        byte.* = @truncate(index *% 31 +% offset);
    };
    var checksum: usize = 0;
    const started = std.Io.Clock.awake.now(io);
    for (0..10_000) |index| {
        parts[0][0] = @truncate(index);
        parts[1][0] = @truncate(index >> 8);
        const id: u16 = @intCast(index);
        if (scatter) {
            if (try state.pushScatter(id, 2, 0, &parts[0], index, &checksum, ReassemblySum.consume)) return error.BenchmarkDeliveryMismatch;
            if (!try state.pushScatter(id, 2, 1, &parts[1], index, &checksum, ReassemblySum.consume)) return error.BenchmarkDeliveryMismatch;
        } else {
            if (try state.push(id, 2, 0, &parts[0], index) != null) return error.BenchmarkDeliveryMismatch;
            const payload = (try state.push(id, 2, 1, &parts[1], index)).?;
            for (payload.bytes) |byte| checksum +%= byte;
            payload.deinit();
        }
    }
    const elapsed_ns: u64 = @intCast(started.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    std.debug.print("reassembly_{s}: {d:.2} ns/message, {d} retained bytes\n", .{
        if (scatter) @as([]const u8, "scatter") else "owned",
        @as(f64, @floatFromInt(elapsed_ns)) / 10_000.0,
        state.retainedCapacity(),
    });
    return checksum;
}

fn retransmission(io: std.Io) !void {
    const recovery = raknet.advanced.reliability.recovery;
    var state = try recovery.Recovery.init(std.heap.page_allocator, 256, 256 * 576, 8, 576);
    defer state.deinit();
    var payload: [64]u8 = undefined;
    for (&payload, 0..) |*byte, index| byte.* = @truncate(index *% 17);
    var due: [1]recovery.Due = undefined;
    var checksum: usize = 0;
    const started = std.Io.Clock.awake.now(io);
    for (0..10_000) |index| {
        const sequence: u32 = @intCast(index);
        const now = index * 100;
        payload[0] = @truncate(index);
        try state.track(sequence, &payload, payload.len, now, 50);
        const batch = state.collectDue(now + 50, 50, &due, 1);
        if (batch.items.len != 1 or batch.items[0].sequence != sequence) return error.BenchmarkDeliveryMismatch;
        for (batch.items[0].data) |byte| checksum +%= byte;
        const acknowledged = try state.acknowledge(&.{ack.Record{ .first = sequence, .last = sequence }}, now + 51, 1);
        if (acknowledged.packets != 1) return error.BenchmarkDeliveryMismatch;
    }
    const elapsed_ns: u64 = @intCast(started.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    if (checksum == 0) return error.BenchmarkDeliveryMismatch;
    std.debug.print("retransmission: {d:.2} ns/message, {d} retained bytes\n", .{
        @as(f64, @floatFromInt(elapsed_ns)) / 10_000.0, state.retainedCapacity(),
    });
}

pub fn run(io: std.Io) !void {
    if (try reassembly(io, false) != try reassembly(io, true)) return error.BenchmarkDeliveryMismatch;
    try retransmission(io);
    for ([_]usize{ 1, 100, 1000, 10_000 }) |count| try sessions(io, count);
    for ([_]u8{ 0, 1, 5, 10 }) |loss| {
        try lossCase(io, loss, 20, 5);
        try lossCase(io, loss, 100, 20);
    }
}
