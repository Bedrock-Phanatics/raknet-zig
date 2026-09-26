const std = @import("std");
const raknet = @import("raknet");
const Core = raknet.advanced.session.Core;
const datagram = raknet.advanced.protocol.datagram;
const ack = raknet.advanced.protocol.ack;
const recovery = raknet.advanced.reliability.recovery;
const receipt_batch = raknet.advanced.session.receipt_batch;

const mtu = 1400;
const maximum_messages = 4096;

const Packet = struct { due: u64, len: u16, bytes: [mtu]u8 };

const Queue = struct {
    packets: []Packet,
    head: usize = 0,
    len: usize = 0,

    fn push(self: *Queue, packet: Packet) bool {
        if (self.len == self.packets.len) return false;
        self.packets[(self.head + self.len) % self.packets.len] = packet;
        self.len += 1;
        return true;
    }
    fn peek(self: *Queue) ?*Packet {
        return if (self.len == 0) null else &self.packets[self.head];
    }
    fn pop(self: *Queue) void {
        self.head = (self.head + 1) % self.packets.len;
        self.len -= 1;
    }
};

const Link = struct {
    now: u64 = 0,
    delay_ms: u64,
    rate_per_ms: usize,
    bottleneck: Queue,
    forward: Queue,
    reverse: Queue,
    dropped: usize = 0,
    data_datagrams: usize = 0,
    ack_datagrams: usize = 0,

    fn toReceiver(raw: *anyopaque, wire: []const u8) error{TransportFailure}!void {
        const self: *Link = @ptrCast(@alignCast(raw));
        self.data_datagrams += 1;
        var packet: Packet = .{ .due = 0, .len = @intCast(wire.len), .bytes = undefined };
        @memcpy(packet.bytes[0..wire.len], wire);
        if (!self.bottleneck.push(packet)) self.dropped += 1;
    }
    fn toSender(self: *Link, wire: []const u8) void {
        var packet: Packet = .{ .due = self.now + self.delay_ms, .len = @intCast(wire.len), .bytes = undefined };
        @memcpy(packet.bytes[0..wire.len], wire);
        _ = self.reverse.push(packet);
    }
    fn service(self: *Link) void {
        for (0..self.rate_per_ms) |_| {
            const packet = self.bottleneck.peek() orelse break;
            var moved = packet.*;
            moved.due = self.now + self.delay_ms;
            self.bottleneck.pop();
            _ = self.forward.push(moved);
        }
    }
};

const Receiver = struct {
    sent_at: []u64,
    latencies: []u64,
    count: usize = 0,
    now: u64 = 0,

    fn deliver(raw: *anyopaque, payload: raknet.BorrowedPayload) error{ PeerProtocolFailure, ResourceLimitFailure, TransportFailure, ApplicationFailure, InternalFailure }!void {
        const self: *Receiver = @ptrCast(@alignCast(raw));
        const index = std.mem.readInt(u32, payload.bytes[0..4], .little);
        if (index != self.count) return error.ApplicationFailure;
        self.latencies[self.count] = self.now - self.sent_at[index];
        self.count += 1;
    }
};

const Discard = struct {
    fn payload(_: *anyopaque, _: raknet.BorrowedPayload) !void {}
};

pub const Workload = struct {
    name: []const u8,
    idle_messages: usize,
    idle_interval_ms: u64,
    burst_messages: usize,
    burst_size: usize,
    tick_messages: usize,
    tick_size: usize,
    ticks: usize,
};

pub const Scenario = struct {
    workload: Workload,
    delay_ms: u64 = 20,
    rate_per_ms: usize = 8,
    queue_packets: usize = 64,
    ack_delay_ms: u64 = 0,
};

pub const Result = struct {
    completed_ms: u64,
    cpu_us: u64,
    data_datagrams: usize,
    ack_datagrams: usize,
    retransmitted: u64,
    dropped: usize,
    bytes: usize,
    p50: u64,
    p95: u64,
    p99: u64,
};

pub fn simulate(io: std.Io, scenario: Scenario) !Result {
    const allocator = std.heap.page_allocator;
    const workload = scenario.workload;
    var config: raknet.Config = .{};
    config.protocol.maximum_mtu = mtu;
    var sender = try Core.init(allocator, mtu, config);
    defer sender.deinit();
    var receiver = try Core.init(allocator, mtu, config);
    defer receiver.deinit();

    const packets = try allocator.alloc(Packet, scenario.queue_packets + 2 * 4096);
    defer allocator.free(packets);
    var link: Link = .{
        .delay_ms = scenario.delay_ms,
        .rate_per_ms = scenario.rate_per_ms,
        .bottleneck = .{ .packets = packets[0..scenario.queue_packets] },
        .forward = .{ .packets = packets[scenario.queue_packets..][0..4096] },
        .reverse = .{ .packets = packets[scenario.queue_packets + 4096 ..][0..4096] },
    };
    const total = workload.idle_messages + workload.burst_messages + workload.tick_messages * workload.ticks;
    if (total > maximum_messages) return error.TooManyMessages;
    var sent_at: [maximum_messages]u64 = undefined;
    var latencies: [maximum_messages]u64 = undefined;
    var sink: Receiver = .{ .sent_at = &sent_at, .latencies = &latencies };

    var scratch: [mtu]u8 = undefined;
    var frames: [256]raknet.advanced.protocol.frame.Frame = undefined;
    var due: [256]recovery.Due = undefined;
    var pending_acks: [4096]u32 = undefined;
    var pending_count: usize = 0;
    var ack_deadline: ?u64 = null;
    var records: [4096]ack.Record = undefined;
    var wire: [mtu]u8 = undefined;
    var payload: [8192]u8 = @splat(7);
    var unused: u8 = 0;
    var created: usize = 0;
    var queued: usize = 0;
    var bytes: usize = 0;

    const started = std.Io.Clock.awake.now(io);
    while (sink.count < total) : (link.now += 1) {
        if (link.now > 120_000) return error.SimulationStalled;
        const now = link.now;
        sink.now = now;

        const idle_end = workload.idle_messages * workload.idle_interval_ms;
        const release = if (created < workload.idle_messages)
            @as(usize, @intFromBool(now % workload.idle_interval_ms == 0))
        else if (created < workload.idle_messages + workload.burst_messages)
            workload.burst_messages
        else if (now >= idle_end and (now - idle_end) % 50 == 0)
            workload.tick_messages
        else
            0;
        const size: usize = if (created < workload.idle_messages) 64 else if (created < workload.idle_messages + workload.burst_messages) workload.burst_size else workload.tick_size;
        const target = @min(total, created + release);
        while (created < target) : (created += 1) {
            sent_at[created] = now;
            bytes += size;
        }
        while (queued < created) {
            std.mem.writeInt(u32, payload[0..4], @intCast(queued), .little);
            const message_size: usize = if (queued < workload.idle_messages) 64 else if (queued < workload.idle_messages + workload.burst_messages) workload.burst_size else workload.tick_size;
            _ = sender.enqueueOutbound(.application, payload[0..message_size], .reliable_ordered, 0) catch break;
            queued += 1;
        }
        _ = try sender.flushOutbound(.application, &scratch, 256, now, &link, Link.toReceiver);
        const batch = sender.collectRetransmissions(now, &due, due.len);
        if (batch.exhausted != 0) return error.RetransmissionLimitExceeded;
        for (batch.items) |item| try Link.toReceiver(&link, item.data);

        link.service();
        while (link.forward.peek()) |packet| {
            if (packet.due > now) break;
            const incoming = receiver.processIncomingWithScratch(packet.bytes[0..packet.len], now, &frames, &sink, Receiver.deliver) catch null;
            link.forward.pop();
            const value = incoming orelse continue;
            if (value != .data) continue;
            if (value.data.acknowledge) |sequence| {
                pending_acks[pending_count] = sequence;
                pending_count += 1;
                if (ack_deadline == null) ack_deadline = now + scenario.ack_delay_ms;
            }
            if (value.data.missing) |gap| {
                link.ack_datagrams += 1;
                link.toSender(try datagram.encodeControl(.nack, &.{.{ .first = gap.first, .last = gap.last }}, &wire));
            }
        }
        if (ack_deadline) |deadline| if (now >= deadline or pending_count > 200) {
            const canonical = receipt_batch.canonicalizeValues(pending_acks[0..pending_count], &records);
            var offset: usize = 0;
            while (offset < canonical.len) {
                const take = @min(canonical.len - offset, 128);
                link.ack_datagrams += 1;
                link.toSender(try datagram.encodeControl(.ack, canonical[offset..][0..take], &wire));
                offset += take;
            }
            pending_count = 0;
            ack_deadline = null;
        };
        while (link.reverse.peek()) |packet| {
            if (packet.due > now) break;
            _ = sender.processIncomingWithScratch(packet.bytes[0..packet.len], now, &frames, &unused, Discard.payload) catch {};
            link.reverse.pop();
        }
    }
    const cpu_us: u64 = @intCast(@divTrunc(started.durationTo(std.Io.Clock.awake.now(io)).nanoseconds, 1000));
    const sorted = latencies[0..total];
    std.mem.sort(u64, sorted, {}, std.sort.asc(u64));
    return .{
        .completed_ms = link.now,
        .cpu_us = cpu_us,
        .data_datagrams = link.data_datagrams,
        .ack_datagrams = link.ack_datagrams,
        .retransmitted = sender.statistics().retransmitted_datagrams,
        .dropped = link.dropped,
        .bytes = bytes,
        .p50 = sorted[total / 2],
        .p95 = sorted[total * 95 / 100],
        .p99 = sorted[total * 99 / 100],
    };
}

fn report(label: []const u8, scenario: Scenario, value: Result) void {
    const seconds = @as(f64, @floatFromInt(value.completed_ms)) / 1000.0;
    std.debug.print("{s}_{s}: {d:.0} pkt/s, {d:.0} ack/s, {d:.2} MiB/s, cpu {d} us, retransmits {d}, drops {d}, p50 {d} ms, p95 {d} ms, p99 {d} ms\n", .{
        label,
        scenario.workload.name,
        @as(f64, @floatFromInt(value.data_datagrams)) / seconds,
        @as(f64, @floatFromInt(value.ack_datagrams)) / seconds,
        @as(f64, @floatFromInt(value.bytes)) / seconds / (1024 * 1024),
        value.cpu_us,
        value.retransmitted,
        value.dropped,
        value.p50,
        value.p95,
        value.p99,
    });
}

pub const workloads = [_]Workload{
    .{ .name = "gameplay", .idle_messages = 0, .idle_interval_ms = 1, .burst_messages = 0, .burst_size = 0, .tick_messages = 6, .tick_size = 220, .ticks = 200 },
    .{ .name = "idle_then_chunks", .idle_messages = 100, .idle_interval_ms = 20, .burst_messages = 1500, .burst_size = 1100, .tick_messages = 0, .tick_size = 0, .ticks = 0 },
    .{ .name = "chunks_during_gameplay", .idle_messages = 0, .idle_interval_ms = 1, .burst_messages = 800, .burst_size = 4000, .tick_messages = 6, .tick_size = 220, .ticks = 100 },
};

pub fn run(io: std.Io) !void {
    for (workloads) |workload| {
        for ([_]usize{ 8, 1 }) |rate| {
            for ([_]u64{ 0, 1, 2, 5, 10 }) |delay| {
                const scenario: Scenario = .{ .workload = workload, .ack_delay_ms = delay, .rate_per_ms = rate, .queue_packets = if (rate == 1) 32 else 64 };
                var label: [48]u8 = undefined;
                const name = try std.fmt.bufPrint(&label, "link_rate{d}_ack_delay_{d}ms", .{ rate, delay });
                const result = simulate(io, scenario) catch |err| {
                    std.debug.print("{s}_{s}: failed {s}\n", .{ name, workload.name, @errorName(err) });
                    continue;
                };
                report(name, scenario, result);
            }
        }
    }
}
