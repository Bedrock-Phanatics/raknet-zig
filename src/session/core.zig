const std = @import("std");
const Config = @import("../config.zig").Config;
const ack = @import("../protocol/ack.zig");
const datagram = @import("../protocol/datagram.zig");
const congestion = @import("../reliability/congestion.zig");
const recovery = @import("../reliability/recovery.zig");
const rtt = @import("../reliability/rtt.zig");
const receiver = @import("receiver.zig");
const transmitter = @import("transmitter.zig");
const frame = @import("../protocol/frame.zig");

pub const Incoming = union(enum) {
    data: receiver.Receipt,
    acknowledged: recovery.Acknowledged,
    nack_marked: usize,
};

/// Protocol state owned by one event-loop context. It performs no socket I/O and needs no locks.
pub const Core = struct {
    allocator: std.mem.Allocator,
    config: Config,
    receiver_state: receiver.Receiver,
    transmitter_state: transmitter.Transmitter,
    recovery_state: recovery.Recovery,
    congestion_state: congestion.Controller,
    rtt_state: rtt.Estimator,
    ack_records: []ack.Record,
    newest_sent: u32 = 0,

    pub fn init(allocator: std.mem.Allocator, mtu: u16, config: Config) !Core {
        try config.validate();
        if (mtu < config.minimum_mtu or mtu > config.maximum_mtu) return error.InvalidMtu;
        var receiver_state = try receiver.Receiver.init(allocator, config);
        errdefer receiver_state.deinit();
        var recovery_state = try recovery.Recovery.init(allocator, config.maximum_retransmissions, config.maximum_queued_outbound_bytes, 8);
        errdefer recovery_state.deinit();
        const ack_records = try allocator.alloc(ack.Record, config.maximum_ack_records);
        return .{
            .allocator = allocator,
            .config = config,
            .receiver_state = receiver_state,
            .transmitter_state = try transmitter.Transmitter.init(mtu, config),
            .recovery_state = recovery_state,
            .congestion_state = try congestion.Controller.init(mtu),
            .rtt_state = try rtt.Estimator.init(config.minimum_rto_ms, config.maximum_rto_ms),
            .ack_records = ack_records,
        };
    }
    pub fn deinit(self: *Core) void {
        self.receiver_state.deinit();
        self.recovery_state.deinit();
        self.allocator.free(self.ack_records);
        self.* = undefined;
    }

    pub const SendFn = *const fn (context: *anyopaque, wire: []const u8) anyerror!void;

    pub fn send(self: *Core, payload: []const u8, reliability: frame.Reliability, channel: u8, scratch: []u8, now_ms: u64, context: *anyopaque, emit: SendFn) !transmitter.Sent {
        const wire_bytes = try self.transmitter_state.estimateWireBytes(payload.len, reliability, channel);
        if (wire_bytes > self.congestion_state.available()) return error.CongestionWindowFull;
        const Bridge = struct {
            core: *Core,
            user_context: *anyopaque,
            user_emit: SendFn,
            now_ms: u64,
            fn forward(raw: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) !void {
                const bridge: *@This() = @ptrCast(@alignCast(raw));
                if (reliable) try bridge.core.trackSent(sequence, wire, wire.len, bridge.now_ms);
                try bridge.user_emit(bridge.user_context, wire);
            }
        };
        var bridge: Bridge = .{ .core = self, .user_context = context, .user_emit = emit, .now_ms = now_ms };
        return self.transmitter_state.send(payload, reliability, channel, scratch, &bridge, Bridge.forward);
    }
    /// Copies a reliable datagram into bounded recovery storage before it is handed to the socket.
    pub fn trackSent(self: *Core, sequence: u32, wire: []const u8, in_flight_bytes: usize, now_ms: u64) !void {
        try self.congestion_state.sent(in_flight_bytes);
        errdefer self.congestion_state.cancel(in_flight_bytes);
        try self.recovery_state.track(sequence, wire, in_flight_bytes, now_ms, self.rtt_state.rto());
        self.newest_sent = sequence;
    }

    pub fn processIncoming(self: *Core, wire: []const u8, now_ms: u64, context: *anyopaque, deliver: receiver.DeliverFn) !Incoming {
        return self.processIncomingImpl(wire, now_ms, null, context, deliver);
    }

    pub fn processIncomingWithScratch(self: *Core, wire: []const u8, now_ms: u64, frame_scratch: []frame.Frame, context: *anyopaque, deliver: receiver.DeliverFn) !Incoming {
        return self.processIncomingImpl(wire, now_ms, frame_scratch, context, deliver);
    }

    fn processIncomingImpl(self: *Core, wire: []const u8, now_ms: u64, frame_scratch: ?[]frame.Frame, context: *anyopaque, deliver: receiver.DeliverFn) !Incoming {
        if (wire.len > self.config.maximum_datagram_size) return error.DatagramTooLarge;
        return switch (try datagram.decode(wire, self.ack_records, self.config.maximum_ack_records, self.config.maximum_acknowledged_datagrams)) {
            .data => .{ .data = if (frame_scratch) |scratch|
                try self.receiver_state.processWithScratch(wire, now_ms, scratch, context, deliver)
            else
                try self.receiver_state.process(wire, now_ms, context, deliver) },
            .ack => |decoded| blk: {
                const result = try self.recovery_state.acknowledge(decoded.records, now_ms, self.config.maximum_acknowledged_datagrams);
                if (result.packets != 0) self.congestion_state.acknowledged(decoded.records[decoded.records.len - 1].last, result.bytes);
                if (result.rtt_sample_ms) |sample| self.rtt_state.observe(sample);
                break :blk .{ .acknowledged = result };
            },
            .nack => |decoded| blk: {
                const marked = try self.recovery_state.markNack(decoded.records, now_ms, self.config.maximum_acknowledged_datagrams);
                if (marked != 0) self.congestion_state.lost(self.newest_sent);
                break :blk .{ .nack_marked = marked };
            },
        };
    }
    pub fn collectRetransmissions(self: *Core, now_ms: u64, output: []recovery.Due) recovery.DueBatch {
        const batch = self.recovery_state.collectDue(now_ms, self.rtt_state.rto(), output, self.config.maximum_packets_per_iteration);
        for (batch.items) |item| if (item.timed_out) {
            self.congestion_state.timeout(self.newest_sent);
            break;
        };
        return batch;
    }
};

test "core validates ACKs against actual send state" {
    const Collector = struct {
        fn discard(_: *anyopaque, _: []const u8) !void {}
    };
    var core = try Core.init(std.testing.allocator, 1200, .{});
    defer core.deinit();
    try core.trackSent(3, "wire", 10, 100);
    var bytes: [32]u8 = undefined;
    const wire = try datagram.encodeControl(.ack, &.{.{ .first = 2, .last = 4 }}, &bytes);
    var unused: u8 = 0;
    const result = try core.processIncoming(wire, 150, &unused, Collector.discard);
    try std.testing.expectEqual(@as(usize, 1), result.acknowledged.packets);
    try std.testing.expectEqual(@as(?u64, 50), result.acknowledged.rtt_sample_ms);
    try std.testing.expectEqual(@as(usize, 0), (try core.processIncoming(wire, 160, &unused, Collector.discard)).acknowledged.packets);
}
