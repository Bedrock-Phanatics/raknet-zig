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

pub const IncomingErrorClass = enum {
    protocol,
    resource,
    transport,
    application,
    internal,
};

pub const IncomingErrorDisposition = enum {
    reject,
    retry,
    close_session,
};

pub const SessionTransition = enum {
    receive,
    application_send,
    receipt,
    retransmission,
    handshake,
};

pub const IncomingFailure = struct {
    class: IncomingErrorClass,
    disposition: IncomingErrorDisposition,
};

pub const ApplicationCallbackError = error{ApplicationFailure};
pub const SendError = error{TransportFailure};

pub fn classifyIncomingError(err: anyerror) IncomingFailure {
    return switch (err) {
        error.DatagramTooLarge,
        error.Truncated,
        error.NotDatagram,
        error.InvalidDatagramFlags,
        error.EmptyDatagram,
        error.TooManyRecords,
        error.InvalidRecordType,
        error.ReversedRange,
        error.TooManyAcknowledgements,
        error.OverlappingRanges,
        error.TrailingData,
        error.InvalidFrameFlags,
        error.EmptyPayload,
        error.PayloadTooLarge,
        error.InvalidSplit,
        error.InvalidOrderChannel,
        error.PacketWorkLimitExceeded,
        error.DatagramWindowExceeded,
        => .{ .class = .protocol, .disposition = .reject },

        error.PeerProtocolFailure,
        error.ReliableWindowExceeded,
        error.OrderWindowExceeded,
        error.SplitIdCollision,
        error.ConflictingFragment,
        => .{ .class = .protocol, .disposition = .close_session },

        error.OutOfMemory,
        error.ResourceLimitFailure,
        error.TooManyAssemblies,
        error.ReassemblyLimitExceeded,
        error.OrderQueueFull,
        error.OrderBytesExceeded,
        error.RecoveryFull,
        error.RecoveryBytesExceeded,
        error.CongestionWindowFull,
        => .{ .class = .resource, .disposition = .close_session },

        error.TransportFailure => .{ .class = .transport, .disposition = .close_session },
        error.ApplicationFailure => .{ .class = .application, .disposition = .close_session },
        else => .{ .class = .internal, .disposition = .close_session },
    };
}

pub fn incomingErrorDisposition(err: anyerror) IncomingErrorDisposition {
    return classifyIncomingError(err).disposition;
}

pub fn deliverySendFailure(err: anyerror) receiver.DeliveryError {
    return switch (err) {
        error.OutOfMemory,
        error.RecoveryFull,
        error.RecoveryBytesExceeded,
        error.CongestionWindowFull,
        => error.ResourceLimitFailure,
        error.TransportFailure => error.TransportFailure,
        else => error.InternalFailure,
    };
}
pub fn classifyTransitionError(transition: SessionTransition, err: anyerror) IncomingFailure {
    return switch (transition) {
        .receive => classifyIncomingError(err),
        .application_send => switch (err) {
            error.EmptyPayload,
            error.MessageTooLarge,
            error.InvalidOrderChannel,
            error.UnreliableMessageTooLarge,
            error.NotConnected,
            error.ConnectionClosed,
            => .{ .class = .application, .disposition = .reject },

            error.CongestionWindowFull => .{ .class = .resource, .disposition = .retry },

            error.OutOfMemory,
            error.ResourceLimitFailure,
            error.RecoveryFull,
            error.RecoveryBytesExceeded,
            => .{ .class = .resource, .disposition = .close_session },

            error.TransportFailure => .{ .class = .transport, .disposition = .close_session },
            else => .{ .class = .internal, .disposition = .close_session },
        },
        .receipt => switch (err) {
            error.TransportFailure => .{ .class = .transport, .disposition = .close_session },
            error.OutOfMemory => .{ .class = .resource, .disposition = .close_session },
            else => .{ .class = .internal, .disposition = .close_session },
        },
        .retransmission => switch (err) {
            error.TransportFailure,
            error.RetransmissionLimitExceeded,
            => .{ .class = .transport, .disposition = .close_session },
            else => .{ .class = .internal, .disposition = .close_session },
        },
        .handshake => switch (err) {
            error.IncompatibleProtocol,
            error.HandshakeMismatch,
            error.PeerProtocolFailure,
            => .{ .class = .protocol, .disposition = .close_session },
            error.Timeout,
            error.HandshakeWorkLimitExceeded,
            error.TransportFailure,
            => .{ .class = .transport, .disposition = .close_session },
            error.OutOfMemory,
            error.ResourceLimitFailure,
            => .{ .class = .resource, .disposition = .close_session },
            else => .{ .class = .internal, .disposition = .close_session },
        },
    };
}
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

    pub const SendFn = *const fn (context: *anyopaque, wire: []const u8) SendError!void;

    pub fn send(self: *Core, payload: []const u8, reliability: frame.Reliability, channel: u8, scratch: []u8, now_ms: u64, context: *anyopaque, emit: SendFn) !transmitter.Sent {
        const wire_bytes = try self.transmitter_state.estimateWireBytes(payload.len, reliability, channel);
        if (wire_bytes > self.congestion_state.available()) return error.CongestionWindowFull;
        const Bridge = struct {
            core: *Core,
            user_context: *anyopaque,
            user_emit: SendFn,
            now_ms: u64,
            fn forward(raw: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) transmitter.EmitError!void {
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
    pub fn nextRetransmissionDeadline(self: Core) ?u64 {
        return self.recovery_state.nextDeadline();
    }

    pub fn nextSplitDeadline(self: Core) ?u64 {
        return self.receiver_state.nextSplitDeadline();
    }

    pub fn expireSplits(self: *Core, now_ms: u64) usize {
        return self.receiver_state.expireSplits(now_ms);
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
        fn discard(_: *anyopaque, _: receiver.BorrowedPayload) !void {}
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
test "incoming failures keep their origin and commit safety" {
    const truncated = classifyIncomingError(error.Truncated);
    try std.testing.expectEqual(IncomingErrorClass.protocol, truncated.class);
    try std.testing.expectEqual(IncomingErrorDisposition.reject, truncated.disposition);

    const peer = classifyIncomingError(error.PeerProtocolFailure);
    try std.testing.expectEqual(IncomingErrorClass.protocol, peer.class);
    try std.testing.expectEqual(IncomingErrorDisposition.close_session, peer.disposition);

    try std.testing.expectEqual(IncomingErrorClass.resource, classifyIncomingError(error.OutOfMemory).class);
    try std.testing.expectEqual(IncomingErrorClass.transport, classifyIncomingError(error.TransportFailure).class);
    try std.testing.expectEqual(IncomingErrorClass.application, classifyIncomingError(error.ApplicationFailure).class);
    try std.testing.expectEqual(IncomingErrorClass.internal, classifyIncomingError(error.InternalInvariant).class);

    try std.testing.expectEqual(IncomingErrorDisposition.reject, incomingErrorDisposition(error.PacketWorkLimitExceeded));
    try std.testing.expectEqual(IncomingErrorDisposition.reject, incomingErrorDisposition(error.DatagramWindowExceeded));
    try std.testing.expectEqual(IncomingErrorDisposition.close_session, incomingErrorDisposition(error.ReliableWindowExceeded));
    try std.testing.expectEqual(IncomingErrorDisposition.close_session, incomingErrorDisposition(error.OrderQueueFull));

    try std.testing.expect(deliverySendFailure(error.OutOfMemory) == error.ResourceLimitFailure);
    try std.testing.expect(deliverySendFailure(error.TransportFailure) == error.TransportFailure);
    try std.testing.expect(deliverySendFailure(error.UnexpectedOrderIndex) == error.InternalFailure);
}

test "transition policy distinguishes rejection, retry, and closure" {
    const bad_send = classifyTransitionError(.application_send, error.EmptyPayload);
    try std.testing.expectEqual(IncomingErrorClass.application, bad_send.class);
    try std.testing.expectEqual(IncomingErrorDisposition.reject, bad_send.disposition);

    const pressure = classifyTransitionError(.application_send, error.CongestionWindowFull);
    try std.testing.expectEqual(IncomingErrorClass.resource, pressure.class);
    try std.testing.expectEqual(IncomingErrorDisposition.retry, pressure.disposition);

    const allocation = classifyTransitionError(.application_send, error.OutOfMemory);
    try std.testing.expectEqual(IncomingErrorClass.resource, allocation.class);
    try std.testing.expectEqual(IncomingErrorDisposition.close_session, allocation.disposition);

    const send_transport = classifyTransitionError(.application_send, error.TransportFailure);
    try std.testing.expectEqual(IncomingErrorDisposition.close_session, send_transport.disposition);
    try std.testing.expectEqual(IncomingErrorClass.transport, send_transport.class);

    try std.testing.expectEqual(IncomingErrorClass.internal, classifyTransitionError(.receipt, error.NoSpaceLeft).class);
    try std.testing.expectEqual(IncomingErrorClass.transport, classifyTransitionError(.retransmission, error.RetransmissionLimitExceeded).class);
    try std.testing.expectEqual(IncomingErrorClass.protocol, classifyTransitionError(.handshake, error.IncompatibleProtocol).class);
    try std.testing.expectEqual(IncomingErrorClass.transport, classifyTransitionError(.handshake, error.Timeout).class);
}
