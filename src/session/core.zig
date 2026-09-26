const std = @import("std");

const Config = @import("../config.zig").Config;
const ack = @import("../protocol/ack.zig");
const datagram = @import("../protocol/datagram.zig");
const frame = @import("../protocol/frame.zig");
const offline = @import("../protocol/offline.zig");
const congestion = @import("../reliability/congestion.zig");
const reassembly = @import("../reliability/reassembly.zig");
const recovery = @import("../reliability/recovery.zig");
const rtt = @import("../reliability/rtt.zig");
const outbound_queue = @import("outbound_queue.zig");
const receiver = @import("receiver.zig");
const transmitter = @import("transmitter.zig");
pub const FlushResult = transmitter.Sent;

pub const Statistics = struct {
    mtu: u16,
    smoothed_rtt_ms: ?u64,
    retransmission_timeout_ms: u32,
    congestion_window_bytes: u64,
    in_flight_bytes: u64,
    acknowledged_datagrams: u64,
    lost_datagrams: u64,
    retransmitted_datagrams: u64,
    recovery_packets: usize,
    recovery_payload_bytes: usize,
    queued_packets: usize,
    queued_payload_bytes: usize,
    ordered_packets: usize,
    ordered_payload_bytes: usize,
    split_assemblies: usize,
    split_payload_bytes: usize,
    retained_payload_capacity_bytes: usize,
    queued_packets_high_water: usize,
    ack_records_received: u64,
    nack_records_received: u64,
    ack_records_sent: u64 = 0,
    nack_records_sent: u64 = 0,
};

var next_send_owner: std.atomic.Value(u64) = .init(1);

pub const Incoming = union(enum) {
    data: receiver.Receipt,
    acknowledged: recovery.Acknowledged,
    nack_marked: usize,
};

pub const CloseStep = enum { pending, flush, done };

pub const ProcessedIncoming = struct {
    incoming: Incoming,
    work_units: usize,
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
pub const SendHandle = struct { id: outbound_queue.Id, owner: u64 };
pub const CancelResult = enum { canceled, not_found, in_progress };
const QueuedMessages = struct {
    iterator: outbound_queue.Iterator,

    pub fn next(self: *@This()) ?transmitter.PackedMessage {
        const message = self.iterator.next() orelse return null;
        return .{
            .payload = message.payload.bytes,
            .reliability = message.reliability,
            .channel = message.channel,
        };
    }
};

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

        error.TooManyAssemblies,
        error.SplitBudgetExceeded,
        => .{ .class = .resource, .disposition = .reject },

        error.PeerProtocolFailure,
        error.ReliableWindowExceeded,
        error.OrderWindowExceeded,
        error.ConflictingFragment,
        => .{ .class = .protocol, .disposition = .close_session },

        error.OutOfMemory,
        error.ResourceLimitFailure,
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
        error.TransportFailure,
        error.PartialSendFailure,
        => error.TransportFailure,
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

            error.CongestionWindowFull,
            error.OutboundQueuePending,
            error.OutboundQueueFull,
            error.OutboundQueueBytesExceeded,
            error.OutboundQueueIdExhausted,
            => .{ .class = .resource, .disposition = .retry },

            error.OutOfMemory,
            error.ResourceLimitFailure,
            error.RecoveryFull,
            error.RecoveryBytesExceeded,
            => .{ .class = .resource, .disposition = .close_session },

            error.TransportFailure,
            error.PartialSendFailure,
            => .{ .class = .transport, .disposition = .close_session },
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
            error.PartialSendFailure,
            => .{ .class = .transport, .disposition = .close_session },
            error.OutOfMemory,
            error.ResourceLimitFailure,
            => .{ .class = .resource, .disposition = .close_session },
            else => .{ .class = .internal, .disposition = .close_session },
        },
    };
}
pub const Core = struct {
    allocator: std.mem.Allocator,
    config: Config,
    receiver_state: receiver.Receiver,
    transmitter_state: transmitter.Transmitter,
    outbound_state: outbound_queue.Queue,
    outbound_packetization: [2]?transmitter.Packetization = @splat(null),
    recovery_state: recovery.Recovery,
    congestion_state: congestion.Controller,
    rtt_state: rtt.Estimator,
    ack_records: []ack.Record,
    send_owner: u64,
    terminal_send_failure: bool = false,
    newest_sent: u32 = 0,
    close_deadline_ms: ?u64 = null,
    disconnect_queued: bool = false,
    acknowledged_datagrams: u64 = 0,
    lost_datagrams: u64 = 0,
    retransmitted_datagrams: u64 = 0,
    ack_records_received: u64 = 0,
    nack_records_received: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, mtu: u16, config: Config) !Core {
        try config.validate();
        if (mtu < config.protocol.minimum_mtu or mtu > config.protocol.maximum_mtu) return error.InvalidMtu;
        var receiver_state = try receiver.Receiver.init(allocator, config);
        errdefer receiver_state.deinit();
        var recovery_state = try recovery.Recovery.init(allocator, config.session.maximum_retransmissions, config.session.maximum_recovery_bytes, 8, mtu);
        recovery_state.maximum_delay_ms = config.timing.maximum_rto_ms;
        errdefer recovery_state.deinit();
        var outbound_state = try outbound_queue.Queue.init(
            allocator,
            config.session.maximum_queued_outbound_packets,
            config.session.maximum_queued_outbound_bytes,
            config.session.reserved_control_queue_packets,
            config.session.reserved_control_queue_bytes,
        );
        errdefer outbound_state.deinit();
        const ack_records = try allocator.alloc(ack.Record, config.batching.maximum_ack_records);
        return .{
            .allocator = allocator,
            .config = config,
            .receiver_state = receiver_state,
            .transmitter_state = try transmitter.Transmitter.init(mtu, config),
            .outbound_state = outbound_state,
            .recovery_state = recovery_state,
            .congestion_state = try congestion.Controller.init(mtu),
            .rtt_state = try rtt.Estimator.init(config.timing.minimum_rto_ms, config.timing.maximum_rto_ms),
            .ack_records = ack_records,
            .send_owner = next_send_owner.fetchAdd(1, .monotonic),
        };
    }
    pub fn deinit(self: *Core) void {
        self.receiver_state.deinit();
        self.recovery_state.deinit();
        self.outbound_state.deinit();
        self.allocator.free(self.ack_records);
        self.* = undefined;
    }

    pub const SendFn = *const fn (context: *anyopaque, wire: []const u8) SendError!void;

    pub fn send(self: *Core, payload: []const u8, reliability: frame.Reliability, channel: u8, scratch: []u8, now_ms: u64, context: *anyopaque, emit: SendFn) !transmitter.Sent {
        return self.sendImmediate(.application, payload, reliability, channel, scratch, now_ms, context, emit);
    }

    pub fn sendControl(
        self: *Core,
        payload: []const u8,
        reliability: frame.Reliability,
        channel: u8,
        scratch: []u8,
        now_ms: u64,
        context: *anyopaque,
        emit: SendFn,
    ) !transmitter.Sent {
        return self.sendImmediate(.control, payload, reliability, channel, scratch, now_ms, context, emit) catch |err| switch (err) {
            error.CongestionWindowFull, error.OutboundQueuePending => {
                _ = try self.enqueueOutbound(.control, payload, reliability, channel);
                return .{ .datagrams = 0, .wire_bytes = 0 };
            },
            else => return err,
        };
    }

    fn sendImmediate(
        self: *Core,
        lane: outbound_queue.Lane,
        payload: []const u8,
        reliability: frame.Reliability,
        channel: u8,
        scratch: []u8,
        now_ms: u64,
        context: *anyopaque,
        emit: SendFn,
    ) !transmitter.Sent {
        if (self.terminal_send_failure) return error.ConnectionClosed;
        const wire_bytes = try self.transmitter_state.estimateWireBytes(payload.len, reliability, channel);
        if (self.outbound_state.count(lane) != 0) return error.OutboundQueuePending;
        if (wire_bytes > self.congestion_state.available()) return error.CongestionWindowFull;
        if (reliability.hasReliableIndex()) {
            const fragments = try self.transmitter_state.fragmentCount(payload.len, reliability, channel);
            if (self.recoverySlots(fragments, now_ms) < fragments) return error.CongestionWindowFull;
        }
        var packetization = try self.transmitter_state.beginPacketization(payload.len, reliability, channel);
        const sent = try self.sendAvailable(&packetization, payload, scratch, std.math.maxInt(usize), now_ms, context, emit);
        if (!packetization.complete()) return error.CongestionWindowFull;
        return sent;
    }

    pub fn beginPacketization(self: *Core, payload_len: usize, reliability: frame.Reliability, channel: u8) !transmitter.Packetization {
        if (self.terminal_send_failure) return error.ConnectionClosed;
        return self.transmitter_state.beginPacketization(payload_len, reliability, channel);
    }

    pub fn sendAvailable(
        self: *Core,
        packetization: *transmitter.Packetization,
        payload: []const u8,
        scratch: []u8,
        maximum_datagrams: usize,
        now_ms: u64,
        context: *anyopaque,
        emit: SendFn,
    ) !transmitter.Sent {
        if (self.terminal_send_failure) return error.ConnectionClosed;
        const Bridge = struct {
            core: *Core,
            user_context: *anyopaque,
            user_emit: SendFn,
            now_ms: u64,
            fn forward(raw: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) transmitter.EmitError!void {
                const bridge: *@This() = @ptrCast(@alignCast(raw));
                const previous_newest = bridge.core.newest_sent;
                if (reliable) try bridge.core.trackSent(sequence, wire, wire.len, bridge.now_ms);
                bridge.user_emit(bridge.user_context, wire) catch |err| {
                    if (reliable) bridge.core.rollbackSent(sequence, previous_newest);
                    return err;
                };
            }
        };
        var bridge: Bridge = .{ .core = self, .user_context = context, .user_emit = emit, .now_ms = now_ms };
        const datagrams = if (packetization.reliability.hasReliableIndex())
            self.recoverySlots(@min(maximum_datagrams, packetization.fragment_count - packetization.next_fragment), now_ms)
        else
            maximum_datagrams;
        const available = std.math.cast(usize, self.congestion_state.available()) orelse std.math.maxInt(usize);
        return self.transmitter_state.sendAvailable(packetization, payload, scratch, available, datagrams, &bridge, Bridge.forward) catch |err| {
            if (err == error.PartialSendFailure) self.terminal_send_failure = true;
            return err;
        };
    }

    fn packQueued(self: *Core, lane: outbound_queue.Lane, scratch: []u8, now_ms: u64, context: *anyopaque, emit: SendFn) !transmitter.PackResult {
        const Bridge = struct {
            core: *Core,
            user_context: *anyopaque,
            user_emit: SendFn,
            now_ms: u64,
            fn forward(raw: *anyopaque, sequence: u32, reliable: bool, wire: []const u8) transmitter.EmitError!void {
                const bridge: *@This() = @ptrCast(@alignCast(raw));
                const previous_newest = bridge.core.newest_sent;
                if (reliable) try bridge.core.trackSent(sequence, wire, wire.len, bridge.now_ms);
                bridge.user_emit(bridge.user_context, wire) catch |err| {
                    if (reliable) bridge.core.rollbackSent(sequence, previous_newest);
                    return err;
                };
            }
        };
        var bridge: Bridge = .{ .core = self, .user_context = context, .user_emit = emit, .now_ms = now_ms };
        if (self.recoverySlots(1, now_ms) == 0) return .{};
        const available = std.math.cast(usize, self.congestion_state.available()) orelse std.math.maxInt(usize);
        return self.transmitter_state.pack(
            QueuedMessages{ .iterator = self.outbound_state.iterator(lane) },
            scratch,
            available,
            &bridge,
            Bridge.forward,
        );
    }

    pub fn enqueueOutbound(self: *Core, lane: outbound_queue.Lane, payload: []const u8, reliability: frame.Reliability, channel: u8) !SendHandle {
        if (self.terminal_send_failure) return error.ConnectionClosed;
        _ = try self.transmitter_state.estimateWireBytes(payload.len, reliability, channel);
        return .{ .id = try self.outbound_state.enqueue(lane, payload, reliability, channel), .owner = self.send_owner };
    }

    pub fn outboundReady(self: *const Core, lane: outbound_queue.Lane) !bool {
        return self.transmitter_state.packReady(QueuedMessages{ .iterator = self.outbound_state.iterator(lane) });
    }

    pub fn outboundCount(self: *const Core, lane: outbound_queue.Lane) usize {
        return self.outbound_state.count(lane);
    }

    pub fn cancelOutbound(self: *Core, handle: SendHandle) CancelResult {
        if (handle.owner != self.send_owner) return .not_found;
        inline for (.{ outbound_queue.Lane.control, outbound_queue.Lane.application }) |lane| {
            const lane_index = @intFromEnum(lane);
            if (self.outbound_state.peek(lane)) |head| {
                if (head.id == handle.id and self.outbound_packetization[lane_index] != null) return .in_progress;
            }
            if (self.outbound_state.cancel(lane, handle.id)) |message| {
                message.payload.deinit();
                return .canceled;
            }
        }
        return .not_found;
    }

    pub fn statistics(self: *const Core) Statistics {
        const receiver_state = self.receiver_state;
        const recovery_capacity = self.recovery_state.retainedCapacity();
        const ordered_capacity = receiver_state.ordered.retainedCapacity();
        const split_capacity = receiver_state.splits.retainedCapacity();
        return .{
            .mtu = self.transmitter_state.mtu,
            .smoothed_rtt_ms = if (self.rtt_state.initialized) self.rtt_state.smoothed_ms else null,
            .retransmission_timeout_ms = self.rtt_state.rto(),
            .congestion_window_bytes = self.congestion_state.window,
            .in_flight_bytes = self.congestion_state.in_flight,
            .acknowledged_datagrams = self.acknowledged_datagrams,
            .lost_datagrams = self.lost_datagrams,
            .retransmitted_datagrams = self.retransmitted_datagrams,
            .recovery_packets = self.recovery_state.count(),
            .recovery_payload_bytes = self.recovery_state.payloadBytes(),
            .queued_packets = self.outbound_state.countAll(),
            .queued_payload_bytes = self.outbound_state.byteCountAll(),
            .ordered_packets = receiver_state.ordered.count(),
            .ordered_payload_bytes = receiver_state.ordered.payloadBytes(),
            .split_assemblies = receiver_state.splits.count(),
            .split_payload_bytes = receiver_state.splits.payloadBytes(),
            .retained_payload_capacity_bytes = recovery_capacity +| ordered_capacity +| split_capacity,
            .queued_packets_high_water = self.outbound_state.high_water,
            .ack_records_received = self.ack_records_received,
            .nack_records_received = self.nack_records_received,
        };
    }

    pub fn flushOutbound(self: *Core, lane: outbound_queue.Lane, scratch: []u8, maximum_datagrams: usize, now_ms: u64, context: *anyopaque, emit: SendFn) !transmitter.Sent {
        if (self.terminal_send_failure) return error.ConnectionClosed;
        var total: transmitter.Sent = .{ .datagrams = 0, .wire_bytes = 0 };
        const lane_index = @intFromEnum(lane);
        while (total.datagrams < maximum_datagrams) {
            const message = self.outbound_state.peek(lane) orelse break;
            if (self.outbound_packetization[lane_index] == null) {
                const batch = self.packQueued(lane, scratch, now_ms, context, emit) catch |err| {
                    if (total.datagrams != 0) {
                        self.terminal_send_failure = true;
                        return error.PartialSendFailure;
                    }
                    return err;
                };
                if (batch.messages != 0) {
                    total.datagrams = try std.math.add(usize, total.datagrams, batch.sent.datagrams);
                    total.wire_bytes = try std.math.add(usize, total.wire_bytes, batch.sent.wire_bytes);
                    for (0..batch.messages) |_| {
                        const completed = self.outbound_state.pop(lane).?;
                        completed.payload.deinit();
                    }
                    continue;
                }
                self.outbound_packetization[lane_index] = try self.transmitter_state.beginPacketization(message.payload.bytes.len, message.reliability, message.channel);
            }
            const sent = self.sendAvailable(
                &self.outbound_packetization[lane_index].?,
                message.payload.bytes,
                scratch,
                maximum_datagrams - total.datagrams,
                now_ms,
                context,
                emit,
            ) catch |err| {
                if (total.datagrams != 0 and err != error.PartialSendFailure) {
                    self.terminal_send_failure = true;
                    return error.PartialSendFailure;
                }
                return err;
            };
            total.datagrams = try std.math.add(usize, total.datagrams, sent.datagrams);
            total.wire_bytes = try std.math.add(usize, total.wire_bytes, sent.wire_bytes);
            if (!self.outbound_packetization[lane_index].?.complete()) break;
            self.outbound_packetization[lane_index] = null;
            const completed = self.outbound_state.pop(lane).?;
            completed.payload.deinit();
        }
        return total;
    }
    pub fn flushAllOutbound(self: *Core, scratch: []u8, maximum_datagrams: usize, now_ms: u64, context: *anyopaque, emit: SendFn) !transmitter.Sent {
        const control = try self.flushOutbound(.control, scratch, maximum_datagrams, now_ms, context, emit);
        const application = try self.flushOutbound(.application, scratch, maximum_datagrams - control.datagrams, now_ms, context, emit);
        return .{ .datagrams = control.datagrams + application.datagrams, .wire_bytes = control.wire_bytes + application.wire_bytes };
    }

    pub fn beginClose(self: *Core, now_ms: u64) void {
        if (self.close_deadline_ms == null) self.close_deadline_ms = now_ms +| self.config.timing.shutdown_timeout_ms;
    }

    pub fn advanceClose(self: *Core, now_ms: u64) !CloseStep {
        const deadline = self.close_deadline_ms orelse return .pending;
        if (now_ms >= deadline) return .done;
        if (self.outbound_state.countAll() != 0 or self.recovery_state.count() != 0) return .pending;
        if (self.disconnect_queued) return .done;
        _ = try self.enqueueOutbound(.control, &.{@intFromEnum(offline.Id.disconnect_notification)}, .reliable_ordered, 0);
        self.disconnect_queued = true;
        return .flush;
    }

    fn recoverySlots(self: *Core, needed: usize, now_ms: u64) usize {
        const first = self.transmitter_state.datagram_sequence;
        const free = self.recovery_state.freeSlots(first, needed);
        if (free < needed and self.recovery_state.expedite(first, free, now_ms)) {
            self.lost_datagrams +|= 1;
            self.congestion_state.lost(self.newest_sent);
        }
        return free;
    }

    /// Copies a reliable datagram into bounded recovery storage before it is handed to the socket.
    pub fn trackSent(self: *Core, sequence: u32, wire: []const u8, in_flight_bytes: usize, now_ms: u64) !void {
        if (self.terminal_send_failure) return error.ConnectionClosed;
        try self.congestion_state.sent(in_flight_bytes);
        errdefer self.congestion_state.cancel(in_flight_bytes);
        try self.recovery_state.track(sequence, wire, in_flight_bytes, now_ms, self.rtt_state.rto());
        self.newest_sent = sequence;
    }

    fn rollbackSent(self: *Core, sequence: u32, previous_newest: u32) void {
        const in_flight_bytes = self.recovery_state.untrack(sequence) orelse return;
        self.congestion_state.cancel(in_flight_bytes);
        self.newest_sent = previous_newest;
    }

    pub fn processIncoming(self: *Core, wire: []const u8, now_ms: u64, context: *anyopaque, deliver: receiver.DeliverFn) !Incoming {
        return (try self.processIncomingImpl(wire, now_ms, null, context, deliver)).incoming;
    }

    pub fn processIncomingWithScratch(self: *Core, wire: []const u8, now_ms: u64, frame_scratch: []frame.Frame, context: *anyopaque, deliver: receiver.DeliverFn) !Incoming {
        return (try self.processIncomingImpl(wire, now_ms, frame_scratch, context, deliver)).incoming;
    }

    pub fn processIncomingCountedWithScratch(
        self: *Core,
        wire: []const u8,
        now_ms: u64,
        frame_scratch: []frame.Frame,
        context: *anyopaque,
        deliver: receiver.DeliverFn,
    ) !ProcessedIncoming {
        return self.processIncomingImpl(wire, now_ms, frame_scratch, context, deliver);
    }

    fn processIncomingImpl(self: *Core, wire: []const u8, now_ms: u64, frame_scratch: ?[]frame.Frame, context: *anyopaque, deliver: receiver.DeliverFn) !ProcessedIncoming {
        if (wire.len > self.config.protocol.maximum_datagram_size) return error.DatagramTooLarge;
        return switch (try datagram.decode(wire, self.ack_records, self.config.batching.maximum_ack_records, self.config.protocol.maximum_acknowledged_datagrams)) {
            .data => blk: {
                const receipt = if (frame_scratch) |scratch|
                    try self.receiver_state.processWithScratch(wire, now_ms, scratch, context, deliver)
                else
                    try self.receiver_state.process(wire, now_ms, context, deliver);
                break :blk .{ .incoming = .{ .data = receipt }, .work_units = receipt.workUnits() };
            },
            .ack => |decoded| blk: {
                const result = try self.recovery_state.acknowledge(decoded.records, now_ms, self.config.protocol.maximum_acknowledged_datagrams);
                self.ack_records_received += decoded.records.len;
                self.acknowledged_datagrams +|= result.packets;
                if (result.packets != 0) self.congestion_state.acknowledged(decoded.records[decoded.records.len - 1].last, result.bytes);
                if (result.rtt_sample_ms) |sample| self.rtt_state.observe(sample);
                break :blk .{ .incoming = .{ .acknowledged = result }, .work_units = 1 + decoded.acknowledged_count };
            },
            .nack => |decoded| blk: {
                const marked = try self.recovery_state.markNack(decoded.records, now_ms, self.config.protocol.maximum_acknowledged_datagrams);
                self.nack_records_received += decoded.records.len;
                self.lost_datagrams +|= marked;
                if (marked != 0) self.congestion_state.lost(self.newest_sent);
                break :blk .{ .incoming = .{ .nack_marked = marked }, .work_units = 1 + decoded.acknowledged_count };
            },
        };
    }
    pub fn nextRetransmissionDeadline(self: Core) ?u64 {
        return self.recovery_state.nextDeadline();
    }

    pub fn nextSplitDeadline(self: Core) ?u64 {
        return self.receiver_state.nextSplitDeadline();
    }

    pub fn expireSplits(self: *Core, now_ms: u64, maximum_work: usize) reassembly.ExpiryBatch {
        return self.receiver_state.expireSplits(now_ms, maximum_work);
    }

    pub fn collectRetransmissions(self: *Core, now_ms: u64, output: []recovery.Due, maximum_work: usize) recovery.DueBatch {
        const batch = self.recovery_state.collectDue(now_ms, self.rtt_state.rto(), output, maximum_work);
        self.retransmitted_datagrams +|= batch.items.len;
        var timed_out: usize = 0;
        for (batch.items) |item| if (item.timed_out) {
            timed_out += 1;
            self.congestion_state.timeout(self.newest_sent);
        };
        self.lost_datagrams +|= timed_out;
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
    const counted = try core.processIncomingCountedWithScratch(wire, 150, &.{}, &unused, Collector.discard);
    try std.testing.expectEqual(@as(usize, 1), counted.incoming.acknowledged.packets);
    try std.testing.expectEqual(@as(?u64, 50), counted.incoming.acknowledged.rtt_sample_ms);
    try std.testing.expectEqual(@as(usize, 4), counted.work_units);
    const statistics = core.statistics();
    try std.testing.expectEqual(@as(u16, 1200), statistics.mtu);
    try std.testing.expectEqual(@as(?u64, 50), statistics.smoothed_rtt_ms);
    try std.testing.expectEqual(@as(u64, 1), statistics.acknowledged_datagrams);
    try std.testing.expectEqual(@as(u64, 0), statistics.lost_datagrams);
    try std.testing.expectEqual(@as(usize, 0), statistics.recovery_packets);
    try std.testing.expectEqual(@as(usize, 0), statistics.recovery_payload_bytes);
    try std.testing.expectEqual(@as(usize, 0), statistics.queued_packets);
    try std.testing.expectEqual(@as(usize, 0), (try core.processIncoming(wire, 160, &unused, Collector.discard)).acknowledged.packets);
    try std.testing.expectEqual(@as(u64, 1), core.statistics().acknowledged_datagrams);
}

test "recovery and outbound queue byte limits are independent" {
    var recovery_limited: Config = .{};
    recovery_limited.session.maximum_recovery_bytes = 1;
    recovery_limited.session.maximum_queued_outbound_bytes = 1024;
    recovery_limited.session.reserved_control_queue_bytes = 128;
    var first = try Core.init(std.testing.allocator, 1200, recovery_limited);
    defer first.deinit();
    try std.testing.expectError(error.RecoveryBytesExceeded, first.trackSent(1, "xx", 2, 0));

    var queue_limited: Config = .{};
    queue_limited.session.maximum_recovery_bytes = 2;
    queue_limited.session.maximum_queued_outbound_bytes = 2;
    queue_limited.session.reserved_control_queue_bytes = 1;
    var second = try Core.init(std.testing.allocator, 1200, queue_limited);
    defer second.deinit();
    try second.trackSent(1, "xx", 2, 0);
}

test "core packetization stops at the available congestion window" {
    const Collector = struct {
        count: usize = 0,
        fn emit(raw: *anyopaque, _: []const u8) SendError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };
    var core = try Core.init(std.testing.allocator, 576, .{});
    defer core.deinit();
    core.congestion_state.window = 576;
    var scratch: [576]u8 = undefined;
    var payload: [1200]u8 = @splat(1);
    var packetization = try core.beginPacketization(payload.len, .reliable_ordered, 0);
    var collector: Collector = .{};

    const first = try core.sendAvailable(&packetization, &payload, &scratch, 1, 0, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 1), first.datagrams);
    try std.testing.expectEqual(@as(u64, 0), core.congestion_state.available());
    const blocked = try core.sendAvailable(&packetization, &payload, &scratch, 1, 0, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 0), blocked.datagrams);
    try std.testing.expectEqual(@as(usize, 1), collector.count);
    try std.testing.expect(!packetization.complete());
}

test "queued compatible messages share one datagram" {
    const Collector = struct {
        frames: usize = 0,
        wire_bytes: usize = 0,
        fn emit(raw: *anyopaque, wire: []const u8) SendError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.wire_bytes = wire.len;
            var decoded = frame.decodeDatagram(wire) catch return error.TransportFailure;
            while (decoded.frames.remaining() != 0) {
                _ = frame.decodeOne(&decoded.frames, 8192, 2048) catch return error.TransportFailure;
                self.frames += 1;
            }
        }
    };
    var core = try Core.init(std.testing.allocator, 576, .{});
    defer core.deinit();
    core.congestion_state.window = 576;
    var scratch: [576]u8 = undefined;
    var first: [276]u8 = @splat(1);
    var second: [276]u8 = @splat(2);
    _ = try core.enqueueOutbound(.application, &first, .reliable_ordered, 3);
    _ = try core.enqueueOutbound(.application, &second, .reliable_ordered, 3);
    _ = try core.enqueueOutbound(.application, "later", .reliable_ordered, 3);
    var collector: Collector = .{};

    const sent = try core.flushOutbound(.application, &scratch, 1, 0, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 1), sent.datagrams);
    try std.testing.expectEqual(@as(usize, 576), sent.wire_bytes);
    try std.testing.expectEqual(@as(usize, 2), collector.frames);
    try std.testing.expectEqual(@as(usize, 1), core.outbound_state.count(.application));
    try std.testing.expectEqual(@as(u32, 2), core.transmitter_state.reliable_index);
    try std.testing.expectEqual(@as(u32, 2), core.transmitter_state.order_indices[3]);
}

test "failed packed send keeps the queue and protocol indices" {
    const Failing = struct {
        fn emit(_: *anyopaque, _: []const u8) SendError!void {
            return error.TransportFailure;
        }
    };
    var core = try Core.init(std.testing.allocator, 576, .{});
    defer core.deinit();
    var scratch: [576]u8 = undefined;
    _ = try core.enqueueOutbound(.application, "first", .reliable_ordered, 1);
    _ = try core.enqueueOutbound(.application, "second", .reliable_ordered, 1);
    const available = core.congestion_state.available();
    var unused: u8 = 0;

    try std.testing.expectError(error.TransportFailure, core.flushOutbound(.application, &scratch, 1, 0, &unused, Failing.emit));
    try std.testing.expectEqual(@as(usize, 2), core.outbound_state.count(.application));
    try std.testing.expectEqual(@as(u32, 0), core.transmitter_state.datagram_sequence);
    try std.testing.expectEqual(@as(u32, 0), core.transmitter_state.reliable_index);
    try std.testing.expectEqual(@as(u32, 0), core.transmitter_state.order_indices[1]);
    try std.testing.expectEqual(available, core.congestion_state.available());
}

test "queued split message resumes after congestion capacity returns" {
    const Collector = struct {
        count: usize = 0,
        fn emit(raw: *anyopaque, _: []const u8) SendError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };
    const Discard = struct {
        fn deliver(_: *anyopaque, _: receiver.BorrowedPayload) !void {}
    };
    var core = try Core.init(std.testing.allocator, 576, .{});
    defer core.deinit();
    core.congestion_state.window = 576;
    var scratch: [576]u8 = undefined;
    var payload: [1200]u8 = @splat(1);
    var collector: Collector = .{};
    _ = try core.enqueueOutbound(.application, &payload, .reliable_ordered, 0);

    const first = try core.flushOutbound(.application, &scratch, 8, 0, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 1), first.datagrams);
    try std.testing.expectEqual(@as(usize, 1), core.outbound_state.count(.application));
    const blocked = try core.flushOutbound(.application, &scratch, 8, 1, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 0), blocked.datagrams);

    var ack_wire: [32]u8 = undefined;
    const encoded_ack = try datagram.encodeControl(.ack, &.{.{ .first = 0, .last = 0 }}, &ack_wire);
    var unused: u8 = 0;
    _ = try core.processIncoming(encoded_ack, 2, &unused, Discard.deliver);
    const completed = try core.flushOutbound(.application, &scratch, 8, 2, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 2), completed.datagrams);
    try std.testing.expectEqual(@as(usize, 3), collector.count);
    try std.testing.expectEqual(@as(usize, 0), core.outbound_state.count(.application));
    try std.testing.expectEqual(@as(usize, 0), core.outbound_state.total_bytes);
}

test "queued send cancellation is explicit and ownership safe" {
    const Collector = struct {
        fn emit(_: *anyopaque, _: []const u8) SendError!void {}
    };
    var core = try Core.init(std.testing.allocator, 576, .{});
    defer core.deinit();
    core.congestion_state.window = 576;
    var scratch: [576]u8 = undefined;
    var payload: [1200]u8 = @splat(1);
    const active = try core.enqueueOutbound(.application, &payload, .reliable_ordered, 0);
    const waiting = try core.enqueueOutbound(.application, "later", .reliable_ordered, 0);

    try std.testing.expectEqual(CancelResult.canceled, core.cancelOutbound(waiting));
    try std.testing.expectEqual(CancelResult.not_found, core.cancelOutbound(waiting));
    var unused: u8 = 0;
    _ = try core.flushOutbound(.application, &scratch, 1, 0, &unused, Collector.emit);
    try std.testing.expectEqual(CancelResult.in_progress, core.cancelOutbound(active));
    try std.testing.expectEqual(@as(usize, 1), core.outbound_state.count(.application));
}

test "recovery slot collisions apply backpressure instead of failing sends" {
    const Collector = struct {
        count: usize = 0,
        fn emit(raw: *anyopaque, _: []const u8) SendError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };
    var config: Config = .{};
    config.session.maximum_retransmissions = 4;
    var core = try Core.init(std.testing.allocator, 576, config);
    defer core.deinit();
    var scratch: [576]u8 = undefined;
    var collector: Collector = .{};
    _ = try core.send("pinned", .reliable, 0, &scratch, 0, &collector, Collector.emit);
    var unused: u8 = 0;
    const Discard = struct {
        fn deliver(_: *anyopaque, _: receiver.BorrowedPayload) !void {}
    };
    for (1..4) |sequence| {
        _ = try core.send("acked", .reliable, 0, &scratch, 0, &collector, Collector.emit);
        var wire: [32]u8 = undefined;
        const seq: u32 = @intCast(sequence);
        _ = try core.processIncoming(try datagram.encodeControl(.ack, &.{.{ .first = seq, .last = seq }}, &wire), 1, &unused, Discard.deliver);
    }
    const order_index = core.transmitter_state.order_indices[0];
    try std.testing.expectError(error.CongestionWindowFull, core.send("blocked", .reliable_ordered, 0, &scratch, 1, &collector, Collector.emit));
    try std.testing.expectEqual(order_index, core.transmitter_state.order_indices[0]);
    try std.testing.expectEqual(@as(?u64, 1), core.nextRetransmissionDeadline());
    try std.testing.expectEqual(@as(u64, 1), core.lost_datagrams);
    var large: [1200]u8 = @splat(1);
    _ = try core.enqueueOutbound(.application, &large, .reliable_ordered, 0);
    try std.testing.expectEqual(@as(usize, 0), (try core.flushOutbound(.application, &scratch, 8, 1, &collector, Collector.emit)).datagrams);
    try std.testing.expect(!core.terminal_send_failure);
    var wire: [32]u8 = undefined;
    _ = try core.processIncoming(try datagram.encodeControl(.ack, &.{.{ .first = 0, .last = 0 }}, &wire), 2, &unused, Discard.deliver);
    try std.testing.expect((try core.flushOutbound(.application, &scratch, 8, 2, &collector, Collector.emit)).datagrams >= 3);
    try std.testing.expectEqual(@as(usize, 0), core.outboundCount(.application));
}

test "control replies queue instead of failing when the window is full" {
    const Collector = struct {
        count: usize = 0,
        fn emit(raw: *anyopaque, _: []const u8) SendError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };
    var core = try Core.init(std.testing.allocator, 576, .{});
    defer core.deinit();
    var scratch: [576]u8 = undefined;
    var collector: Collector = .{};
    core.congestion_state.in_flight = core.congestion_state.window;
    const queued = try core.sendControl("pong", .unreliable, 0, &scratch, 0, &collector, Collector.emit);
    try std.testing.expectEqual(@as(usize, 0), queued.datagrams);
    try std.testing.expectEqual(@as(usize, 1), core.outboundCount(.control));
    try std.testing.expectError(error.CongestionWindowFull, core.send("data", .reliable, 0, &scratch, 0, &collector, Collector.emit));
    core.congestion_state.in_flight = 0;
    try std.testing.expectEqual(@as(usize, 1), (try core.flushAllOutbound(&scratch, 8, 1, &collector, Collector.emit)).datagrams);
    try std.testing.expectEqual(@as(usize, 0), core.outboundCount(.control));
    try std.testing.expectEqual(@as(usize, 1), collector.count);
}

test "immediate application sends cannot bypass queued progress" {
    const Collector = struct {
        count: usize = 0,
        fn emit(raw: *anyopaque, _: []const u8) SendError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };
    var core = try Core.init(std.testing.allocator, 576, .{});
    defer core.deinit();
    core.congestion_state.window = 576;
    var scratch: [576]u8 = undefined;
    var payload: [1200]u8 = @splat(1);
    _ = try core.enqueueOutbound(.application, &payload, .reliable_ordered, 0);
    var collector: Collector = .{};

    _ = try core.sendControl("control", .unreliable, 0, &scratch, 0, &collector, Collector.emit);
    _ = try core.flushOutbound(.application, &scratch, 1, 0, &collector, Collector.emit);
    const order_index = core.transmitter_state.order_indices[0];
    try std.testing.expectError(error.OutboundQueuePending, core.send("later", .reliable_ordered, 0, &scratch, 0, &collector, Collector.emit));
    try std.testing.expectEqual(order_index, core.transmitter_state.order_indices[0]);
    try std.testing.expectEqual(@as(usize, 2), collector.count);
}

test "partial send failure rolls back only the failed datagram" {
    const FailingEmitter = struct {
        successful: usize = 0,
        fn emit(raw: *anyopaque, _: []const u8) SendError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.successful == 1) return error.TransportFailure;
            self.successful += 1;
        }
    };
    const Collector = struct {
        count: usize = 0,
        fn emit(raw: *anyopaque, _: []const u8) SendError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };
    var core = try Core.init(std.testing.allocator, 576, .{});
    defer core.deinit();
    var scratch: [576]u8 = undefined;
    var payload: [1200]u8 = @splat(1);
    _ = try core.enqueueOutbound(.application, &payload, .reliable_ordered, 0);
    var failing: FailingEmitter = .{};

    try std.testing.expectError(error.PartialSendFailure, core.flushOutbound(.application, &scratch, 8, 0, &failing, FailingEmitter.emit));
    try std.testing.expectEqual(@as(usize, 1), failing.successful);
    try std.testing.expectEqual(@as(u32, 1), core.transmitter_state.datagram_sequence);
    try std.testing.expectEqual(@as(u32, 1), core.transmitter_state.reliable_index);
    try std.testing.expectEqual(@as(usize, 1), core.recovery_state.count());
    try std.testing.expectEqual(@as(u64, 576), core.congestion_state.in_flight);
    try std.testing.expectEqual(@as(usize, 1), core.outbound_state.count(.application));
    const progress = core.outbound_packetization[@intFromEnum(outbound_queue.Lane.application)].?;
    try std.testing.expectEqual(progress.capacity, progress.offset);
    try std.testing.expect(core.terminal_send_failure);

    var collector: Collector = .{};
    try std.testing.expectError(error.ConnectionClosed, core.flushOutbound(.application, &scratch, 8, 1, &collector, Collector.emit));
    try std.testing.expectError(error.ConnectionClosed, core.enqueueOutbound(.application, "later", .reliable, 0));
    try std.testing.expectError(error.ConnectionClosed, core.beginPacketization(1, .reliable, 0));
    try std.testing.expectError(error.ConnectionClosed, core.trackSent(2, "wire", 4, 1));
    try std.testing.expectEqual(@as(usize, 0), collector.count);
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
    try std.testing.expectEqual(IncomingFailure{ .class = .resource, .disposition = .reject }, classifyIncomingError(error.TooManyAssemblies));
    try std.testing.expectEqual(IncomingFailure{ .class = .resource, .disposition = .reject }, classifyIncomingError(error.SplitBudgetExceeded));
    try std.testing.expectEqual(IncomingErrorDisposition.close_session, incomingErrorDisposition(error.ReassemblyLimitExceeded));
    try std.testing.expectEqual(IncomingErrorDisposition.close_session, incomingErrorDisposition(error.ConflictingFragment));

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
    try std.testing.expectEqual(IncomingErrorDisposition.retry, classifyTransitionError(.application_send, error.OutboundQueueFull).disposition);
    try std.testing.expectEqual(IncomingErrorDisposition.retry, classifyTransitionError(.application_send, error.OutboundQueueBytesExceeded).disposition);
    try std.testing.expectEqual(IncomingErrorDisposition.retry, classifyTransitionError(.application_send, error.OutboundQueuePending).disposition);

    const allocation = classifyTransitionError(.application_send, error.OutOfMemory);
    try std.testing.expectEqual(IncomingErrorClass.resource, allocation.class);
    try std.testing.expectEqual(IncomingErrorDisposition.close_session, allocation.disposition);

    const send_transport = classifyTransitionError(.application_send, error.TransportFailure);
    try std.testing.expectEqual(IncomingErrorDisposition.close_session, send_transport.disposition);
    try std.testing.expectEqual(IncomingErrorClass.transport, send_transport.class);
    const partial_send = classifyTransitionError(.application_send, error.PartialSendFailure);
    try std.testing.expectEqual(IncomingErrorDisposition.close_session, partial_send.disposition);
    try std.testing.expectEqual(IncomingErrorClass.transport, partial_send.class);

    try std.testing.expectEqual(IncomingErrorClass.internal, classifyTransitionError(.receipt, error.NoSpaceLeft).class);
    try std.testing.expectEqual(IncomingErrorClass.transport, classifyTransitionError(.retransmission, error.RetransmissionLimitExceeded).class);
    try std.testing.expectEqual(IncomingErrorClass.protocol, classifyTransitionError(.handshake, error.IncompatibleProtocol).class);
    try std.testing.expectEqual(IncomingErrorClass.transport, classifyTransitionError(.handshake, error.Timeout).class);
}

const ClosePeer = struct {
    sequences: [16]u32 = undefined,
    disconnects: [16]bool = undefined,
    count: usize = 0,

    fn emit(raw: *anyopaque, wire: []const u8) SendError!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var decoded = frame.decodeDatagram(wire) catch return error.TransportFailure;
        var disconnect = false;
        while (decoded.frames.remaining() != 0) {
            const value = frame.decodeOne(&decoded.frames, 8192, 2048) catch return error.TransportFailure;
            disconnect = disconnect or value.payload[0] == @intFromEnum(offline.Id.disconnect_notification);
        }
        self.sequences[self.count] = decoded.sequence;
        self.disconnects[self.count] = disconnect;
        self.count += 1;
    }

    fn acknowledge(self: *@This(), core: *Core, index: usize, now_ms: u64) !void {
        const Discard = struct {
            fn deliver(_: *anyopaque, _: receiver.BorrowedPayload) !void {}
        };
        var wire: [32]u8 = undefined;
        const sequence = self.sequences[index];
        var unused: u8 = 0;
        _ = try core.processIncoming(try datagram.encodeControl(.ack, &.{.{ .first = sequence, .last = sequence }}, &wire), now_ms, &unused, Discard.deliver);
    }

    fn step(self: *@This(), core: *Core, now_ms: u64) !CloseStep {
        const result = try core.advanceClose(now_ms);
        var scratch: [576]u8 = undefined;
        if (result == .flush) _ = try core.flushAllOutbound(&scratch, 8, now_ms, self, emit);
        return result;
    }
};

fn expectClosedClean(core: *const Core) !void {
    try std.testing.expectEqual(@as(usize, 0), core.recovery_state.count());
    try std.testing.expectEqual(@as(usize, 0), core.outbound_state.countAll());
    try std.testing.expectEqual(@as(?u64, null), core.nextRetransmissionDeadline());
    try std.testing.expectEqual(@as(u64, 0), core.congestion_state.in_flight);
}

test "close with empty queues sends a reliable disconnect and finishes on its ACK" {
    var core = try Core.init(std.testing.allocator, 576, .{});
    defer core.deinit();
    var peer: ClosePeer = .{};
    try std.testing.expectEqual(CloseStep.pending, try peer.step(&core, 0));
    core.beginClose(0);
    core.beginClose(1_000);
    try std.testing.expectEqual(@as(?u64, 5_000), core.close_deadline_ms);
    try std.testing.expectEqual(CloseStep.flush, try peer.step(&core, 0));
    try std.testing.expectEqual(@as(usize, 1), peer.count);
    try std.testing.expect(peer.disconnects[0]);
    try std.testing.expectEqual(@as(usize, 1), core.recovery_state.count());
    try std.testing.expectEqual(CloseStep.pending, try peer.step(&core, 1));
    try peer.acknowledge(&core, 0, 2);
    try std.testing.expectEqual(CloseStep.done, try peer.step(&core, 2));
    try std.testing.expectEqual(@as(usize, 1), peer.count);
    try expectClosedClean(&core);
}

test "close drains queued application and control data before the disconnect" {
    for ([_][]const outbound_queue.Lane{ &.{.application}, &.{.control}, &.{ .control, .application } }) |lanes| {
        var core = try Core.init(std.testing.allocator, 576, .{});
        defer core.deinit();
        var peer: ClosePeer = .{};
        for (lanes) |lane| _ = try core.enqueueOutbound(lane, "\xfequeued", .reliable_ordered, 0);
        core.beginClose(0);
        try std.testing.expectEqual(CloseStep.pending, try peer.step(&core, 0));
        var scratch: [576]u8 = undefined;
        _ = try core.flushAllOutbound(&scratch, 8, 0, &peer, ClosePeer.emit);
        try std.testing.expect(peer.count != 0 and !peer.disconnects[0]);
        try std.testing.expectEqual(CloseStep.pending, try peer.step(&core, 1));
        for (0..peer.count) |index| try peer.acknowledge(&core, index, 2);
        const data_datagrams = peer.count;
        try std.testing.expectEqual(CloseStep.flush, try peer.step(&core, 2));
        try std.testing.expect(peer.disconnects[data_datagrams]);
        try std.testing.expect(peer.sequences[data_datagrams] > peer.sequences[data_datagrams - 1]);
        try peer.acknowledge(&core, data_datagrams, 3);
        try std.testing.expectEqual(CloseStep.done, try peer.step(&core, 3));
        try expectClosedClean(&core);
    }
}

test "close waits for outstanding reliable data and retransmits a lost disconnect" {
    var core = try Core.init(std.testing.allocator, 576, .{});
    defer core.deinit();
    var peer: ClosePeer = .{};
    var scratch: [576]u8 = undefined;
    _ = try core.send("\xfeinflight", .reliable_ordered, 0, &scratch, 0, &peer, ClosePeer.emit);
    core.beginClose(0);
    try std.testing.expectEqual(CloseStep.pending, try peer.step(&core, 0));
    try peer.acknowledge(&core, 0, 10);
    try std.testing.expectEqual(CloseStep.flush, try peer.step(&core, 10));
    try std.testing.expect(peer.disconnects[1]);

    const deadline = core.nextRetransmissionDeadline().?;
    var due: [4]recovery.Due = undefined;
    const batch = core.collectRetransmissions(deadline, &due, 4);
    try std.testing.expectEqual(@as(usize, 1), batch.items.len);
    try ClosePeer.emit(&peer, batch.items[0].data);
    try std.testing.expect(peer.disconnects[2]);
    try std.testing.expectEqual(CloseStep.pending, try peer.step(&core, deadline));
    try peer.acknowledge(&core, 2, deadline + 1);
    try std.testing.expectEqual(CloseStep.done, try peer.step(&core, deadline + 1));
    try expectClosedClean(&core);
}

test "close is forced at the shutdown deadline when the ACK never arrives" {
    var config: Config = .{};
    config.timing.shutdown_timeout_ms = 100;
    var core = try Core.init(std.testing.allocator, 576, config);
    defer core.deinit();
    var peer: ClosePeer = .{};
    core.beginClose(0);
    try std.testing.expectEqual(CloseStep.flush, try peer.step(&core, 0));
    try std.testing.expectEqual(CloseStep.pending, try peer.step(&core, 99));
    try std.testing.expectEqual(CloseStep.done, try peer.step(&core, 100));
    try std.testing.expectEqual(@as(usize, 1), peer.count);
}
