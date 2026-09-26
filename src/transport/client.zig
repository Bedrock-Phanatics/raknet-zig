const std = @import("std");

const Config = @import("../config.zig").Config;
const net_address = @import("address.zig");
const backend = @import("socket.zig");
const connected = @import("../protocol/connected.zig");
const frame = @import("../protocol/frame.zig");
const offline = @import("../protocol/offline.zig");
const recovery = @import("../reliability/recovery.zig");
const core_mod = @import("../session/core.zig");
const receipt_batch = @import("receipt_batch.zig");
const receiver = @import("../session/receiver.zig");
const client_handshake = @import("../session/client_handshake.zig");
const socket_emitter = @import("socket_emitter.zig");
const time = @import("../util/time.zig");

pub const Options = struct {
    config: Config = .{},
    protocol_version: u8 = 11,
    mtu: u16 = 1492,
    mtu_fallbacks: []const u16 = &.{ 1200, 576 },
    mtu_attempts: u8 = 4,
    client_guid: u64 = 0,
    handshake_timeout_ms: u32 = 5_000,
    handshake_retry_ms: u32 = 500,
    handshake_transient_errors: u16 = 10,
    receive_batch_size: usize = 32,
    socket_buffers: backend.BufferOptions = .{},
};
pub const MessageFn = *const fn (context: *anyopaque, payload: receiver.BorrowedPayload) core_mod.ApplicationCallbackError!void;

pub const Client = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    socket: backend.Socket,
    server: std.Io.net.IpAddress,
    core: core_mod.Core,
    scratch: []u8,
    receive_buffer: []u8,
    messages: []std.Io.net.IncomingMessage,
    receive_storage: []u8,
    frame_scratch: []frame.Frame,
    receipts: receipt_batch.Batch,
    client_guid: u64,
    server_guid: u64,
    mtu: u16,
    last_seen_ms: u64,
    outbound_deadline_ms: ?u64 = null,
    ack_deadline_ms: ?u64 = null,
    timer_cursor: u8 = 0,
    pending_message_index: usize = 0,
    pending_message_count: usize = 0,
    pending_receive_error: ?anyerror = null,
    closing: bool = false,
    closed: bool = false,
    rejected_datagrams: u64 = 0,

    const PollBridge = struct {
        client: *Client,
        context: *anyopaque,
        callback: MessageFn,
        now_ms: u64,
        remote_disconnect: bool = false,

        fn deliver(raw: *anyopaque, payload: receiver.BorrowedPayload) receiver.DeliveryError!void {
            const self: *PollBridge = @ptrCast(@alignCast(raw));
            const packet = connected.decode(payload.bytes) catch return error.PeerProtocolFailure;
            switch (packet) {
                .connected_ping => |sent| {
                    var wire: [17]u8 = undefined;
                    const pong = connected.encodePong(sent, self.now_ms, &wire) catch return error.InternalFailure;
                    _ = self.client.sendControlWire(pong, .unreliable, 0, self.now_ms) catch |err| return core_mod.deliverySendFailure(err);
                },
                .disconnect => self.remote_disconnect = true,
                .detect_lost_connections => {
                    var wire: [9]u8 = undefined;
                    const ping = connected.encodePing(self.now_ms, &wire) catch return error.InternalFailure;
                    _ = self.client.sendControlWire(ping, .reliable, 0, self.now_ms) catch |err| return core_mod.deliverySendFailure(err);
                },
                .user => |data| if (!self.client.closing) self.callback(self.context, .init(data)) catch return error.ApplicationFailure,
                else => {},
            }
        }
    };

    const HandshakeState = struct {
        client: *Client,
        now_ms: u64,
        accepted: bool = false,

        fn deliver(raw: *anyopaque, payload: receiver.BorrowedPayload) receiver.DeliveryError!void {
            const self: *HandshakeState = @ptrCast(@alignCast(raw));
            const packet = connected.decode(payload.bytes) catch return error.PeerProtocolFailure;
            if (packet != .connection_request_accepted) return;
            var wire: [1024]u8 = undefined;
            const server = net_address.toRakNet(self.client.server);
            const systems: [20]@TypeOf(server) = @splat(net_address.unspecified(server));
            const incoming = connected.encodeAddressList(
                .incoming,
                server,
                0,
                &systems,
                self.now_ms,
                self.now_ms,
                &wire,
            ) catch return error.InternalFailure;
            _ = self.client.sendControlWire(incoming, .reliable_ordered, 0, self.now_ms) catch |err| return core_mod.deliverySendFailure(err);
            self.accepted = true;
        }
    };

    pub fn connect(allocator: std.mem.Allocator, io: std.Io, server: std.Io.net.IpAddress, options: Options) !*Client {
        try validateOptions(options);
        const self = try allocator.create(Client);
        errdefer allocator.destroy(self);
        const local: std.Io.net.IpAddress = switch (server) {
            .ip4 => .{ .ip4 = .unspecified(0) },
            .ip6 => .{ .ip6 = .unspecified(0) },
        };
        var socket = try backend.Socket.bindWithBuffers(io, local, options.config.protocol.maximum_datagram_size, options.socket_buffers);
        errdefer socket.close();
        const scratch = try allocator.alloc(u8, options.config.protocol.maximum_datagram_size);
        errdefer allocator.free(scratch);
        const receive_buffer = try allocator.alloc(u8, options.config.protocol.maximum_datagram_size);
        errdefer allocator.free(receive_buffer);
        var random: [8]u8 = undefined;
        io.random(&random);
        const guid = if (options.client_guid != 0) options.client_guid else (std.mem.readInt(u64, &random, .little) | 0x8000_0000_0000_0000);
        const deadline = time.after(io, options.handshake_timeout_ms);
        const deadline_ms = time.deadline(time.nowMilliseconds(io), options.handshake_timeout_ms);

        var ladder: [max_mtu_rungs]u16 = undefined;
        ladder[0] = options.mtu;
        var rungs: usize = 1;
        for (options.mtu_fallbacks) |fallback| if (fallback < options.mtu) {
            ladder[rungs] = fallback;
            rungs += 1;
        };
        var negotiator: client_handshake.Negotiator = .init(.{
            .protocol_version = options.protocol_version,
            .mtus = ladder[0..rungs],
            .attempts_per_mtu = options.mtu_attempts,
            .minimum_mtu = options.config.protocol.minimum_mtu,
            .retry_ms = options.handshake_retry_ms,
            .client_guid = guid,
            .server_address = net_address.toRakNet(server),
            .maximum_transient_errors = options.handshake_transient_errors,
        });
        var transport: HandshakeTransport = .{ .socket = &socket, .server = server, .buffer = receive_buffer };
        const reply2 = try negotiator.run(&transport, deadline_ms, scratch);

        const frame_scratch = try allocator.alloc(frame.Frame, options.config.batching.maximum_packets_per_iteration);
        errdefer allocator.free(frame_scratch);
        const messages = try allocator.alloc(std.Io.net.IncomingMessage, options.receive_batch_size);
        errdefer allocator.free(messages);
        const receive_storage = try allocator.alloc(u8, try std.math.mul(usize, options.receive_batch_size, options.config.protocol.maximum_datagram_size));
        errdefer allocator.free(receive_storage);

        var core = try core_mod.Core.init(allocator, reply2.mtu, options.config);
        errdefer core.deinit();
        var receipts = try receipt_batch.Batch.init(allocator, options.config.batching.maximum_ack_records, reply2.mtu, options.config.protocol.maximum_acknowledged_datagrams);
        errdefer receipts.deinit();
        self.* = .{
            .allocator = allocator,
            .io = io,
            .socket = socket,
            .server = server,
            .core = core,
            .scratch = scratch,
            .receive_buffer = receive_buffer,
            .messages = messages,
            .receive_storage = receive_storage,
            .frame_scratch = frame_scratch,
            .receipts = receipts,
            .client_guid = guid,
            .server_guid = reply2.server_guid,
            .mtu = reply2.mtu,
            .last_seen_ms = time.nowMilliseconds(io),
        };
        try self.finishConnectedHandshake(&negotiator, deadline, options.handshake_retry_ms, options.config.batching.maximum_packets_per_iteration);
        self.last_seen_ms = time.nowMilliseconds(io);
        return self;
    }

    pub fn kernelBufferSizes(self: *const Client) backend.BufferSizes {
        return self.socket.kernelBufferSizes();
    }
    pub fn statistics(self: *const Client) core_mod.Statistics {
        var stats = self.core.statistics();
        stats.ack_records_sent = self.receipts.ack_records_sent;
        stats.nack_records_sent = self.receipts.nack_records_sent;
        return stats;
    }

    pub fn traffic(self: *const Client) backend.Traffic {
        return self.socket.traffic;
    }
    pub fn close(self: *Client) void {
        if (self.closed or self.closing) return;
        self.closing = true;
        const now_ms = time.nowMilliseconds(self.io);
        self.core.beginClose(now_ms);
        self.advanceClose(now_ms) catch self.abort();
    }
    pub fn isClosed(self: *const Client) bool {
        return self.closed;
    }
    fn advanceClose(self: *Client, now_ms: u64) !void {
        if (!self.closing or self.closed) return;
        switch (try self.core.advanceClose(now_ms)) {
            .pending => {},
            .flush => _ = try self.flushQueuedAt(now_ms),
            .done => {
                self.flushReceipts() catch {};
                self.abort();
            },
        }
    }
    fn abort(self: *Client) void {
        if (self.closed) return;
        self.closed = true;
        self.socket.close();
    }
    pub fn destroy(self: *Client) void {
        if (!self.closed and !self.core.disconnect_queued) {
            var payload = [_]u8{@intFromEnum(offline.Id.disconnect_notification)};
            _ = self.sendControlWire(&payload, .reliable_ordered, 0, time.nowMilliseconds(self.io)) catch {};
        }
        self.abort();
        self.receipts.deinit();
        self.core.deinit();
        self.allocator.free(self.receive_storage);
        self.allocator.free(self.messages);
        self.allocator.free(self.frame_scratch);
        self.allocator.free(self.receive_buffer);
        self.allocator.free(self.scratch);
        self.allocator.destroy(self);
    }
    pub fn send(self: *Client, payload: []const u8, reliability: frame.Reliability, channel: u8) !void {
        if (!try self.trySend(payload, reliability, channel)) _ = try self.queueSend(payload, reliability, channel);
    }

    pub fn trySend(self: *Client, payload: []const u8, reliability: frame.Reliability, channel: u8) !bool {
        if (self.closed or self.closing) return error.ConnectionClosed;
        _ = self.sendWire(payload, reliability, channel, time.nowMilliseconds(self.io)) catch |err| {
            if (err == error.CongestionWindowFull or err == error.OutboundQueuePending) return false;
            self.abortOnError(.application_send, err);
            return err;
        };
        return true;
    }

    pub fn queueSend(self: *Client, payload: []const u8, reliability: frame.Reliability, channel: u8) !core_mod.SendHandle {
        if (self.closed or self.closing) return error.ConnectionClosed;
        const handle = self.core.enqueueOutbound(.application, payload, reliability, channel) catch |err| {
            self.abortOnError(.application_send, err);
            return err;
        };
        const now_ms = time.nowMilliseconds(self.io);
        self.outbound_deadline_ms = now_ms;
        if (try self.core.outboundReady(.application)) {
            _ = self.flushQueuedAt(now_ms) catch |err| {
                self.abortOnError(.application_send, err);
                return err;
            };
        }
        return handle;
    }

    pub fn cancelSend(self: *Client, handle: core_mod.SendHandle) core_mod.CancelResult {
        if (self.closed) return .not_found;
        const result = self.core.cancelOutbound(handle);
        if (result == .canceled and self.core.outboundCount(.application) == 0) self.outbound_deadline_ms = null;
        return result;
    }

    pub fn flush(self: *Client) !core_mod.FlushResult {
        if (self.closed) return error.ConnectionClosed;
        return self.flushQueuedAt(time.nowMilliseconds(self.io)) catch |err| {
            self.abortOnError(.application_send, err);
            return err;
        };
    }

    pub fn nextDeadline(self: *const Client) ?u64 {
        if (self.closed) return null;
        var deadline = time.deadline(self.last_seen_ms, self.core.config.timing.idle_timeout_ms);
        if (self.ack_deadline_ms) |ack_deadline| deadline = @min(deadline, ack_deadline);
        if (self.outbound_deadline_ms) |outbound| deadline = @min(deadline, outbound);
        if (self.core.nextRetransmissionDeadline()) |retransmission| deadline = @min(deadline, retransmission);
        if (self.core.nextSplitDeadline()) |split| deadline = @min(deadline, split);
        if (self.core.close_deadline_ms) |close_deadline| deadline = @min(deadline, close_deadline);
        return deadline;
    }

    pub fn processTimers(self: *Client, now_ms: u64) !void {
        if (self.closed) return error.ConnectionClosed;
        if (time.reached(now_ms, time.deadline(self.last_seen_ms, self.core.config.timing.idle_timeout_ms))) {
            self.abort();
            return error.ConnectionTimedOut;
        }
        self.processDueTimers(now_ms, self.core.config.batching.maximum_packets_per_iteration) catch |err| {
            self.abort();
            return err;
        };
        self.advanceClose(now_ms) catch |err| {
            self.abort();
            return err;
        };
    }

    pub fn poll(self: *Client, timeout: std.Io.Timeout, context: *anyopaque, on_message: MessageFn) !usize {
        if (self.closed) return error.ConnectionClosed;
        if (self.pending_receive_error) |err| {
            self.pending_receive_error = null;
            return err;
        }
        if (self.pending_message_index == self.pending_message_count) {
            const wait = if (self.nextDeadline()) |deadline| time.earliest(self.io, timeout, time.atMilliseconds(deadline)) else timeout;
            const batch = self.socket.receiveMany(self.messages, self.receive_storage, wait) catch |err| switch (err) {
                error.Timeout => {
                    try self.processTimers(time.nowMilliseconds(self.io));
                    return error.Timeout;
                },
                else => return err,
            };
            self.pending_message_index = 0;
            self.pending_message_count = batch.messages.len;
            self.pending_receive_error = batch.trailing_error;
        }
        var delivered: usize = 0;
        var latest_ms = time.nowMilliseconds(self.io);
        var remaining = self.core.config.batching.maximum_packets_per_iteration;
        var processed: usize = 0;
        while (self.pending_message_index < self.pending_message_count and remaining != 0) {
            if (processed != 0 and processed % 32 == 0) latest_ms = time.nowMilliseconds(self.io);
            const message = self.messages[self.pending_message_index];
            self.pending_message_index += 1;
            processed += 1;
            remaining -= 1;
            if (!std.meta.eql(message.from, self.server) or isLateOfflineReply(message.data)) continue;
            const now_ms = latest_ms;
            var bridge: PollBridge = .{ .client = self, .context = context, .callback = on_message, .now_ms = now_ms };
            const processed_incoming = self.core.processIncomingCountedWithScratch(message.data, now_ms, self.frame_scratch, &bridge, PollBridge.deliver) catch |err| {
                if (core_mod.incomingErrorDisposition(err) == .reject) {
                    self.rejected_datagrams += 1;
                    continue;
                }
                self.abort();
                return err;
            };
            self.last_seen_ms = now_ms;
            const incoming = processed_incoming.incoming;
            const extra_work = processed_incoming.work_units -| 1;
            remaining -= @min(remaining, extra_work);
            if (incoming == .data) {
                delivered += incoming.data.delivered;
                self.queueReceipt(incoming.data, now_ms) catch |err| {
                    self.abortOnError(.receipt, err);
                    return err;
                };
            }
            if (bridge.remote_disconnect) {
                self.flushReceipts() catch {};
                self.abort();
                return delivered;
            }
        }
        if (self.pending_message_index == self.pending_message_count) {
            self.pending_message_index = 0;
            self.pending_message_count = 0;
        }
        const flushed = self.flushQueuedAtLimit(latest_ms, remaining) catch |err| {
            self.abortOnError(.application_send, err);
            return err;
        };
        remaining -= @min(remaining, flushed.datagrams);
        self.processDueTimers(latest_ms, remaining) catch |err| {
            self.abortOnError(.retransmission, err);
            return err;
        };
        self.advanceClose(latest_ms) catch |err| {
            self.abort();
            return err;
        };
        return delivered;
    }

    fn finishConnectedHandshake(self: *Client, negotiator: *client_handshake.Negotiator, deadline: std.Io.Timeout, retry_ms: u32, maximum_work: usize) !void {
        var control: [18]u8 = undefined;
        const request_time = time.nowMilliseconds(self.io);
        const request = try connected.encodeConnectionRequest(self.client_guid, request_time, &control);
        _ = try self.sendControlWire(request, .reliable_ordered, 0, request_time);
        var work: usize = 0;
        while (work < maximum_work) : (work += 1) {
            const attempt = time.earliest(self.io, deadline, time.after(self.io, retry_ms));
            const message = self.socket.receive(self.receive_buffer, attempt) catch |err| switch (err) {
                error.Timeout => {
                    if (std.Io.Clock.awake.now(self.io).nanoseconds >= deadline.deadline.raw.nanoseconds) return error.Timeout;
                    _ = try self.flushRetransmissions(time.nowMilliseconds(self.io), self.core.config.batching.maximum_packets_per_iteration);
                    continue;
                },
                else => {
                    try negotiator.transient(err);
                    continue;
                },
            };
            if (!std.meta.eql(message.from, self.server) or message.flags.trunc) continue;
            const now_ms = time.nowMilliseconds(self.io);
            var state: HandshakeState = .{ .client = self, .now_ms = now_ms };
            const incoming = self.core.processIncomingWithScratch(message.data, now_ms, self.frame_scratch, &state, HandshakeState.deliver) catch |err| {
                if (core_mod.incomingErrorDisposition(err) == .reject) continue;
                return err;
            };
            if (incoming == .data) {
                try self.queueReceipt(incoming.data, now_ms);
                try self.flushReceipts();
            }
            if (state.accepted) return;
        }
        return error.HandshakeWorkLimitExceeded;
    }

    fn sendWire(self: *Client, payload: []const u8, reliability: frame.Reliability, channel: u8, now_ms: u64) !usize {
        return self.sendWireAs(payload, reliability, channel, now_ms, false);
    }
    fn sendControlWire(self: *Client, payload: []const u8, reliability: frame.Reliability, channel: u8, now_ms: u64) !usize {
        return self.sendWireAs(payload, reliability, channel, now_ms, true);
    }
    fn sendWireAs(self: *Client, payload: []const u8, reliability: frame.Reliability, channel: u8, now_ms: u64, control: bool) !usize {
        if (control) try self.flushReceipts();
        var emitter: socket_emitter.Emitter = .{ .socket = &self.socket, .address = self.server };
        _ = if (control)
            try self.core.sendControl(payload, reliability, channel, self.scratch, now_ms, &emitter, socket_emitter.Emitter.emit)
        else
            try self.core.send(payload, reliability, channel, self.scratch, now_ms, &emitter, socket_emitter.Emitter.emit);
        if (control and self.core.outboundCount(.control) != 0) self.outbound_deadline_ms = now_ms;
        return emitter.count;
    }
    fn flushQueuedAt(self: *Client, now_ms: u64) !core_mod.FlushResult {
        return self.flushQueuedAtLimit(now_ms, self.core.config.batching.maximum_packets_per_iteration);
    }
    fn flushQueuedAtLimit(self: *Client, now_ms: u64, maximum_datagrams: usize) !core_mod.FlushResult {
        var emitter: socket_emitter.Emitter = .{ .socket = &self.socket, .address = self.server };
        self.outbound_deadline_ms = null;
        const sent = try self.core.flushAllOutbound(self.scratch, maximum_datagrams, now_ms, &emitter, socket_emitter.Emitter.emit);
        if (sent.datagrams == maximum_datagrams and self.core.outbound_state.countAll() != 0) self.outbound_deadline_ms = now_ms;
        return sent;
    }
    fn queueReceipt(self: *Client, receipt: receiver.Receipt, now_ms: u64) !void {
        self.receipts.append(receipt) catch {
            try self.flushReceipts();
            try self.receipts.append(receipt);
        };
        if (self.receipts.isEmpty()) return;
        const delay = if (receipt.missing != null) 0 else self.core.config.timing.maximum_ack_delay_ms;
        const deadline = time.deadline(now_ms, delay);
        self.ack_deadline_ms = if (self.ack_deadline_ms) |current| @min(current, deadline) else deadline;
    }
    fn flushReceipts(self: *Client) !void {
        _ = try self.flushReceiptsUpTo(self.receipts.count());
    }
    fn flushReceiptsUpTo(self: *Client, maximum_work: usize) !usize {
        const sent = try self.receipts.flush(&self.socket, &self.server, maximum_work);
        if (self.receipts.isEmpty()) self.ack_deadline_ms = null;
        return sent;
    }
    fn processDueTimers(self: *Client, now_ms: u64, maximum_work: usize) !void {
        if (maximum_work == 0) return;
        var remaining = maximum_work;
        var active: usize = @as(usize, @intFromBool(self.ack_deadline_ms != null and self.ack_deadline_ms.? <= now_ms)) +
            @intFromBool(self.outbound_deadline_ms != null and self.outbound_deadline_ms.? <= now_ms) +
            @intFromBool(if (self.core.nextSplitDeadline()) |deadline| deadline <= now_ms else false) +
            @intFromBool(if (self.core.nextRetransmissionDeadline()) |deadline| deadline <= now_ms else false);
        const start = self.timer_cursor;
        var visited: usize = 0;
        while (visited < 4 and remaining != 0 and active != 0) : (visited += 1) {
            const timer = (start + visited) % 4;
            const due = switch (timer) {
                0 => self.ack_deadline_ms != null and self.ack_deadline_ms.? <= now_ms,
                1 => self.outbound_deadline_ms != null and self.outbound_deadline_ms.? <= now_ms,
                2 => if (self.core.nextSplitDeadline()) |deadline| deadline <= now_ms else false,
                else => if (self.core.nextRetransmissionDeadline()) |deadline| deadline <= now_ms else false,
            };
            if (!due) continue;
            const quota = @max(@as(usize, 1), remaining / active);
            const used = switch (timer) {
                0 => try self.flushReceiptsUpTo(quota),
                1 => (try self.flushQueuedAtLimit(now_ms, quota)).datagrams,
                2 => self.core.expireSplits(now_ms, quota).inspected,
                else => try self.flushRetransmissions(now_ms, quota),
            };
            remaining -= @min(remaining, @max(@as(usize, 1), used));
            active -= 1;
            self.timer_cursor = @intCast((timer + 1) % 4);
        }
    }

    fn flushRetransmissions(self: *Client, now_ms: u64, maximum_work: usize) !usize {
        const receipts_sent = try self.flushReceiptsUpTo(maximum_work);
        const remaining = maximum_work - receipts_sent;
        if (remaining == 0) return receipts_sent;
        var due: [256]recovery.Due = undefined;
        const batch = self.core.collectRetransmissions(now_ms, due[0..@min(due.len, remaining)], remaining);
        if (batch.exhausted != 0) return error.RetransmissionLimitExceeded;
        for (batch.items) |item| self.socket.send(self.server, item.data) catch return error.TransportFailure;
        return receipts_sent + batch.inspected;
    }

    fn abortOnError(self: *Client, transition: core_mod.SessionTransition, err: anyerror) void {
        if (core_mod.classifyTransitionError(transition, err).disposition == .close_session) self.abort();
    }
};

fn validateOptions(options: Options) !void {
    try options.config.validate();
    try options.socket_buffers.validate();
    const invalid =
        options.mtu < options.config.protocol.minimum_mtu or
        options.mtu > options.config.protocol.maximum_mtu or
        options.handshake_timeout_ms == 0 or
        options.handshake_retry_ms == 0 or
        options.handshake_retry_ms > options.handshake_timeout_ms or
        options.mtu_attempts == 0 or
        options.mtu_fallbacks.len >= max_mtu_rungs or
        options.receive_batch_size == 0 or
        options.receive_batch_size > 256;
    if (invalid) return error.InvalidConfiguration;
    var previous = options.config.protocol.maximum_mtu + 1;
    for (options.mtu_fallbacks) |fallback| {
        if (fallback < options.config.protocol.minimum_mtu or fallback > options.config.protocol.maximum_mtu or fallback >= previous) return error.InvalidConfiguration;
        previous = fallback;
    }
}

const max_mtu_rungs = 8;

const HandshakeTransport = struct {
    socket: *backend.Socket,
    server: std.Io.net.IpAddress,
    buffer: []u8,

    pub fn now(self: *HandshakeTransport) u64 {
        return time.nowMilliseconds(self.socket.io);
    }
    pub fn send(self: *HandshakeTransport, data: []const u8) !void {
        try self.socket.send(self.server, data);
    }
    pub fn receive(self: *HandshakeTransport, deadline_ms: u64) !?[]const u8 {
        const message = self.socket.receive(self.buffer, time.atMilliseconds(deadline_ms)) catch |err| switch (err) {
            error.Timeout => return null,
            else => return err,
        };
        if (!std.meta.eql(message.from, self.server) or message.flags.trunc) return &.{};
        return message.data;
    }
};

fn isLateOfflineReply(data: []const u8) bool {
    return data.len != 0 and (data[0] == @intFromEnum(offline.Id.open_connection_reply_1) or data[0] == @intFromEnum(offline.Id.open_connection_reply_2));
}

test "client validates a descending MTU ladder" {
    try validateOptions(.{});
    try validateOptions(.{ .mtu = 1200 });
    try std.testing.expectError(error.InvalidConfiguration, validateOptions(.{ .mtu_fallbacks = &.{ 1200, 1200 } }));
    try std.testing.expectError(error.InvalidConfiguration, validateOptions(.{ .mtu_fallbacks = &.{ 576, 1200 } }));
}
