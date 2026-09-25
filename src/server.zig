const std = @import("std");

const Config = @import("config.zig").Config;
const net_address = @import("net/address.zig");
const backend = @import("net/backend.zig");
const connected = @import("protocol/connected.zig");
const frame = @import("protocol/frame.zig");
const offline = @import("protocol/offline.zig");
const recovery = @import("reliability/recovery.zig");
const cookie = @import("security/cookie.zig");
const rate = @import("security/rate_limit.zig");
const core_mod = @import("session/core.zig");
const deadline_queue = @import("session/deadline_queue.zig");
const EndpointKey = deadline_queue.Key;
const handshake = @import("session/offline_handshake.zig");
const receipt_batch = @import("session/receipt_batch.zig");
const receiver = @import("session/receiver.zig");
const socket_emitter = @import("session/socket_emitter.zig");
const transmitter_mod = @import("session/transmitter.zig");
const QuotaAllocator = @import("util/quota_allocator.zig").QuotaAllocator;
const time = @import("util/time.zig");

const State = enum { connecting, connected, closed };

pub const Options = struct {
    config: Config = .{},
    server_guid: u64 = 0,
    protocol_version: u8 = 11,
    advertisement: []const u8,
    receive_batch_size: usize = 32,
    socket_buffers: backend.BufferOptions = .{},
    offline_rate_per_second: u32 = 20,
    offline_burst: u32 = 40,
    global_offline_rate_per_second: u32 = 20_000,
    global_offline_burst: u32 = 40_000,
    /// Caps the session table and all remote session state.
    maximum_session_memory_bytes: usize = 512 * 1024 * 1024,
    handshake_timeout_ms: u32 = 5_000,
    /// Kept for compatibility. Exact deadlines ignore it.
    maintenance_interval_ms: u32 = 10,
};

pub const Callbacks = struct {
    context: *anyopaque,
    connected: *const fn (context: *anyopaque, session: *Session) core_mod.ApplicationCallbackError!void,
    /// The payload is valid only during the callback.
    message: *const fn (context: *anyopaque, session: *Session, payload: receiver.BorrowedPayload) core_mod.ApplicationCallbackError!void,
    disconnected: ?*const fn (context: *anyopaque, session: *Session) void = null,
};

const DeliveryBridge = struct {
    callbacks: Callbacks,
    session: *Session,
    now_ms: u64,

    fn deliver(raw: *anyopaque, payload: receiver.BorrowedPayload) receiver.DeliveryError!void {
        const self: *DeliveryBridge = @ptrCast(@alignCast(raw));
        const packet = connected.decode(payload.bytes) catch return error.PeerProtocolFailure;
        switch (packet) {
            .connected_ping => |sent| {
                var wire: [17]u8 = undefined;
                const pong = connected.encodePong(sent, self.now_ms, &wire) catch return error.InternalFailure;
                self.session.sendControl(pong, .unreliable, self.now_ms) catch |err| return core_mod.deliverySendFailure(err);
            },
            .connection_request => |request| {
                var wire: [1024]u8 = undefined;
                const remote = net_address.toRakNet(self.session.address);
                var systems: [20]offline.Address = @splat(emptyAddress(remote));
                const accepted = connected.encodeAddressList(.accepted, remote, 0, &systems, request.request_time, self.now_ms, &wire) catch return error.InternalFailure;
                self.session.sendControl(accepted, .reliable_ordered, self.now_ms) catch |err| return core_mod.deliverySendFailure(err);
            },
            .new_incoming_connection => {
                if (self.session.state == .connecting) {
                    self.session.state = .connected;
                    self.callbacks.connected(self.callbacks.context, self.session) catch return error.ApplicationFailure;
                }
            },
            .disconnect => self.session.state = .closed,
            .detect_lost_connections => {
                var wire: [9]u8 = undefined;
                const ping = connected.encodePing(self.now_ms, &wire) catch return error.InternalFailure;
                self.session.sendControl(ping, .reliable, self.now_ms) catch |err| return core_mod.deliverySendFailure(err);
            },
            .user => |user| {
                if (self.session.state == .connected) {
                    self.callbacks.message(self.callbacks.context, self.session, .init(user)) catch return error.ApplicationFailure;
                }
            },
            .connected_pong, .connection_request_accepted => {},
        }
    }
};

pub const PollStats = struct {
    datagrams: usize = 0,
    malformed: usize = 0,
    rate_limited_or_dropped: usize = 0,
    sessions_expired: usize = 0,
    sessions_failed: usize = 0,
    resource_failures: usize = 0,
    transport_failures: usize = 0,
    application_failures: usize = 0,
    internal_failures: usize = 0,
};

pub const ListenerStatistics = struct {
    active_sessions: usize,
    session_memory_bytes: usize,
    maximum_session_memory_bytes: usize,
    socket_buffers: backend.BufferSizes,
};

pub const Session = struct {
    allocator: std.mem.Allocator,
    socket: *backend.Socket,
    address: std.Io.net.IpAddress,
    key: EndpointKey,
    core: core_mod.Core,
    scratch: []u8,
    receipts: receipt_batch.Batch,
    ack_deadline_ms: ?u64 = null,
    outbound_deadline_ms: ?u64 = null,
    timer_cursor: u8 = 0,
    deadlines: *deadline_queue.Queue,
    client_guid: u64,
    mtu: u16,
    state: State = .connecting,
    last_seen_ms: u64,
    handshake_deadline_ms: u64,
    idle_timeout_ms: u32,

    fn create(
        allocator: std.mem.Allocator,
        socket: *backend.Socket,
        deadlines: *deadline_queue.Queue,
        address: std.Io.net.IpAddress,
        key: EndpointKey,
        client_guid: u64,
        mtu: u16,
        now_ms: u64,
        handshake_timeout_ms: u32,
        ack_capacity: usize,
        config: Config,
    ) !*Session {
        const self = try allocator.create(Session);
        errdefer allocator.destroy(self);
        var core = try core_mod.Core.init(allocator, mtu, config);
        errdefer core.deinit();
        const scratch = try allocator.alloc(u8, mtu);
        errdefer allocator.free(scratch);
        var receipts = try receipt_batch.Batch.init(allocator, @min(ack_capacity, config.batching.maximum_ack_records), mtu, config.protocol.maximum_acknowledged_datagrams);
        errdefer receipts.deinit();
        self.* = .{
            .allocator = allocator,
            .socket = socket,
            .address = address,
            .key = key,
            .core = core,
            .scratch = scratch,
            .receipts = receipts,
            .deadlines = deadlines,
            .client_guid = client_guid,
            .mtu = mtu,
            .last_seen_ms = now_ms,
            .handshake_deadline_ms = time.deadline(now_ms, handshake_timeout_ms),
            .idle_timeout_ms = config.timing.idle_timeout_ms,
        };
        return self;
    }
    fn destroy(self: *Session) void {
        self.core.deinit();
        self.receipts.deinit();
        self.allocator.free(self.scratch);
        self.allocator.destroy(self);
    }
    pub fn isConnected(self: Session) bool {
        return self.state == .connected;
    }
    pub fn statistics(self: *const Session) core_mod.Statistics {
        return self.core.statistics();
    }
    pub fn close(self: *Session) void {
        if (self.state == .closed) return;
        const now_ms = time.nowMilliseconds(self.socket.io);
        if (self.state == .connected) {
            const payload = [_]u8{@intFromEnum(offline.Id.disconnect_notification)};
            self.sendControl(&payload, .reliable_ordered, now_ms) catch {};
        }
        self.closeAt(now_ms);
    }
    pub fn send(self: *Session, payload: []const u8, reliability: frame.Reliability, channel: u8) !void {
        if (!try self.trySend(payload, reliability, channel)) return error.CongestionWindowFull;
    }
    pub fn trySend(self: *Session, payload: []const u8, reliability: frame.Reliability, channel: u8) !bool {
        if (self.state != .connected) return error.NotConnected;
        _ = self.sendAt(payload, reliability, channel, time.nowMilliseconds(self.socket.io)) catch |err| {
            if (err == error.CongestionWindowFull or err == error.OutboundQueuePending) return false;
            self.closeOnError(.application_send, err);
            return err;
        };
        return true;
    }
    pub fn queueSend(self: *Session, payload: []const u8, reliability: frame.Reliability, channel: u8) !core_mod.SendHandle {
        if (self.state != .connected) return error.NotConnected;
        const handle = self.core.enqueueOutbound(.application, payload, reliability, channel) catch |err| {
            self.closeOnError(.application_send, err);
            return err;
        };
        const now_ms = time.nowMilliseconds(self.socket.io);
        self.outbound_deadline_ms = now_ms;
        if (try self.core.outboundReady(.application)) {
            _ = self.flushQueuedAt(now_ms) catch |err| {
                self.closeOnErrorAt(.application_send, err, now_ms);
                return err;
            };
        } else {
            try self.schedule();
        }
        return handle;
    }
    pub fn cancelSend(self: *Session, handle: core_mod.SendHandle) core_mod.CancelResult {
        if (self.state != .connected) return .not_found;
        const result = self.core.cancelOutbound(handle);
        if (result == .canceled and self.core.outboundCount(.application) == 0) self.outbound_deadline_ms = null;
        return result;
    }
    pub fn flush(self: *Session) !core_mod.FlushResult {
        if (self.state != .connected) return error.NotConnected;
        return self.flushQueuedAt(time.nowMilliseconds(self.socket.io)) catch |err| {
            self.closeOnError(.application_send, err);
            return err;
        };
    }
    fn sendAt(self: *Session, payload: []const u8, reliability: frame.Reliability, channel: u8, now_ms: u64) !usize {
        return self.sendAtLane(payload, reliability, channel, now_ms, false);
    }
    fn sendAtLane(self: *Session, payload: []const u8, reliability: frame.Reliability, channel: u8, now_ms: u64, control: bool) !usize {
        if (control) try self.flushReceipts();
        var emitter: socket_emitter.Emitter = .{ .socket = self.socket, .address = self.address };
        _ = if (control)
            try self.core.sendControl(payload, reliability, channel, self.scratch, now_ms, &emitter, socket_emitter.Emitter.emit)
        else
            try self.core.send(payload, reliability, channel, self.scratch, now_ms, &emitter, socket_emitter.Emitter.emit);
        try self.schedule();
        return emitter.count;
    }
    fn sendControl(self: *Session, payload: []const u8, reliability: frame.Reliability, now_ms: u64) !void {
        _ = try self.sendAtLane(payload, reliability, 0, now_ms, true);
    }
    fn flushQueuedAt(self: *Session, now_ms: u64) !core_mod.FlushResult {
        return self.flushQueuedAtLimit(now_ms, self.core.config.batching.maximum_packets_per_iteration);
    }
    fn flushQueuedAtLimit(self: *Session, now_ms: u64, maximum_datagrams: usize) !core_mod.FlushResult {
        var emitter: socket_emitter.Emitter = .{ .socket = self.socket, .address = self.address };
        self.outbound_deadline_ms = null;
        const sent = try self.core.flushOutbound(.application, self.scratch, maximum_datagrams, now_ms, &emitter, socket_emitter.Emitter.emit);
        if (sent.datagrams == maximum_datagrams and self.core.outboundCount(.application) != 0) self.outbound_deadline_ms = now_ms;
        if (sent.datagrams != 0) try self.schedule();
        return sent;
    }
    fn queueReceipt(self: *Session, receipt: receiver.Receipt, now_ms: u64) !void {
        self.receipts.append(receipt) catch {
            try self.flushReceipts();
            try self.receipts.append(receipt);
        };
        if (self.receipts.isEmpty()) return;
        const delay = if (receipt.missing != null) 0 else self.core.config.timing.maximum_ack_delay_ms;
        const deadline = time.deadline(now_ms, delay);
        self.ack_deadline_ms = if (self.ack_deadline_ms) |current| @min(current, deadline) else deadline;
        try self.schedule();
    }
    fn flushReceipts(self: *Session) !void {
        _ = try self.flushReceiptsUpTo(self.receipts.count());
    }
    fn flushReceiptsUpTo(self: *Session, maximum_work: usize) !usize {
        const sent = try self.receipts.flush(self.socket, &self.address, maximum_work);
        if (self.receipts.isEmpty()) self.ack_deadline_ms = null;
        return sent;
    }
    fn schedule(self: *Session) !void {
        var deadline = time.deadline(self.last_seen_ms, self.idle_timeout_ms);
        if (self.state == .connecting) deadline = @min(deadline, self.handshake_deadline_ms);
        if (self.ack_deadline_ms) |ack_deadline| deadline = @min(deadline, ack_deadline);
        if (self.outbound_deadline_ms) |outbound| deadline = @min(deadline, outbound);
        if (self.core.nextRetransmissionDeadline()) |retransmission| deadline = @min(deadline, retransmission);
        if (self.core.nextSplitDeadline()) |split| deadline = @min(deadline, split);
        try self.deadlines.upsert(self.key, deadline);
    }
    fn processDueTimers(self: *Session, now_ms: u64, maximum_work: usize) !usize {
        var remaining = maximum_work;
        var active: usize = @intFromBool(self.ack_deadline_ms != null and self.ack_deadline_ms.? <= now_ms) +
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
        return maximum_work - remaining;
    }
    fn flushRetransmissions(self: *Session, now_ms: u64, maximum_work: usize) !usize {
        const receipts_sent = try self.flushReceiptsUpTo(maximum_work);
        const remaining = maximum_work - receipts_sent;
        if (remaining == 0) return receipts_sent;
        var due: [256]recovery.Due = undefined;
        const batch = self.core.collectRetransmissions(now_ms, due[0..@min(due.len, remaining)], remaining);
        if (batch.exhausted != 0) return error.RetransmissionLimitExceeded;
        for (batch.items) |item| self.socket.send(self.address, item.data) catch return error.TransportFailure;
        return receipts_sent + batch.inspected;
    }

    fn closeOnError(self: *Session, transition: core_mod.SessionTransition, err: anyerror) void {
        self.closeOnErrorAt(transition, err, time.nowMilliseconds(self.socket.io));
    }

    fn closeOnErrorAt(self: *Session, transition: core_mod.SessionTransition, err: anyerror, now_ms: u64) void {
        if (core_mod.classifyTransitionError(transition, err).disposition == .close_session) self.closeAt(now_ms);
    }

    fn closeAt(self: *Session, now_ms: u64) void {
        self.state = .closed;
        self.deadlines.upsert(self.key, now_ms) catch {};
    }
};

pub const Listener = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    config: Config,
    socket: backend.Socket,
    session_quota: QuotaAllocator,
    sessions: std.AutoHashMapUnmanaged(EndpointKey, *Session) = .empty,
    deadlines: deadline_queue.Queue,
    advertisement: []u8,
    rate_entries: []rate.Entry,
    limiter: rate.Limiter,
    handshake_handler: handshake.Handler,
    source_secret: u64,
    messages: []std.Io.net.IncomingMessage,
    receive_storage: []u8,
    frame_scratch: []frame.Frame,
    timer_entries: []deadline_queue.Entry,
    handshake_output: []u8,
    pending_message_index: usize = 0,
    pending_message_count: usize = 0,
    closed: bool = false,
    handshake_timeout_ms: u32,
    ack_capacity: usize,

    pub fn listen(allocator: std.mem.Allocator, io: std.Io, address: std.Io.net.IpAddress, options: Options) !*Listener {
        try validateOptions(options);
        const self = try allocator.create(Listener);
        errdefer allocator.destroy(self);
        var socket = try backend.Socket.bindWithBuffers(io, address, options.config.protocol.maximum_datagram_size, options.socket_buffers);
        errdefer socket.close();
        const advertisement = try allocator.dupe(u8, options.advertisement);
        errdefer allocator.free(advertisement);
        const limiter_count = @min(options.config.listener.maximum_pending_handshakes, 65_536);
        const rate_entries = try allocator.alloc(rate.Entry, limiter_count);
        errdefer allocator.free(rate_entries);
        const limiter = try rate.Limiter.init(
            rate_entries,
            .{
                .tokens_per_second = options.offline_rate_per_second,
                .burst = options.offline_burst,
                .global_tokens_per_second = options.global_offline_rate_per_second,
                .global_burst = options.global_offline_burst,
            },
            time.nowMilliseconds(io),
        );
        const messages = try allocator.alloc(std.Io.net.IncomingMessage, options.receive_batch_size);
        errdefer allocator.free(messages);
        const receive_size = try std.math.mul(usize, options.receive_batch_size, options.config.protocol.maximum_datagram_size);
        const receive_storage = try allocator.alloc(u8, receive_size);
        errdefer allocator.free(receive_storage);
        const handshake_output = try allocator.alloc(u8, options.config.protocol.maximum_datagram_size);
        errdefer allocator.free(handshake_output);
        const frame_scratch = try allocator.alloc(frame.Frame, options.config.batching.maximum_packets_per_iteration);
        errdefer allocator.free(frame_scratch);
        const timer_entries = try allocator.alloc(deadline_queue.Entry, options.config.batching.maximum_packets_per_iteration);
        errdefer allocator.free(timer_entries);
        var deadlines = try deadline_queue.Queue.init(allocator, options.config.listener.maximum_connections);
        errdefer deadlines.deinit();
        var random: [80]u8 = undefined;
        io.random(&random);
        const guid = if (options.server_guid != 0) options.server_guid else std.mem.readInt(u64, random[0..8], .little);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .config = options.config,
            .socket = socket,
            .session_quota = QuotaAllocator.init(allocator, options.maximum_session_memory_bytes),
            .deadlines = deadlines,
            .advertisement = advertisement,
            .rate_entries = rate_entries,
            .limiter = limiter,
            .handshake_handler = undefined,
            .source_secret = std.mem.readInt(u64, random[72..80], .little),
            .messages = messages,
            .receive_storage = receive_storage,
            .frame_scratch = frame_scratch,
            .timer_entries = timer_entries,
            .handshake_output = handshake_output,
            .handshake_timeout_ms = options.handshake_timeout_ms,
            .ack_capacity = options.config.batching.maximum_ack_records,
        };
        self.handshake_handler = try handshake.Handler.init(
            guid,
            options.protocol_version,
            options.config.protocol.minimum_mtu,
            options.config.protocol.maximum_mtu,
            self.advertisement,
            .{ .current_key = random[8..40].*, .previous_key = random[40..72].* },
            &self.limiter,
        );
        return self;
    }

    pub fn kernelBufferSizes(self: *const Listener) backend.BufferSizes {
        return self.socket.kernelBufferSizes();
    }
    pub fn statistics(self: *const Listener) ListenerStatistics {
        return .{
            .active_sessions = self.sessions.count(),
            .session_memory_bytes = self.session_quota.used_bytes,
            .maximum_session_memory_bytes = self.session_quota.maximum_bytes,
            .socket_buffers = self.socket.kernelBufferSizes(),
        };
    }
    pub fn close(self: *Listener) void {
        if (self.closed) return;
        var iterator = self.sessions.valueIterator();
        while (iterator.next()) |session| session.*.close();
        self.closed = true;
        self.socket.close();
    }
    pub fn destroy(self: *Listener) void {
        self.close();
        var iterator = self.sessions.valueIterator();
        while (iterator.next()) |session| session.*.destroy();
        self.sessions.deinit(self.session_quota.allocator());
        std.debug.assert(self.session_quota.used_bytes == 0);
        self.deadlines.deinit();
        self.allocator.free(self.timer_entries);
        self.allocator.free(self.frame_scratch);
        self.allocator.free(self.handshake_output);
        self.allocator.free(self.receive_storage);
        self.allocator.free(self.messages);
        self.allocator.free(self.rate_entries);
        self.allocator.free(self.advertisement);
        self.allocator.destroy(self);
    }

    pub fn nextDeadline(self: *const Listener) ?u64 {
        if (self.closed) return null;
        const entry = self.deadlines.peek() orelse return null;
        return entry.deadline_ms;
    }

    pub fn processTimers(self: *Listener, now_ms: u64, callbacks: Callbacks) !PollStats {
        if (self.closed) return error.ConnectionClosed;
        var stats: PollStats = .{};
        self.processTimersInto(now_ms, callbacks, &stats, self.config.batching.maximum_packets_per_iteration);
        return stats;
    }

    pub fn poll(self: *Listener, timeout: std.Io.Timeout, callbacks: Callbacks) !PollStats {
        if (self.closed) return error.ConnectionClosed;
        var stats: PollStats = .{};
        if (self.pending_message_index == self.pending_message_count) {
            const wait = if (self.nextDeadline()) |deadline| time.earliest(self.io, timeout, time.atMilliseconds(deadline)) else timeout;
            const batch = self.socket.receiveMany(self.messages, self.receive_storage, wait) catch |err| switch (err) {
                error.Timeout => {
                    self.processTimersInto(time.nowMilliseconds(self.io), callbacks, &stats, self.config.batching.maximum_packets_per_iteration);
                    return stats;
                },
                else => return err,
            };
            stats.malformed += batch.dropped_oversize;
            stats.transport_failures += @intFromBool(batch.trailing_error != null);
            self.pending_message_index = 0;
            self.pending_message_count = batch.messages.len;
        }
        var remaining = self.config.batching.maximum_packets_per_iteration;
        var now_ms = time.nowMilliseconds(self.io);
        var processed: usize = 0;
        while (self.pending_message_index < self.pending_message_count and remaining != 0) {
            if (processed != 0 and processed % 32 == 0) now_ms = time.nowMilliseconds(self.io);
            const message = self.messages[self.pending_message_index];
            self.pending_message_index += 1;
            processed += 1;
            stats.datagrams += 1;
            remaining -= 1;
            const key = endpointKey(message.from);
            if (self.sessions.get(key)) |session| {
                try self.processExisting(session, key, message, now_ms, callbacks, &stats, &remaining);
                continue;
            }

            const action = self.handshake_handler.handle(message.data, &key, std.hash.Wyhash.hash(self.source_secret, &key), now_ms / 30_000, now_ms, self.handshake_output);
            switch (action) {
                .drop => stats.rate_limited_or_dropped += 1,
                .response => |wire| try self.socket.send(message.from, wire),
                .accepted => |accepted| {
                    if (self.sessions.count() >= self.config.listener.maximum_connections) {
                        stats.rate_limited_or_dropped += 1;
                        const response = offline.encodeNoFreeIncomingConnections(self.handshake_handler.server_guid, self.handshake_output) catch continue;
                        try self.socket.send(message.from, response);
                        continue;
                    }
                    const session_allocator = self.session_quota.allocator();
                    const session = Session.create(
                        session_allocator,
                        &self.socket,
                        &self.deadlines,
                        message.from,
                        key,
                        accepted.client_guid,
                        accepted.mtu,
                        now_ms,
                        self.handshake_timeout_ms,
                        self.ack_capacity,
                        self.config,
                    ) catch {
                        stats.rate_limited_or_dropped += 1;
                        continue;
                    };
                    self.sessions.put(session_allocator, key, session) catch |err| {
                        session.destroy();
                        return err;
                    };
                    session.schedule() catch |err| {
                        _ = self.sessions.remove(key);
                        session.destroy();
                        return err;
                    };
                    self.socket.send(message.from, accepted.response) catch |err| {
                        _ = self.deadlines.remove(key);
                        _ = self.sessions.remove(key);
                        session.destroy();
                        return err;
                    };
                },
            }
        }
        if (self.pending_message_index == self.pending_message_count) {
            self.pending_message_index = 0;
            self.pending_message_count = 0;
        }
        self.processTimersInto(now_ms, callbacks, &stats, remaining);
        return stats;
    }

    fn processExisting(
        self: *Listener,
        session: *Session,
        key: EndpointKey,
        message: std.Io.net.IncomingMessage,
        now_ms: u64,
        callbacks: Callbacks,
        stats: *PollStats,
        remaining: *usize,
    ) !void {
        if (isOfflineHandshake(message.data)) {
            session.flushReceipts() catch |err| {
                recordSessionFailure(stats, core_mod.classifyTransitionError(.receipt, err).class);
                session.state = .closed;
                self.removeSession(key, callbacks);
                return;
            };
            const repeated = self.handshake_handler.handle(
                message.data,
                &key,
                std.hash.Wyhash.hash(self.source_secret, &key),
                now_ms / 30_000,
                now_ms,
                self.handshake_output,
            );
            switch (repeated) {
                .drop => stats.rate_limited_or_dropped += 1,
                .response => |wire| try self.socket.send(message.from, wire),
                .accepted => |accepted| try self.socket.send(message.from, accepted.response),
            }
            return;
        }
        var bridge: DeliveryBridge = .{ .callbacks = callbacks, .session = session, .now_ms = now_ms };
        const processed_incoming = session.core.processIncomingCountedWithScratch(message.data, now_ms, self.frame_scratch, &bridge, DeliveryBridge.deliver) catch |err| {
            remaining.* = 0;
            const failure = core_mod.classifyIncomingError(err);
            if (failure.disposition == .reject) {
                stats.malformed += 1;
                return;
            }

            recordSessionFailure(stats, failure.class);
            session.state = .closed;
            self.removeSession(key, callbacks);
            return;
        };
        session.last_seen_ms = now_ms;
        const incoming = processed_incoming.incoming;
        const extra_work = processed_incoming.work_units -| 1;
        remaining.* -= @min(remaining.*, extra_work);
        if (incoming == .data) {
            session.queueReceipt(incoming.data, now_ms) catch |err| {
                const failure = core_mod.classifyTransitionError(.receipt, err);
                recordSessionFailure(stats, failure.class);
                session.state = .closed;
                self.removeSession(key, callbacks);
                return;
            };
        }
        if (session.state != .closed) {
            const flushed = session.flushQueuedAtLimit(now_ms, remaining.*) catch |err| {
                recordSessionFailure(stats, core_mod.classifyTransitionError(.application_send, err).class);
                session.state = .closed;
                self.removeSession(key, callbacks);
                return;
            };
            remaining.* -= @min(remaining.*, flushed.datagrams);
        }
        if (session.state == .closed) {
            session.flushReceipts() catch |err| {
                recordSessionFailure(stats, core_mod.classifyTransitionError(.receipt, err).class);
            };
            self.removeSession(key, callbacks);
        } else {
            session.schedule() catch {
                recordSessionFailure(stats, .internal);
                session.state = .closed;
                self.removeSession(key, callbacks);
            };
        }
    }

    fn processTimersInto(self: *Listener, now_ms: u64, callbacks: Callbacks, stats: *PollStats, maximum_work: usize) void {
        if (maximum_work == 0) return;
        var due_count: usize = 0;
        while (due_count < @min(self.timer_entries.len, maximum_work)) : (due_count += 1) {
            const entry = self.deadlines.popDue(now_ms) orelse break;
            self.timer_entries[due_count] = entry;
        }
        var remaining = maximum_work;
        for (self.timer_entries[0..due_count], 0..) |entry, index| {
            const session = self.sessions.get(entry.key) orelse {
                remaining -= 1;
                continue;
            };
            if (session.state == .closed) {
                self.removeSession(entry.key, callbacks);
                remaining -= 1;
                continue;
            }
            if ((session.state == .connecting and time.reached(now_ms, session.handshake_deadline_ms)) or
                time.reached(now_ms, time.deadline(session.last_seen_ms, session.idle_timeout_ms)))
            {
                stats.sessions_expired += 1;
                self.removeSession(entry.key, callbacks);
                remaining -= 1;
                continue;
            }
            const sessions_left = due_count - index;
            const quota = @max(@as(usize, 1), remaining / sessions_left);
            const used = session.processDueTimers(now_ms, quota) catch |err| {
                recordSessionFailure(stats, core_mod.classifyTransitionError(.retransmission, err).class);
                session.state = .closed;
                self.removeSession(entry.key, callbacks);
                continue;
            };
            remaining -= @min(remaining, @max(@as(usize, 1), used));
            session.schedule() catch {
                recordSessionFailure(stats, .internal);
                session.state = .closed;
                self.removeSession(entry.key, callbacks);
            };
        }
    }
    fn removeSession(self: *Listener, key: EndpointKey, callbacks: Callbacks) void {
        _ = self.deadlines.remove(key);
        const removed = self.sessions.fetchRemove(key) orelse return;
        if (callbacks.disconnected) |notify| notify(callbacks.context, removed.value);
        removed.value.destroy();
    }
};

fn validateOptions(options: Options) !void {
    try options.config.validate();
    try options.socket_buffers.validate();
    const invalid =
        options.receive_batch_size == 0 or
        options.receive_batch_size > 256 or
        options.advertisement.len > options.config.protocol.maximum_datagram_size -| 35 or
        options.maximum_session_memory_bytes == 0 or
        options.handshake_timeout_ms == 0;
    if (invalid) return error.InvalidConfiguration;
}

fn isOfflineHandshake(data: []const u8) bool {
    if (data.len == 0) return false;
    return switch (data[0]) {
        @intFromEnum(offline.Id.unconnected_ping),
        @intFromEnum(offline.Id.unconnected_ping_open_connections),
        @intFromEnum(offline.Id.open_connection_request_1),
        @intFromEnum(offline.Id.open_connection_request_2),
        => true,
        else => false,
    };
}

fn emptyAddress(address: offline.Address) offline.Address {
    return switch (address) {
        .ipv4 => .{ .ipv4 = .{ .octets = .{ 0, 0, 0, 0 }, .port = 0 } },
        .ipv6 => .{ .ipv6 = .{ .octets = @splat(0), .port = 0 } },
    };
}

fn recordSessionFailure(stats: *PollStats, class: core_mod.IncomingErrorClass) void {
    stats.sessions_failed += 1;
    switch (class) {
        .protocol => stats.malformed += 1,
        .resource => stats.resource_failures += 1,
        .transport => stats.transport_failures += 1,
        .application => stats.application_failures += 1,
        .internal => stats.internal_failures += 1,
    }
}
fn endpointKey(address: std.Io.net.IpAddress) EndpointKey {
    var key: EndpointKey = @splat(0);
    switch (address) {
        .ip4 => |value| {
            key[0] = 4;
            @memcpy(key[1..5], &value.bytes);
            std.mem.writeInt(u16, key[17..19], value.port, .big);
        },
        .ip6 => |value| {
            key[0] = 6;
            @memcpy(key[1..17], &value.bytes);
            std.mem.writeInt(u16, key[17..19], value.port, .big);
            std.mem.writeInt(u32, key[19..23], value.interface.index, .big);
        },
    }
    return key;
}
test "listener answers an offline ping over loopback" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var io_instance: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer io_instance.deinit();
    const io = io_instance.io();
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var listener = try Listener.listen(std.testing.allocator, io, address, .{ .advertisement = "MCPE;zig-raknet" });
    defer listener.destroy();
    var client = try backend.Socket.bind(io, address, 2048);
    defer client.close();
    var ping: [33]u8 = undefined;
    var writer: @import("protocol/cursor.zig").Writer = .{ .data = &ping };
    try writer.byte(@intFromEnum(offline.Id.unconnected_ping));
    try writer.u64be(123);
    try writer.bytes(&offline.magic);
    try writer.u64be(456);
    try client.send(listener.socket.value.address, &ping);
    const TestContext = struct { connections: usize = 0 };
    const Noop = struct {
        fn connected(raw: *anyopaque, _: *Session) !void {
            const value: *TestContext = @ptrCast(@alignCast(raw));
            value.connections += 1;
        }
        fn message(_: *anyopaque, _: *Session, _: receiver.BorrowedPayload) !void {}
    };
    var context: TestContext = .{};
    const stats = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    try std.testing.expectEqual(@as(usize, 1), stats.datagrams);
    var response: [2048]u8 = undefined;
    const message = try client.value.receive(io, &response);
    try std.testing.expectEqual(@intFromEnum(offline.Id.unconnected_pong), message.data[0]);

    var request1: [548]u8 = @splat(0);
    request1[0] = @intFromEnum(offline.Id.open_connection_request_1);
    @memcpy(request1[1..17], &offline.magic);
    request1[17] = 11;
    try client.send(listener.socket.value.address, &request1);
    _ = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    const reply1 = try client.value.receive(io, &response);
    try std.testing.expectEqual(@intFromEnum(offline.Id.open_connection_reply_1), reply1.data[0]);
    const cookie_value = std.mem.readInt(u32, reply1.data[26..30], .big);
    const mtu = std.mem.readInt(u16, reply1.data[30..32], .big);

    var request2_buffer: [64]u8 = undefined;
    var request2: @import("protocol/cursor.zig").Writer = .{ .data = &request2_buffer };
    try request2.byte(@intFromEnum(offline.Id.open_connection_request_2));
    try request2.bytes(&offline.magic);
    try request2.u32be(cookie_value);
    try request2.byte(0);
    try offline.encodeAddress(net_address.toRakNet(listener.socket.value.address), &request2);
    try request2.u16be(mtu);
    try request2.u64be(0x8000_0000_0000_0001);
    try client.send(listener.socket.value.address, request2.written());
    _ = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    const reply2 = try client.value.receive(io, &response);
    try std.testing.expectEqual(@intFromEnum(offline.Id.open_connection_reply_2), reply2.data[0]);
    try std.testing.expectEqual(@as(u32, 1), listener.sessions.count());
    try std.testing.expectEqual(@as(usize, 1), listener.deadlines.count());
    const scheduled = listener.sessions.get(endpointKey(client.value.address)).?;
    try std.testing.expectEqual(scheduled.handshake_deadline_ms, listener.nextDeadline().?);

    try client.send(listener.socket.value.address, request2.written());
    _ = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    _ = try client.value.receive(io, &response);
    try std.testing.expectEqual(@as(u32, 1), listener.sessions.count());
    try std.testing.expectEqual(@as(usize, 1), listener.deadlines.count());

    scheduled.last_seen_ms = 1;
    try client.send(listener.socket.value.address, &.{0x80});
    const malformed_stats = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    try std.testing.expectEqual(@as(usize, 1), malformed_stats.malformed);
    try std.testing.expectEqual(@as(u64, 1), scheduled.last_seen_ms);

    const Sender = struct {
        socket: *backend.Socket,
        destination: std.Io.net.IpAddress,
        fn emit(raw: *anyopaque, _: u32, _: bool, wire: []const u8) transmitter_mod.EmitError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.socket.send(self.destination, wire) catch return error.TransportFailure;
        }
    };
    var sender: Sender = .{ .socket = &client, .destination = listener.socket.value.address };
    var transmitter = try transmitter_mod.Transmitter.init(mtu, .{});
    var send_scratch: [1492]u8 = undefined;
    var control: [512]u8 = undefined;
    const connection_request = try connected.encodeConnectionRequest(0x8000_0000_0000_0001, 1000, &control);
    _ = try transmitter.send(connection_request, .reliable_ordered, 0, send_scratch[0..mtu], &sender, Sender.emit);
    _ = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    try std.testing.expectEqual(@as(usize, 0), scheduled.receipts.count());
    const retransmission_deadline = scheduled.core.nextRetransmissionDeadline().?;
    try std.testing.expectEqual(retransmission_deadline, listener.nextDeadline().?);
    var accepted = false;
    for (0..4) |_| {
        const accepted_datagram = try client.value.receive(io, &response);
        if (accepted_datagram.data[0] & 0x40 != 0) continue;
        var decoded_datagram = try @import("protocol/frame.zig").decodeDatagram(accepted_datagram.data);
        const accepted_frame = try @import("protocol/frame.zig").decodeOne(&decoded_datagram.frames, 8192, 2048);
        if ((try connected.decode(accepted_frame.payload)) == .connection_request_accepted) {
            accepted = true;
            break;
        }
    }
    try std.testing.expect(accepted);

    _ = try transmitter.send(connection_request, .reliable_ordered, 0, send_scratch[0..mtu], &sender, Sender.emit);
    _ = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    accepted = false;
    for (0..4) |_| {
        const repeated_accepted = try client.value.receive(io, &response);
        if (repeated_accepted.data[0] & 0x40 != 0) continue;
        var repeated_datagram = try @import("protocol/frame.zig").decodeDatagram(repeated_accepted.data);
        const repeated_frame = try @import("protocol/frame.zig").decodeOne(&repeated_datagram.frames, 8192, 2048);
        if ((try connected.decode(repeated_frame.payload)) == .connection_request_accepted) {
            accepted = true;
            break;
        }
    }
    try std.testing.expect(accepted);
    try std.testing.expectEqual(@as(usize, 0), context.connections);

    const new_incoming = try connected.encodeAddressList(.incoming, net_address.toRakNet(listener.socket.value.address), 0, &.{}, 1000, 1001, &control);
    _ = try transmitter.send(new_incoming, .reliable_ordered, 0, send_scratch[0..mtu], &sender, Sender.emit);
    _ = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    try std.testing.expectEqual(@as(usize, 1), context.connections);
    _ = try transmitter.send(new_incoming, .reliable_ordered, 0, send_scratch[0..mtu], &sender, Sender.emit);
    _ = try listener.poll(.none, .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    try std.testing.expectEqual(@as(usize, 1), context.connections);
    const timer_stats = try listener.processTimers(std.math.maxInt(u64), .{ .context = &context, .connected = Noop.connected, .message = Noop.message });
    try std.testing.expectEqual(@as(usize, 1), timer_stats.sessions_expired);
    try std.testing.expectEqual(@as(u32, 0), listener.sessions.count());
    try std.testing.expect(listener.nextDeadline() == null);
}

test "closing a connected session sends a disconnect" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var socket = try backend.Socket.bind(io, address, 576);
    defer socket.close();
    var peer = try backend.Socket.bind(io, address, 576);
    defer peer.close();
    var deadlines = try deadline_queue.Queue.init(std.testing.allocator, 1);
    defer deadlines.deinit();
    const key = endpointKey(peer.value.address);
    const session = try Session.create(std.testing.allocator, &socket, &deadlines, peer.value.address, key, 1, 576, 100, 1000, 8, .{});
    defer session.destroy();
    session.state = .connected;

    session.close();
    try std.testing.expectEqual(State.closed, session.state);
    try std.testing.expectEqual(@as(usize, 1), deadlines.count());

    var storage: [576]u8 = undefined;
    const message = try peer.value.receive(io, &storage);
    var decoded = try @import("protocol/frame.zig").decodeDatagram(message.data);
    const value = try @import("protocol/frame.zig").decodeOne(&decoded.frames, 8192, 2048);
    try std.testing.expectEqual(@intFromEnum(offline.Id.disconnect_notification), value.payload[0]);
}

test "ACK delay schedules while NACK remains urgent" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var socket = try backend.Socket.bind(io, address, 576);
    defer socket.close();
    var deadlines = try deadline_queue.Queue.init(std.testing.allocator, 1);
    defer deadlines.deinit();
    var config: Config = .{};
    config.timing.maximum_ack_delay_ms = 5;
    const key = endpointKey(socket.value.address);
    const session = try Session.create(std.testing.allocator, &socket, &deadlines, socket.value.address, key, 1, 576, 100, 1000, 8, config);
    defer session.destroy();
    try session.queueReceipt(.{ .acknowledge = 1 }, 100);
    try std.testing.expectEqual(@as(?u64, 105), session.ack_deadline_ms);
    try session.queueReceipt(.{ .missing = .{ .first = 2, .last = 2, .count = 1 } }, 102);
    try std.testing.expectEqual(@as(?u64, 102), session.ack_deadline_ms);
}

test "global turn budget carries unread batch entries fairly" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var config: Config = .{};
    config.batching.maximum_packets_per_iteration = 1;
    var listener = try Listener.listen(std.testing.allocator, io, address, .{ .advertisement = "MCPE;budget", .receive_batch_size = 4, .config = config });
    defer listener.destroy();
    var peer = try backend.Socket.bind(io, address, 2048);
    defer peer.close();

    var ping: [33]u8 = undefined;
    var writer: @import("protocol/cursor.zig").Writer = .{ .data = &ping };
    try writer.byte(@intFromEnum(offline.Id.unconnected_ping));
    try writer.u64be(1);
    try writer.bytes(&offline.magic);
    try writer.u64be(2);
    const flags: std.Io.net.IncomingMessage.Flags = @bitCast(@as(u8, 0));
    for (listener.messages[0..3]) |*message| message.* = .{ .from = peer.value.address, .data = &ping, .control = &.{}, .flags = flags };
    listener.pending_message_index = 0;
    listener.pending_message_count = 3;

    const Noop = struct {
        fn connected(_: *anyopaque, _: *Session) !void {}
        fn message(_: *anyopaque, _: *Session, _: receiver.BorrowedPayload) !void {}
    };
    var unused: u8 = 0;
    const callbacks: Callbacks = .{ .context = &unused, .connected = Noop.connected, .message = Noop.message };
    const first = try listener.poll(.none, callbacks);
    try std.testing.expectEqual(@as(usize, 1), first.datagrams);
    try std.testing.expectEqual(@as(usize, 1), listener.pending_message_index);
    const second = try listener.poll(.none, callbacks);
    try std.testing.expectEqual(@as(usize, 1), second.datagrams);
    try std.testing.expectEqual(@as(usize, 2), listener.pending_message_index);
    const third = try listener.poll(.none, callbacks);
    try std.testing.expectEqual(@as(usize, 1), third.datagrams);
    try std.testing.expectEqual(@as(usize, 0), listener.pending_message_count);
}

fn listenerAllocationScenario(allocator: std.mem.Allocator) !void {
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var config: Config = .{};
    config.listener.maximum_pending_handshakes = 8;
    config.listener.maximum_connections = 1;
    config.batching.maximum_packets_per_iteration = 8;
    const listener = try Listener.listen(allocator, std.testing.io, address, .{
        .advertisement = "MCPE;allocation",
        .config = config,
        .receive_batch_size = 1,
    });
    listener.destroy();
}

test "listener initialization handles every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, listenerAllocationScenario, .{});
}
