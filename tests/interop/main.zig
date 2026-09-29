const std = @import("std");
const builtin = @import("builtin");
const raknet = @import("raknet");

const usage =
    \\usage:
    \\  raknet-interop server <ip:port> <seconds> [listeners] [ack_ms] [receive_batch] [send_batch] [receive_buffer_bytes]
    \\  raknet-interop client <ip:port> <connections> <payload> <seconds> <warmup_ms> [window] [interval_ms]
    \\
;

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    var threaded: std.Io.Threaded = .init(std.heap.smp_allocator, .{ .async_limit = .unlimited });
    defer threaded.deinit();
    const io = threaded.io();
    if (args.len >= 4 and args.len <= 9 and std.mem.eql(u8, args[1], "server")) {
        const listeners = if (args.len >= 5) try std.fmt.parseInt(usize, args[4], 10) else 1;
        const ack_ms = if (args.len >= 6) try std.fmt.parseInt(u32, args[5], 10) else 0;
        const receive_batch = if (args.len >= 7) try std.fmt.parseInt(usize, args[6], 10) else 32;
        const send_batch = if (args.len >= 8) try std.fmt.parseInt(usize, args[7], 10) else 64;
        const receive_buffer = if (args.len >= 9) try std.fmt.parseInt(u32, args[8], 10) else null;
        return server(io, try std.Io.net.IpAddress.parseLiteral(args[2]), try std.fmt.parseInt(u32, args[3], 10), listeners, ack_ms, receive_batch, send_batch, receive_buffer, init.environ_map.get("RAKNET_SPLIT_TIMING") != null);
    }
    if (args.len >= 7 and args.len <= 9 and std.mem.eql(u8, args[1], "client")) {
        return client(io, .{
            .address = try std.Io.net.IpAddress.parseLiteral(args[2]),
            .connections = try std.fmt.parseInt(usize, args[3], 10),
            .payload = try std.fmt.parseInt(usize, args[4], 10),
            .seconds = try std.fmt.parseInt(u32, args[5], 10),
            .warmup_ms = try std.fmt.parseInt(u32, args[6], 10),
            .window = if (args.len >= 8) try std.fmt.parseInt(usize, args[7], 10) else 32,
            .interval_ms = if (args.len >= 9) try std.fmt.parseInt(u32, args[8], 10) else 0,
        });
    }
    std.debug.print("{s}", .{usage});
    return error.InvalidArguments;
}

fn nowNs(io: std.Io) u64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn wait(ms: u32) std.Io.Timeout {
    return .{ .duration = .{ .raw = std.Io.Duration.fromMilliseconds(ms), .clock = .awake } };
}

const Process = struct {
    cpu_ms: u64,
    rss_kb: u64,
    peak_rss_kb: u64,

    fn sample() Process {
        if (builtin.os.tag != .linux) return .{ .cpu_ms = 0, .rss_kb = 0, .peak_rss_kb = 0 };
        const linux = std.os.linux;
        var usage_value: linux.rusage = undefined;
        _ = linux.getrusage(linux.rusage.SELF, &usage_value);
        const cpu_us = (usage_value.utime.sec + usage_value.stime.sec) * 1_000_000 + usage_value.utime.usec + usage_value.stime.usec;
        var buffer: [4096]u8 = undefined;
        var len: usize = 0;
        const fd = linux.open("/proc/self/status", .{}, 0);
        if (@as(isize, @bitCast(fd)) >= 0) {
            len = linux.read(@intCast(fd), &buffer, buffer.len);
            _ = linux.close(@intCast(fd));
            if (@as(isize, @bitCast(len)) < 0) len = 0;
        }
        return .{ .cpu_ms = @intCast(@divTrunc(cpu_us, 1000)), .rss_kb = field(buffer[0..len], "VmRSS:"), .peak_rss_kb = field(buffer[0..len], "VmHWM:") };
    }

    fn field(status: []const u8, name: []const u8) u64 {
        const start = std.mem.indexOf(u8, status, name) orelse return 0;
        var tokens = std.mem.tokenizeAny(u8, status[start + name.len ..], " \t");
        return std.fmt.parseInt(u64, tokens.next() orelse return 0, 10) catch 0;
    }
};

const Server = struct {
    echoed: u64 = 0,
    dropped: u64 = 0,
    connected: u64 = 0,

    fn onConnect(raw: *anyopaque, session: *raknet.Session) error{ApplicationFailure}!void {
        const self: *Server = @ptrCast(@alignCast(raw));
        self.connected += 1;
        if (self.connected == 1) {
            const c = &session.core;
            const r = &c.receiver_state;
            const rec = &c.recovery_state;
            const receipts = &session.receipts;
            std.debug.print("memory session_struct={d} recovery_metadata={d} receive_windows={d} split_metadata={d} outbound_metadata={d} ordered_metadata={d} sequenced={d} ack_decode={d} receipts={d} tx_scratch={d} transmitter_inline={d}\n", .{
                @sizeOf(raknet.Session),
                sliceBytes(rec.slots) + sliceBytes(rec.heap) + sliceBytes(rec.blocks),
                sliceBytes(r.datagram_storage) + sliceBytes(r.reliable_storage),
                sliceBytes(r.splits.assemblies) + sliceBytes(r.splits.fragment_blocks) + sliceBytes(r.splits.heap),
                sliceBytes(c.outbound_state.slots),
                r.ordered.metadataCapacity(),
                sliceBytes(r.sequenced),
                sliceBytes(c.ack_records),
                sliceBytes(receipts.ack_values) + sliceBytes(receipts.nack_values) + sliceBytes(receipts.records) + sliceBytes(receipts.wire_storage) + sliceBytes(receipts.messages),
                session.scratch.len,
                @sizeOf(@TypeOf(c.transmitter_state)),
            });
        }
    }
    fn onMessage(raw: *anyopaque, session: *raknet.Session, payload: raknet.BorrowedPayload) error{ApplicationFailure}!void {
        const self: *Server = @ptrCast(@alignCast(raw));
        _ = session.queueSend(payload.bytes, .reliable_ordered, 0) catch {
            self.dropped += 1;
            return;
        };
        self.echoed += 1;
    }
};

fn sliceBytes(slice: anytype) usize {
    return slice.len * @sizeOf(@TypeOf(slice[0]));
}

const Shard = struct {
    listener: *raknet.Server,
    state: Server = .{},
    peak_sessions: usize = 0,
    busy_turns: u64 = 0,
    turn_ns: u64 = 0,
    maximum_turn_ns: u64 = 0,
    turns: [2048]u64 = undefined,

    fn run(self: *Shard, io: std.Io, deadline: u64, report: bool, baseline: Process, split: bool) void {
        var next_progress = nowNs(io) + progress_interval_ns;
        while (nowNs(io) < deadline) {
            if (split) self.listener.waitReadable(self.listener.pollTimeout(wait(50))) catch {};
            const began = nowNs(io);
            const polled = self.listener.poll(if (split) wait(0) else wait(50), .{ .context = &self.state, .connected = Server.onConnect, .message = Server.onMessage }) catch |err| {
                std.debug.print("poll error: {s}\n", .{@errorName(err)});
                continue;
            };
            if (polled.datagrams != 0) {
                const elapsed = nowNs(io) - began;
                self.turns[self.busy_turns % self.turns.len] = elapsed / std.time.ns_per_us;
                self.busy_turns += 1;
                self.turn_ns += elapsed;
                self.maximum_turn_ns = @max(self.maximum_turn_ns, elapsed);
            }
            self.peak_sessions = @max(self.peak_sessions, self.listener.sessions.count());
            if (report and nowNs(io) >= next_progress) {
                next_progress += progress_interval_ns;
                const progress = Process.sample();
                const snapshot_start = nowNs(io);
                const live = self.listener.statistics();
                const snapshot_ns = nowNs(io) - snapshot_start;
                const d = self.listener.deadlines.diagnostics;
                const t = live.traffic;
                const q = self.listener.session_quota;
                var turns = self.turns;
                const samples = turns[0..@min(self.busy_turns, turns.len)];
                std.mem.sort(u64, samples, {}, std.sort.asc(u64));
                std.debug.print("network shard={d} native_calls={d} native_datagrams={d} turn_p50_us={d} turn_p95_us={d} turn_p99_us={d} turn_max_us={d} turn_includes_wait={d}\n", .{ @intFromPtr(self), t.native_receive_calls, t.native_received_datagrams, percentile(samples, 0.5), percentile(samples, 0.95), percentile(samples, 0.99), if (samples.len == 0) 0 else samples[samples.len - 1], @intFromBool(!split) });
                std.debug.print("progress impl=zig role=server shard={d} sessions={d} echoed={d} recovery_bytes={d} session_memory_bytes={d} cpu_ms={d} rss_kb={d} upserts={d} unchanged={d} sifts={d} swaps={d} received={d} sent={d} receive_calls={d} receive_batches={d} send_calls={d} receive_max={d} send_max={d} ack_datagrams={d} nack_datagrams={d} ack_records={d} retransmits={d} allocations={d} frees={d} timer_visits={d} timer_lateness_ms={d} timer_lateness_max_ms={d} deferred={d} busy_turns={d} turn_ns={d} turn_max_ns={d} statistics_ns={d}\n", .{ @intFromPtr(self), live.active_sessions, self.state.echoed, live.recovery_bytes, live.session_memory_bytes, progress.cpu_ms - baseline.cpu_ms, progress.rss_kb, d.upserts, d.unchanged, d.sifts, d.swaps, t.datagrams_received, t.datagrams_sent, t.receive_calls, t.receive_batches, t.send_calls, t.maximum_receive_batch, t.maximum_send_batch, t.ack_datagrams_sent, t.nack_datagrams_sent, live.ack_records_sent, live.retransmitted_datagrams, q.allocations, q.frees, self.listener.timer_visits, self.listener.timer_lateness_ms, self.listener.maximum_timer_lateness_ms, self.listener.deferred_receive_packets, self.busy_turns, self.turn_ns, self.maximum_turn_ns, snapshot_ns });
            }
        }
    }
};

fn server(io: std.Io, address: std.Io.net.IpAddress, seconds: u32, listeners: usize, ack_ms: u32, receive_batch: usize, send_batch: usize, receive_buffer: ?u32, split: bool) !void {
    if (send_batch == 0 or send_batch > 256) return error.InvalidArguments;
    const allocator = std.heap.smp_allocator;
    const baseline = Process.sample();
    const shards = try allocator.alloc(Shard, @max(listeners, 1));
    defer allocator.free(shards);
    var opened: usize = 0;
    defer for (shards[0..opened]) |shard| shard.listener.destroy();
    for (shards) |*shard| {
        var options: raknet.ServerOptions = .{
            .advertisement = "MCPE;raknet-zig interop;11;1.21;0;1000;0;interop;Survival;1;19132;19133;",
            .server_guid = 0x7a69_6e74_6572_6f70,
            .reuse_port = shards.len > 1,
            .config = .{ .timing = .{ .maximum_ack_delay_ms = ack_ms } },
            .receive_batch_size = receive_batch,
        };
        if (receive_buffer) |bytes| options.socket_buffers.receive_bytes = bytes;
        shard.* = .{ .listener = try raknet.Server.listen(allocator, io, address, options) };
        opened += 1;
        const buffers = shard.listener.kernelBufferSizes();
        std.debug.print("buffers receive_bytes={d} send_bytes={d}\n", .{ buffers.receive_bytes orelse 0, buffers.send_bytes orelse 0 });
        if (send_batch != 64) {
            const replacement = try @TypeOf(shard.listener.send_batch).init(allocator, send_batch, shard.listener.config.protocol.maximum_datagram_size);
            shard.listener.send_batch.deinit();
            shard.listener.send_batch = replacement;
        }
    }
    const deadline = nowNs(io) + @as(u64, seconds) * std.time.ns_per_s;
    const futures = try allocator.alloc(std.Io.Future(void), shards.len - 1);
    defer allocator.free(futures);
    var launched: usize = 0;
    errdefer for (futures[0..launched]) |*future| future.await(io);
    for (futures, shards[1..]) |*future, *shard| {
        future.* = try io.concurrent(Shard.run, .{ shard, io, deadline, true, baseline, split });
        launched += 1;
    }
    shards[0].run(io, deadline, true, baseline, split);
    for (futures) |*future| future.await(io);

    var total: Server = .{};
    var peak_sessions: usize = 0;
    var retransmits: u64 = 0;
    var malformed: u64 = 0;
    var rejected: u64 = 0;
    for (shards) |*shard| {
        const stats = shard.listener.statistics();
        total.connected += shard.state.connected;
        total.echoed += shard.state.echoed;
        total.dropped += shard.state.dropped;
        peak_sessions += shard.peak_sessions;
        retransmits += stats.retransmitted_datagrams;
        malformed += stats.malformed_datagrams;
        rejected += stats.handshakes_rejected;
    }
    const process = Process.sample();
    std.debug.print("server impl=zig listeners={d} sessions={d} connected={d} echoed={d} dropped={d} retransmits={d} malformed={d} rejected={d} cpu_ms={d} rss_kb={d} peak_rss_kb={d} baseline_rss_kb={d}\n", .{
        shards.len,
        peak_sessions,
        total.connected,
        total.echoed,
        total.dropped,
        retransmits,
        malformed,
        rejected,
        process.cpu_ms - baseline.cpu_ms,
        process.rss_kb,
        process.peak_rss_kb,
        baseline.rss_kb,
    });
}

const ClientOptions = struct {
    address: std.Io.net.IpAddress,
    connections: usize,
    payload: usize,
    seconds: u32,
    warmup_ms: u32,
    window: usize = 32,
    interval_ms: u32 = 0,
};

const maximum_samples = 2048;
const progress_interval_ns = std.time.ns_per_s;

const Shared = struct {
    io: std.Io,
    options: ClientOptions,
    ready: std.atomic.Value(usize) = .init(0),
    finished: std.atomic.Value(usize) = .init(0),
    start_ns: std.atomic.Value(u64) = .init(0),
};

const Connection = struct {
    shared: *Shared,
    ordinal: usize = 0,
    setup_us: u64 = 0,
    failed: bool = false,
    incomplete: bool = false,
    mismatches: u64 = 0,
    messages: u64 = 0,
    bytes: u64 = 0,
    retransmits: u64 = 0,
    samples: [maximum_samples]u64 = undefined,
    sample_count: usize = 0,
    outstanding: usize = 0,
    measure_from: u64 = 0,
    measure_until: u64 = 0,

    fn onMessage(raw: *anyopaque, payload: raknet.BorrowedPayload) error{ApplicationFailure}!void {
        const self: *Connection = @ptrCast(@alignCast(raw));
        self.outstanding -|= 1;
        const bytes = payload.bytes;
        if (bytes.len < 16 or bytes[0] != 0xfe or bytes.len != messageSize(self.shared.options.payload, bytes[9]) or bytes[bytes.len - 1] != bytes[9]) {
            self.mismatches += 1;
            return;
        }
        const sent = std.mem.readInt(u64, bytes[1..9], .little);
        const now = nowNs(self.shared.io);
        if (sent < self.measure_from or sent >= self.measure_until) return;
        if (now < self.measure_until) {
            self.messages += 1;
            self.bytes += bytes.len;
        }
        const rtt_us = (now - sent) / 1000;
        self.samples[self.sample_count % maximum_samples] = rtt_us;
        self.sample_count += 1;
    }

    fn run(self: *Connection) void {
        defer _ = self.shared.finished.fetchAdd(1, .release);
        self.runInner() catch |err| {
            std.debug.print("connection error: {s}\n", .{@errorName(err)});
            self.failed = true;
        };
    }

    fn runInner(self: *Connection) !void {
        const io = self.shared.io;
        const options = self.shared.options;
        const started = nowNs(io);
        const connection = raknet.Client.connect(std.heap.smp_allocator, io, options.address, .{}) catch |err| {
            _ = self.shared.ready.fetchAdd(1, .release);
            return err;
        };
        defer connection.destroy();
        self.setup_us = (nowNs(io) - started) / 1000;
        _ = self.shared.ready.fetchAdd(1, .release);
        while (self.shared.start_ns.load(.acquire) == 0) {
            _ = connection.poll(wait(5), self, onMessage) catch |err| if (err != error.Timeout) return err;
        }
        const start = self.shared.start_ns.load(.acquire);
        self.measure_from = start + @as(u64, options.warmup_ms) * std.time.ns_per_ms;
        self.measure_until = self.measure_from + @as(u64, options.seconds) * std.time.ns_per_s;
        const capacity = if (options.payload == 0) 8192 else options.payload;
        const window = @min(std.math.clamp(262_144 / capacity, 1, 32), options.window);
        const storage = try std.heap.smp_allocator.alloc(u8, capacity);
        defer std.heap.smp_allocator.free(storage);
        var sequence: u32 = 0;
        var next_send: u64 = start + @as(u64, options.interval_ms) * std.time.ns_per_ms * self.ordinal / options.connections;
        const drain_until = self.measure_until + 3 * std.time.ns_per_s;
        while (true) {
            const now = nowNs(io);
            if (now >= self.measure_until and (self.outstanding == 0 or now >= drain_until)) break;
            while (now < self.measure_until and self.outstanding < window and now >= next_send) {
                const payload = storage[0..messageSize(options.payload, @truncate(sequence))];
                payload[0] = 0xfe;
                std.mem.writeInt(u64, payload[1..9], nowNs(io), .little);
                payload[9] = @truncate(sequence);
                @memset(payload[10..], payload[9]);
                connection.send(payload, .reliable_ordered, 0) catch |err| switch (err) {
                    error.OutboundQueueFull, error.OutboundQueueBytesExceeded => break,
                    else => return err,
                };
                sequence +%= 1;
                self.outstanding += 1;
                next_send = now + @as(u64, options.interval_ms) * std.time.ns_per_ms;
                if (options.interval_ms != 0) break;
            }
            _ = connection.poll(wait(5), self, onMessage) catch |err| if (err != error.Timeout) return err;
        }
        self.retransmits = connection.statistics().retransmitted_datagrams;
        if (self.outstanding != 0) self.incomplete = true;
    }
};

fn percentile(values: []u64, fraction: f64) u64 {
    if (values.len == 0) return 0;
    const index: usize = @intFromFloat(@as(f64, @floatFromInt(values.len - 1)) * fraction);
    return values[index];
}

fn client(io: std.Io, options: ClientOptions) !void {
    if (options.payload != 0 and options.payload < 16) return error.PayloadTooSmall;
    if (options.connections == 0 or options.seconds == 0 or options.window > 32) return error.InvalidArguments;
    const allocator = std.heap.smp_allocator;
    const baseline = Process.sample();
    const ramp_start = nowNs(io);
    var shared: Shared = .{ .io = io, .options = options };
    const connections = try allocator.alloc(Connection, options.connections);
    defer allocator.free(connections);
    const futures = try allocator.alloc(std.Io.Future(void), options.connections);
    defer allocator.free(futures);
    var launched: usize = 0;
    errdefer {
        shared.start_ns.store(nowNs(io), .release);
        for (futures[0..launched]) |*future| future.await(io);
    }
    for (connections, futures, 0..) |*connection, *future, index| {
        while (index - shared.ready.load(.acquire) >= 64) try io.sleep(.fromMilliseconds(1), .awake);
        connection.* = .{ .shared = &shared, .ordinal = index };
        future.* = try io.concurrent(Connection.run, .{connection});
        launched += 1;
    }
    while (shared.ready.load(.acquire) < options.connections) try io.sleep(.fromMilliseconds(5), .awake);
    const connected_rss = Process.sample().rss_kb;
    std.debug.print("phase name=ready ramp_us={d}\n", .{(nowNs(io) - ramp_start) / 1000});
    shared.start_ns.store(nowNs(io), .release);
    try io.sleep(.fromMilliseconds(options.warmup_ms), .awake);
    std.debug.print("phase name=measure_start\n", .{});
    try io.sleep(.fromMilliseconds(@as(i64, options.seconds) * 1000), .awake);
    std.debug.print("phase name=measure_end\n", .{});
    if (options.seconds >= 60) {
        var next_progress = nowNs(io) + progress_interval_ns;
        while (shared.finished.load(.acquire) < options.connections) {
            try io.sleep(.fromMilliseconds(100), .awake);
            if (nowNs(io) < next_progress) continue;
            next_progress += progress_interval_ns;
            const progress = Process.sample();
            std.debug.print("progress impl=zig role=client running={d} cpu_ms={d} rss_kb={d}\n", .{ options.connections - shared.finished.load(.acquire), progress.cpu_ms - baseline.cpu_ms, progress.rss_kb });
        }
    }
    for (futures) |*future| future.await(io);

    var setup: std.ArrayList(u64) = .empty;
    defer setup.deinit(allocator);
    var rtt: std.ArrayList(u64) = .empty;
    defer rtt.deinit(allocator);
    var messages: u64 = 0;
    var bytes: u64 = 0;
    var failures: u64 = 0;
    var incomplete: u64 = 0;
    var mismatches: u64 = 0;
    var retransmits: u64 = 0;
    var minimum_messages: u64 = std.math.maxInt(u64);
    var maximum_messages: u64 = 0;
    var squared_messages: f64 = 0;
    for (connections) |*connection| {
        if (connection.failed) failures += 1;
        if (connection.incomplete) incomplete += 1;
        if (connection.setup_us != 0) try setup.append(allocator, connection.setup_us);
        try rtt.appendSlice(allocator, connection.samples[0..@min(connection.sample_count, maximum_samples)]);
        messages += connection.messages;
        bytes += connection.bytes;
        mismatches += connection.mismatches;
        retransmits += connection.retransmits;
        minimum_messages = @min(minimum_messages, connection.messages);
        maximum_messages = @max(maximum_messages, connection.messages);
        squared_messages += @as(f64, @floatFromInt(connection.messages)) * @as(f64, @floatFromInt(connection.messages));
    }
    std.mem.sort(u64, setup.items, {}, std.sort.asc(u64));
    std.mem.sort(u64, rtt.items, {}, std.sort.asc(u64));
    const process = Process.sample();
    const seconds: f64 = @floatFromInt(options.seconds);
    std.debug.print("fairness min_messages={d} max_messages={d} jain={d:.6}\n", .{ minimum_messages, maximum_messages, if (squared_messages == 0) @as(f64, 0) else @as(f64, @floatFromInt(messages)) * @as(f64, @floatFromInt(messages)) / (@as(f64, @floatFromInt(options.connections)) * squared_messages) });
    std.debug.print("client impl=zig connections={d} payload={d} setup_p50_us={d} setup_p95_us={d} setup_p99_us={d} rtt_p50_us={d} rtt_p95_us={d} rtt_p99_us={d} msgs_per_s={d:.0} mib_per_s={d:.2} cpu_ms={d} rss_kb={d} peak_rss_kb={d} kb_per_conn={d} retransmits={d} mismatches={d} incomplete={d} failures={d}\n", .{
        options.connections,
        options.payload,
        percentile(setup.items, 0.50),
        percentile(setup.items, 0.95),
        percentile(setup.items, 0.99),
        percentile(rtt.items, 0.50),
        percentile(rtt.items, 0.95),
        percentile(rtt.items, 0.99),
        @as(f64, @floatFromInt(messages)) / seconds,
        @as(f64, @floatFromInt(bytes)) / seconds / (1024 * 1024),
        process.cpu_ms - baseline.cpu_ms,
        process.rss_kb,
        process.peak_rss_kb,
        (connected_rss -| baseline.rss_kb) / @max(options.connections, 1),
        retransmits,
        mismatches,
        incomplete,
        failures,
    });
}

fn messageSize(size: usize, sequence: u8) usize {
    return if (size != 0) size else ([_]usize{ 32, 128, 512, 1200, 8192 })[sequence % 5];
}
