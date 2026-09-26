const std = @import("std");
const builtin = @import("builtin");
const raknet = @import("raknet");

const usage =
    \\usage:
    \\  raknet-interop server <ip:port> <seconds> [listeners]
    \\  raknet-interop client <ip:port> <connections> <payload> <seconds> <warmup_ms>
    \\
;

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    var threaded: std.Io.Threaded = .init(std.heap.smp_allocator, .{ .async_limit = .unlimited });
    defer threaded.deinit();
    const io = threaded.io();
    if ((args.len == 4 or args.len == 5) and std.mem.eql(u8, args[1], "server")) {
        const listeners = if (args.len == 5) try std.fmt.parseInt(usize, args[4], 10) else 1;
        return server(io, try std.Io.net.IpAddress.parseLiteral(args[2]), try std.fmt.parseInt(u32, args[3], 10), listeners);
    }
    if (args.len == 7 and std.mem.eql(u8, args[1], "client")) {
        return client(io, .{
            .address = try std.Io.net.IpAddress.parseLiteral(args[2]),
            .connections = try std.fmt.parseInt(usize, args[3], 10),
            .payload = try std.fmt.parseInt(usize, args[4], 10),
            .seconds = try std.fmt.parseInt(u32, args[5], 10),
            .warmup_ms = try std.fmt.parseInt(u32, args[6], 10),
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

    fn onConnect(raw: *anyopaque, _: *raknet.Session) error{ApplicationFailure}!void {
        const self: *Server = @ptrCast(@alignCast(raw));
        self.connected += 1;
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

const Shard = struct {
    listener: *raknet.Server,
    state: Server = .{},
    peak_sessions: usize = 0,

    fn run(self: *Shard, io: std.Io, deadline: u64, report: bool, baseline: Process) void {
        var next_progress = nowNs(io) + progress_interval_ns;
        while (nowNs(io) < deadline) {
            _ = self.listener.poll(wait(50), .{ .context = &self.state, .connected = Server.onConnect, .message = Server.onMessage }) catch |err| std.debug.print("poll error: {s}\n", .{@errorName(err)});
            self.peak_sessions = @max(self.peak_sessions, self.listener.sessions.count());
            if (report and nowNs(io) >= next_progress) {
                next_progress += progress_interval_ns;
                const progress = Process.sample();
                const live = self.listener.statistics();
                std.debug.print("progress impl=zig role=server sessions={d} echoed={d} recovery_bytes={d} session_memory_bytes={d} cpu_ms={d} rss_kb={d}\n", .{ live.active_sessions, self.state.echoed, live.recovery_bytes, live.session_memory_bytes, progress.cpu_ms - baseline.cpu_ms, progress.rss_kb });
            }
        }
    }
};

fn server(io: std.Io, address: std.Io.net.IpAddress, seconds: u32, listeners: usize) !void {
    const allocator = std.heap.smp_allocator;
    const baseline = Process.sample();
    const shards = try allocator.alloc(Shard, @max(listeners, 1));
    defer allocator.free(shards);
    var opened: usize = 0;
    defer for (shards[0..opened]) |shard| shard.listener.destroy();
    for (shards) |*shard| {
        shard.* = .{ .listener = try raknet.Server.listen(allocator, io, address, .{
            .advertisement = "MCPE;raknet-zig interop;11;1.21;0;1000;0;interop;Survival;1;19132;19133;",
            .server_guid = 0x7a69_6e74_6572_6f70,
            .reuse_port = shards.len > 1,
        }) };
        opened += 1;
    }
    const deadline = nowNs(io) + @as(u64, seconds) * std.time.ns_per_s;
    const futures = try allocator.alloc(std.Io.Future(void), shards.len - 1);
    defer allocator.free(futures);
    for (futures, shards[1..]) |*future, *shard| future.* = try io.concurrent(Shard.run, .{ shard, io, deadline, false, baseline });
    shards[0].run(io, deadline, seconds >= 60, baseline);
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
};

const maximum_samples = 2048;
const progress_interval_ns = 10 * std.time.ns_per_s;

const Shared = struct {
    io: std.Io,
    options: ClientOptions,
    ready: std.atomic.Value(usize) = .init(0),
    finished: std.atomic.Value(usize) = .init(0),
    start_ns: std.atomic.Value(u64) = .init(0),
};

const Connection = struct {
    shared: *Shared,
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
        if (bytes.len != self.shared.options.payload or bytes[0] != 0xfe or bytes[bytes.len - 1] != bytes[9]) {
            self.mismatches += 1;
            return;
        }
        const sent = std.mem.readInt(u64, bytes[1..9], .little);
        const now = nowNs(self.shared.io);
        if (sent < self.measure_from or sent >= self.measure_until) return;
        self.messages += 1;
        self.bytes += bytes.len;
        const rtt_us = (now - sent) / 1000;
        if (self.sample_count < maximum_samples) {
            self.samples[self.sample_count] = rtt_us;
        } else {
            self.samples[self.messages % maximum_samples] = rtt_us;
        }
        self.sample_count = @min(self.sample_count + 1, maximum_samples);
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
        const window = std.math.clamp(262_144 / options.payload, 1, 32);
        const payload = try std.heap.smp_allocator.alloc(u8, options.payload);
        defer std.heap.smp_allocator.free(payload);
        var sequence: u32 = 0;
        const drain_until = self.measure_until + 3 * std.time.ns_per_s;
        while (true) {
            const now = nowNs(io);
            if (now >= self.measure_until and (self.outstanding == 0 or now >= drain_until)) break;
            while (now < self.measure_until and self.outstanding < window) {
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
    if (options.payload < 16) return error.PayloadTooSmall;
    const allocator = std.heap.smp_allocator;
    const baseline = Process.sample();
    var shared: Shared = .{ .io = io, .options = options };
    const connections = try allocator.alloc(Connection, options.connections);
    defer allocator.free(connections);
    const futures = try allocator.alloc(std.Io.Future(void), options.connections);
    defer allocator.free(futures);
    for (connections, futures) |*connection, *future| {
        connection.* = .{ .shared = &shared };
        future.* = try io.concurrent(Connection.run, .{connection});
    }
    while (shared.ready.load(.acquire) < options.connections) try io.sleep(.fromMilliseconds(5), .awake);
    const connected_rss = Process.sample().rss_kb;
    shared.start_ns.store(nowNs(io), .release);
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
    for (connections) |*connection| {
        if (connection.failed) failures += 1;
        if (connection.incomplete) incomplete += 1;
        if (connection.setup_us != 0) try setup.append(allocator, connection.setup_us);
        try rtt.appendSlice(allocator, connection.samples[0..connection.sample_count]);
        messages += connection.messages;
        bytes += connection.bytes;
        mismatches += connection.mismatches;
        retransmits += connection.retransmits;
    }
    std.mem.sort(u64, setup.items, {}, std.sort.asc(u64));
    std.mem.sort(u64, rtt.items, {}, std.sort.asc(u64));
    const process = Process.sample();
    const seconds: f64 = @floatFromInt(options.seconds);
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
