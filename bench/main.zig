const std = @import("std");
const raknet = @import("raknet");
const ack = raknet.protocol.ack;
const frame = raknet.protocol.frame;
const cursor = raknet.protocol.cursor;
const datagram = raknet.protocol.datagram;
const receive_window = raknet.reliability.receive_window;
const deadline_queue = raknet.session.deadline_queue;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const records = [_]ack.Record{ .{ .first = 1, .last = 64 }, .{ .first = 100, .last = 300 } };
    var ack_wire: [64]u8 = undefined;
    const encoded_ack = try ack.encode(&records, &ack_wire);
    var ack_storage: [16]ack.Record = undefined;
    const iterations: usize = 2_000_000;
    var start = std.Io.Clock.awake.now(io);
    var checksum: usize = 0;
    for (0..iterations) |_| {
        const decoded = try ack.decode(encoded_ack, &ack_storage, 16, 4096);
        checksum +%= decoded.acknowledged_count;
        std.mem.doNotOptimizeAway(decoded.records.ptr);
    }
    const ack_ns: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    start = std.Io.Clock.awake.now(io);

    var frame_wire: [64]u8 = undefined;
    var writer: cursor.Writer = .{ .data = &frame_wire };
    try frame.encode(.{ .reliability = .reliable_ordered, .reliable_index = 1, .order_index = 1, .order_channel = 0, .payload = "bedrock" }, &writer);
    const encoded_frame = writer.written();
    for (0..iterations) |_| {
        var reader: cursor.Reader = .{ .data = encoded_frame };
        const decoded = try frame.decodeOne(&reader, 1492, 128);
        checksum +%= decoded.payload.len;
        std.mem.doNotOptimizeAway(decoded.payload.ptr);
    }
    const frame_ns: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    start = std.Io.Clock.awake.now(io);

    var slots: [4096]bool = undefined;
    var window = try receive_window.Window.init(&slots, 0);
    for (0..iterations) |i| {
        const index: u32 = @intCast(i & 0xffffff);
        _ = window.add(index, 256);
        std.mem.doNotOptimizeAway(window.expected);
    }
    const window_ns: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);

    const scheduler_capacity = 4096;
    var scheduler = try deadline_queue.Queue.init(std.heap.page_allocator, scheduler_capacity);
    defer scheduler.deinit();
    var scheduler_keys: [scheduler_capacity]deadline_queue.Key = undefined;
    var linear_deadlines: [scheduler_capacity]u64 = undefined;
    for (0..scheduler_capacity) |index| {
        var key: deadline_queue.Key = @splat(0);
        key[0] = @truncate(index);
        key[1] = @truncate(index >> 8);
        scheduler_keys[index] = key;
        linear_deadlines[index] = index + 1;
        try scheduler.upsert(key, index + 1);
    }

    const scheduler_iterations: usize = 500_000;
    start = std.Io.Clock.awake.now(io);
    for (0..scheduler_iterations) |iteration| {
        const index = iteration & (scheduler_capacity - 1);
        try scheduler.upsert(scheduler_keys[index], scheduler_capacity + iteration + 1);
        checksum +%= scheduler.peek().?.deadline_ms;
    }
    const scheduler_update_ns: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);

    const timer_turns: usize = 25_000;
    start = std.Io.Clock.awake.now(io);
    for (0..timer_turns) |_| {
        checksum +%= scheduler.peek().?.deadline_ms;
        std.mem.doNotOptimizeAway(scheduler.peek());
    }
    const scheduler_peek_ns: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);

    start = std.Io.Clock.awake.now(io);
    for (0..timer_turns) |turn| {
        linear_deadlines[turn & (scheduler_capacity - 1)] +%= scheduler_capacity;
        var earliest = linear_deadlines[0];
        for (linear_deadlines[1..]) |deadline| earliest = @min(earliest, deadline);
        checksum +%= earliest;
        std.mem.doNotOptimizeAway(earliest);
    }
    const linear_scan_ns: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);

    const due_turns: usize = 100_000;
    const due_one_256 = try benchmarkDueBatch(io, 256, 1, due_turns);
    const due_one_4096 = try benchmarkDueBatch(io, 4096, 1, due_turns);
    const due_many_turns: usize = 10_000;
    const due_64_4096 = try benchmarkDueBatch(io, 4096, 64, due_many_turns);
    checksum +%= due_one_256.checksum +% due_one_4096.checksum +% due_64_4096.checksum;

    const wire_count = 64;
    const frames_per_datagram = 8;
    var datagram_storage: [wire_count][512]u8 = undefined;
    var datagram_lengths: [wire_count]usize = undefined;
    for (0..wire_count) |wire_index| {
        var payloads: [frames_per_datagram][24]u8 = undefined;
        var frames: [frames_per_datagram]frame.Frame = undefined;
        for (0..frames_per_datagram) |frame_index| {
            for (&payloads[frame_index], 0..) |*byte, byte_index| {
                byte.* = @truncate(wire_index *% 17 +% frame_index *% 31 +% byte_index);
            }
            const index: u32 = @intCast(wire_index * frames_per_datagram + frame_index);
            frames[frame_index] = .{
                .reliability = .reliable_ordered,
                .reliable_index = index,
                .order_index = index,
                .order_channel = @intCast(frame_index & 3),
                .payload = &payloads[frame_index],
            };
        }
        datagram_lengths[wire_index] = (try datagram.encodeData(@intCast(wire_index), &frames, &datagram_storage[wire_index])).len;
    }
    std.mem.doNotOptimizeAway(&datagram_storage);

    const parser_iterations: usize = 500_000;
    var descriptors: [frames_per_datagram]frame.Frame = undefined;
    start = std.Io.Clock.awake.now(io);
    for (0..parser_iterations) |iteration| {
        const wire_index = iteration & (wire_count - 1);
        checksum +%= try parseOnePass(datagram_storage[wire_index][0..datagram_lengths[wire_index]]);
    }
    const one_pass_ns: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);

    start = std.Io.Clock.awake.now(io);
    for (0..parser_iterations) |iteration| {
        const wire_index = iteration & (wire_count - 1);
        checksum +%= try parseTwoPass(datagram_storage[wire_index][0..datagram_lengths[wire_index]]);
    }
    const two_pass_ns: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);

    start = std.Io.Clock.awake.now(io);
    for (0..parser_iterations) |iteration| {
        const wire_index = iteration & (wire_count - 1);
        checksum +%= try parseDescriptors(datagram_storage[wire_index][0..datagram_lengths[wire_index]], &descriptors);
    }
    const descriptor_ns: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);

    std.debug.print(
        "ack_decode: {d:.2} ns/op\nframe_decode: {d:.2} ns/op\nwindow_add: {d:.2} ns/op\n" ++
            "deadline_reschedule_4096: {d:.2} ns/op\n" ++
            "deadline_next_4096: {d:.2} ns/turn\n" ++
            "deadline_linear_scan_4096: {d:.2} ns/turn\n" ++
            "timer_due_1_of_256: {d:.2} ns/turn\n" ++
            "timer_due_1_of_4096: {d:.2} ns/turn\n" ++
            "timer_due_64_of_4096: {d:.2} ns/turn\n" ++
            "datagram_parse_1pass_8_frames: {d:.2} ns/op\n" ++
            "datagram_parse_2pass_8_frames: {d:.2} ns/op\n" ++
            "datagram_parse_descriptors_8_frames: {d:.2} ns/op\n" ++
            "descriptor_scratch_256_frames: {d} bytes\nchecksum: {d}\n",
        .{
            @as(f64, @floatFromInt(ack_ns)) / @as(f64, @floatFromInt(iterations)),
            @as(f64, @floatFromInt(frame_ns)) / @as(f64, @floatFromInt(iterations)),
            @as(f64, @floatFromInt(window_ns)) / @as(f64, @floatFromInt(iterations)),
            @as(f64, @floatFromInt(scheduler_update_ns)) / @as(f64, @floatFromInt(scheduler_iterations)),
            @as(f64, @floatFromInt(scheduler_peek_ns)) / @as(f64, @floatFromInt(timer_turns)),
            @as(f64, @floatFromInt(linear_scan_ns)) / @as(f64, @floatFromInt(timer_turns)),
            @as(f64, @floatFromInt(due_one_256.nanoseconds)) / @as(f64, @floatFromInt(due_turns)),
            @as(f64, @floatFromInt(due_one_4096.nanoseconds)) / @as(f64, @floatFromInt(due_turns)),
            @as(f64, @floatFromInt(due_64_4096.nanoseconds)) / @as(f64, @floatFromInt(due_many_turns)),
            @as(f64, @floatFromInt(one_pass_ns)) / @as(f64, @floatFromInt(parser_iterations)),
            @as(f64, @floatFromInt(two_pass_ns)) / @as(f64, @floatFromInt(parser_iterations)),
            @as(f64, @floatFromInt(descriptor_ns)) / @as(f64, @floatFromInt(parser_iterations)),
            @sizeOf(frame.Frame) * 256,
            checksum,
        },
    );
}

const DueMeasurement = struct { nanoseconds: u64, checksum: usize };

fn benchmarkDueBatch(io: std.Io, capacity: usize, due_per_turn: usize, turns: usize) !DueMeasurement {
    std.debug.assert(due_per_turn > 0 and due_per_turn <= capacity);
    var queue = try deadline_queue.Queue.init(std.heap.page_allocator, capacity);
    defer queue.deinit();
    const popped = try std.heap.page_allocator.alloc(deadline_queue.Entry, due_per_turn);
    defer std.heap.page_allocator.free(popped);

    for (0..capacity) |index| {
        var key: deadline_queue.Key = @splat(0);
        key[0] = @truncate(index);
        key[1] = @truncate(index >> 8);
        try queue.upsert(key, if (index < due_per_turn) 0 else std.math.maxInt(u64));
    }

    var checksum: usize = 0;
    const start = std.Io.Clock.awake.now(io);
    for (0..turns) |_| {
        for (popped) |*entry| entry.* = queue.popDue(0).?;
        for (popped) |entry| {
            checksum +%= entry.key[0];
            try queue.upsert(entry.key, 0);
        }
        std.mem.doNotOptimizeAway(queue.peek());
    }
    const nanoseconds: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    return .{ .nanoseconds = nanoseconds, .checksum = checksum };
}

fn parseOnePass(wire: []const u8) !usize {
    var decoded = try frame.decodeDatagram(wire);
    var checksum: usize = decoded.sequence;
    while (decoded.frames.remaining() != 0) {
        const value = try frame.decodeOne(&decoded.frames, 8192, 2048);
        checksum +%= consumeFrame(value);
    }
    return checksum;
}

fn parseTwoPass(wire: []const u8) !usize {
    var validation = try frame.decodeDatagram(wire);
    while (validation.frames.remaining() != 0) {
        const value = try frame.decodeOne(&validation.frames, 8192, 2048);
        if (value.order_channel) |channel| {
            if (channel >= 32) return error.InvalidOrderChannel;
        }
    }
    return parseOnePass(wire);
}

fn parseDescriptors(wire: []const u8, scratch: []frame.Frame) !usize {
    var decoded = try frame.decodeDatagram(wire);
    var count: usize = 0;
    while (decoded.frames.remaining() != 0) {
        if (count == scratch.len) return error.PacketWorkLimitExceeded;
        const value = try frame.decodeOne(&decoded.frames, 8192, 2048);
        if (value.order_channel) |channel| {
            if (channel >= 32) return error.InvalidOrderChannel;
        }
        scratch[count] = value;
        count += 1;
    }

    var checksum: usize = decoded.sequence;
    for (scratch[0..count]) |value| checksum +%= consumeFrame(value);
    return checksum;
}

fn consumeFrame(value: frame.Frame) usize {
    var result = value.payload.len + value.payload[0];
    if (value.reliable_index) |index| result +%= index;
    if (value.order_index) |index| result +%= index;
    if (value.order_channel) |channel| result +%= channel;
    std.mem.doNotOptimizeAway(value.payload.ptr);
    return result;
}
