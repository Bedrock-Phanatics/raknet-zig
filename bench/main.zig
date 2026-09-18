const std = @import("std");
const raknet = @import("raknet");
const ack = raknet.protocol.ack;
const frame = raknet.protocol.frame;
const cursor = raknet.protocol.cursor;
const datagram = raknet.protocol.datagram;
const receive_window = raknet.reliability.receive_window;
const recovery = raknet.reliability.recovery;
const ordered_store = raknet.reliability.ordered_store;
const deadline_queue = raknet.session.deadline_queue;
const receipt_batch = raknet.session.receipt_batch;

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

    const send_iterations: usize = 100_000;
    const single_send = try benchmarkFramePacking(io, send_iterations, false);
    const packed_send = try benchmarkFramePacking(io, send_iterations, true);
    checksum +%= single_send.checksum +% packed_send.checksum;
    const send_messages = send_iterations * bedrock_payload_sizes.len;
    const recovery_iterations: usize = 5_000;
    const recovery_classes = try benchmarkRecoveryStorage(io, recovery_iterations, .size_classes);
    const recovery_slabs = try benchmarkRecoveryStorage(io, recovery_iterations, .mtu_slabs);
    checksum +%= recovery_classes.checksum +% recovery_slabs.checksum;
    const recovery_messages = recovery_iterations * 256;
    const ordered_iterations: usize = 2_000;
    const ordered_ring = try benchmarkOrderedRing(io, ordered_iterations);
    const ordered_hash = try benchmarkOrderedHash(io, ordered_iterations);
    const retention_exact = try benchmarkRetention(io, ordered_iterations, false);
    const retention_classes = try benchmarkRetention(io, ordered_iterations, true);
    checksum +%= ordered_ring.checksum +% ordered_hash.checksum +% retention_exact.checksum +% retention_classes.checksum;
    const ordered_messages = ordered_iterations * 256;
    const ack_batch_messages: usize = 1_000_000;
    const ack_singleton = benchmarkAckBatching(io, ack_batch_messages, 1);
    const ack_input_batch = benchmarkAckBatching(io, ack_batch_messages, 32);
    const ack_delay_0 = benchmarkAckBatching(io, ack_batch_messages, 1);
    const ack_delay_1 = benchmarkAckBatching(io, ack_batch_messages, 2);
    const ack_delay_2 = benchmarkAckBatching(io, ack_batch_messages, 3);
    const ack_delay_5 = benchmarkAckBatching(io, ack_batch_messages, 6);
    const ack_delay_10 = benchmarkAckBatching(io, ack_batch_messages, 11);
    checksum +%= ack_singleton.checksum +% ack_input_batch.checksum +% ack_delay_0.checksum +% ack_delay_1.checksum +% ack_delay_2.checksum +% ack_delay_5.checksum +% ack_delay_10.checksum;

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
            "descriptor_scratch_256_frames: {d} bytes\n" ++
            "bedrock_send_single: {d:.2} ns/message, {d:.2} bytes/message, {d} datagrams\n" ++
            "bedrock_send_packed: {d:.2} ns/message, {d:.2} bytes/message, {d} datagrams\n" ++
            "recovery_size_classes: {d:.2} ns/message, {d} retained bytes\n" ++
            "recovery_mtu_slabs: {d:.2} ns/message, {d} retained bytes\n" ++
            "ordered_circular: {d:.2} ns/message, {d} metadata bytes\n" ++
            "ordered_hash: {d:.2} ns/message, {d} metadata bytes\n" ++
            "retention_exact: {d:.2} ns/message, {d} retained bytes\n" ++
            "retention_size_classes: {d:.2} ns/message, {d} retained bytes\n" ++
            "checksum: {d}\n",
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
            @as(f64, @floatFromInt(single_send.nanoseconds)) / @as(f64, @floatFromInt(send_messages)),
            @as(f64, @floatFromInt(single_send.wire_bytes)) / @as(f64, @floatFromInt(send_messages)),
            single_send.datagrams,
            @as(f64, @floatFromInt(packed_send.nanoseconds)) / @as(f64, @floatFromInt(send_messages)),
            @as(f64, @floatFromInt(packed_send.wire_bytes)) / @as(f64, @floatFromInt(send_messages)),
            packed_send.datagrams,
            @as(f64, @floatFromInt(recovery_classes.nanoseconds)) / @as(f64, @floatFromInt(recovery_messages)),
            recovery_classes.retained_bytes,
            @as(f64, @floatFromInt(recovery_slabs.nanoseconds)) / @as(f64, @floatFromInt(recovery_messages)),
            recovery_slabs.retained_bytes,
            @as(f64, @floatFromInt(ordered_ring.nanoseconds)) / @as(f64, @floatFromInt(ordered_messages)),
            ordered_ring.retained_bytes,
            @as(f64, @floatFromInt(ordered_hash.nanoseconds)) / @as(f64, @floatFromInt(ordered_messages)),
            ordered_hash.retained_bytes,
            @as(f64, @floatFromInt(retention_exact.nanoseconds)) / @as(f64, @floatFromInt(ordered_messages)),
            retention_exact.retained_bytes,
            @as(f64, @floatFromInt(retention_classes.nanoseconds)) / @as(f64, @floatFromInt(ordered_messages)),
            retention_classes.retained_bytes,
            checksum,
        },
    );
    std.debug.print(
        "ack_singleton: {d:.2} ns/message, {d} datagrams\n" ++
            "ack_input_batch_32: {d:.2} ns/message, {d} datagrams\n" ++
            "ack_timer_0ms: {d:.2} ns/message, {d} datagrams\n" ++
            "ack_timer_1ms: {d:.2} ns/message, {d} datagrams\n" ++
            "ack_timer_2ms: {d:.2} ns/message, {d} datagrams\n" ++
            "ack_timer_5ms: {d:.2} ns/message, {d} datagrams\n" ++
            "ack_timer_10ms: {d:.2} ns/message, {d} datagrams\n",
        .{
            @as(f64, @floatFromInt(ack_singleton.nanoseconds)) / @as(f64, @floatFromInt(ack_batch_messages)),
            ack_singleton.datagrams,
            @as(f64, @floatFromInt(ack_input_batch.nanoseconds)) / @as(f64, @floatFromInt(ack_batch_messages)),
            ack_input_batch.datagrams,
            @as(f64, @floatFromInt(ack_delay_0.nanoseconds)) / @as(f64, @floatFromInt(ack_batch_messages)),
            ack_delay_0.datagrams,
            @as(f64, @floatFromInt(ack_delay_1.nanoseconds)) / @as(f64, @floatFromInt(ack_batch_messages)),
            ack_delay_1.datagrams,
            @as(f64, @floatFromInt(ack_delay_2.nanoseconds)) / @as(f64, @floatFromInt(ack_batch_messages)),
            ack_delay_2.datagrams,
            @as(f64, @floatFromInt(ack_delay_5.nanoseconds)) / @as(f64, @floatFromInt(ack_batch_messages)),
            ack_delay_5.datagrams,
            @as(f64, @floatFromInt(ack_delay_10.nanoseconds)) / @as(f64, @floatFromInt(ack_batch_messages)),
            ack_delay_10.datagrams,
        },
    );
}

const DueMeasurement = struct { nanoseconds: u64, checksum: usize };
const PackingMeasurement = struct { nanoseconds: u64, wire_bytes: usize, datagrams: usize, checksum: usize };
const RecoveryMeasurement = struct { nanoseconds: u64, retained_bytes: usize, checksum: usize };
const StorageMeasurement = struct { nanoseconds: u64, retained_bytes: usize, checksum: usize };
const AckBatchMeasurement = struct { nanoseconds: u64, datagrams: usize, checksum: usize };
const bedrock_payload_sizes = [_]usize{ 5, 7, 9, 12, 16, 20, 24, 32, 40, 52, 68, 96, 140, 220, 360, 700 };

fn benchmarkFramePacking(io: std.Io, iterations: usize, pack: bool) !PackingMeasurement {
    const mtu = 1492;
    var payloads: [bedrock_payload_sizes.len][700]u8 = undefined;
    for (&payloads, 0..) |*payload, index| {
        for (payload, 0..) |*byte, byte_index| byte.* = @truncate(index *% 31 +% byte_index);
    }
    var frames: [bedrock_payload_sizes.len]frame.Frame = undefined;
    var wire_storage: [mtu]u8 = undefined;
    var sequence: u32 = 0;
    var reliable_index: u32 = 0;
    var order_index: u32 = 0;
    var wire_bytes: usize = 0;
    var datagrams: usize = 0;
    var checksum: usize = 0;

    const start = std.Io.Clock.awake.now(io);
    for (0..iterations) |iteration| {
        var message_index: usize = 0;
        while (message_index < bedrock_payload_sizes.len) {
            var frame_count: usize = 0;
            var encoded_bytes: usize = 4;
            while (message_index < bedrock_payload_sizes.len) {
                const payload_len = bedrock_payload_sizes[message_index];
                payloads[message_index][0] = @truncate(iteration +% message_index);
                const value: frame.Frame = .{
                    .reliability = .reliable_ordered,
                    .reliable_index = reliable_index,
                    .order_index = order_index,
                    .order_channel = 0,
                    .payload = payloads[message_index][0..payload_len],
                };
                const encoded_size = try frame.encodedSize(value);
                if (frame_count != 0 and (encoded_bytes + encoded_size > mtu or !pack)) break;
                frames[frame_count] = value;
                frame_count += 1;
                encoded_bytes += encoded_size;
                reliable_index = (reliable_index + 1) & 0xffffff;
                order_index = (order_index + 1) & 0xffffff;
                message_index += 1;
            }
            const wire = try datagram.encodeData(sequence, frames[0..frame_count], &wire_storage);
            sequence = (sequence + 1) & 0xffffff;
            datagrams += 1;
            wire_bytes += wire.len;
            checksum +%= wire.len + wire[wire.len - 1] + frame_count;
            std.mem.doNotOptimizeAway(wire.ptr);
        }
    }
    const nanoseconds: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    return .{ .nanoseconds = nanoseconds, .wire_bytes = wire_bytes, .datagrams = datagrams, .checksum = checksum };
}

fn benchmarkRecoveryStorage(io: std.Io, iterations: usize, policy: recovery.StoragePolicy) !RecoveryMeasurement {
    const capacity = 256;
    const mtu = 1492;
    var state = try recovery.Recovery.initWithPolicy(std.heap.page_allocator, capacity, capacity * mtu, 8, mtu, policy);
    defer state.deinit();
    var payloads: [bedrock_payload_sizes.len][700]u8 = undefined;
    for (&payloads, 0..) |*payload, index| {
        for (payload, 0..) |*byte, byte_index| byte.* = @truncate(index +% byte_index);
    }
    var checksum: usize = 0;
    var sequence: u32 = 0;
    const start = std.Io.Clock.awake.now(io);
    for (0..iterations) |iteration| {
        const first = sequence;
        for (0..capacity) |index| {
            const sample = index & (bedrock_payload_sizes.len - 1);
            const payload_len = bedrock_payload_sizes[sample];
            payloads[sample][0] = @truncate(iteration +% index);
            try state.track(sequence, payloads[sample][0..payload_len], payload_len, iteration, 100);
            sequence = (sequence + 1) & 0xffffff;
        }
        const records = [_]ack.Record{.{ .first = first, .last = sequence - 1 }};
        const acknowledged = try state.acknowledge(&records, iteration + 1, capacity);
        checksum +%= acknowledged.packets + acknowledged.bytes;
    }
    const nanoseconds: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    return .{ .nanoseconds = nanoseconds, .retained_bytes = state.retainedCapacity(), .checksum = checksum };
}

fn benchmarkOrderedRing(io: std.Io, iterations: usize) !StorageMeasurement {
    const capacity = 256;
    var store = try ordered_store.Store.init(std.heap.page_allocator, 1, capacity, capacity * 32, 512);
    defer store.deinit();
    var checksum: usize = 0;
    var expected: u32 = 0;
    const start = std.Io.Clock.awake.now(io);
    for (0..iterations) |_| {
        for (1..capacity + 1) |offset| std.debug.assert(try store.push(0, expected + @as(u32, @intCast(offset)), "bedrock"));
        try store.advanceBorrowed(0, expected);
        for (0..capacity) |_| {
            const payload = (try store.pop(0)).?;
            checksum +%= payload.bytes[0] + payload.bytes.len;
            payload.deinit();
        }
        expected = (expected + capacity + 1) & 0xffffff;
    }
    const nanoseconds: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    return .{ .nanoseconds = nanoseconds, .retained_bytes = store.metadataCapacity(), .checksum = checksum };
}

fn benchmarkOrderedHash(io: std.Io, iterations: usize) !StorageMeasurement {
    const capacity = 256;
    var packets: std.AutoHashMapUnmanaged(u32, []u8) = .empty;
    defer packets.deinit(std.heap.page_allocator);
    try packets.ensureTotalCapacity(std.heap.page_allocator, capacity);
    var checksum: usize = 0;
    var expected: u32 = 0;
    const start = std.Io.Clock.awake.now(io);
    for (0..iterations) |_| {
        for (1..capacity + 1) |offset| {
            const copy = try std.heap.page_allocator.dupe(u8, "bedrock");
            try packets.put(std.heap.page_allocator, (expected + @as(u32, @intCast(offset))) & 0xffffff, copy);
        }
        expected = (expected + 1) & 0xffffff;
        for (0..capacity) |_| {
            const removed = packets.fetchRemove(expected).?;
            checksum +%= removed.value[0] + removed.value.len;
            std.heap.page_allocator.free(removed.value);
            expected = (expected + 1) & 0xffffff;
        }
    }
    const nanoseconds: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    return .{ .nanoseconds = nanoseconds, .retained_bytes = packets.capacity() * (@sizeOf(u32) + @sizeOf([]u8)), .checksum = checksum };
}

fn benchmarkRetention(io: std.Io, iterations: usize, pooled: bool) !StorageMeasurement {
    const capacity = 256;
    var blocks: [capacity][]u8 = undefined;
    var retained_bytes: usize = 0;
    if (pooled) {
        for (&blocks, 0..) |*block, index| {
            const size = retentionClass(bedrock_payload_sizes[index & (bedrock_payload_sizes.len - 1)]);
            block.* = try std.heap.page_allocator.alloc(u8, size);
            retained_bytes += size;
        }
    }
    defer if (pooled) for (blocks) |block| std.heap.page_allocator.free(block);
    var checksum: usize = 0;
    const start = std.Io.Clock.awake.now(io);
    for (0..iterations) |iteration| {
        for (&blocks, 0..) |*block, index| {
            const size = bedrock_payload_sizes[index & (bedrock_payload_sizes.len - 1)];
            if (!pooled) block.* = try std.heap.page_allocator.alloc(u8, size);
            block.*[0] = @truncate(iteration +% index);
            checksum +%= block.*[0] + size;
        }
        if (!pooled) for (blocks) |block| std.heap.page_allocator.free(block);
    }
    const nanoseconds: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    return .{ .nanoseconds = nanoseconds, .retained_bytes = retained_bytes, .checksum = checksum };
}

fn retentionClass(size: usize) usize {
    const classes = [_]usize{ 64, 256, 576, 1200, 1492 };
    for (classes) |class| if (size <= class) return class;
    return size;
}

fn benchmarkAckBatching(io: std.Io, message_count: usize, group_size: usize) AckBatchMeasurement {
    var values: [32]u32 = undefined;
    var records: [32]ack.Record = undefined;
    var wire: [1492]u8 = undefined;
    var datagrams_count: usize = 0;
    var checksum: usize = 0;
    var sequence: u32 = 0;
    const start = std.Io.Clock.awake.now(io);
    var remaining = message_count;
    while (remaining != 0) {
        const count = @min(remaining, group_size);
        for (values[0..count]) |*value| {
            value.* = sequence;
            sequence += 1;
        }
        const canonical = receipt_batch.canonicalizeValues(values[0..count], &records);
        const encoded = datagram.encodeControl(.ack, canonical, &wire) catch unreachable;
        checksum +%= encoded.len + encoded[encoded.len - 1];
        datagrams_count += 1;
        remaining -= count;
        std.mem.doNotOptimizeAway(encoded.ptr);
    }
    const nanoseconds: u64 = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds);
    return .{ .nanoseconds = nanoseconds, .datagrams = datagrams_count, .checksum = checksum };
}

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
