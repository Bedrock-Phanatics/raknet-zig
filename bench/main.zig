const std = @import("std");
const raknet = @import("raknet");
const ack = raknet.protocol.ack;
const frame = raknet.protocol.frame;
const cursor = raknet.protocol.cursor;
const receive_window = raknet.reliability.receive_window;

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
    std.debug.print("ack_decode: {d:.2} ns/op\nframe_decode: {d:.2} ns/op\nwindow_add: {d:.2} ns/op\nchecksum: {d}\n", .{ @as(f64, @floatFromInt(ack_ns)) / @as(f64, @floatFromInt(iterations)), @as(f64, @floatFromInt(frame_ns)) / @as(f64, @floatFromInt(iterations)), @as(f64, @floatFromInt(window_ns)) / @as(f64, @floatFromInt(iterations)), checksum });
}
