const std = @import("std");
const raknet = @import("raknet");

const Core = raknet.advanced.session.Core;
const Frame = raknet.advanced.protocol.frame.Frame;

fn boundedConfig() raknet.Config {
    var config: raknet.Config = .{};
    config.protocol.maximum_frame_payload = 2048;
    config.protocol.maximum_acknowledged_datagrams = 128;
    config.protocol.receive_window = 64;
    config.protocol.reliable_window = 64;
    config.protocol.maximum_order_channels = 4;
    config.protocol.maximum_split_parts = 16;
    config.protocol.maximum_split_bytes = 4096;
    config.session.maximum_retransmissions = 64;
    config.session.maximum_recovery_bytes = 64 * 576;
    config.session.maximum_ordered_packets = 64;
    config.session.maximum_ordered_bytes = 16 * 1024;
    config.session.maximum_concurrent_splits = 4;
    config.session.maximum_split_bytes_per_connection = 8192;
    config.session.maximum_queued_outbound_packets = 32;
    config.session.maximum_queued_outbound_bytes = 16 * 1024;
    config.session.reserved_control_queue_packets = 4;
    config.session.reserved_control_queue_bytes = 1024;
    config.batching.maximum_ack_records = 16;
    config.batching.maximum_packets_per_iteration = 64;
    return config;
}

const Sink = struct {
    fn deliver(_: *anyopaque, _: raknet.BorrowedPayload) !void {}
    fn emit(_: *anyopaque, _: []const u8) error{TransportFailure}!void {}
};

fn exercise(input: []const u8) !void {
    var core = try Core.init(std.testing.allocator, 576, boundedConfig());
    defer core.deinit();
    var frames: [16]Frame = undefined;
    var scratch: [576]u8 = undefined;
    var unused: u8 = 0;

    var cursor: usize = 0;
    var now: u64 = 0;
    while (cursor < input.len) {
        const control = input[cursor];
        cursor += 1;
        const amount = @min(input.len - cursor, @as(usize, control & 0x3f));
        const bytes = input[cursor..][0..amount];
        cursor += amount;
        now +%= control;

        switch (control >> 6) {
            0 => _ = core.processIncomingCountedWithScratch(bytes, now, &frames, &unused, Sink.deliver) catch {},
            1 => {
                if (bytes.len != 0) {
                    const reliability: raknet.advanced.protocol.frame.Reliability = switch (control & 3) {
                        0 => .unreliable,
                        1 => .reliable,
                        2 => .reliable_ordered,
                        else => .unreliable_sequenced,
                    };
                    _ = core.enqueueOutbound(.application, bytes, reliability, control % 4) catch {};
                    _ = core.flushOutbound(.application, &scratch, 2, now, &unused, Sink.emit) catch {};
                }
            },
            2 => {
                var due: [8]raknet.advanced.reliability.recovery.Due = undefined;
                _ = core.collectRetransmissions(now, &due, 8);
            },
            else => _ = core.expireSplits(now, 8),
        }
    }
}

fn fuzzOne(_: void, smith: *std.testing.Smith) !void {
    var bytes: [2048]u8 = undefined;
    const len = smith.sliceWithHash(&bytes, 0x53544154);
    try exercise(bytes[0..len]);
}

test "fuzz session state-machine entry points" {
    try std.testing.fuzz({}, fuzzOne, .{ .corpus = &.{ "", &.{0x80}, &.{ 0xc0, 0xff, 0xff, 0xff } } });
}

test "deterministic session mutation campaign" {
    var prng = std.Random.DefaultPrng.init(0x5e5510f0);
    var bytes: [256]u8 = undefined;
    for (0..512) |iteration| {
        const length = iteration % (bytes.len + 1);
        prng.random().bytes(bytes[0..length]);
        try exercise(bytes[0..length]);
    }
}
