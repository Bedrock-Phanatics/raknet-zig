const std = @import("std");
const build_options = @import("build_options");
const root = @import("src/root.zig");

test {
    _ = root;
}

fn exercise(input: []u8) void {
    var ack_storage: [16]root.protocol.ack.Record = undefined;
    _ = root.protocol.ack.decode(input, &ack_storage, ack_storage.len, 256) catch {};
    var reader = @import("src/protocol/cursor.zig").Reader{ .data = input };
    var work: usize = 0;
    while (reader.remaining() > 0 and work < 32) : (work += 1) {
        const before = reader.offset;
        _ = root.protocol.frame.decodeOne(&reader, 2048, 32) catch break;
        if (reader.offset <= before) break;
    }
    _ = root.protocol.frame.decodeDatagram(input) catch {};
    _ = root.protocol.datagram.decode(input, &ack_storage, ack_storage.len, 256) catch {};
    _ = root.protocol.connected.decode(input) catch {};
    _ = root.protocol.offline.decodeUnconnectedPing(input) catch {};
    _ = root.protocol.offline.decodeOpenConnectionRequest1(input, 576, 1492) catch {};
    _ = root.protocol.offline.decodeOpenConnectionRequest2(input, false, 576, 1492) catch {};
    _ = root.protocol.offline.decodeOpenConnectionRequest2(input, true, 576, 1492) catch {};
}

fn fuzzOne(_: void, smith: *std.testing.Smith) !void {
    var bytes: [4096]u8 = undefined;
    const len = smith.sliceWithHash(&bytes, 0x4e4554);
    exercise(bytes[0..len]);
}

test "fuzz all attacker-facing codecs" {
    try std.testing.fuzz({}, fuzzOne, .{ .corpus = &.{ "", &.{0x80}, &.{ 0, 1, 0, 0, 0, 0 } } });
}

test "deterministic malformed campaign" {
    var prng = std.Random.DefaultPrng.init(0xbed0_16);
    var bytes: [4096]u8 = undefined;
    for (0..build_options.fuzz_iterations) |i| {
        const len = i % (bytes.len + 1);
        prng.random().bytes(bytes[0..len]);
        exercise(bytes[0..len]);
    }
}
