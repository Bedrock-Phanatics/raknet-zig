const std = @import("std");
const build_options = @import("build_options");
const raknet = @import("raknet");

fn exercise(input: []u8) void {
    var ack_storage: [16]raknet.advanced.protocol.ack.Record = undefined;
    _ = raknet.advanced.protocol.ack.decode(input, &ack_storage, ack_storage.len, 256) catch {};
    var reader: raknet.advanced.protocol.cursor.Reader = .{ .data = input };
    var work: usize = 0;
    while (reader.remaining() > 0 and work < 32) : (work += 1) {
        const before = reader.offset;
        _ = raknet.advanced.protocol.frame.decodeOne(&reader, 2048, 32) catch break;
        if (reader.offset <= before) break;
    }
    _ = raknet.advanced.protocol.frame.decodeDatagram(input) catch {};
    _ = raknet.advanced.protocol.datagram.decode(input, &ack_storage, ack_storage.len, 256) catch {};
    _ = raknet.advanced.protocol.connected.decode(input) catch {};
    _ = raknet.advanced.protocol.offline.decodeUnconnectedPing(input) catch {};
    _ = raknet.advanced.protocol.offline.decodeOpenConnectionRequest1(input, 576, 1492) catch {};
    _ = raknet.advanced.protocol.offline.decodeOpenConnectionRequest2(input, false, 576, 1492) catch {};
    _ = raknet.advanced.protocol.offline.decodeOpenConnectionRequest2(input, true, 576, 1492) catch {};
    _ = raknet.advanced.protocol.offline.decodeOpenConnectionReply1(input) catch {};
    _ = raknet.advanced.protocol.offline.decodeOpenConnectionReply2(input, 0, 65535) catch {};
    exerciseHandshakes(input);
}

fn exerciseHandshakes(input: []const u8) void {
    const rate = raknet.advanced.security.rate_limit;
    var entries: [4]rate.Entry = undefined;
    var limiter = rate.Limiter.init(&entries, .{ .tokens_per_second = 1000, .burst = 1000, .global_tokens_per_second = 1000, .global_burst = 1000 }, 0) catch return;
    const jar: raknet.advanced.security.cookie.Jar = .{ .current_key = @splat(1), .previous_key = @splat(2) };
    var handler = raknet.advanced.session.offline_handshake.Handler.init(7, 11, 576, 1492, "MCPE;fuzz", jar, &limiter) catch return;
    var output: [1500]u8 = undefined;
    _ = handler.handle(input, "127.0.0.1:19132", 1, 0, 0, &output);

    const address: raknet.advanced.protocol.offline.Address = .{ .ipv4 = .{ .octets = .{ 127, 0, 0, 1 }, .port = 19132 } };
    var negotiator: raknet.advanced.session.client_handshake.Negotiator = .init(.{
        .protocol_version = 11,
        .mtus = &.{ 1492, 1200, 576 },
        .minimum_mtu = 576,
        .retry_ms = 500,
        .client_guid = 9,
        .server_address = address,
    });
    var chunks = std.mem.window(u8, input, 64, 64);
    while (chunks.next()) |chunk| {
        _ = negotiator.receive(chunk) catch break;
        while (negotiator.poll(0, &output) catch null) |_| {}
    }
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
