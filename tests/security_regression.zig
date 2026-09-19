const std = @import("std");
const raknet = @import("raknet");

const ack = raknet.advanced.protocol.ack;
const Core = raknet.advanced.session.Core;

const Discard = struct {
    fn deliver(_: *anyopaque, _: raknet.BorrowedPayload) !void {}
    fn emit(_: *anyopaque, _: []const u8) error{TransportFailure}!void {}
};

fn allocationScenario(allocator: std.mem.Allocator) !void {
    var config: raknet.Config = .{};
    config.session.maximum_retransmissions = 8;
    config.session.maximum_recovery_bytes = 8 * 576;
    config.session.maximum_queued_outbound_packets = 8;
    config.session.maximum_queued_outbound_bytes = 4096;
    config.session.reserved_control_queue_packets = 2;
    config.session.reserved_control_queue_bytes = 512;
    var core = try Core.init(allocator, 576, config);
    defer core.deinit();
    _ = try core.enqueueOutbound(.application, "allocation-boundary", .reliable_ordered, 0);
    var scratch: [576]u8 = undefined;
    var unused: u8 = 0;
    _ = try core.flushOutbound(.application, &scratch, 1, 0, &unused, Discard.emit);
}

test "session handles every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationScenario, .{});
}

test "hostile ACK and NACK ranges are rejected atomically" {
    var storage: [8]ack.Record = undefined;
    try std.testing.expectError(error.ReversedRange, ack.decode(&.{ 0, 1, 0, 5, 0, 0, 4, 0, 0 }, &storage, storage.len, 64));
    try std.testing.expectError(error.OverlappingRanges, ack.decode(&.{ 0, 2, 1, 4, 0, 0, 1, 4, 0, 0 }, &storage, storage.len, 64));
    try std.testing.expectError(error.OverlappingRanges, ack.decode(&.{ 0, 2, 0, 1, 0, 0, 3, 0, 0, 0, 3, 0, 0, 5, 0, 0 }, &storage, storage.len, 64));
    try std.testing.expectError(error.TooManyAcknowledgements, ack.decode(&.{ 0, 1, 0, 0, 0, 0, 0xff, 0xff, 0xff }, &storage, storage.len, 64));

    var core = try Core.init(std.testing.allocator, 576, .{});
    defer core.deinit();
    const repeated = &.{ 0xa0, 0, 2, 1, 7, 0, 0, 1, 7, 0, 0 };
    var unused: u8 = 0;
    try std.testing.expectError(error.OverlappingRanges, core.processIncoming(repeated, 0, &unused, Discard.deliver));
    try std.testing.expectEqual(@as(u64, 0), core.statistics().lost_datagrams);
}

test "sequence windows reject huge jumps and cross 24-bit wrap" {
    var storage: [8]bool = undefined;
    var window = try raknet.advanced.reliability.receive_window.Window.init(&storage, 0xfffffc);
    try std.testing.expect(window.add(0xfffffc, 8) == .accepted);
    try std.testing.expect(window.add(0xffffff, 8) == .accepted);
    try std.testing.expect(window.add(0, 8) == .accepted);
    try std.testing.expect(window.add(3, 8) == .accepted);
    try std.testing.expect(window.add(0x61, 8) == .too_far_ahead);
    try std.testing.expect(window.add(0x7ffffd, 8) == .ambiguous);
    try std.testing.expect(window.add(0, 8) == .duplicate);
}

test "borrowed batch storage is reusable and owned batches stay independent" {
    const Capture = struct {
        copy: [3]u8 = undefined,
        fn packet(raw: *anyopaque, packet_value: raknet.minecraft.batch.BorrowedPacket) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            @memcpy(&self.copy, packet_value.bytes);
        }
    };
    const first = [_]u8{ 0xfe, 1, 4, 0x0c, 3, 'a', 'b', 'c' };
    const second = [_]u8{ 0xfe, 1, 4, 0x0c, 3, 'x', 'y', 'z' };
    var decoder = try raknet.minecraft.batch.Decoder.init(std.testing.allocator, .{ .maximum_retained_capacity = 64 });
    defer decoder.deinit();
    var capture: Capture = .{};
    _ = try decoder.decodeBorrowed(&first, .declared, 0, &capture, Capture.packet);
    try std.testing.expectEqualStrings("abc", &capture.copy);
    _ = try decoder.decodeBorrowed(&second, .declared, 1000, &capture, Capture.packet);
    try std.testing.expectEqualStrings("xyz", &capture.copy);

    var owned = try decoder.decodeOwned(&first, .declared, 2000);
    defer owned.deinit();
    _ = try decoder.decodeBorrowed(&second, .declared, 3000, &capture, Capture.packet);
    try std.testing.expectEqualStrings("abc", owned.packet(0));
}

test "repeated session and batch lifetimes do not leak or retain bursts" {
    const wire = [_]u8{ 0xfe, 1, 4, 0x0c, 3, 'a', 'b', 'c' };
    var decoder = try raknet.minecraft.batch.Decoder.init(std.testing.allocator, .{ .maximum_retained_capacity = 2 });
    defer decoder.deinit();
    var unused: u8 = 0;
    const Packet = struct {
        fn discard(_: *anyopaque, _: raknet.minecraft.batch.BorrowedPacket) !void {}
    };
    for (0..2000) |iteration| {
        _ = try decoder.decodeBorrowed(&wire, .declared, iteration * 1000, &unused, Packet.discard);
        try std.testing.expectEqual(@as(usize, 0), decoder.retainedCapacity());
        var core = try Core.init(std.testing.allocator, 576, .{});
        _ = try core.enqueueOutbound(.application, "x", .reliable, 0);
        core.deinit();
    }
}
