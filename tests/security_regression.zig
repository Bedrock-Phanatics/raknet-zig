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
    try std.testing.expectEqual(@as(usize, 1), (try ack.decode(&.{ 0, 2, 1, 4, 0, 0, 1, 4, 0, 0 }, &storage, storage.len, 64)).records.len);
    try std.testing.expectEqual(@as(usize, 5), (try ack.decode(&.{ 0, 2, 0, 1, 0, 0, 3, 0, 0, 0, 3, 0, 0, 5, 0, 0 }, &storage, storage.len, 64)).acknowledged_count);
    try std.testing.expectError(error.TooManyAcknowledgements, ack.decode(&.{ 0, 2, 0, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 40, 0, 0 }, &storage, storage.len, 64));
    try std.testing.expectError(error.TooManyAcknowledgements, ack.decode(&.{ 0, 1, 0, 0, 0, 0, 0xff, 0xff, 0xff }, &storage, storage.len, 64));

    var core = try Core.init(std.testing.allocator, 576, .{});
    defer core.deinit();
    const repeated = &.{ 0xa0, 0, 2, 1, 7, 0, 0, 1, 7, 0, 0 };
    var unused: u8 = 0;
    try std.testing.expectEqual(@as(usize, 0), (try core.processIncoming(repeated, 0, &unused, Discard.deliver)).nack_marked);
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

test "repeated session lifetimes do not leak or retain bursts" {
    for (0..2000) |_| {
        var core = try Core.init(std.testing.allocator, 576, .{});
        _ = try core.enqueueOutbound(.application, "x", .reliable, 0);
        core.deinit();
    }
}
