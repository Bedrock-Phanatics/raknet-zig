const std = @import("std");

/// All attacker-influenced state has a corresponding explicit bound here.
pub const Config = struct {
    minimum_mtu: u16 = 576,
    maximum_mtu: u16 = 1492,
    maximum_datagram_size: usize = 2048,
    maximum_frame_payload: usize = 8192,
    maximum_ack_records: usize = 256,
    maximum_acknowledged_datagrams: usize = 4096,
    /// Zero flushes ACKs at the end of each input batch.
    maximum_ack_delay_ms: u32 = 0,
    receive_window: usize = 4096,
    reliable_window: usize = 4096,
    maximum_retransmissions: usize = 4096,
    maximum_recovery_bytes: usize = 16 * 1024 * 1024,
    maximum_order_channels: usize = 32,
    maximum_ordered_packets: usize = 4096,
    maximum_ordered_bytes: usize = 16 * 1024 * 1024,
    maximum_split_parts: usize = 2048,
    maximum_split_bytes: usize = 4 * 1024 * 1024,
    maximum_concurrent_splits: usize = 16,
    maximum_split_bytes_per_connection: usize = 16 * 1024 * 1024,
    split_timeout_ms: u32 = 15_000,
    maximum_pending_handshakes: usize = 4096,
    maximum_connections: usize = 4096,
    maximum_queued_outbound_packets: usize = 256,
    maximum_queued_outbound_bytes: usize = 16 * 1024 * 1024,
    reserved_control_queue_packets: usize = 16,
    reserved_control_queue_bytes: usize = 64 * 1024,
    maximum_packets_per_iteration: usize = 256,
    idle_timeout_ms: u32 = 10_000,
    minimum_rto_ms: u32 = 50,
    maximum_rto_ms: u32 = 5_000,

    pub fn validate(self: Config) !void {
        if (self.minimum_mtu < 400 or self.minimum_mtu > self.maximum_mtu) return error.InvalidConfiguration;
        if (self.maximum_datagram_size < self.maximum_mtu or self.maximum_datagram_size > 65_507) return error.InvalidConfiguration;
        if (self.maximum_frame_payload == 0 or self.maximum_frame_payload > self.maximum_split_bytes) return error.InvalidConfiguration;
        if (self.maximum_ack_records < 2 or self.maximum_ack_records > 65_535) return error.InvalidConfiguration;
        if (self.maximum_acknowledged_datagrams == 0 or self.maximum_acknowledged_datagrams > 0x800000) return error.InvalidConfiguration;
        if (self.maximum_ack_delay_ms > 10) return error.InvalidConfiguration;
        if (self.receive_window == 0 or self.receive_window > 0x7fffff) return error.InvalidConfiguration;
        if (self.reliable_window == 0 or self.reliable_window > 0x7fffff) return error.InvalidConfiguration;
        if (self.maximum_retransmissions == 0 or self.maximum_recovery_bytes == 0 or self.maximum_order_channels == 0 or self.maximum_order_channels > 256) return error.InvalidConfiguration;
        if (self.maximum_ordered_packets == 0 or self.maximum_ordered_bytes == 0 or self.maximum_split_parts < 2) return error.InvalidConfiguration;
        if (self.maximum_split_bytes == 0 or self.maximum_split_bytes_per_connection < self.maximum_split_bytes) return error.InvalidConfiguration;
        if (self.maximum_concurrent_splits == 0 or self.split_timeout_ms == 0) return error.InvalidConfiguration;
        if (self.maximum_pending_handshakes == 0 or self.maximum_connections == 0 or self.maximum_connections > 65_536) return error.InvalidConfiguration;
        if (self.maximum_queued_outbound_packets == 0 or self.maximum_queued_outbound_packets >= std.math.maxInt(u32) or self.maximum_queued_outbound_bytes == 0) return error.InvalidConfiguration;
        if (self.reserved_control_queue_packets == 0 or self.reserved_control_queue_packets >= self.maximum_queued_outbound_packets) return error.InvalidConfiguration;
        if (self.reserved_control_queue_bytes == 0 or self.reserved_control_queue_bytes >= self.maximum_queued_outbound_bytes) return error.InvalidConfiguration;
        if (self.maximum_packets_per_iteration == 0 or self.maximum_packets_per_iteration > 4096) return error.InvalidConfiguration;
        if (self.idle_timeout_ms == 0 or self.minimum_rto_ms == 0 or self.minimum_rto_ms > self.maximum_rto_ms) return error.InvalidConfiguration;
    }
};

test "default configuration is internally consistent" {
    try Config.validate(.{});
    var bad: Config = .{};
    bad.receive_window = 0x800000;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.maximum_ack_records = 1;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.maximum_ack_delay_ms = 11;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.maximum_recovery_bytes = 0;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.maximum_queued_outbound_packets = 0;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.maximum_queued_outbound_bytes = 0;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.reserved_control_queue_packets = bad.maximum_queued_outbound_packets;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.reserved_control_queue_packets = 0;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.reserved_control_queue_bytes = bad.maximum_queued_outbound_bytes;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.reserved_control_queue_bytes = 0;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
}
