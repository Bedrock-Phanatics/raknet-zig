const std = @import("std");

pub const ProtocolLimits = struct {
    minimum_mtu: u16 = 576,
    maximum_mtu: u16 = 1492,
    maximum_datagram_size: usize = 2048,
    maximum_frame_payload: usize = 8192,
    maximum_acknowledged_datagrams: usize = 4096,
    receive_window: usize = 4096,
    reliable_window: usize = 4096,
    maximum_order_channels: usize = 32,
    maximum_split_parts: usize = 2048,
    maximum_split_bytes: usize = 4 * 1024 * 1024,
};

pub const SessionLimits = struct {
    maximum_retransmissions: usize = 4096,
    maximum_recovery_bytes: usize = 16 * 1024 * 1024,
    maximum_ordered_packets: usize = 4096,
    maximum_ordered_bytes: usize = 16 * 1024 * 1024,
    maximum_concurrent_splits: usize = 16,
    maximum_split_bytes_per_connection: usize = 16 * 1024 * 1024,
    maximum_queued_outbound_packets: usize = 256,
    maximum_queued_outbound_bytes: usize = 16 * 1024 * 1024,
    reserved_control_queue_packets: usize = 16,
    reserved_control_queue_bytes: usize = 64 * 1024,
};

pub const ListenerLimits = struct {
    maximum_pending_handshakes: usize = 4096,
    maximum_connections: usize = 4096,
};

pub const TimingOptions = struct {
    maximum_ack_delay_ms: u32 = 0,
    split_timeout_ms: u32 = 15_000,
    idle_timeout_ms: u32 = 10_000,
    minimum_rto_ms: u32 = 50,
    maximum_rto_ms: u32 = 5_000,
};

pub const BatchingOptions = struct {
    maximum_ack_records: usize = 256,
    maximum_packets_per_iteration: usize = 256,
};

pub const Config = struct {
    pub const Protocol = ProtocolLimits;
    pub const Session = SessionLimits;
    pub const Listener = ListenerLimits;
    pub const Timing = TimingOptions;
    pub const Batching = BatchingOptions;

    protocol: ProtocolLimits = .{},
    session: SessionLimits = .{},
    listener: ListenerLimits = .{},
    timing: TimingOptions = .{},
    batching: BatchingOptions = .{},

    pub fn validate(self: Config) !void {
        const protocol = self.protocol;
        const session = self.session;
        const listener = self.listener;
        const timing = self.timing;
        const batching = self.batching;

        if (protocol.minimum_mtu < 400 or protocol.minimum_mtu > protocol.maximum_mtu) return error.InvalidConfiguration;
        if (protocol.maximum_datagram_size < protocol.maximum_mtu or protocol.maximum_datagram_size > 65_507) return error.InvalidConfiguration;
        if (protocol.maximum_frame_payload == 0 or protocol.maximum_frame_payload > protocol.maximum_split_bytes) return error.InvalidConfiguration;
        if (batching.maximum_ack_records < 2 or batching.maximum_ack_records > 65_535) return error.InvalidConfiguration;
        if (protocol.maximum_acknowledged_datagrams == 0 or protocol.maximum_acknowledged_datagrams > 0x800000) return error.InvalidConfiguration;
        if (timing.maximum_ack_delay_ms > 10) return error.InvalidConfiguration;
        if (protocol.receive_window == 0 or protocol.receive_window > 0x7fffff) return error.InvalidConfiguration;
        if (protocol.reliable_window == 0 or protocol.reliable_window > 0x7fffff) return error.InvalidConfiguration;
        if (session.maximum_retransmissions == 0 or
            session.maximum_recovery_bytes == 0 or
            protocol.maximum_order_channels == 0 or
            protocol.maximum_order_channels > 256) return error.InvalidConfiguration;
        if (session.maximum_ordered_packets == 0 or session.maximum_ordered_bytes == 0 or protocol.maximum_split_parts < 2) return error.InvalidConfiguration;
        if (protocol.maximum_split_bytes == 0 or session.maximum_split_bytes_per_connection < protocol.maximum_split_bytes) return error.InvalidConfiguration;
        if (session.maximum_concurrent_splits == 0 or timing.split_timeout_ms == 0) return error.InvalidConfiguration;
        if (listener.maximum_pending_handshakes == 0 or listener.maximum_connections == 0 or listener.maximum_connections > 65_536) return error.InvalidConfiguration;
        if (session.maximum_queued_outbound_packets == 0 or
            session.maximum_queued_outbound_packets >= std.math.maxInt(u32) or
            session.maximum_queued_outbound_bytes == 0) return error.InvalidConfiguration;
        if (session.reserved_control_queue_packets == 0 or session.reserved_control_queue_packets >= session.maximum_queued_outbound_packets) return error.InvalidConfiguration;
        if (session.reserved_control_queue_bytes == 0 or session.reserved_control_queue_bytes >= session.maximum_queued_outbound_bytes) return error.InvalidConfiguration;
        if (batching.maximum_packets_per_iteration == 0 or batching.maximum_packets_per_iteration > 4096) return error.InvalidConfiguration;
        if (timing.idle_timeout_ms == 0 or timing.minimum_rto_ms == 0 or timing.minimum_rto_ms > timing.maximum_rto_ms) return error.InvalidConfiguration;
    }
};

test "default configuration is internally consistent" {
    try Config.validate(.{});
    var bad: Config = .{};
    bad.protocol.receive_window = 0x800000;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.batching.maximum_ack_records = 1;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.timing.maximum_ack_delay_ms = 11;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.session.maximum_recovery_bytes = 0;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.session.maximum_queued_outbound_packets = 0;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.session.maximum_queued_outbound_bytes = 0;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.session.reserved_control_queue_packets = bad.session.maximum_queued_outbound_packets;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.session.reserved_control_queue_packets = 0;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.session.reserved_control_queue_bytes = bad.session.maximum_queued_outbound_bytes;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
    bad = .{};
    bad.session.reserved_control_queue_bytes = 0;
    try std.testing.expectError(error.InvalidConfiguration, bad.validate());
}
