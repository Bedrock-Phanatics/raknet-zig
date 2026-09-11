//! Bounded RakNet protocol primitives for Minecraft: Bedrock Edition.

pub const Config = @import("config.zig").Config;
pub const BorrowedPayload = @import("payload.zig").BorrowedPayload;
pub const OwnedPayload = @import("payload.zig").OwnedPayload;
pub const Client = @import("client.zig").Client;
pub const ClientOptions = @import("client.zig").Options;
pub const Server = @import("server.zig").Listener;
pub const Session = @import("server.zig").Session;
pub const ServerOptions = @import("server.zig").Options;
pub const net = struct {
    pub const Socket = @import("net/backend.zig").Socket;
};
pub const protocol = struct {
    pub const cursor = @import("protocol/cursor.zig");
    pub const connected = @import("protocol/connected.zig");
    pub const datagram = @import("protocol/datagram.zig");
    pub const ack = @import("protocol/ack.zig");
    pub const frame = @import("protocol/frame.zig");
    pub const offline = @import("protocol/offline.zig");
};
pub const reliability = struct {
    pub const congestion = @import("reliability/congestion.zig");
    pub const receive_window = @import("reliability/receive_window.zig");
    pub const ordering = @import("reliability/ordering.zig");
    pub const ordered_store = @import("reliability/ordered_store.zig");
    pub const reassembly = @import("reliability/reassembly.zig");
    pub const recovery = @import("reliability/recovery.zig");
    pub const rtt = @import("reliability/rtt.zig");
};
pub const session = struct {
    pub const deadline_queue = @import("session/deadline_queue.zig");
    pub const offline_handshake = @import("session/offline_handshake.zig");
    pub const Receiver = @import("session/receiver.zig").Receiver;
    pub const Transmitter = @import("session/transmitter.zig").Transmitter;
    pub const Core = @import("session/core.zig").Core;
};
pub const uint24 = @import("util/uint24.zig");
pub const QuotaAllocator = @import("util/quota_allocator.zig").QuotaAllocator;

test {
    _ = @import("config.zig");
    _ = @import("payload.zig");
    _ = @import("server.zig");
    _ = @import("client.zig");
    _ = @import("net/backend.zig");
    _ = @import("util/uint24.zig");
    _ = @import("util/quota_allocator.zig");
    _ = @import("protocol/cursor.zig");
    _ = @import("protocol/connected.zig");
    _ = @import("protocol/datagram.zig");
    _ = @import("protocol/ack.zig");
    _ = @import("protocol/frame.zig");
    _ = @import("protocol/offline.zig");
    _ = @import("reliability/receive_window.zig");
    _ = @import("reliability/ordering.zig");
    _ = @import("reliability/ordered_store.zig");
    _ = @import("reliability/reassembly.zig");
    _ = @import("reliability/recovery.zig");
    _ = @import("reliability/rtt.zig");
    _ = @import("reliability/congestion.zig");
    _ = @import("security/cookie.zig");
    _ = @import("security/rate_limit.zig");
    _ = @import("session/offline_handshake.zig");
    _ = @import("session/deadline_queue.zig");
    _ = @import("session/receiver.zig");
    _ = @import("session/transmitter.zig");
    _ = @import("session/core.zig");
}
