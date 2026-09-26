const std = @import("std");

pub const BorrowedPayload = @import("payload.zig").BorrowedPayload;
pub const CancelResult = @import("session/core.zig").CancelResult;
pub const Client = @import("transport/client.zig").Client;
pub const ClientOptions = @import("transport/client.zig").Options;
pub const Config = @import("config.zig").Config;
pub const FlushResult = @import("session/core.zig").FlushResult;
pub const ListenerStatistics = @import("transport/server.zig").ListenerStatistics;
pub const OwnedPayload = @import("payload.zig").OwnedPayload;
pub const SendHandle = @import("session/core.zig").SendHandle;
pub const Server = @import("transport/server.zig").Listener;
pub const ServerOptions = @import("transport/server.zig").Options;
pub const Session = @import("transport/server.zig").Session;
pub const SessionStatistics = @import("session/core.zig").Statistics;


pub const advanced = struct {
    pub const QuotaAllocator = @import("util/quota_allocator.zig").QuotaAllocator;
    pub const uint24 = @import("util/uint24.zig");

    pub const net = struct {
        pub const BufferOptions = @import("transport/socket.zig").BufferOptions;
        pub const BufferSizes = @import("transport/socket.zig").BufferSizes;
        pub const Traffic = @import("transport/socket.zig").Traffic;
        pub const Socket = @import("transport/socket.zig").Socket;
    };
    pub const protocol = struct {
        pub const ack = @import("protocol/ack.zig");
        pub const connected = @import("protocol/connected.zig");
        pub const cursor = @import("protocol/cursor.zig");
        pub const datagram = @import("protocol/datagram.zig");
        pub const frame = @import("protocol/frame.zig");
        pub const offline = @import("protocol/offline.zig");
    };
    pub const reliability = struct {
        pub const congestion = @import("reliability/congestion.zig");
        pub const ordered_store = @import("reliability/ordered_store.zig");
        pub const reassembly = @import("reliability/reassembly.zig");
        pub const receive_window = @import("reliability/receive_window.zig");
        pub const recovery = @import("reliability/recovery.zig");
        pub const rtt = @import("reliability/rtt.zig");
    };
    pub const security = struct {
        pub const cookie = @import("security/cookie.zig");
        pub const rate_limit = @import("security/rate_limit.zig");
    };
    pub const session = struct {
        pub const Core = @import("session/core.zig").Core;
        pub const Receiver = @import("session/receiver.zig").Receiver;
        pub const Transmitter = @import("session/transmitter.zig").Transmitter;
        pub const client_handshake = @import("session/client_handshake.zig");
        pub const deadline_queue = @import("session/deadline_queue.zig");
        pub const offline_handshake = @import("session/offline_handshake.zig");
        pub const receipt_batch = @import("transport/receipt_batch.zig");
    };
};

test {
    std.testing.refAllDecls(@This());
    std.testing.refAllDecls(advanced);
    std.testing.refAllDecls(advanced.net);
    std.testing.refAllDecls(advanced.protocol);
    std.testing.refAllDecls(advanced.reliability);
    std.testing.refAllDecls(advanced.security);
    std.testing.refAllDecls(advanced.session);
}
