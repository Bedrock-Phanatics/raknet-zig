const std = @import("std");

pub const BorrowedPayload = @import("payload.zig").BorrowedPayload;
pub const CancelResult = @import("session/core.zig").CancelResult;
pub const Client = @import("client.zig").Client;
pub const ClientOptions = @import("client.zig").Options;
pub const Config = @import("config.zig").Config;
pub const FlushResult = @import("session/core.zig").FlushResult;
pub const ListenerStatistics = @import("server.zig").ListenerStatistics;
pub const OwnedPayload = @import("payload.zig").OwnedPayload;
pub const SendHandle = @import("session/core.zig").SendHandle;
pub const Server = @import("server.zig").Listener;
pub const ServerOptions = @import("server.zig").Options;
pub const Session = @import("server.zig").Session;
pub const SessionStatistics = @import("session/core.zig").Statistics;

pub const minecraft = struct {
    pub const batch = @import("minecraft/batch.zig");
};

pub const advanced = struct {
    pub const QuotaAllocator = @import("util/quota_allocator.zig").QuotaAllocator;
    pub const uint24 = @import("util/uint24.zig");

    pub const net = struct {
        pub const BufferOptions = @import("net/backend.zig").BufferOptions;
        pub const BufferSizes = @import("net/backend.zig").BufferSizes;
        pub const Socket = @import("net/backend.zig").Socket;
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
    pub const session = struct {
        pub const Core = @import("session/core.zig").Core;
        pub const Receiver = @import("session/receiver.zig").Receiver;
        pub const Transmitter = @import("session/transmitter.zig").Transmitter;
        pub const deadline_queue = @import("session/deadline_queue.zig");
        pub const offline_handshake = @import("session/offline_handshake.zig");
        pub const receipt_batch = @import("session/receipt_batch.zig");
    };
};

test {
    std.testing.refAllDecls(@This());
}
