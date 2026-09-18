const std = @import("std");
const backend = @import("../net/backend.zig");
const session_core = @import("core.zig");

pub const Emitter = struct {
    socket: *backend.Socket,
    address: std.Io.net.IpAddress,
    count: usize = 0,

    pub fn emit(raw: *anyopaque, wire: []const u8) session_core.SendError!void {
        const self: *Emitter = @ptrCast(@alignCast(raw));
        self.socket.send(self.address, wire) catch return error.TransportFailure;
        self.count += 1;
    }
};
