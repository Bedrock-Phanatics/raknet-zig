const std = @import("std");

pub const ReceiveBatch = struct {
    messages: []std.Io.net.IncomingMessage,
    dropped_oversize: usize,
    trailing_error: ?anyerror,
};

pub const Socket = struct {
    io: std.Io,
    value: std.Io.net.Socket,
    maximum_datagram_size: usize,

    pub fn bind(io: std.Io, address: std.Io.net.IpAddress, maximum_datagram_size: usize) !Socket {
        if (maximum_datagram_size == 0 or maximum_datagram_size > 65_507) return error.InvalidConfiguration;
        return .{ .io = io, .value = try address.bind(io, .{ .mode = .dgram, .protocol = .udp }), .maximum_datagram_size = maximum_datagram_size };
    }
    pub fn close(self: *Socket) void {
        self.value.close(self.io);
        self.* = undefined;
    }
    pub fn send(self: *const Socket, destination: std.Io.net.IpAddress, data: []const u8) !void {
        if (data.len > self.maximum_datagram_size) return error.DatagramTooLarge;
        try self.value.send(self.io, &destination, data);
    }
    pub fn sendMany(self: *const Socket, messages: []std.Io.net.OutgoingMessage) !void {
        for (messages) |message| if (message.data_len > self.maximum_datagram_size) return error.DatagramTooLarge;
        try self.value.sendMany(self.io, messages, .{});
    }

    /// `data_storage` must reserve maximum_datagram_size bytes per message slot.
    /// On Windows Zig currently returns one message; on Linux this maps to non-waiting batch drains.
    pub fn receiveMany(self: *const Socket, messages: []std.Io.net.IncomingMessage, data_storage: []u8, timeout: std.Io.Timeout) !ReceiveBatch {
        if (messages.len == 0) return error.InvalidConfiguration;
        const required = try std.math.mul(usize, messages.len, self.maximum_datagram_size);
        if (data_storage.len < required) return error.NoSpaceLeft;
        @memset(messages, .init);
        const failure, const received = self.value.receiveManyTimeout(self.io, messages, data_storage[0..required], .{}, timeout);
        var actual_received = received;
        var actual_failure = failure;
        if (actual_received == 0) if (actual_failure) |err| switch (err) {
            error.ConcurrencyUnavailable => {
                if (timeout != .none) return err;
                messages[0] = try self.value.receive(self.io, data_storage[0..self.maximum_datagram_size]);
                actual_received = 1;
                actual_failure = null;
            },
            else => return err,
        };
        var valid: usize = 0;
        var dropped: usize = 0;
        for (messages[0..actual_received]) |message| {
            if (message.flags.trunc or message.data.len > self.maximum_datagram_size) {
                dropped += 1;
                continue;
            }
            messages[valid] = message;
            valid += 1;
        }
        return .{ .messages = messages[0..valid], .dropped_oversize = dropped, .trailing_error = actual_failure };
    }
};

test "batched backend receives available loopback datagrams without filling batch" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var server = try Socket.bind(io, address, 64);
    defer server.close();
    var client = try Socket.bind(io, address, 64);
    defer client.close();
    try client.send(server.value.address, "hello");
    var messages: [4]std.Io.net.IncomingMessage = undefined;
    var data: [4 * 64]u8 = undefined;
    const result = try server.receiveMany(&messages, &data, .none);
    try std.testing.expect(result.messages.len >= 1);
    try std.testing.expectEqualStrings("hello", result.messages[0].data);
}
