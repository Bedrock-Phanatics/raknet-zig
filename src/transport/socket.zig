const std = @import("std");
const builtin = @import("builtin");
const time = @import("../util/time.zig");

pub const BufferOptions = struct {
    receive_bytes: ?u32 = null,
    send_bytes: ?u32 = null,

    pub fn validate(self: BufferOptions) !void {
        const maximum = 256 * 1024 * 1024;
        if (self.receive_bytes) |value| if (value == 0 or value > maximum) return error.InvalidConfiguration;
        if (self.send_bytes) |value| if (value == 0 or value > maximum) return error.InvalidConfiguration;
        if (builtin.os.tag != .linux and (self.receive_bytes != null or self.send_bytes != null)) return error.SocketBufferConfigurationUnsupported;
    }
};

pub const BufferSizes = struct {
    receive_bytes: ?u32,
    send_bytes: ?u32,
};

pub const Traffic = struct {
    datagrams_received: u64 = 0,
    datagrams_sent: u64 = 0,
    bytes_received: u64 = 0,
    bytes_sent: u64 = 0,
    send_drops: u64 = 0,
    // Backend calls, not syscalls.
    send_calls: u64 = 0,
    receive_calls: u64 = 0,
    receive_batches: u64 = 0,
    maximum_receive_batch: usize = 0,
    maximum_send_batch: usize = 0,
    ack_datagrams_sent: u64 = 0,
    nack_datagrams_sent: u64 = 0,
    native_receive_calls: u64 = 0,
    native_received_datagrams: u64 = 0,
    connection_resets: u64 = 0,
};

pub const SendBatch = struct {
    allocator: std.mem.Allocator,
    messages: []std.Io.net.OutgoingMessage,
    addresses: []std.Io.net.IpAddress,
    storage: []u8,
    stride: usize,
    len: usize = 0,

    pub fn init(allocator: std.mem.Allocator, capacity: usize, maximum_datagram_size: usize) !SendBatch {
        if (capacity == 0) return error.InvalidConfiguration;
        const messages = try allocator.alloc(std.Io.net.OutgoingMessage, capacity);
        errdefer allocator.free(messages);
        const addresses = try allocator.alloc(std.Io.net.IpAddress, capacity);
        errdefer allocator.free(addresses);
        const storage = try allocator.alloc(u8, try std.math.mul(usize, capacity, maximum_datagram_size));
        return .{ .allocator = allocator, .messages = messages, .addresses = addresses, .storage = storage, .stride = maximum_datagram_size };
    }

    pub fn deinit(self: *SendBatch) void {
        self.allocator.free(self.storage);
        self.allocator.free(self.addresses);
        self.allocator.free(self.messages);
        self.* = undefined;
    }
};

pub const ReceiveBatch = struct {
    messages: []std.Io.net.IncomingMessage,
    dropped_oversize: usize,
    trailing_error: ?anyerror,
};

pub const Socket = struct {
    io: std.Io,
    value: std.Io.net.Socket,
    maximum_datagram_size: usize,
    buffer_sizes: BufferSizes,
    traffic: Traffic = .{},
    batch: ?*SendBatch = null,

    pub fn bind(io: std.Io, address: std.Io.net.IpAddress, maximum_datagram_size: usize) !Socket {
        return bindWithBuffers(io, address, maximum_datagram_size, .{});
    }
    pub fn bindWithBuffers(io: std.Io, address: std.Io.net.IpAddress, maximum_datagram_size: usize, buffers: BufferOptions) !Socket {
        return bindWithOptions(io, address, maximum_datagram_size, buffers, false);
    }
    pub fn bindWithOptions(io: std.Io, address: std.Io.net.IpAddress, maximum_datagram_size: usize, buffers: BufferOptions, reuse_port: bool) !Socket {
        if (maximum_datagram_size == 0 or maximum_datagram_size > 65_507) return error.InvalidConfiguration;
        try buffers.validate();
        if (reuse_port and builtin.os.tag != .linux) return error.ReusePortUnsupported;
        var value = if (builtin.os.tag == .linux and reuse_port) try bindReusePort(address) else try address.bind(io, .{ .mode = .dgram, .protocol = .udp });
        errdefer value.close(io);
        try ignoreConnectionResets(value.handle);
        return .{ .io = io, .value = value, .maximum_datagram_size = maximum_datagram_size, .buffer_sizes = try configureBuffers(value.handle, buffers) };
    }
    pub fn close(self: *Socket) void {
        self.value.close(self.io);
        self.* = undefined;
    }
    pub fn beginBatch(self: *Socket, batch: *SendBatch) void {
        if (builtin.os.tag != .linux or batch.stride < self.maximum_datagram_size) return;
        batch.len = 0;
        self.batch = batch;
    }
    pub fn endBatch(self: *Socket) void {
        self.flushBatch();
        self.batch = null;
    }
    fn queue(self: *Socket, batch: *SendBatch, destination: std.Io.net.IpAddress, data: []const u8) void {
        if (batch.len == batch.messages.len) self.flushBatch();
        const slot = batch.len;
        const storage = batch.storage[slot * batch.stride ..][0..data.len];
        @memcpy(storage, data);
        batch.addresses[slot] = destination;
        batch.messages[slot] = .{ .address = &batch.addresses[slot], .data_ptr = storage.ptr, .data_len = data.len };
        batch.len += 1;
    }
    fn flushBatch(self: *Socket) void {
        const batch = self.batch orelse return;
        var offset: usize = 0;
        while (offset < batch.len) {
            const pending = batch.messages[offset..batch.len];
            self.traffic.send_calls += 1;
            const failure, const sent = self.value.sendManyTimeout(self.io, pending, .{}, .none);
            for (pending[0..sent]) |message| self.recordSent(message.data_ptr[0..message.data_len]);
            self.traffic.maximum_send_batch = @max(self.traffic.maximum_send_batch, sent);
            self.traffic.datagrams_sent += sent;
            offset += sent;
            if (failure == null) break;
            if (failure.? == error.Canceled) {
                self.traffic.send_drops += batch.len - offset;
                break;
            }
            self.traffic.send_drops += 1;
            offset += 1;
        }
        batch.len = 0;
    }
    pub fn send(self: *Socket, destination: std.Io.net.IpAddress, data: []const u8) !void {
        if (data.len > self.maximum_datagram_size) return error.DatagramTooLarge;
        if (self.batch) |batch| return self.queue(batch, destination, data);
        self.traffic.send_calls += 1;
        self.value.send(self.io, &destination, data) catch |err| switch (err) {
            error.SystemResources => {
                self.traffic.send_drops += 1;
                return;
            },
            else => return err,
        };
        self.traffic.datagrams_sent += 1;
        self.traffic.maximum_send_batch = @max(self.traffic.maximum_send_batch, 1);
        self.recordSent(data);
    }
    fn recordSent(self: *Socket, data: []const u8) void {
        self.traffic.bytes_sent += data.len;
        if (data.len == 0) return;
        if (data[0] == 0xc0) self.traffic.ack_datagrams_sent += 1;
        if (data[0] == 0xa0) self.traffic.nack_datagrams_sent += 1;
    }
    pub fn sendMany(self: *Socket, messages: []std.Io.net.OutgoingMessage) !void {
        for (messages) |message| {
            if (message.data_len > self.maximum_datagram_size) return error.DatagramTooLarge;
        }
        if (self.batch) |batch| {
            for (messages) |message| self.queue(batch, message.address.*, message.data_ptr[0..message.data_len]);
            return;
        }
        self.traffic.send_calls += 1;
        self.value.sendMany(self.io, messages, .{}) catch |err| switch (err) {
            error.SystemResources => {
                self.traffic.send_drops += messages.len;
                return;
            },
            else => return err,
        };
        self.traffic.datagrams_sent += messages.len;
        self.traffic.maximum_send_batch = @max(self.traffic.maximum_send_batch, messages.len);
        for (messages) |message| self.recordSent(message.data_ptr[0..message.data_len]);
    }
    pub fn kernelBufferSizes(self: *const Socket) BufferSizes {
        return self.buffer_sizes;
    }

    pub fn receive(self: *Socket, buffer: []u8, timeout: std.Io.Timeout) !std.Io.net.IncomingMessage {
        const limit = self.resetSafeTimeout(timeout);
        while (true) {
            self.traffic.receive_calls += 1;
            return self.receiveOnce(buffer, limit) catch |err| {
                if (!self.skipReset(err)) return err;
                continue;
            };
        }
    }

    fn receiveOnce(self: *Socket, buffer: []u8, timeout: std.Io.Timeout) !std.Io.net.IncomingMessage {
        return self.value.receiveTimeout(self.io, buffer, timeout) catch |err| switch (err) {
            error.ConcurrencyUnavailable => {
                if (timeout != .none) try self.waitReadable(timeout);
                return self.value.receive(self.io, buffer);
            },
            else => return err,
        };
    }

    pub fn waitReadable(self: *const Socket, timeout: std.Io.Timeout) !void {
        var message: [1]std.Io.net.IncomingMessage = .{.init};
        var byte: [1]u8 = undefined;
        const failure, _ = self.value.receiveManyTimeout(self.io, &message, &byte, .{ .peek = true }, timeout);
        const err = failure orelse return;
        switch (err) {
            // Windows fails a peek into a short buffer instead of truncating
            error.MessageOversize => {},
            error.ConcurrencyUnavailable => if (builtin.os.tag == .windows) return self.pollAfd(timeout) else return err,
            else => return err,
        }
    }

    // Threaded cannot wait on Windows datagram sockets.
    fn pollAfd(self: *const Socket, timeout: std.Io.Timeout) !void {
        const windows = std.os.windows;
        const PollHandle = extern struct { handle: windows.HANDLE, events: windows.ULONG, status: windows.NTSTATUS };
        const PollInfo = extern struct { timeout: windows.LARGE_INTEGER, count: windows.ULONG, exclusive: windows.ULONG, handles: [1]PollHandle };
        // RECEIVE | DISCONNECT | ABORT | LOCAL_CLOSE
        const events: windows.ULONG = 0x0001 | 0x0008 | 0x0010 | 0x0020;
        var info: PollInfo = .{
            .timeout = afdTimeout(self.io, timeout),
            .count = 1,
            .exclusive = 0,
            .handles = .{.{ .handle = self.value.handle, .events = events, .status = .SUCCESS }},
        };
        const result = try self.io.operate(.{ .device_io_control = .{
            .file = .{ .handle = self.value.handle, .flags = .{ .nonblocking = true } },
            .code = windows.IOCTL.AFD.POLL,
            .in = std.mem.asBytes(&info),
            .out = std.mem.asBytes(&info),
        } });
        const status = result.device_io_control.u.Status;
        if (status == .TIMEOUT or (status == .SUCCESS and info.count == 0)) return error.Timeout;
        if (status != .SUCCESS) return windows.unexpectedStatus(status);
    }

    // Windows reports ICMP unreachable from an earlier send on a later receive
    fn skipReset(self: *Socket, err: anyerror) bool {
        if (builtin.os.tag != .windows or (err != error.PortUnreachable and err != error.ConnectionResetByPeer)) return false;
        self.traffic.connection_resets += 1;
        return true;
    }

    fn resetSafeTimeout(self: *const Socket, timeout: std.Io.Timeout) std.Io.Timeout {
        return if (builtin.os.tag == .windows) time.earliest(self.io, timeout, .none) else timeout;
    }

    pub fn receiveMany(self: *Socket, messages: []std.Io.net.IncomingMessage, data_storage: []u8, timeout: std.Io.Timeout) !ReceiveBatch {
        const limit = self.resetSafeTimeout(timeout);
        while (true) {
            var batch = self.receiveManyOnce(messages, data_storage, limit) catch |err| {
                if (!self.skipReset(err)) return err;
                continue;
            };
            if (batch.trailing_error) |err| {
                if (self.skipReset(err)) batch.trailing_error = null;
            }
            return batch;
        }
    }

    fn receiveManyOnce(self: *Socket, messages: []std.Io.net.IncomingMessage, data_storage: []u8, timeout: std.Io.Timeout) !ReceiveBatch {
        if (messages.len == 0) return error.InvalidConfiguration;
        const required = try std.math.mul(usize, messages.len, self.maximum_datagram_size);
        if (data_storage.len < required) return error.NoSpaceLeft;
        // Custom providers may use virtual handles.
        if (builtin.os.tag == .linux and messages.len > 1 and self.io.vtable == std.Io.Threaded.global_single_threaded.io().vtable) {
            if (try self.receiveManyLinux(messages, data_storage)) |batch| return batch;
            // Threaded floors sub-ms waits to zero and spins, this wait is uncancelable but under 1 ms
            if (remainingNanoseconds(self.io, timeout)) |remaining| if (remaining < std.time.ns_per_ms) {
                if (remaining == 0 or !try self.pollLinux(remaining)) return error.Timeout;
                return try self.receiveManyLinux(messages, data_storage) orelse error.Timeout;
            };
        }
        @memset(messages, .init);
        self.traffic.receive_calls += 1;
        const failure, const received = self.value.receiveManyTimeout(self.io, messages, data_storage[0..required], .{}, timeout);
        var actual_received = received;
        var actual_failure = failure;
        if (actual_received == 0) if (actual_failure) |err| switch (err) {
            error.ConcurrencyUnavailable => {
                if (timeout != .none) try self.waitReadable(timeout);
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
            self.traffic.bytes_received += message.data.len;
        }
        self.traffic.datagrams_received += valid;
        self.traffic.receive_batches += @intFromBool(actual_received != 0);
        self.traffic.maximum_receive_batch = @max(self.traffic.maximum_receive_batch, actual_received);
        return .{ .messages = messages[0..valid], .dropped_oversize = dropped, .trailing_error = actual_failure };
    }

    fn pollLinux(self: *Socket, nanoseconds: u64) !bool {
        const linux = std.os.linux;
        var fds = [1]linux.pollfd{.{ .fd = self.value.handle, .events = linux.POLL.IN, .revents = 0 }};
        var wait: linux.timespec = .{ .sec = 0, .nsec = @intCast(nanoseconds) };
        while (true) {
            try self.io.checkCancel();
            const ready = linux.ppoll(&fds, 1, &wait, null);
            switch (linux.errno(ready)) {
                .SUCCESS => return ready != 0,
                .INTR => continue,
                .NOMEM => return error.SystemResources,
                else => |err| return std.posix.unexpectedErrno(err),
            }
        }
    }

    fn receiveManyLinux(self: *Socket, messages: []std.Io.net.IncomingMessage, data_storage: []u8) !?ReceiveBatch {
        const linux = std.os.linux;
        const Address = extern union { any: linux.sockaddr, ip4: linux.sockaddr.in, ip6: linux.sockaddr.in6 };
        var headers: [256]linux.mmsghdr = undefined;
        var addresses: [256]Address = undefined;
        var vectors: [256]std.posix.iovec = undefined;
        const count = @min(messages.len, headers.len);
        for (0..count) |index| {
            vectors[index] = .{ .base = data_storage[index * self.maximum_datagram_size ..].ptr, .len = self.maximum_datagram_size };
            headers[index] = .{ .hdr = .{
                .name = &addresses[index].any,
                .namelen = @sizeOf(Address),
                .iov = (&vectors[index])[0..1],
                .iovlen = 1,
                .control = null,
                .controllen = 0,
                .flags = 0,
            }, .len = 0 };
        }
        try self.io.checkCancel();
        self.traffic.native_receive_calls += 1;
        // Avoid recvmmsg's timeout bug; std.Io handles waiting and cancellation.
        const received = linux.recvmmsg(self.value.handle, &headers, @intCast(count), linux.MSG.DONTWAIT, null);
        switch (linux.errno(received)) {
            .SUCCESS => {},
            .AGAIN, .INTR, .NOSYS => return null,
            .NFILE => return error.SystemFdQuotaExceeded,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NOBUFS, .NOMEM => return error.SystemResources,
            .NOTCONN, .PIPE => return error.SocketUnconnected,
            .MSGSIZE => return error.MessageOversize,
            .CONNRESET => return error.ConnectionResetByPeer,
            .NETDOWN => return error.NetworkDown,
            else => |err| return std.posix.unexpectedErrno(err),
        }
        self.traffic.native_received_datagrams += received;
        self.traffic.receive_calls += 1;
        self.traffic.receive_batches += @intFromBool(received != 0);
        self.traffic.maximum_receive_batch = @max(self.traffic.maximum_receive_batch, received);
        var valid: usize = 0;
        var dropped: usize = 0;
        for (headers[0..received], 0..) |header, index| {
            if (header.hdr.flags & linux.MSG.TRUNC != 0 or header.len > self.maximum_datagram_size) {
                dropped += 1;
                continue;
            }
            const address: std.Io.net.IpAddress = switch (addresses[index].any.family) {
                linux.AF.INET => .{ .ip4 = .{ .bytes = @bitCast(addresses[index].ip4.addr), .port = std.mem.bigToNative(u16, addresses[index].ip4.port) } },
                linux.AF.INET6 => .{ .ip6 = .{ .bytes = addresses[index].ip6.addr, .port = std.mem.bigToNative(u16, addresses[index].ip6.port), .flow = addresses[index].ip6.flowinfo, .interface = .{ .index = addresses[index].ip6.scope_id } } },
                else => return error.Unexpected,
            };
            messages[valid] = .{
                .from = address,
                .data = data_storage[index * self.maximum_datagram_size ..][0..header.len],
                .control = &.{},
                .flags = .{
                    .eor = header.hdr.flags & linux.MSG.EOR != 0,
                    .trunc = false,
                    .ctrunc = header.hdr.flags & linux.MSG.CTRUNC != 0,
                    .oob = header.hdr.flags & linux.MSG.OOB != 0,
                    .errqueue = header.hdr.flags & linux.MSG.ERRQUEUE != 0,
                },
            };
            self.traffic.bytes_received += header.len;
            valid += 1;
        }
        self.traffic.datagrams_received += valid;
        return .{ .messages = messages[0..valid], .dropped_oversize = dropped, .trailing_error = null };
    }
};

fn bindReusePort(address: std.Io.net.IpAddress) !std.Io.net.Socket {
    const linux = std.os.linux;
    const family: u32 = switch (address) {
        .ip4 => linux.AF.INET,
        .ip6 => linux.AF.INET6,
    };
    const created = linux.socket(family, linux.SOCK.DGRAM | linux.SOCK.CLOEXEC, linux.IPPROTO.UDP);
    if (linux.errno(created) != .SUCCESS) return error.SocketCreateFailed;
    const fd: std.Io.net.Socket.Handle = @intCast(created);
    errdefer _ = linux.close(fd);
    var enabled: c_int = 1;
    try std.posix.setsockopt(fd, linux.SOL.SOCKET, linux.SO.REUSEPORT, std.mem.asBytes(&enabled));
    var bound = address;
    switch (address) {
        .ip4 => |value| {
            var raw: linux.sockaddr.in = .{ .port = std.mem.nativeToBig(u16, value.port), .addr = @bitCast(value.bytes) };
            if (linux.errno(linux.bind(fd, @ptrCast(&raw), @sizeOf(linux.sockaddr.in))) != .SUCCESS) return error.AddressUnavailable;
            var len: linux.socklen_t = @sizeOf(linux.sockaddr.in);
            if (linux.errno(linux.getsockname(fd, @ptrCast(&raw), &len)) != .SUCCESS) return error.AddressUnavailable;
            bound.ip4.port = std.mem.bigToNative(u16, raw.port);
        },
        .ip6 => |value| {
            var raw: linux.sockaddr.in6 = .{ .port = std.mem.nativeToBig(u16, value.port), .flowinfo = value.flow, .addr = value.bytes, .scope_id = value.interface.index };
            if (linux.errno(linux.bind(fd, @ptrCast(&raw), @sizeOf(linux.sockaddr.in6))) != .SUCCESS) return error.AddressUnavailable;
            var len: linux.socklen_t = @sizeOf(linux.sockaddr.in6);
            if (linux.errno(linux.getsockname(fd, @ptrCast(&raw), &len)) != .SUCCESS) return error.AddressUnavailable;
            bound.ip6.port = std.mem.bigToNative(u16, raw.port);
        },
    }
    return .{ .handle = fd, .address = bound };
}

// Negative means relative, in 100ns units
fn afdTimeout(io: std.Io, timeout: std.Io.Timeout) i64 {
    const remaining = remainingNanoseconds(io, timeout) orelse return std.math.maxInt(i64);
    return -@as(i64, @intCast(@min(std.math.divCeil(u64, remaining, 100) catch unreachable, std.math.maxInt(i64))));
}

fn remainingNanoseconds(io: std.Io, timeout: std.Io.Timeout) ?u64 {
    const remaining: i96 = switch (timeout) {
        .none => return null,
        .duration => |duration| duration.raw.nanoseconds,
        .deadline => |deadline| deadline.raw.nanoseconds - deadline.clock.now(io).nanoseconds,
    };
    return @intCast(std.math.clamp(remaining, 0, std.math.maxInt(u64)));
}

// std.Io's AFD handles reject this, skipReset covers them
fn ignoreConnectionResets(handle: std.Io.net.Socket.Handle) !void {
    if (builtin.os.tag != .windows) return;
    const sio_udp_connreset: u32 = 0x9800000C;
    const not_socket = 10038;
    const not_initialised = 10093;
    var enabled: u32 = 0;
    var returned: u32 = 0;
    if (winsock.WSAIoctl(handle, sio_udp_connreset, &enabled, @sizeOf(u32), null, 0, &returned, null, null) == 0) return;
    switch (winsock.WSAGetLastError()) {
        not_socket, not_initialised => {},
        else => return error.SocketOptionFailed,
    }
}

const winsock = struct {
    extern "ws2_32" fn WSAIoctl(
        socket: std.Io.net.Socket.Handle,
        code: u32,
        in_buffer: ?*const anyopaque,
        in_len: u32,
        out_buffer: ?*anyopaque,
        out_len: u32,
        returned: *u32,
        overlapped: ?*anyopaque,
        completion: ?*const anyopaque,
    ) callconv(.winapi) i32;
    extern "ws2_32" fn WSAGetLastError() callconv(.winapi) i32;
};

fn configureBuffers(handle: std.Io.net.Socket.Handle, options: BufferOptions) !BufferSizes {
    if (builtin.os.tag != .linux) return .{ .receive_bytes = null, .send_bytes = null };
    if (options.receive_bytes) |value| try setLinuxBuffer(handle, std.os.linux.SO.RCVBUF, value);
    if (options.send_bytes) |value| try setLinuxBuffer(handle, std.os.linux.SO.SNDBUF, value);
    return .{
        .receive_bytes = try getLinuxBuffer(handle, std.os.linux.SO.RCVBUF),
        .send_bytes = try getLinuxBuffer(handle, std.os.linux.SO.SNDBUF),
    };
}

fn setLinuxBuffer(handle: std.Io.net.Socket.Handle, option: u32, value: u32) !void {
    var native: c_int = @intCast(value);
    try std.posix.setsockopt(handle, std.os.linux.SOL.SOCKET, option, std.mem.asBytes(&native));
}

fn getLinuxBuffer(handle: std.Io.net.Socket.Handle, option: u32) !u32 {
    var value: c_int = 0;
    var length: std.os.linux.socklen_t = @sizeOf(c_int);
    const result = std.os.linux.getsockopt(handle, std.os.linux.SOL.SOCKET, option, @ptrCast(&value), &length);
    switch (std.posix.errno(result)) {
        .SUCCESS => {},
        else => return error.SocketOptionQueryFailed,
    }
    if (length != @sizeOf(c_int) or value < 0) return error.SocketOptionQueryFailed;
    return @intCast(value);
}

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

test "native receive keeps datagram boundaries and drops truncation for IPv4 and IPv6" {
    if (builtin.os.tag != .linux) return;
    for ([_][]const u8{ "127.0.0.1:0", "[::1]:0" }) |literal| {
        const address = try std.Io.net.IpAddress.parseLiteral(literal);
        var receiver = try Socket.bind(std.testing.io, address, 8);
        defer receiver.close();
        var sender = try Socket.bind(std.testing.io, address, 64);
        defer sender.close();
        try sender.send(receiver.value.address, "one");
        try sender.send(receiver.value.address, "oversized");
        try sender.send(receiver.value.address, "");
        try sender.send(receiver.value.address, "last");
        var messages: [8]std.Io.net.IncomingMessage = undefined;
        var storage: [64]u8 = undefined;
        const batch = try receiver.receiveMany(&messages, &storage, .none);
        try std.testing.expectEqual(@as(usize, 3), batch.messages.len);
        try std.testing.expectEqual(@as(usize, 1), batch.dropped_oversize);
        try std.testing.expectEqualStrings("one", batch.messages[0].data);
        try std.testing.expectEqualStrings("", batch.messages[1].data);
        try std.testing.expectEqualStrings("last", batch.messages[2].data);
        try std.testing.expectEqual(sender.value.address, batch.messages[0].from);
        try std.testing.expectEqual(@as(u64, 1), receiver.traffic.native_receive_calls);
        try std.testing.expectEqual(@as(u64, 4), receiver.traffic.native_received_datagrams);
    }
}

test "receive batching preserves a custom Io provider" {
    var vtable = std.testing.io.vtable.*;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var receiver = try Socket.bind(io, address, 64);
    defer receiver.close();
    var sender = try Socket.bind(io, address, 64);
    defer sender.close();
    try sender.send(receiver.value.address, "provider");
    var messages: [2]std.Io.net.IncomingMessage = undefined;
    var storage: [128]u8 = undefined;
    const batch = try receiver.receiveMany(&messages, &storage, .none);
    try std.testing.expectEqualStrings("provider", batch.messages[0].data);
    try std.testing.expectEqual(@as(u64, 0), receiver.traffic.native_receive_calls);
}

test "batched sends are flushed together and keep their destinations" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var first = try Socket.bind(io, address, 64);
    defer first.close();
    var second = try Socket.bind(io, address, 64);
    defer second.close();
    var sender = try Socket.bind(io, address, 64);
    defer sender.close();
    var batch = try SendBatch.init(std.testing.allocator, 2, 64);
    defer batch.deinit();
    sender.beginBatch(&batch);
    var buffer: [4]u8 = "one!".*;
    try sender.send(first.value.address, &buffer);
    buffer = "two!".*;
    try sender.send(second.value.address, &buffer);
    buffer = "3rd!".*;
    try sender.send(first.value.address, &buffer);
    sender.endBatch();
    try std.testing.expectEqual(@as(u64, 3), sender.traffic.datagrams_sent);
    var storage: [64]u8 = undefined;
    try std.testing.expectEqualStrings("one!", (try first.value.receive(io, &storage)).data);
    try std.testing.expectEqualStrings("3rd!", (try first.value.receive(io, &storage)).data);
    try std.testing.expectEqualStrings("two!", (try second.value.receive(io, &storage)).data);
}

test "timed receive waits for data or times out on every platform" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var receiver = try Socket.bind(io, address, 64);
    defer receiver.close();
    var sender = try Socket.bind(io, address, 64);
    defer sender.close();
    var storage: [64]u8 = undefined;
    const before = std.Io.Clock.awake.now(io);
    try std.testing.expectError(error.Timeout, receiver.receive(&storage, .{ .duration = .{ .raw = .fromMilliseconds(30), .clock = .awake } }));
    try std.testing.expect(before.durationTo(std.Io.Clock.awake.now(io)).nanoseconds >= 20 * std.time.ns_per_ms);
    try sender.send(receiver.value.address, "ping");
    try std.testing.expectEqualStrings("ping", (try receiver.receive(&storage, .{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } })).data);
    var messages: [2]std.Io.net.IncomingMessage = undefined;
    var batch_storage: [128]u8 = undefined;
    try std.testing.expectError(error.Timeout, receiver.receiveMany(&messages, &batch_storage, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }));
    try sender.send(receiver.value.address, "pong");
    try std.testing.expectEqualStrings("pong", (try receiver.receiveMany(&messages, &batch_storage, .{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } })).messages[0].data);
}

test "sub-millisecond batch receive waits for its deadline" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var receiver = try Socket.bind(io, address, 64);
    defer receiver.close();
    var sender = try Socket.bind(io, address, 64);
    defer sender.close();
    var messages: [2]std.Io.net.IncomingMessage = undefined;
    var storage: [128]u8 = undefined;
    const short: std.Io.Timeout = .{ .duration = .{ .raw = .fromMicroseconds(600), .clock = .awake } };
    const before = std.Io.Clock.awake.now(io);
    try std.testing.expectError(error.Timeout, receiver.receiveMany(&messages, &storage, short));
    if (builtin.os.tag == .linux) try std.testing.expect(before.durationTo(std.Io.Clock.awake.now(io)).nanoseconds >= 500 * std.time.ns_per_us);
    try sender.send(receiver.value.address, "late");
    try receiver.waitReadable(.{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } });
    try std.testing.expectEqualStrings("late", (try receiver.receiveMany(&messages, &storage, short)).messages[0].data);
}

test "listeners can share a port with reuse_port" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    if (builtin.os.tag != .linux) {
        try std.testing.expectError(error.ReusePortUnsupported, Socket.bindWithOptions(io, address, 64, .{}, true));
        return;
    }
    var first = try Socket.bindWithOptions(io, address, 64, .{}, true);
    defer first.close();
    var second = try Socket.bindWithOptions(io, first.value.address, 64, .{}, true);
    defer second.close();
    try std.testing.expectEqual(first.value.address.ip4.port, second.value.address.ip4.port);
    try std.testing.expectError(error.AddressInUse, Socket.bind(io, first.value.address, 64));
}

test "socket buffer options are bounded" {
    try BufferOptions.validate(.{});
    try std.testing.expectError(error.InvalidConfiguration, BufferOptions.validate(.{ .receive_bytes = 0 }));
    try std.testing.expectError(error.InvalidConfiguration, BufferOptions.validate(.{ .send_bytes = 256 * 1024 * 1024 + 1 }));
}

test "socket reports kernel buffer sizes where supported" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    if (builtin.os.tag != .linux) {
        try std.testing.expectError(error.SocketBufferConfigurationUnsupported, Socket.bindWithBuffers(io, address, 64, .{ .receive_bytes = 64 * 1024 }));
        return;
    }
    var socket = try Socket.bindWithBuffers(io, address, 64, .{ .receive_bytes = 64 * 1024, .send_bytes = 64 * 1024 });
    defer socket.close();
    if (builtin.os.tag == .linux) {
        try std.testing.expect(socket.kernelBufferSizes().receive_bytes.? >= 64 * 1024);
        try std.testing.expect(socket.kernelBufferSizes().send_bytes.? >= 64 * 1024);
    }
}

fn testWait(socket: *const Socket, timeout: std.Io.Timeout) !void {
    return socket.waitReadable(timeout);
}

test "waitReadable sleeps until data arrives, wakes without consuming, and cancels" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .unlimited });
    defer threaded.deinit();
    const io = threaded.io();
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var receiver = try Socket.bind(io, address, 64);
    defer receiver.close();
    var other = try Socket.bind(io, address, 64);
    defer other.close();
    var sender = try Socket.bind(io, address, 64);
    defer sender.close();
    const short: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(30), .clock = .awake } };
    const long: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(5_000), .clock = .awake } };

    const before = std.Io.Clock.awake.now(io);
    try std.testing.expectError(error.Timeout, receiver.waitReadable(short));
    try std.testing.expect(before.durationTo(std.Io.Clock.awake.now(io)).nanoseconds >= 20 * std.time.ns_per_ms);

    var storage: [64]u8 = undefined;
    for (0..200) |round| {
        var waiter = try io.concurrent(testWait, .{ &receiver, .none });
        defer _ = waiter.cancel(io) catch {};
        var sibling = try io.concurrent(testWait, .{ &other, long });
        defer _ = sibling.cancel(io) catch {};
        var payload: [4]u8 = undefined;
        std.mem.writeInt(u32, &payload, @intCast(round), .little);
        try sender.send(receiver.value.address, &payload);
        try waiter.await(io);
        try receiver.waitReadable(.none);
        const message = try receiver.receive(&storage, short);
        try std.testing.expectEqual(@as(u32, @intCast(round)), std.mem.readInt(u32, message.data[0..4], .little));
        try std.testing.expectError(error.Canceled, sibling.cancel(io));
    }
    try std.testing.expectError(error.Timeout, receiver.waitReadable(short));
    try std.testing.expectEqual(@as(u64, 0), other.traffic.receive_calls);

    var blocked = try io.concurrent(testWait, .{ &receiver, .none });
    try std.testing.expectError(error.Canceled, blocked.cancel(io));
}

test "receives skip an ICMP reset from a departed peer" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var socket = try Socket.bind(io, address, 64);
    defer socket.close();
    var departed = try Socket.bind(io, address, 64);
    const departed_address = departed.value.address;
    departed.close();
    const timeout: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(1_000), .clock = .awake } };

    var storage: [64]u8 = undefined;
    try socket.send(departed_address, "gone");
    if (builtin.os.tag == .windows) try socket.waitReadable(timeout);
    try socket.send(socket.value.address, "one");
    try std.testing.expectEqualStrings("one", (try socket.receive(&storage, timeout)).data);

    var messages: [2]std.Io.net.IncomingMessage = undefined;
    var batch_storage: [128]u8 = undefined;
    try socket.send(departed_address, "gone");
    if (builtin.os.tag == .windows) try socket.waitReadable(timeout);
    try socket.send(socket.value.address, "two");
    const batch = try socket.receiveMany(&messages, &batch_storage, timeout);
    try std.testing.expectEqualStrings("two", batch.messages[0].data);
    try std.testing.expect(batch.trailing_error == null);
    if (builtin.os.tag == .windows) try std.testing.expectEqual(@as(u64, 2), socket.traffic.connection_resets);
}
