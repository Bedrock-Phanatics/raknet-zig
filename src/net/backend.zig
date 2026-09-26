const std = @import("std");
const builtin = @import("builtin");

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
    poll_event: if (builtin.os.tag == .windows) ?std.os.windows.HANDLE else void = if (builtin.os.tag == .windows) null else {},

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
        return .{ .io = io, .value = value, .maximum_datagram_size = maximum_datagram_size, .buffer_sizes = try configureBuffers(value.handle, buffers) };
    }
    pub fn close(self: *Socket) void {
        if (builtin.os.tag == .windows) if (self.poll_event) |event| {
            _ = std.os.windows.ntdll.NtClose(event);
        };
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
            const failure, const sent = self.io.vtable.netSend(self.io.userdata, self.value.handle, pending, .{});
            for (pending[0..sent]) |message| self.traffic.bytes_sent += message.data_len;
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
        self.value.send(self.io, &destination, data) catch |err| switch (err) {
            error.SystemResources => {
                self.traffic.send_drops += 1;
                return;
            },
            else => return err,
        };
        self.traffic.datagrams_sent += 1;
        self.traffic.bytes_sent += data.len;
    }
    pub fn sendMany(self: *Socket, messages: []std.Io.net.OutgoingMessage) !void {
        var bytes: u64 = 0;
        for (messages) |message| {
            if (message.data_len > self.maximum_datagram_size) return error.DatagramTooLarge;
            bytes += message.data_len;
        }
        if (self.batch) |batch| {
            for (messages) |message| self.queue(batch, message.address.*, message.data_ptr[0..message.data_len]);
            return;
        }
        self.value.sendMany(self.io, messages, .{}) catch |err| switch (err) {
            error.SystemResources => {
                self.traffic.send_drops += messages.len;
                return;
            },
            else => return err,
        };
        self.traffic.datagrams_sent += messages.len;
        self.traffic.bytes_sent += bytes;
    }
    pub fn kernelBufferSizes(self: *const Socket) BufferSizes {
        return self.buffer_sizes;
    }

    pub fn receive(self: *Socket, buffer: []u8, timeout: std.Io.Timeout) !std.Io.net.IncomingMessage {
        return self.value.receiveTimeout(self.io, buffer, timeout) catch |err| switch (err) {
            error.ConcurrencyUnavailable => {
                if (timeout != .none and !try self.waitReadable(timeout)) return error.Timeout;
                return self.value.receive(self.io, buffer);
            },
            else => return err,
        };
    }

    fn waitReadable(self: *Socket, timeout: std.Io.Timeout) !bool {
        if (builtin.os.tag != .windows) return error.ConcurrencyUnavailable;
        const windows = std.os.windows;
        const PollHandle = extern struct { handle: windows.HANDLE, events: windows.ULONG, status: windows.NTSTATUS };
        const PollInfo = extern struct { timeout: windows.LARGE_INTEGER, count: windows.ULONG, exclusive: windows.ULONG, handles: [1]PollHandle };
        const readable: windows.ULONG = 0x0001 | 0x0008 | 0x0010;
        if (self.poll_event == null) {
            var event: windows.HANDLE = undefined;
            if (windows.ntdll.NtCreateEvent(&event, @bitCast(@as(u32, 0x1f0003)), null, .Synchronization, .FALSE) != .SUCCESS) return error.SystemResources;
            self.poll_event = event;
        }
        const handle: windows.HANDLE = self.value.handle;
        var info: PollInfo = .{
            .timeout = -@as(i64, @intCast(@min(remainingMilliseconds(self.io, timeout), std.math.maxInt(i64) / 10_000))) * 10_000,
            .count = 1,
            .exclusive = 0,
            .handles = .{.{ .handle = handle, .events = readable, .status = .SUCCESS }},
        };
        var status_block: windows.IO_STATUS_BLOCK = undefined;
        var status = windows.ntdll.NtDeviceIoControlFile(handle, self.poll_event.?, null, null, &status_block, windows.IOCTL.AFD.POLL, &info, @sizeOf(PollInfo), &info, @sizeOf(PollInfo));
        if (status == .PENDING) {
            _ = windows.ntdll.NtWaitForSingleObject(self.poll_event.?, .FALSE, null);
            status = status_block.u.Status;
        }
        if (status == .TIMEOUT) return false;
        if (status != .SUCCESS) return error.Unexpected;
        return info.count != 0 and info.handles[0].events & readable != 0;
    }

    pub fn receiveMany(self: *Socket, messages: []std.Io.net.IncomingMessage, data_storage: []u8, timeout: std.Io.Timeout) !ReceiveBatch {
        if (messages.len == 0) return error.InvalidConfiguration;
        const required = try std.math.mul(usize, messages.len, self.maximum_datagram_size);
        if (data_storage.len < required) return error.NoSpaceLeft;
        @memset(messages, .init);
        const failure, const received = self.value.receiveManyTimeout(self.io, messages, data_storage[0..required], .{}, timeout);
        var actual_received = received;
        var actual_failure = failure;
        if (actual_received == 0) if (actual_failure) |err| switch (err) {
            error.ConcurrencyUnavailable => {
                if (timeout != .none and !try self.waitReadable(timeout)) return error.Timeout;
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
        return .{ .messages = messages[0..valid], .dropped_oversize = dropped, .trailing_error = actual_failure };
    }
};

fn remainingMilliseconds(io: std.Io, timeout: std.Io.Timeout) u64 {
    const nanoseconds: i96 = switch (timeout) {
        .none => return std.math.maxInt(u64),
        .duration => |duration| duration.raw.nanoseconds,
        .deadline => |deadline| deadline.raw.nanoseconds - deadline.clock.now(io).nanoseconds,
    };
    if (nanoseconds <= 0) return 0;
    return @intCast(@min(@divTrunc(nanoseconds + std.time.ns_per_ms - 1, std.time.ns_per_ms), std.math.maxInt(u64)));
}

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
