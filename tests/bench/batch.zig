const std = @import("std");
const raknet = @import("raknet");
const batch = raknet.minecraft.batch;

const CountingAllocator = struct {
    child: std.mem.Allocator,
    events: usize = 0,
    bytes: usize = 0,

    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn record(self: *CountingAllocator, bytes: usize) void {
        if (bytes == 0) return;
        self.events += 1;
        self.bytes += bytes;
    }

    fn alloc(context: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(context));
        const result = self.child.vtable.alloc(self.child.ptr, len, alignment, ret_addr);
        if (result != null) self.record(len);
        return result;
    }

    fn resize(context: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(context));
        const success = self.child.vtable.resize(self.child.ptr, memory, alignment, new_len, ret_addr);
        if (success and new_len > memory.len) self.record(new_len - memory.len);
        return success;
    }

    fn remap(context: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(context));
        const result = self.child.vtable.remap(self.child.ptr, memory, alignment, new_len, ret_addr);
        if (result != null and new_len > memory.len) self.record(new_len - memory.len);
        return result;
    }

    fn free(context: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(context));
        self.child.vtable.free(self.child.ptr, memory, alignment, ret_addr);
    }
};

const Measurement = struct {
    nanoseconds: u64,
    events: usize,
    bytes: usize,
    retained: usize,
    checksum: usize,
};

fn addPacket(context: *anyopaque, packet: batch.BorrowedPacket) anyerror!void {
    const checksum: *usize = @ptrCast(@alignCast(context));
    for (packet.bytes) |byte| checksum.* +%= byte;
}

fn measure(io: std.Io, wire: []const u8, owned: bool, iterations: usize) !Measurement {
    var counting: CountingAllocator = .{ .child = std.heap.page_allocator };
    var decoder = try batch.Decoder.init(counting.allocator(), .{
        .maximum_work_bytes_per_window = std.math.maxInt(usize),
    });
    defer decoder.deinit();
    var checksum: usize = 0;
    _ = try decoder.decodeBorrowed(wire, .declared, 0, &checksum, addPacket);
    counting.events = 0;
    counting.bytes = 0;
    const start = std.Io.Clock.awake.now(io);
    for (0..iterations) |_| {
        if (owned) {
            var result = try decoder.decodeOwned(wire, .declared, 0);
            for (0..result.count()) |index| for (result.packet(index)) |byte| {
                checksum +%= byte;
            };
            result.deinit();
        } else {
            _ = try decoder.decodeBorrowed(wire, .declared, 0, &checksum, addPacket);
        }
    }
    return .{
        .nanoseconds = @intCast(start.durationTo(std.Io.Clock.awake.now(io)).nanoseconds),
        .events = counting.events,
        .bytes = counting.bytes,
        .retained = decoder.retainedCapacity(),
        .checksum = checksum,
    };
}

fn writeVarUInt(output: []u8, cursor: *usize, value: u32) void {
    var remaining = value;
    while (remaining >= 0x80) {
        output[cursor.*] = @as(u8, @truncate(remaining)) | 0x80;
        cursor.* += 1;
        remaining >>= 7;
    }
    output[cursor.*] = @truncate(remaining);
    cursor.* += 1;
}

fn poolCycle(small: []const u8) !void {
    const large_packet_size = 1024 * 1024 + 64;
    const raw = try std.heap.page_allocator.alloc(u8, large_packet_size + 4);
    defer std.heap.page_allocator.free(raw);
    var raw_len: usize = 0;
    writeVarUInt(raw, &raw_len, large_packet_size);
    for (raw[raw_len..][0..large_packet_size], 0..) |*byte, index| byte.* = @truncate(index);
    raw_len += large_packet_size;
    const wire = try std.heap.page_allocator.alloc(u8, raw_len + 12);
    defer std.heap.page_allocator.free(wire);
    wire[0] = batch.header;
    wire[1] = @intFromEnum(batch.Algorithm.snappy);
    var wire_len: usize = 2;
    writeVarUInt(wire, &wire_len, @intCast(raw_len));
    wire[wire_len] = 0xfc;
    wire_len += 1;
    std.mem.writeInt(u32, wire[wire_len..][0..4], @intCast(raw_len - 1), .little);
    wire_len += 4;
    @memcpy(wire[wire_len..][0..raw_len], raw[0..raw_len]);
    wire_len += raw_len;

    var counting: CountingAllocator = .{ .child = std.heap.page_allocator };
    var decoder = try batch.Decoder.init(counting.allocator(), .{
        .maximum_work_bytes_per_window = std.math.maxInt(usize),
    });
    defer decoder.deinit();
    var checksum: usize = 0;
    _ = try decoder.decodeBorrowed(small, .declared, 0, &checksum, addPacket);
    counting.events = 0;
    _ = try decoder.decodeBorrowed(small, .declared, 0, &checksum, addPacket);
    const warm_allocations = counting.events;
    const warm_retained = decoder.retainedCapacity();
    counting.events = 0;
    _ = try decoder.decodeBorrowed(wire[0..wire_len], .declared, 0, &checksum, addPacket);
    const burst_allocations = counting.events;
    const burst_retained = decoder.retainedCapacity();
    counting.events = 0;
    _ = try decoder.decodeBorrowed(small, .declared, 0, &checksum, addPacket);
    const idle_allocations = counting.events;
    const idle_retained = decoder.retainedCapacity();
    if (warm_allocations != 0 or warm_retained == 0 or burst_retained != 0 or idle_retained == 0 or idle_allocations == 0) return error.BenchmarkPoolMismatch;
    std.debug.print("batch_pool: warm {d} allocs/{d} retained, burst {d} allocs/{d} retained, idle {d} allocs/{d} retained, checksum {d}\n", .{
        warm_allocations, warm_retained, burst_allocations, burst_retained, idle_allocations, idle_retained, checksum,
    });
}

pub fn run(io: std.Io) !void {
    const packet_count = 16;
    const packet_size = 256;
    const iterations = 10_000;
    var raw: [packet_count * (packet_size + 2)]u8 = undefined;
    for (0..packet_count) |packet_index| {
        const offset = packet_index * (packet_size + 2);
        raw[offset] = 0x80;
        raw[offset + 1] = 0x02;
        for (raw[offset + 2 ..][0..packet_size], 0..) |*byte, index| {
            byte.* = @truncate(packet_index *% 31 +% index *% 17);
        }
    }

    var zlib_wire: [8192]u8 = undefined;
    zlib_wire[0] = batch.header;
    zlib_wire[1] = @intFromEnum(batch.Algorithm.zlib);
    var output: std.Io.Writer = .fixed(zlib_wire[2..]);
    var work: [std.compress.flate.max_window_len * 2]u8 = undefined;
    var compressor = try std.compress.flate.Compress.init(&output, &work, .zlib, .fastest);
    try compressor.writer.writeAll(&raw);
    try compressor.finish();
    const zlib = zlib_wire[0 .. 2 + output.buffered().len];

    var snappy_wire: [raw.len + 8]u8 = undefined;
    snappy_wire[0] = batch.header;
    snappy_wire[1] = @intFromEnum(batch.Algorithm.snappy);
    var cursor: usize = 2;
    var remaining: u32 = raw.len;
    while (remaining >= 0x80) {
        snappy_wire[cursor] = @as(u8, @truncate(remaining)) | 0x80;
        cursor += 1;
        remaining >>= 7;
    }
    snappy_wire[cursor] = @truncate(remaining);
    cursor += 1;
    snappy_wire[cursor] = 0xf4;
    cursor += 1;
    std.mem.writeInt(u16, snappy_wire[cursor..][0..2], raw.len - 1, .little);
    cursor += 2;
    @memcpy(snappy_wire[cursor..][0..raw.len], &raw);
    cursor += raw.len;
    const snappy = snappy_wire[0..cursor];

    std.mem.doNotOptimizeAway(&raw);
    var expected: usize = 0;
    for (0..packet_count) |packet_index| {
        const offset = packet_index * (packet_size + 2);
        for (raw[offset + 2 ..][0..packet_size]) |byte| expected +%= byte;
    }
    const cases = .{ .{ "zlib", zlib }, .{ "snappy_literal", snappy } };
    inline for (cases) |case| {
        const borrowed = try measure(io, case[1], false, iterations);
        const owned = try measure(io, case[1], true, iterations);
        if (borrowed.checksum != expected *% (iterations + 1) or owned.checksum != borrowed.checksum) return error.BenchmarkChecksumMismatch;
        std.debug.print("batch_{s}: {d} packets x {d} bytes, wire {d} bytes, {d} iterations\n", .{ case[0], packet_count, packet_size, case[1].len, iterations });
        inline for (.{ .{ "borrowed", borrowed }, .{ "owned", owned } }) |mode| {
            std.debug.print("batch_{s}_{s}: {d:.2} us/op, {d:.2} alloc events/op, {d:.2} requested bytes/op, {d} retained bytes, checksum {d}\n", .{
                case[0],                                                                                     mode[0],
                @as(f64, @floatFromInt(mode[1].nanoseconds)) / @as(f64, @floatFromInt(iterations)) / 1000.0, @as(f64, @floatFromInt(mode[1].events)) / @as(f64, @floatFromInt(iterations)),
                @as(f64, @floatFromInt(mode[1].bytes)) / @as(f64, @floatFromInt(iterations)),                mode[1].retained,
                mode[1].checksum,
            });
        }
    }
    try poolCycle(snappy);
}
