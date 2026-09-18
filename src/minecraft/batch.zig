const std = @import("std");

pub const header: u8 = 0xfe;
pub const default_maximum_decompressed_bytes: usize = 16 * 1024 * 1024;
pub const default_maximum_packets: usize = 1600;
pub const default_retained_capacity: usize = 1024 * 1024;

pub const Compression = enum {
    disabled,
    declared,
};

pub const Algorithm = enum(u8) {
    zlib = 0,
    snappy = 1,
    none = 0xff,
};

pub const Options = struct {
    /// Limits bytes after the batch header, including a declared algorithm byte.
    maximum_compressed_bytes: usize = default_maximum_decompressed_bytes,
    maximum_decompressed_bytes: usize = default_maximum_decompressed_bytes,
    maximum_packets: usize = default_maximum_packets,
    maximum_retained_capacity: usize = default_retained_capacity,
    /// Compressed plus decompressed bytes allowed per decoder window.
    maximum_work_bytes_per_window: usize = 64 * 1024 * 1024,
    work_window_ms: u64 = 1000,
};

pub const BorrowedPacket = struct {
    bytes: []const u8,
};

pub const PacketFn = *const fn (context: *anyopaque, packet: BorrowedPacket) anyerror!void;

const Span = struct {
    offset: usize,
    length: usize,
};

pub const OwnedBatch = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,
    spans: []Span,

    pub fn deinit(self: *OwnedBatch) void {
        self.allocator.free(self.spans);
        self.allocator.free(self.bytes);
        self.* = undefined;
    }

    pub fn count(self: OwnedBatch) usize {
        return self.spans.len;
    }

    pub fn packet(self: OwnedBatch, index: usize) []const u8 {
        const span = self.spans[index];
        return self.bytes[span.offset..][0..span.length];
    }
};

pub const Decoder = struct {
    allocator: std.mem.Allocator,
    options: Options,
    scratch: std.ArrayList(u8) = .empty,
    window_started_ms: ?u64 = null,
    window_work: usize = 0,

    pub fn init(allocator: std.mem.Allocator, options: Options) !Decoder {
        if (options.maximum_compressed_bytes == 0 or
            options.maximum_decompressed_bytes == 0 or
            options.maximum_packets == 0 or
            options.maximum_work_bytes_per_window == 0 or
            options.work_window_ms == 0)
        {
            return error.InvalidConfiguration;
        }
        return .{ .allocator = allocator, .options = options };
    }

    pub fn deinit(self: *Decoder) void {
        self.scratch.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn retainedCapacity(self: Decoder) usize {
        return self.scratch.capacity;
    }

    /// Packet bytes expire when the callback returns.
    pub fn decodeBorrowed(self: *Decoder, wire: []const u8, compression: Compression, now_ms: u64, context: *anyopaque, callback: PacketFn) !usize {
        defer self.trimScratch();
        const data = try self.decodePayload(wire, compression, now_ms);
        const packet_count = try validateBatch(data, self.options.maximum_packets);
        var cursor: usize = 0;
        while (cursor < data.len) {
            const length = try readVarUInt32(data, &cursor);
            const end = cursor + @as(usize, length);
            try callback(context, .{ .bytes = data[cursor..end] });
            cursor = end;
        }
        return packet_count;
    }

    pub fn decodeOwned(self: *Decoder, wire: []const u8, compression: Compression, now_ms: u64) !OwnedBatch {
        defer self.trimScratch();
        const data = try self.decodePayload(wire, compression, now_ms);
        const packet_count = try validateBatch(data, self.options.maximum_packets);
        const bytes = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(bytes);
        const spans = try self.allocator.alloc(Span, packet_count);
        errdefer self.allocator.free(spans);

        var cursor: usize = 0;
        var index: usize = 0;
        while (cursor < bytes.len) : (index += 1) {
            const length = try readVarUInt32(bytes, &cursor);
            spans[index] = .{ .offset = cursor, .length = length };
            cursor += length;
        }
        return .{ .allocator = self.allocator, .bytes = bytes, .spans = spans };
    }

    fn decodePayload(self: *Decoder, wire: []const u8, compression: Compression, now_ms: u64) ![]const u8 {
        if (wire.len == 0) return error.MissingBatchHeader;
        if (wire[0] != header) return error.InvalidBatchHeader;
        var payload = wire[1..];
        if (payload.len > self.options.maximum_compressed_bytes) return error.CompressedBatchTooLarge;
        try self.chargeWork(now_ms, payload.len);

        const data = switch (compression) {
            .disabled => payload,
            .declared => blk: {
                if (payload.len == 0) return error.MissingCompressionAlgorithm;
                const algorithm: Algorithm = switch (payload[0]) {
                    0 => .zlib,
                    1 => .snappy,
                    0xff => .none,
                    else => return error.UnknownCompressionAlgorithm,
                };
                payload = payload[1..];
                break :blk switch (algorithm) {
                    .none => payload,
                    .zlib => try self.decompressZlib(payload),
                    .snappy => try self.decompressSnappy(payload),
                };
            },
        };
        if (data.len > self.options.maximum_decompressed_bytes) return error.DecompressedBatchTooLarge;
        try self.chargeWork(now_ms, data.len);
        return data;
    }

    fn decompressZlib(self: *Decoder, compressed: []const u8) ![]const u8 {
        self.scratch.clearRetainingCapacity();
        const doubled_hint = std.math.mul(usize, compressed.len, 2) catch self.options.maximum_decompressed_bytes;
        const capacity_hint = @min(self.options.maximum_decompressed_bytes, @max(@as(usize, 32 * 1024), doubled_hint));
        try self.scratch.ensureTotalCapacity(self.allocator, capacity_hint);
        var input: std.Io.Reader = .fixed(compressed);
        var window: [std.compress.flate.max_window_len]u8 = undefined;
        var decompressor: std.compress.flate.Decompress = .init(&input, .zlib, &window);
        var chunk: [32 * 1024]u8 = undefined;
        while (true) {
            const remaining = self.options.maximum_decompressed_bytes - self.scratch.items.len;
            const read_length = @min(chunk.len, std.math.add(usize, remaining, 1) catch chunk.len);
            const amount = decompressor.reader.readSliceShort(chunk[0..read_length]) catch return error.InvalidCompressedData;
            if (amount == 0) break;
            if (amount > remaining) return error.DecompressedBatchTooLarge;
            try self.scratch.appendSlice(self.allocator, chunk[0..amount]);
            if (amount < read_length) break;
        }
        const expected_adler = switch (decompressor.container_metadata) {
            .zlib => |metadata| metadata.adler,
            else => unreachable,
        };
        if (decompressor.err != null or
            input.bufferedLen() != 0 or
            expected_adler != std.hash.Adler32.hash(self.scratch.items))
        {
            return error.InvalidCompressedData;
        }
        return self.scratch.items;
    }

    fn decompressSnappy(self: *Decoder, compressed: []const u8) ![]const u8 {
        var input_index: usize = 0;
        const decoded_length = try readVarUInt32(compressed, &input_index);
        if (decoded_length > self.options.maximum_decompressed_bytes) return error.DecompressedBatchTooLarge;
        self.scratch.clearRetainingCapacity();
        try self.scratch.resize(self.allocator, decoded_length);
        var output_index: usize = 0;

        while (input_index < compressed.len and output_index < decoded_length) {
            const tag = compressed[input_index];
            input_index += 1;
            switch (tag & 0x03) {
                0 => {
                    var length: usize = @as(usize, tag >> 2) + 1;
                    if (length > 60) {
                        const length_bytes = length - 60;
                        if (length_bytes > 4 or length_bytes > compressed.len - input_index) return error.InvalidCompressedData;
                        var encoded_length: u32 = 0;
                        for (0..length_bytes) |shift| encoded_length |= @as(u32, compressed[input_index + shift]) << @intCast(shift * 8);
                        input_index += length_bytes;
                        length = std.math.add(usize, encoded_length, 1) catch return error.InvalidCompressedData;
                    }
                    if (length > compressed.len - input_index or length > decoded_length - output_index) return error.InvalidCompressedData;
                    @memcpy(self.scratch.items[output_index..][0..length], compressed[input_index..][0..length]);
                    input_index += length;
                    output_index += length;
                },
                1 => {
                    if (input_index >= compressed.len) return error.InvalidCompressedData;
                    const length: usize = 4 + ((tag >> 2) & 0x07);
                    const offset: usize = (@as(usize, tag & 0xe0) << 3) | compressed[input_index];
                    input_index += 1;
                    try copySnappy(self.scratch.items, &output_index, offset, length, decoded_length);
                },
                2 => {
                    if (compressed.len - input_index < 2) return error.InvalidCompressedData;
                    const length: usize = 1 + (tag >> 2);
                    const offset = std.mem.readInt(u16, compressed[input_index..][0..2], .little);
                    input_index += 2;
                    try copySnappy(self.scratch.items, &output_index, offset, length, decoded_length);
                },
                3 => {
                    if (compressed.len - input_index < 4) return error.InvalidCompressedData;
                    const length: usize = 1 + (tag >> 2);
                    const offset = std.mem.readInt(u32, compressed[input_index..][0..4], .little);
                    input_index += 4;
                    try copySnappy(self.scratch.items, &output_index, offset, length, decoded_length);
                },
                else => unreachable,
            }
        }
        if (input_index != compressed.len or output_index != decoded_length) return error.InvalidCompressedData;
        return self.scratch.items;
    }

    fn chargeWork(self: *Decoder, now_ms: u64, amount: usize) !void {
        if (self.window_started_ms == null or now_ms -| self.window_started_ms.? >= self.options.work_window_ms) {
            self.window_started_ms = now_ms;
            self.window_work = 0;
        }
        if (amount > self.options.maximum_work_bytes_per_window -| self.window_work) return error.DecompressionRateExceeded;
        self.window_work += amount;
    }

    fn trimScratch(self: *Decoder) void {
        if (self.scratch.capacity <= self.options.maximum_retained_capacity) {
            self.scratch.clearRetainingCapacity();
            return;
        }
        self.scratch.deinit(self.allocator);
        self.scratch = .empty;
    }
};

fn copySnappy(output: []u8, output_index: *usize, offset_value: anytype, length: usize, decoded_length: usize) !void {
    const offset: usize = @intCast(offset_value);
    if (offset == 0 or offset > output_index.* or length > decoded_length - output_index.*) return error.InvalidCompressedData;
    var remaining = length;
    while (remaining != 0) {
        const amount = @min(offset, remaining);
        @memcpy(output[output_index.*..][0..amount], output[output_index.* - offset ..][0..amount]);
        output_index.* += amount;
        remaining -= amount;
    }
}

fn validateBatch(data: []const u8, maximum_packets: usize) !usize {
    var cursor: usize = 0;
    var packet_count: usize = 0;
    while (cursor < data.len) {
        const length = try readVarUInt32(data, &cursor);
        if (length == 0) return error.EmptyPacket;
        if (length > data.len - cursor) return error.PacketLengthOutOfBounds;
        if (packet_count >= maximum_packets) return error.TooManyPackets;
        packet_count += 1;
        cursor += length;
    }
    return packet_count;
}

fn readVarUInt32(data: []const u8, cursor: *usize) !u32 {
    var value: u32 = 0;
    for (0..5) |index| {
        if (cursor.* >= data.len) return error.UnterminatedVarUInt;
        const byte = data[cursor.*];
        cursor.* += 1;
        value |= @truncate(@as(u64, byte & 0x7f) << @intCast(index * 7));
        if (byte & 0x80 == 0) return value;
    }
    return error.UnterminatedVarUInt;
}

test "declared uncompressed batch validates before callbacks" {
    const Collector = struct {
        count: usize = 0,
        fn add(raw: *anyopaque, packet_value: BorrowedPacket) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = packet_value;
            self.count += 1;
        }
    };
    var decoder = try Decoder.init(std.testing.allocator, .{});
    defer decoder.deinit();
    var collector: Collector = .{};
    try std.testing.expectEqual(@as(usize, 2), try decoder.decodeBorrowed(&.{ header, 0xff, 3, 'a', 'b', 'c', 2, 'd', 'e' }, .declared, 0, &collector, Collector.add));
    try std.testing.expectEqual(@as(usize, 2), collector.count);

    collector.count = 0;
    try std.testing.expectError(error.PacketLengthOutOfBounds, decoder.decodeBorrowed(&.{ header, 0xff, 1, 'a', 3, 'b' }, .declared, 1, &collector, Collector.add));
    try std.testing.expectEqual(@as(usize, 0), collector.count);
}

test "snappy batch decodes into borrowed and owned packets" {
    const Collector = struct {
        first: [3]u8 = undefined,
        second: [2]u8 = undefined,
        count: usize = 0,
        fn add(raw: *anyopaque, packet_value: BorrowedPacket) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.count == 0) @memcpy(&self.first, packet_value.bytes) else @memcpy(&self.second, packet_value.bytes);
            self.count += 1;
        }
    };
    const wire = [_]u8{ header, 1, 7, 0x18, 3, 'a', 'b', 'c', 2, 'd', 'e' };
    var decoder = try Decoder.init(std.testing.allocator, .{});
    defer decoder.deinit();
    var collector: Collector = .{};
    try std.testing.expectEqual(@as(usize, 2), try decoder.decodeBorrowed(&wire, .declared, 0, &collector, Collector.add));
    try std.testing.expectEqualStrings("abc", &collector.first);
    try std.testing.expectEqualStrings("de", &collector.second);

    var owned = try decoder.decodeOwned(&wire, .declared, 1);
    defer owned.deinit();
    try std.testing.expectEqual(@as(usize, 2), owned.count());
    try std.testing.expectEqualStrings("abc", owned.packet(0));
    try std.testing.expectEqualStrings("de", owned.packet(1));
}

test "zlib validates checksum and consumes the exact stream" {
    const Collector = struct {
        count: usize = 0,
        fn add(raw: *anyopaque, packet_value: BorrowedPacket) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.count == 0) try std.testing.expectEqualStrings("abc", packet_value.bytes) else try std.testing.expectEqualStrings("de", packet_value.bytes);
            self.count += 1;
        }
    };
    const compressed = [_]u8{ 0x78, 0x9c, 0x63, 0x4e, 0x4c, 0x4a, 0x66, 0x4a, 0x49, 0x05, 0x00, 0x07, 0x0b, 0x01, 0xf5 };
    var wire = [_]u8{ header, 0 } ++ compressed;
    var decoder = try Decoder.init(std.testing.allocator, .{});
    defer decoder.deinit();
    var collector: Collector = .{};
    try std.testing.expectEqual(@as(usize, 2), try decoder.decodeBorrowed(&wire, .declared, 0, &collector, Collector.add));
    try std.testing.expectEqual(@as(usize, 2), collector.count);

    wire[wire.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidCompressedData, decoder.decodeBorrowed(&wire, .declared, 1, &collector, Collector.add));
    const trailing = [_]u8{ header, 0 } ++ compressed ++ [_]u8{0};
    try std.testing.expectError(error.InvalidCompressedData, decoder.decodeBorrowed(&trailing, .declared, 2, &collector, Collector.add));
}

test "snappy copies overlap and malformed offsets fail" {
    const Collector = struct {
        fn check(_: *anyopaque, packet_value: BorrowedPacket) !void {
            try std.testing.expectEqualStrings("aaaaa", packet_value.bytes);
        }
    };
    // Decoded bytes are a one-byte packet length and five repeated 'a' bytes.
    const valid = [_]u8{ header, 1, 6, 0x04, 5, 'a', 0x01, 1 };
    var decoder = try Decoder.init(std.testing.allocator, .{});
    defer decoder.deinit();
    var unused: u8 = 0;
    try std.testing.expectEqual(@as(usize, 1), try decoder.decodeBorrowed(&valid, .declared, 0, &unused, Collector.check));
    try std.testing.expectError(error.InvalidCompressedData, decoder.decodeBorrowed(&.{ header, 1, 4, 0x01, 0 }, .declared, 1, &unused, Collector.check));
}

test "oversized scratch capacity is released" {
    const Discard = struct {
        fn packet(_: *anyopaque, _: BorrowedPacket) !void {}
    };
    const wire = [_]u8{ header, 1, 7, 0x18, 3, 'a', 'b', 'c', 2, 'd', 'e' };
    var decoder = try Decoder.init(std.testing.allocator, .{ .maximum_retained_capacity = 4 });
    defer decoder.deinit();
    var unused: u8 = 0;
    _ = try decoder.decodeBorrowed(&wire, .declared, 0, &unused, Discard.packet);
    try std.testing.expectEqual(@as(usize, 0), decoder.retainedCapacity());
}

test "batch limits reject before large allocation or callback" {
    var decoder = try Decoder.init(std.testing.allocator, .{
        .maximum_decompressed_bytes = 6,
        .maximum_packets = 1,
        .maximum_work_bytes_per_window = 64,
    });
    defer decoder.deinit();
    var unused: u8 = 0;
    const Discard = struct {
        fn packet(_: *anyopaque, _: BorrowedPacket) !void {}
    };
    try std.testing.expectError(error.DecompressedBatchTooLarge, decoder.decodeBorrowed(&.{ header, 1, 7 }, .declared, 0, &unused, Discard.packet));
    try std.testing.expectEqual(@as(usize, 0), decoder.retainedCapacity());
    try std.testing.expectError(error.TooManyPackets, decoder.decodeBorrowed(&.{ header, 0xff, 1, 'a', 1, 'b' }, .declared, 1, &unused, Discard.packet));
    try std.testing.expectError(error.UnterminatedVarUInt, decoder.decodeBorrowed(&.{ header, 0xff, 0x80, 0x80, 0x80, 0x80, 0x80 }, .declared, 2, &unused, Discard.packet));
}

test "decoder enforces its work window" {
    var decoder = try Decoder.init(std.testing.allocator, .{ .maximum_work_bytes_per_window = 8, .work_window_ms = 10 });
    defer decoder.deinit();
    var unused: u8 = 0;
    const Discard = struct {
        fn packet(_: *anyopaque, _: BorrowedPacket) !void {}
    };
    const wire = [_]u8{ header, 0xff, 1, 'a' };
    _ = try decoder.decodeBorrowed(&wire, .declared, 0, &unused, Discard.packet);
    try std.testing.expectError(error.DecompressionRateExceeded, decoder.decodeBorrowed(&wire, .declared, 1, &unused, Discard.packet));
    _ = try decoder.decodeBorrowed(&wire, .declared, 10, &unused, Discard.packet);
}

test "packet limit accepts the boundary atomically" {
    const maximum_packets = 1600;
    const Collector = struct {
        count: usize = 0,
        fn add(raw: *anyopaque, _: BorrowedPacket) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };
    var wire: std.ArrayList(u8) = .empty;
    defer wire.deinit(std.testing.allocator);
    try wire.appendSlice(std.testing.allocator, &.{ header, 0xff });
    for (0..maximum_packets) |_| try wire.appendSlice(std.testing.allocator, &.{ 1, 'a' });
    var decoder = try Decoder.init(std.testing.allocator, .{});
    defer decoder.deinit();
    var collector: Collector = .{};
    try std.testing.expectEqual(maximum_packets, try decoder.decodeBorrowed(wire.items, .declared, 0, &collector, Collector.add));
    try std.testing.expectEqual(maximum_packets, collector.count);

    try wire.appendSlice(std.testing.allocator, &.{ 1, 'b' });
    collector.count = 0;
    try std.testing.expectError(error.TooManyPackets, decoder.decodeBorrowed(wire.items, .declared, 1, &collector, Collector.add));
    try std.testing.expectEqual(@as(usize, 0), collector.count);
}

test "compression-disabled batches omit the algorithm byte" {
    const Collector = struct {
        fn check(_: *anyopaque, packet_value: BorrowedPacket) !void {
            try std.testing.expectEqualStrings("abc", packet_value.bytes);
        }
    };
    var decoder = try Decoder.init(std.testing.allocator, .{});
    defer decoder.deinit();
    var unused: u8 = 0;
    try std.testing.expectEqual(@as(usize, 1), try decoder.decodeBorrowed(&.{ header, 3, 'a', 'b', 'c' }, .disabled, 0, &unused, Collector.check));
}

fn checkOwnedAllocationFailures(allocator: std.mem.Allocator) !void {
    const wire = [_]u8{ header, 1, 7, 0x18, 3, 'a', 'b', 'c', 2, 'd', 'e' };
    var decoder = try Decoder.init(allocator, .{});
    defer decoder.deinit();
    var owned = try decoder.decodeOwned(&wire, .declared, 0);
    defer owned.deinit();
    try std.testing.expectEqualStrings("abc", owned.packet(0));
}

test "owned decoding handles every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkOwnedAllocationFailures, .{});
}
