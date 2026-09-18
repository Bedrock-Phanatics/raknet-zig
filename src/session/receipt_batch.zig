const std = @import("std");
const backend = @import("../net/backend.zig");
const ack = @import("../protocol/ack.zig");
const datagram = @import("../protocol/datagram.zig");
const receiver = @import("receiver.zig");

pub const Batch = struct {
    allocator: std.mem.Allocator,
    ack_values: []u32,
    nack_values: []ack.Record,
    records: []ack.Record,
    wire_storage: []u8,
    messages: []std.Io.net.OutgoingMessage,
    mtu: usize,
    maximum_sequences: usize,
    ack_count: usize = 0,
    nack_count: usize = 0,
    nack_sequences: usize = 0,

    pub fn init(allocator: std.mem.Allocator, capacity: usize, mtu: usize, maximum_sequences: usize) !Batch {
        if (capacity == 0 or capacity > 65_535 or mtu < 8 or maximum_sequences == 0 or maximum_sequences > 0x800000) return error.InvalidConfiguration;
        const bounded_capacity = @min(capacity, (mtu - 3) / 4);
        if (bounded_capacity < 2) return error.InvalidConfiguration;
        const ack_values = try allocator.alloc(u32, bounded_capacity);
        errdefer allocator.free(ack_values);
        const nack_values = try allocator.alloc(ack.Record, bounded_capacity);
        errdefer allocator.free(nack_values);
        const records = try allocator.alloc(ack.Record, bounded_capacity);
        errdefer allocator.free(records);
        const wire_storage = try allocator.alloc(u8, try std.math.mul(usize, mtu, 2));
        errdefer allocator.free(wire_storage);
        const messages = try allocator.alloc(std.Io.net.OutgoingMessage, 2);
        return .{ .allocator = allocator, .ack_values = ack_values, .nack_values = nack_values, .records = records, .wire_storage = wire_storage, .messages = messages, .mtu = mtu, .maximum_sequences = maximum_sequences };
    }

    pub fn deinit(self: *Batch) void {
        self.allocator.free(self.messages);
        self.allocator.free(self.wire_storage);
        self.allocator.free(self.records);
        self.allocator.free(self.nack_values);
        self.allocator.free(self.ack_values);
        self.* = undefined;
    }

    pub fn count(self: Batch) usize {
        return self.ack_count + self.nack_count;
    }

    pub fn isEmpty(self: Batch) bool {
        return self.count() == 0;
    }

    pub fn append(self: *Batch, receipt: receiver.Receipt) !void {
        const nack_needed: usize = if (receipt.missing) |gap| if (gap.first <= gap.last) 1 else 2 else 0;
        const sequence_needed: usize = if (receipt.missing) |gap|
            if (gap.first <= gap.last) gap.last - gap.first + 1 else gap.last + 1 + (0xffffff - gap.first + 1)
        else
            0;
        if (sequence_needed > self.maximum_sequences or self.ack_count + @intFromBool(receipt.acknowledge != null) > self.ack_values.len or self.nack_count + nack_needed > self.nack_values.len or sequence_needed > self.maximum_sequences -| self.nack_sequences) return error.ReceiptBatchFull;
        if (receipt.acknowledge) |sequence| {
            self.ack_values[self.ack_count] = sequence;
            self.ack_count += 1;
        }
        if (receipt.missing) |gap| {
            if (gap.first <= gap.last) {
                self.nack_values[self.nack_count] = .{ .first = gap.first, .last = gap.last };
                self.nack_count += 1;
            } else {
                self.nack_values[self.nack_count] = .{ .first = 0, .last = gap.last };
                self.nack_values[self.nack_count + 1] = .{ .first = gap.first, .last = 0xffffff };
                self.nack_count += 2;
            }
            self.nack_sequences += sequence_needed;
        }
    }

    pub fn flush(self: *Batch, socket: *const backend.Socket, destination: *const std.Io.net.IpAddress, maximum_work: usize) !usize {
        if (maximum_work == 0 or self.isEmpty()) return 0;
        const nack_take = @min(self.nack_count, maximum_work);
        const ack_take = @min(self.ack_count, maximum_work - nack_take);
        var nack_sequences_sent: usize = 0;
        for (self.nack_values[0..nack_take]) |record| nack_sequences_sent += record.count();
        var message_count: usize = 0;

        if (ack_take != 0) {
            const canonical = canonicalizeValues(self.ack_values[0..ack_take], self.records);
            const wire = datagram.encodeControl(.ack, canonical, self.wire_storage[0..self.mtu]) catch return error.InternalFailure;
            self.messages[message_count] = .{ .address = destination, .data_ptr = wire.ptr, .data_len = wire.len };
            message_count += 1;
        }
        if (nack_take != 0) {
            const canonical = canonicalizeRanges(self.nack_values[0..nack_take], self.records);
            const wire = datagram.encodeControl(.nack, canonical, self.wire_storage[self.mtu .. self.mtu * 2]) catch return error.InternalFailure;
            self.messages[message_count] = .{ .address = destination, .data_ptr = wire.ptr, .data_len = wire.len };
            message_count += 1;
        }
        socket.sendMany(self.messages[0..message_count]) catch return error.TransportFailure;
        removePrefix(u32, self.ack_values, &self.ack_count, ack_take);
        removePrefix(ack.Record, self.nack_values, &self.nack_count, nack_take);
        self.nack_sequences -= nack_sequences_sent;
        return ack_take + nack_take;
    }
};

pub fn canonicalizeValues(values: []u32, output: []ack.Record) []ack.Record {
    std.mem.sort(u32, values, {}, std.sort.asc(u32));
    if (values.len == 0) return output[0..0];
    var count: usize = 0;
    var current: ack.Record = .{ .first = values[0], .last = values[0] };
    for (values[1..]) |value| {
        if (value <= current.last or (current.last != 0xffffff and value == current.last + 1)) {
            current.last = @max(current.last, value);
        } else {
            output[count] = current;
            count += 1;
            current = .{ .first = value, .last = value };
        }
    }
    output[count] = current;
    return output[0 .. count + 1];
}

pub fn canonicalizeRanges(values: []ack.Record, output: []ack.Record) []ack.Record {
    const less = struct {
        fn than(_: void, left: ack.Record, right: ack.Record) bool {
            return left.first < right.first or (left.first == right.first and left.last < right.last);
        }
    }.than;
    std.mem.sort(ack.Record, values, {}, less);
    if (values.len == 0) return output[0..0];
    var count: usize = 0;
    var current = values[0];
    for (values[1..]) |value| {
        if (value.first <= current.last or (current.last != 0xffffff and value.first == current.last + 1)) {
            current.last = @max(current.last, value.last);
        } else {
            output[count] = current;
            count += 1;
            current = value;
        }
    }
    output[count] = current;
    return output[0 .. count + 1];
}

fn removePrefix(comptime T: type, values: []T, count: *usize, removed: usize) void {
    if (removed == 0) return;
    std.mem.copyForwards(T, values[0 .. count.* - removed], values[removed..count.*]);
    count.* -= removed;
}

test "canonical ranges are sorted coalesced and deduplicated" {
    var values = [_]u32{ 8, 3, 4, 8, 7, 12 };
    var output: [values.len]ack.Record = undefined;
    const canonical = canonicalizeValues(&values, &output);
    try std.testing.expectEqualSlices(ack.Record, &.{ .{ .first = 3, .last = 4 }, .{ .first = 7, .last = 8 }, .{ .first = 12, .last = 12 } }, canonical);

    var ranges = [_]ack.Record{ .{ .first = 8, .last = 12 }, .{ .first = 1, .last = 3 }, .{ .first = 3, .last = 7 }, .{ .first = 20, .last = 20 } };
    const merged = canonicalizeRanges(&ranges, &output);
    try std.testing.expectEqualSlices(ack.Record, &.{ .{ .first = 1, .last = 12 }, .{ .first = 20, .last = 20 } }, merged);
}

test "receipt storage rejects overflow atomically" {
    var batch = try Batch.init(std.testing.allocator, 2, 576, 32);
    defer batch.deinit();
    try batch.append(.{ .acknowledge = 1 });
    try batch.append(.{ .acknowledge = 2 });
    try std.testing.expectError(error.ReceiptBatchFull, batch.append(.{ .acknowledge = 3 }));
    try std.testing.expectEqual(@as(usize, 2), batch.count());
}

test "flush emits canonical ACK and NACK datagrams" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var receiver_socket = try backend.Socket.bind(io, address, 576);
    defer receiver_socket.close();
    var sender = try backend.Socket.bind(io, address, 576);
    defer sender.close();
    var batch = try Batch.init(std.testing.allocator, 8, 576, 32);
    defer batch.deinit();
    try batch.append(.{ .acknowledge = 3 });
    try batch.append(.{ .acknowledge = 2, .missing = .{ .first = 8, .last = 10 } });
    try std.testing.expectEqual(@as(usize, 3), try batch.flush(&sender, &receiver_socket.value.address, 8));
    try std.testing.expect(batch.isEmpty());

    var wire: [576]u8 = undefined;
    var records: [8]ack.Record = undefined;
    const first = try receiver_socket.value.receive(io, &wire);
    const first_decoded = try datagram.decode(first.data, &records, records.len, 32);
    try std.testing.expect(first_decoded == .ack);
    try std.testing.expectEqualSlices(ack.Record, &.{.{ .first = 2, .last = 3 }}, first_decoded.ack.records);
    const second = try receiver_socket.value.receive(io, &wire);
    const second_decoded = try datagram.decode(second.data, &records, records.len, 32);
    try std.testing.expect(second_decoded == .nack);
    try std.testing.expectEqualSlices(ack.Record, &.{.{ .first = 8, .last = 10 }}, second_decoded.nack.records);
}

test "flush work is bounded and carries remaining receipts" {
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parseLiteral("127.0.0.1:0");
    var receiver_socket = try backend.Socket.bind(io, address, 576);
    defer receiver_socket.close();
    var sender = try backend.Socket.bind(io, address, 576);
    defer sender.close();
    var batch = try Batch.init(std.testing.allocator, 8, 576, 32);
    defer batch.deinit();
    for (1..5) |sequence| try batch.append(.{ .acknowledge = @intCast(sequence) });
    try std.testing.expectEqual(@as(usize, 2), try batch.flush(&sender, &receiver_socket.value.address, 2));
    try std.testing.expectEqual(@as(usize, 2), batch.count());
    var wire: [576]u8 = undefined;
    _ = try receiver_socket.value.receive(io, &wire);
}

test "NACK expansion is capped before coalescing" {
    var batch = try Batch.init(std.testing.allocator, 8, 576, 4);
    defer batch.deinit();
    try batch.append(.{ .missing = .{ .first = 1, .last = 4, .count = 4 } });
    try std.testing.expectError(error.ReceiptBatchFull, batch.append(.{ .missing = .{ .first = 5, .last = 5, .count = 1 } }));
    try std.testing.expectEqual(@as(usize, 1), batch.count());
}

fn checkBatchAllocationFailures(allocator: std.mem.Allocator) !void {
    var batch = try Batch.init(allocator, 8, 576, 32);
    defer batch.deinit();
    try batch.append(.{ .acknowledge = 1, .missing = .{ .first = 2, .last = 3, .count = 2 } });
    try std.testing.expectEqual(@as(usize, 2), batch.count());
}

test "receipt batch initialization handles every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkBatchAllocationFailures, .{});
}
