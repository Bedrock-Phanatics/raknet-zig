const std = @import("std");
const cursor = @import("cursor.zig");
const uint24 = @import("../util/uint24.zig");

pub const Record = struct {
    first: u32,
    last: u32,

    pub fn count(self: Record) u32 {
        return self.last - self.first + 1;
    }
};

pub const Decoded = struct {
    records: []Record,
    acknowledged_count: usize,
};

pub fn decode(data: []const u8, storage: []Record, maximum_records: usize, maximum_acknowledged: usize) !Decoded {
    var reader: cursor.Reader = .{ .data = data };
    const advertised = try reader.u16be();
    if (advertised > maximum_records or advertised > storage.len) return error.TooManyRecords;

    var count: usize = 0;
    var acknowledged: usize = 0;
    var previous: ?Record = null;
    while (count < advertised) : (count += 1) {
        const kind = try reader.byte();
        const first = try reader.u24le();
        const last = switch (kind) {
            0 => try reader.u24le(),
            1 => first,
            else => return error.InvalidRecordType,
        };
        if (first > last) return error.ReversedRange;
        const record: Record = .{ .first = first, .last = last };
        const n: usize = record.count();
        if (n > maximum_acknowledged -| acknowledged) return error.TooManyAcknowledgements;

        if (previous) |last_record| {
            if (record.first <= last_record.last) return error.OverlappingRanges;
        }
        storage[count] = record;
        previous = record;
        acknowledged += n;
    }
    if (reader.remaining() != 0) return error.TrailingData;
    return .{ .records = storage[0..count], .acknowledged_count = acknowledged };
}

pub fn encodedSize(records: []const Record) !usize {
    if (records.len > 65_535) return error.TooManyRecords;
    var size: usize = 2;
    for (records) |record| {
        if (record.first > uint24.mask or record.last > uint24.mask or record.first > record.last) return error.InvalidRange;
        size = try std.math.add(usize, size, if (record.first == record.last) 4 else 7);
    }
    return size;
}

pub fn encode(records: []const Record, output: []u8) ![]u8 {
    const needed = try encodedSize(records);
    if (output.len < needed) return error.NoSpaceLeft;
    var writer: cursor.Writer = .{ .data = output };
    try writer.u16be(@intCast(records.len));
    var previous: ?Record = null;
    for (records) |record| {
        if (previous) |prior| if (record.first <= prior.last) return error.InvalidRange;
        try writer.byte(if (record.first == record.last) 1 else 0);
        try writer.u24le(record.first);
        if (record.first != record.last) try writer.u24le(record.last);
        previous = record;
    }
    return writer.written();
}

pub const SequenceIterator = struct {
    records: []const Record,
    record_index: usize = 0,
    current: u32 = 0,
    started: bool = false,
    emitted: usize = 0,
    maximum: usize,

    pub fn init(records: []const Record, maximum: usize) SequenceIterator {
        return .{ .records = records, .maximum = maximum };
    }

    pub fn next(self: *SequenceIterator) !?u32 {
        if (self.record_index >= self.records.len) return null;
        if (self.emitted >= self.maximum) return error.WorkLimitExceeded;
        const record = self.records[self.record_index];
        if (!self.started) {
            self.current = record.first;
            self.started = true;
        }
        const result = self.current;
        self.emitted += 1;
        if (self.current == record.last) {
            self.record_index += 1;
            self.current = 0;
            self.started = false;
        } else self.current += 1;
        return result;
    }
};

test "bounded ACK ranges round trip without expansion" {
    const input = [_]Record{ .{ .first = 3, .last = 3 }, .{ .first = 8, .last = 12 } };
    var wire: [32]u8 = undefined;
    const bytes = try encode(&input, &wire);
    var records: [4]Record = undefined;
    const decoded = try decode(bytes, &records, 4, 8);
    try std.testing.expectEqual(@as(usize, 6), decoded.acknowledged_count);
    try std.testing.expectEqualSlices(Record, &input, decoded.records);
}

test "malformed and amplification records fail cheaply" {
    var records: [2]Record = undefined;
    try std.testing.expectError(error.Truncated, decode(&.{ 0, 1, 1 }, &records, 2, 10));
    try std.testing.expectError(error.InvalidRecordType, decode(&.{ 0, 1, 2, 0, 0, 0 }, &records, 2, 10));
    try std.testing.expectError(error.ReversedRange, decode(&.{ 0, 1, 0, 10, 0, 0, 2, 0, 0 }, &records, 2, 10));
    try std.testing.expectError(error.TooManyAcknowledgements, decode(&.{ 0, 1, 0, 0, 0, 0, 255, 255, 127 }, &records, 2, 4096));
}
