const std = @import("std");
const ack = @import("../protocol/ack.zig");
const uint24 = @import("../util/uint24.zig");

const Record = struct {
    data: []u8,
    sent_ms: u64,
    deadline_ms: u64,
    in_flight_bytes: usize,
    transmissions: u8 = 1,
};

pub const Acknowledged = struct { packets: usize = 0, bytes: usize = 0, rtt_sample_ms: ?u64 = null };
pub const Due = struct { sequence: u32, data: []const u8, in_flight_bytes: usize, timed_out: bool };
pub const DueBatch = struct { items: []Due, exhausted: usize };

/// Owns one copy of each unacknowledged datagram. Capacity and total bytes are fixed by configuration.
pub const Recovery = struct {
    allocator: std.mem.Allocator,
    records: std.AutoHashMapUnmanaged(u32, Record) = .empty,
    maximum_entries: usize,
    maximum_bytes: usize,
    maximum_transmissions: u8,
    total_bytes: usize = 0,
    next_deadline_ms: ?u64 = null,

    pub fn init(allocator: std.mem.Allocator, maximum_entries: usize, maximum_bytes: usize, maximum_transmissions: u8) !Recovery {
        if (maximum_entries == 0 or maximum_bytes == 0 or maximum_transmissions < 2 or maximum_entries > std.math.maxInt(u32)) return error.InvalidConfiguration;
        const self: Recovery = .{ .allocator = allocator, .maximum_entries = maximum_entries, .maximum_bytes = maximum_bytes, .maximum_transmissions = maximum_transmissions };
        return self;
    }

    pub fn deinit(self: *Recovery) void {
        var iterator = self.records.valueIterator();
        while (iterator.next()) |record| self.allocator.free(record.data);
        self.records.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn track(self: *Recovery, raw_sequence: u32, data: []const u8, in_flight_bytes: usize, now_ms: u64, rto_ms: u32) !void {
        const sequence = uint24.normalize(raw_sequence);
        if (data.len == 0 or self.records.contains(sequence)) return error.InvalidDatagram;
        if (self.records.count() >= self.maximum_entries) return error.RecoveryFull;
        if (data.len > self.maximum_bytes -| self.total_bytes) return error.RecoveryBytesExceeded;
        const copy = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(copy);
        const deadline_ms = now_ms +| rto_ms;
        try self.records.put(self.allocator, sequence, .{ .data = copy, .sent_ms = now_ms, .deadline_ms = deadline_ms, .in_flight_bytes = in_flight_bytes });
        self.total_bytes += copy.len;
        self.next_deadline_ms = if (self.next_deadline_ms) |current| @min(current, deadline_ms) else deadline_ms;
    }

    /// Unknown and duplicate ACKs are ignored, and iteration is bounded independently of the wire ranges.
    pub fn acknowledge(self: *Recovery, ranges: []const ack.Record, now_ms: u64, maximum_work: usize) !Acknowledged {
        var iterator = ack.SequenceIterator.init(ranges, maximum_work);
        var result: Acknowledged = .{};
        var removed_earliest = false;
        errdefer if (removed_earliest) self.recomputeNextDeadline();
        while (try iterator.next()) |sequence| {
            const removed = self.records.fetchRemove(sequence) orelse continue;
            removed_earliest = removed_earliest or self.next_deadline_ms == removed.value.deadline_ms;
            result.packets += 1;
            result.bytes +|= removed.value.in_flight_bytes;
            if (removed.value.transmissions == 1) result.rtt_sample_ms = now_ms -| removed.value.sent_ms;
            self.total_bytes -= removed.value.data.len;
            self.allocator.free(removed.value.data);
        }
        if (removed_earliest) self.recomputeNextDeadline();
        return result;
    }

    pub fn markNack(self: *Recovery, ranges: []const ack.Record, now_ms: u64, maximum_work: usize) !usize {
        var iterator = ack.SequenceIterator.init(ranges, maximum_work);
        var marked: usize = 0;
        while (try iterator.next()) |sequence| if (self.records.getPtr(sequence)) |record| {
            record.deadline_ms = @min(record.deadline_ms, now_ms);
            self.next_deadline_ms = if (self.next_deadline_ms) |current| @min(current, record.deadline_ms) else record.deadline_ms;
            marked += 1;
        };
        return marked;
    }

    /// Scans at most `maximum_work` records and borrows payloads until the next mutation.
    pub fn collectDue(self: *Recovery, now_ms: u64, rto_ms: u32, output: []Due, maximum_work: usize) DueBatch {
        var count: usize = 0;
        var inspected: usize = 0;
        var exhausted: usize = 0;
        var iterator = self.records.iterator();
        while (iterator.next()) |entry| {
            if (inspected >= maximum_work or count >= output.len) break;
            inspected += 1;
            const record = entry.value_ptr;
            if (record.deadline_ms > now_ms) continue;
            if (record.transmissions >= self.maximum_transmissions) {
                exhausted += 1;
                continue;
            }
            output[count] = .{ .sequence = entry.key_ptr.*, .data = record.data, .in_flight_bytes = record.in_flight_bytes, .timed_out = record.deadline_ms < now_ms };
            count += 1;
            record.transmissions += 1;
            record.deadline_ms = now_ms +| rto_ms;
        }
        self.recomputeNextDeadline();
        return .{ .items = output[0..count], .exhausted = exhausted };
    }

    pub fn nextDeadline(self: Recovery) ?u64 {
        return self.next_deadline_ms;
    }

    fn recomputeNextDeadline(self: *Recovery) void {
        self.next_deadline_ms = null;
        var iterator = self.records.valueIterator();
        while (iterator.next()) |record| {
            self.next_deadline_ms = if (self.next_deadline_ms) |current| @min(current, record.deadline_ms) else record.deadline_ms;
        }
    }
};

test "recovery owns once, ignores duplicate ACKs, and bounds retransmits" {
    var recovery = try Recovery.init(std.testing.allocator, 2, 16, 3);
    defer recovery.deinit();
    try recovery.track(0xffffff, "one", 7, 10, 50);
    try recovery.track(0, "two", 8, 10, 50);
    try std.testing.expectEqual(@as(?u64, 60), recovery.nextDeadline());
    try std.testing.expectError(error.RecoveryFull, recovery.track(1, "x", 1, 10, 50));
    const records = [_]ack.Record{.{ .first = 0xffffff, .last = 0xffffff }};
    const first = try recovery.acknowledge(&records, 30, 4);
    try std.testing.expectEqual(@as(usize, 1), first.packets);
    try std.testing.expectEqual(@as(?u64, 20), first.rtt_sample_ms);
    try std.testing.expectEqual(@as(usize, 0), (try recovery.acknowledge(&records, 40, 4)).packets);
    try std.testing.expectEqual(@as(?u64, 60), recovery.nextDeadline());
    var due: [2]Due = undefined;
    try std.testing.expectEqual(@as(usize, 1), recovery.collectDue(61, 50, &due, 2).items.len);
    try std.testing.expectEqualStrings("two", due[0].data);
    try std.testing.expectEqual(@as(?u64, 111), recovery.nextDeadline());
}
