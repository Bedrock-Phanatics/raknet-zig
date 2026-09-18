const std = @import("std");
const Hmac = std.crypto.auth.hmac.sha2.HmacSha256;

pub const Jar = struct {
    current_key: [32]u8,
    previous_key: [32]u8,

    pub fn create(self: Jar, endpoint: []const u8, epoch: u64) u32 {
        var epoch_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &epoch_bytes, epoch, .big);
        var state = Hmac.init(&self.current_key);
        state.update("zig-raknet-cookie-v1\x00");
        state.update(endpoint);
        state.update(&epoch_bytes);
        var mac: [Hmac.mac_length]u8 = undefined;
        state.final(&mac);
        return (@as(u32, mac[0]) << 24) | (@as(u32, mac[1]) << 16) | (@as(u32, mac[2]) << 8) | mac[3];
    }

    pub fn verify(self: Jar, cookie: u32, endpoint: []const u8, epoch: u64) bool {
        const keys = [_][32]u8{ self.current_key, self.previous_key };
        const epochs = [_]u64{ epoch, epoch -| 1 };
        var matched: u32 = 0;
        for (keys) |key| for (epochs) |candidate_epoch| {
            var candidate_jar = self;
            candidate_jar.current_key = key;
            matched |= @intFromBool(candidate_jar.create(endpoint, candidate_epoch) == cookie);
        };
        return matched != 0;
    }
};

test "cookies bind endpoint and tolerate one rotation interval" {
    const jar: Jar = .{ .current_key = [_]u8{1} ** 32, .previous_key = [_]u8{2} ** 32 };
    const cookie = jar.create("192.0.2.1:19132", 10);
    try std.testing.expect(jar.verify(cookie, "192.0.2.1:19132", 10));
    try std.testing.expect(jar.verify(cookie, "192.0.2.1:19132", 11));
    try std.testing.expect(!jar.verify(cookie, "192.0.2.2:19132", 10));
    try std.testing.expect(!jar.verify(cookie, "192.0.2.1:19132", 12));
}
