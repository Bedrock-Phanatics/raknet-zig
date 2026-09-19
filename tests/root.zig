const raknet = @import("raknet");

test {
    _ = raknet;
    _ = @import("codec_fuzz.zig");
    _ = @import("session_fuzz.zig");
    _ = @import("network_simulation.zig");
    _ = @import("security_regression.zig");
}
