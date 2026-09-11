const raknet = @import("raknet");

test {
    _ = raknet;
    _ = @import("codec_fuzz.zig");
}
