const std = @import("std");

fn module(b: *std.Build, path: []const u8, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) *std.Build.Module {
    return b.createModule(.{ .root_source_file = b.path(path), .target = target, .optimize = optimize });
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const fuzz_iterations = b.option(usize, "fuzz-iterations", "Deterministic malformed inputs") orelse 20_000;

    const options = b.addOptions();
    options.addOption(usize, "fuzz_iterations", fuzz_iterations);

    const raknet_module = b.addModule("raknet", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const test_root = module(b, "tests/root.zig", target, optimize);
    test_root.addImport("raknet", raknet_module);
    test_root.addOptions("build_options", options);
    const tests = b.addTest(.{ .root_module = test_root });
    const run_tests = b.addRunArtifact(tests);
    b.step("test", "Run unit, adversarial, and deterministic fuzz tests").dependOn(&run_tests.step);
    const library_tests = b.addTest(.{ .root_module = module(b, "src/root.zig", target, optimize) });
    run_tests.step.dependOn(&b.addRunArtifact(library_tests).step);
    b.default_step.dependOn(&run_tests.step);

    const bench_root = module(b, "bench/main.zig", target, .ReleaseFast);
    bench_root.addImport("raknet", module(b, "src/root.zig", target, .ReleaseFast));
    const bench = b.addExecutable(.{ .name = "raknet-bench", .root_module = bench_root });
    b.step("bench", "Run codec, scheduler, and receive-window microbenchmarks").dependOn(&b.addRunArtifact(bench).step);
}
