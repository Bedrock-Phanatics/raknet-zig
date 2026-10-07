#!/usr/bin/env python3
"""Benchmark interop peers and sample CPU and memory during each run."""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import platform
import queue
import statistics
import subprocess
import threading
import time


def fields(line):
    result = {}
    for word in line.split()[1:]:
        if "=" not in word:
            continue
        key, value = word.split("=", 1)
        try:
            result[key] = float(value) if "." in value else int(value)
        except ValueError:
            result[key] = value
    return result


def process_sample(pid):
    try:
        if os.name == "nt":
            from ctypes import wintypes
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            kernel.OpenProcess.restype = wintypes.HANDLE
            kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.c_void_p] * 4
            handle = kernel.OpenProcess(0x410, False, pid)
            if not handle:
                return None
            try:
                times = (ctypes.c_ulonglong * 4)()
                if not kernel.GetProcessTimes(handle, *(ctypes.byref(times, i * 8) for i in range(4))):
                    return None
                class Memory(ctypes.Structure):
                    _fields_ = [("cb", wintypes.DWORD), ("faults", wintypes.DWORD)] + [(name, ctypes.c_size_t) for name in ("peak", "rss", "a", "b", "c", "d", "e", "f")]
                memory = Memory()
                memory.cb = ctypes.sizeof(memory)
                psapi = ctypes.WinDLL("psapi")
                psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
                if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(memory), memory.cb):
                    return None
                return {"cpu_ms": (times[2] + times[3]) / 10000, "rss_kb": memory.rss / 1024}
            finally:
                kernel.CloseHandle(handle)
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        rss = int(stat[21]) * os.sysconf("SC_PAGE_SIZE") / 1024
        cpu = (int(stat[11]) + int(stat[12])) * 1000 / os.sysconf("SC_CLK_TCK")
        return {"cpu_ms": cpu, "rss_kb": rss}
    except (OSError, ValueError, IndexError):
        return None


def udp_sample(pid, port):
    if os.name == "nt":
        return None
    try:
        sockets = [line.split() for line in Path(f"/proc/{pid}/net/udp").read_text().splitlines()[1:]]
        sockets = [s for s in sockets if int(s[1].split(":")[1], 16) == port]
        return {"drops": sum(int(s[-1]) for s in sockets),
                "queued_bytes": sum(int(s[4].split(":")[1], 16) for s in sockets)}
    except (OSError, ValueError, IndexError):
        return None


def command(binary, cpus, *args):
    cmd = [str(Path(binary).resolve()), *map(str, args)]
    return ["taskset", "-c", cpus, *cmd] if cpus and os.name != "nt" else cmd


def server_command(args, kind, is_baseline=False):
    lifetime = args.seconds + (args.warmup_ms + 999) // 1000 + args.ramp_timeout + 10
    argv = ["server", f"{args.host}:{args.port}", lifetime * args.churn_rounds]
    if kind == "zig":
        argv += [args.listeners, args.ack_ms, args.receive_batch]
        send_batch = None if is_baseline else args.send_batch
        if send_batch is not None or args.receive_buffer is not None:
            argv += [send_batch or 64]
        if args.receive_buffer is not None:
            argv += [args.receive_buffer]
    return command(args.zig if kind == "zig" else args.go, args.server_cpus, *argv)


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def run_case(args, server_kind, client_kind, connections, payload, repeat, output, existing_server=None, is_baseline=False):
    name = f"{server_kind}-{client_kind}-n{connections}-p{payload}-r{repeat}"
    events = queue.Queue()
    processes, readers = [], []

    def launch(role, cmd):
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                env={**os.environ, "GOMAXPROCS": str(args.go_cpus)})
        processes.append(proc)
        def read():
            with (output / f"{name}-{role}.log").open("w") as log:
                for line in proc.stdout:
                    log.write(line)
                    log.flush()
                    events.put((role, line.strip()))
            events.put((role, None))
        reader = threading.Thread(target=read)
        reader.start()
        readers.append(reader)
        return proc

    binaries = {"zig": args.zig, "go": args.go}
    address = f"{args.host}:{args.port}"
    lifetime = args.seconds + (args.warmup_ms + 999) // 1000 + args.ramp_timeout + 10
    row = dict(server=server_kind, client=client_kind, connections=connections, payload=payload,
               repeat=repeat, seconds=args.seconds, warmup_ms=args.warmup_ms, window=args.window,
               interval_ms=args.interval_ms, listeners=args.listeners, ack_ms=args.ack_ms,
               receive_batch=args.receive_batch, send_batch=None if is_baseline else args.send_batch,
               platform=platform.platform(), valid=False)
    phases, snapshots, network = {}, [], []
    final, fairness, ready = {}, {}, {}
    try:
        server = existing_server or launch("server", server_command(args, server_kind, is_baseline))
        time.sleep(0.5)
        baseline = process_sample(server.pid)
        client_binary = args.client_zig if client_kind == "zig" else binaries[client_kind]
        client = launch("client", command(client_binary, args.client_cpus, "client", address,
                        connections, payload, args.seconds, args.warmup_ms, args.window, args.interval_ms))
        deadline = time.monotonic() + lifetime
        while time.monotonic() < deadline:
            try:
                role, line = events.get(timeout=0.1)
            except queue.Empty:
                if server.poll() is not None:
                    break
                continue
            if line is None:
                if role == "client":
                    break
                continue
            if role == "server" and line.startswith("progress "):
                snapshots.append(dict(time=time.monotonic(), **fields(line)))
            if role == "server" and line.startswith("network "):
                network.append(dict(time=time.monotonic(), **fields(line)))
            if role != "client":
                continue
            if line.startswith("phase "):
                event = fields(line)
                phases[event["name"]] = dict(time=time.monotonic(), server=process_sample(server.pid), client=process_sample(client.pid), udp=udp_sample(server.pid, args.port))
                if event["name"] == "ready":
                    ready = event
            elif line.startswith("client "):
                final = fields(line)
            elif line.startswith("fairness "):
                fairness = fields(line)
        row.update(final)
        row.update(fairness)
        row.update({k: v for k, v in ready.items() if k != "name"})
        row["server"] = server_kind
        row["client"] = client_kind
        if "measure_start" in phases and "measure_end" in phases:
            start, end = phases["measure_start"], phases["measure_end"]
            elapsed = end["time"] - start["time"]
            row["measured_seconds"] = elapsed
            if start["udp"] and end["udp"]:
                row["server_kernel_receive_drops"] = end["udp"]["drops"] - start["udp"]["drops"]
            for role in ("server", "client"):
                if start[role] and end[role]:
                    cpu = end[role]["cpu_ms"] - start[role]["cpu_ms"]
                    row[f"{role}_cpu_ms"] = cpu
                    row[f"{role}_cpu_percent"] = cpu / (elapsed * 10)
                    row[f"{role}_rss_kb"] = max(start[role]["rss_kb"], end[role]["rss_kb"])
            if baseline and "server_rss_kb" in row:
                row["server_kb_per_requested_connection"] = max(0, row["server_rss_kb"] - baseline["rss_kb"]) / connections
            row["snapshots"] = [s for s in snapshots if start["time"] <= s["time"] <= end["time"]]
            row["network"] = [s for s in network if start["time"] <= s["time"] <= end["time"]]
            row["valid"] = bool(final) and abs(elapsed - args.seconds) < max(0.25, args.seconds * .05) and all(final.get(k) == 0 for k in ("failures", "incomplete", "mismatches"))
            latest, populations = {}, []
            for snapshot in row["snapshots"]:
                latest[snapshot.get("shard", 0)] = snapshot["sessions"]
                if len(latest) == (args.listeners if server_kind == "zig" else 1):
                    populations.append(sum(latest.values()))
            if populations:
                row["minimum_sessions"] = min(populations)
                row["valid"] = row["valid"] and row["minimum_sessions"] >= connections
            if row.get("msgs_per_s", 0) and "server_cpu_ms" in row:
                row["server_cpu_ns_per_message"] = row["server_cpu_ms"] * 1e6 / (row["msgs_per_s"] * args.seconds)
        row["phases"] = phases
        try:
            row["client_exit"] = client.wait(timeout=5) if client.poll() is not None or final else None
        except subprocess.TimeoutExpired:
            row["client_exit"] = None
        row["valid"] = row["valid"] and row["client_exit"] == 0 and server.poll() is None
    finally:
        for proc in processes:
            stop(proc)
        for reader in readers:
            reader.join()
    return row


def run_rounds(args, server, client, connections, payload, repeat, output, is_baseline=False):
    if args.churn_rounds == 1:
        yield run_case(args, server, client, connections, payload, repeat, output, is_baseline=is_baseline)
        return
    name = f"{server}-{client}-n{connections}-p{payload}-r{repeat}-churn-server.log"
    with (output / name).open("w") as log:
        proc = subprocess.Popen(server_command(args, server, is_baseline), stdout=log, stderr=subprocess.STDOUT,
                                env={**os.environ, "GOMAXPROCS": str(args.go_cpus)})
        try:
            for cohort in range(args.churn_rounds):
                row = run_case(args, server, client, connections, payload, f"{repeat}-c{cohort}", output, proc, is_baseline)
                row["cohort"] = cohort
                if cohort + 1 == args.churn_rounds:
                    time.sleep(11)  # Allow the 10-second idle timeout to expire.
                    row["cleanup_process"] = process_sample(proc.pid)
                    latest = {}
                    for line in (output / name).read_text().splitlines():
                        if line.startswith("progress "):
                            snapshot = fields(line)
                            latest[snapshot.get("shard", 0)] = snapshot["sessions"]
                    if latest:
                        row["cleanup_sessions"] = sum(latest.values())
                        row["valid"] = row["valid"] and row["cleanup_sessions"] == 0
                yield row
        finally:
            stop(proc)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig-out/bin/raknet-interop")
    parser.add_argument("--baseline-zig", help="interleave this saved server with --zig, using the same client")
    parser.add_argument("--client-zig", help="common Zig generator (defaults to --zig)")
    parser.add_argument("--go", default="zig-out/bin/raknet-interop-go")
    parser.add_argument("--pairs", default="zig-go go-go go-zig zig-zig")
    parser.add_argument("--connections", default="1 10 100 500 1000 2000 4096")
    parser.add_argument("--payloads", default="32 128 512 1200 8192", help="space-separated bytes; 0 cycles through all five sizes")
    parser.add_argument("--seconds", type=int, default=20)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--churn-rounds", type=int, default=1, help="client cohorts on one persistent server per sample")
    parser.add_argument("--warmup-ms", type=int, default=3000)
    parser.add_argument("--ramp-timeout", type=int, default=20)
    parser.add_argument("--window", type=int, default=32, help="0 idle, 1 request/reply, 32 saturation")
    parser.add_argument("--interval-ms", type=int, default=0, help="per-connection pacing")
    parser.add_argument("--listeners", type=int, default=1)
    parser.add_argument("--ack-ms", type=int, default=0)
    parser.add_argument("--receive-batch", type=int, default=32)
    parser.add_argument("--send-batch", type=int, help="candidate server only: 1..256, default 64")
    parser.add_argument("--receive-buffer", type=int, help="Linux/Windows socket receive bytes; kernel capacity is logged where available")
    parser.add_argument("--server-cpus", default="")
    parser.add_argument("--client-cpus", default="")
    parser.add_argument("--go-cpus", type=int, default=4)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19400)
    parser.add_argument("--output", required=True)
    parser.add_argument("--resume", action="store_true", help="reuse completed samples after verifying configuration and binary hashes")
    args = parser.parse_args()
    args.client_zig = args.client_zig or args.zig
    if args.send_batch is not None and not 1 <= args.send_batch <= 256:
        parser.error("send batch must be in 1..256")
    if args.seconds <= 0 or args.samples <= 0 or args.churn_rounds <= 0 or not 0 <= args.window <= 32:
        parser.error("seconds/samples must be positive and window must be in 0..32")
    if args.warmup_ms < 0 or args.interval_ms < 0 or args.ramp_timeout <= 0 or args.listeners <= 0 or args.go_cpus <= 0 or args.ack_ms < 0 or not 1 <= args.receive_batch <= 256:
        parser.error("invalid timing, CPU, listener, or batch limits")
    try:
        if not args.connections.split() or any(int(n) <= 0 for n in args.connections.split()):
            raise ValueError()
        if not args.payloads.split() or any(int(n) != 0 and not 16 <= int(n) <= 1048576 for n in args.payloads.split()):
            raise ValueError()
        if not args.pairs.split() or any(p not in ("zig-go", "go-go", "go-zig", "zig-zig") for p in args.pairs.split()):
            raise ValueError()
    except ValueError:
        parser.error("invalid connection count, payload size, or implementation pair")
    output = Path(args.output)
    binaries = [args.zig, args.client_zig, args.go] + ([args.baseline_zig] if args.baseline_zig else [])
    hashes = {name: hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in binaries}
    config = {k: v for k, v in vars(args).items() if k != "resume"}
    config["send_batch_candidate_only"] = True
    rows = []
    if args.resume and output.exists():
        if args.churn_rounds != 1:
            parser.error("churn must use a new output directory to preserve one server per sample")
        saved_config = json.loads((output / "config.json").read_text())
        saved_config.setdefault("receive_buffer", None)
        if saved_config != config or json.loads((output / "binaries.json").read_text()) != hashes:
            parser.error("resume requires identical configuration and binaries")
        rows = [json.loads(line) for line in (output / "samples.jsonl").read_text().splitlines()]
    else:
        output.mkdir(parents=True, exist_ok=False)
        (output / "config.json").write_text(json.dumps(config, indent=2))
        (output / "binaries.json").write_text(json.dumps(hashes, indent=2))
    completed = {(r["variant"], r["server"], r["client"], r["connections"], r["payload"], r["repeat"]) for r in rows}
    with (output / "samples.jsonl").open("a") as log:
        for pair in args.pairs.split():
            server, client = pair.split("-")
            for connections in map(int, args.connections.split()):
                for payload in map(int, args.payloads.split()):
                    for repeat in range(args.samples):
                        candidate = args.zig
                        variants = [("after", candidate)]
                        if args.baseline_zig and server == "zig":
                            variants.insert(0, ("before", args.baseline_zig))
                            if repeat % 2: variants.reverse()
                        for variant, binary in variants:
                            args.zig = binary
                            if (variant, server, client, connections, payload, repeat) in completed:
                                continue
                            destination = output / variant
                            destination.mkdir(exist_ok=True)
                            for row in run_rounds(args, server, client, connections, payload, repeat, destination, is_baseline=variant == "before"):
                                row["variant"] = variant
                                rows.append(row)
                                log.write(json.dumps(row) + "\n")
                                log.flush()
                                print(f"{variant} {pair} n={connections} p={payload} sample={row['repeat']} valid={row['valid']} msgs/s={row.get('msgs_per_s')} p99={row.get('rtt_p99_us')} cpu={row.get('server_cpu_percent', 0):.1f}% failures={row.get('failures')}", flush=True)
                        args.zig = candidate
    keys = ("msgs_per_s", "mib_per_s", "server_cpu_percent", "client_cpu_percent", "rtt_p50_us", "rtt_p95_us", "rtt_p99_us", "server_rss_kb", "server_kb_per_requested_connection", "retransmits", "failures", "incomplete", "jain")
    summary = []
    for variant, server, client, n, payload in dict.fromkeys((r['variant'], r['server'], r['client'], r['connections'], r['payload']) for r in rows):
        group = [r for r in rows if (r['variant'], r['server'], r['client'], r['connections'], r['payload']) == (variant, server, client, n, payload)]
        summary.append(dict(variant=variant, server=server, client=client, connections=n, payload=payload, valid_samples=sum(r['valid'] for r in group), **{key: statistics.median(r[key] for r in group if key in r) for key in keys if any(key in r for r in group)}))
    (output / "medians.json").write_text(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
