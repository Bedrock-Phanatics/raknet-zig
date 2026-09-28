import argparse
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

sys.dont_write_bytecode = True
import scale


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        fake = root / "peer.py"
        fake.write_text('''import sys, time
if sys.argv[1] == "server":
    time.sleep(30)
else:
    print("phase name=ready ramp_us=100", flush=True)
    print("phase name=measure_start", flush=True)
    time.sleep(1)
    print("phase name=measure_end", flush=True)
    failures = int(sys.argv[4] == "32")
    print(f"client msgs_per_s=10 failures={failures} incomplete=0 mismatches=0", flush=True)
''')
        args = argparse.Namespace(seconds=1, warmup_ms=0, ramp_timeout=2, window=1,
                                  interval_ms=0, listeners=1, ack_ms=0, receive_batch=32,
                                  send_batch=None, receive_buffer=None, go_cpus=1, host="127.0.0.1", port=19400,
                                  zig=str(fake), client_zig=str(fake), go=str(fake),
                                  server_cpus="", client_cpus="", churn_rounds=1)
        def command(binary, cpus, *argv):
            return [sys.executable, binary, *map(str, argv)]
        with patch.object(scale, "command", command):
            good = scale.run_case(args, "zig", "go", 1, 128, 0, root)
            assert good["valid"] and good["client_exit"] == 0, good
            bad = scale.run_case(args, "zig", "go", 1, 32, 0, root)
            assert not bad["valid"] and bad["failures"] == 1, bad
            args.churn_rounds = 2
            with patch.object(scale.time, "sleep", lambda _: None):
                rows = list(scale.run_rounds(args, "zig", "go", 1, 128, 1, root))
            assert len(rows) == 2 and all(r["valid"] for r in rows), rows
            assert [r["cohort"] for r in rows] == [0, 1]
            output = root / "resume"
            argv = ["scale.py", "--zig", str(fake), "--go", str(fake), "--pairs", "zig-go", "--connections", "1", "--payloads", "128", "--seconds", "1", "--samples", "1", "--warmup-ms", "0", "--output", str(output)]
            with patch.object(sys, "argv", argv):
                scale.main()
            saved = (output / "samples.jsonl").read_bytes()
            with patch.object(sys, "argv", argv + ["--resume"]), patch.object(scale, "run_case", side_effect=AssertionError("completed sample reran")):
                scale.main()
            assert saved == (output / "samples.jsonl").read_bytes()
        assert scale.fields("client rate=1.25 failures=0 impl=go") == {"rate": 1.25, "failures": 0, "impl": "go"}
    print("scale runner checks passed")


if __name__ == "__main__":
    main()
