import argparse
import json
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
            args.zig = "candidate"
            args.send_batch = 128
            assert scale.server_command(args, "zig")[5:] == ["1", "0", "32", "128"]
            args.zig = "baseline"
            assert scale.server_command(args, "zig", is_baseline=True)[5:] == ["1", "0", "32"]
            args.receive_buffer = 65536
            assert scale.server_command(args, "zig", is_baseline=True)[5:] == ["1", "0", "32", "64", "65536"]
            args.zig = "candidate"
            assert scale.server_command(args, "zig")[5:] == ["1", "0", "32", "128", "65536"]
            assert len(scale.server_command(args, "go")) == 5
            args.zig, args.send_batch, args.receive_buffer = str(fake), 128, None
            good = scale.run_case(args, "zig", "go", 1, 128, 0, root, is_baseline=True)
            assert good["valid"] and good["client_exit"] == 0 and good["send_batch"] is None, good
            bad = scale.run_case(args, "zig", "go", 1, 32, 0, root)
            assert not bad["valid"] and bad["failures"] == 1 and bad["send_batch"] == 128, bad
            args.churn_rounds = 2
            with patch.object(scale.time, "sleep", lambda _: None):
                for baseline in (False, True):
                    rows = list(scale.run_rounds(args, "zig", "go", 1, 128, 1 + int(baseline), root, baseline))
                    assert len(rows) == 2 and all(r["valid"] for r in rows), rows
                    assert [r["cohort"] for r in rows] == [0, 1]
                    assert all(r["send_batch"] == (None if baseline else 128) for r in rows), rows
            output = root / "resume"
            argv = ["scale.py", "--zig", str(fake), "--go", str(fake), "--pairs", "zig-go", "--connections", "1", "--payloads", "128", "--seconds", "1", "--samples", "1", "--warmup-ms", "0", "--output", str(output)]
            argv += ["--baseline-zig", str(fake), "--send-batch", "128"]
            with patch.object(sys, "argv", argv):
                scale.main()
            saved = (output / "samples.jsonl").read_bytes()
            rows = [json.loads(line) for line in saved.splitlines()]
            assert [(row["variant"], row["send_batch"]) for row in rows] == [("before", None), ("after", 128)], rows
            with patch.object(sys, "argv", argv + ["--resume"]), patch.object(scale, "run_case", side_effect=AssertionError("completed sample reran")):
                scale.main()
            assert saved == (output / "samples.jsonl").read_bytes()
            config_path = output / "config.json"
            config = json.loads(config_path.read_text())
            del config["send_batch_candidate_only"]
            config_path.write_text(json.dumps(config))
            with patch.object(sys, "argv", argv + ["--resume"]):
                try:
                    scale.main()
                except SystemExit as error:
                    assert error.code == 2
                else:
                    raise AssertionError("old send-batch semantics resumed")
        assert scale.fields("client rate=1.25 failures=0 impl=go") == {"rate": 1.25, "failures": 0, "impl": "go"}
    print("scale runner checks passed")


if __name__ == "__main__":
    main()
