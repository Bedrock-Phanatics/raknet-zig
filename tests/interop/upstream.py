"""Compare listeners against upstream go-raknet v1.15.2 on Linux."""
import argparse
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--server-cpus", default="0")
    parser.add_argument("--client-cpus", default="2,4,6,8")
    args = parser.parse_args()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).resolve().parent
    harness = output / "go"
    harness.mkdir()
    for path in (source / "go").glob("*.go"):
        text = path.read_text()
        # Upstream has no retransmission counter; -1 means unavailable.
        text = text.replace("raknet.MetricsSnapshot()", "struct{ Retransmits int }{-1}")
        (harness / path.name).write_text(text)
    (harness / "go.mod").write_text(
        "module raknet-interop-go\n\ngo 1.22\n\nrequire github.com/sandertv/go-raknet v1.15.2\n")
    subprocess.run(["go", "mod", "tidy"], cwd=harness, check=True)
    binary = output / "go-raknet"
    subprocess.run(["go", "build", "-o", str(binary), "."], cwd=harness, check=True)
    for repeat in range(3):
        for connections in (100, 1000):
            pairs = "zig-go go-go" if repeat % 2 == 0 else "go-go zig-go"
            subprocess.run([
                sys.executable, str(source / "scale.py"), "--zig", args.zig,
                "--go", str(binary), "--pairs", pairs, "--connections", str(connections),
                "--payloads", "128", "--seconds", "10", "--samples", "1",
                "--warmup-ms", "2000", "--window", "1", "--go-cpus", "4",
                "--server-cpus", args.server_cpus, "--client-cpus", args.client_cpus,
                "--output", str(output / f"r{repeat}-n{connections}"),
            ], check=True)


if __name__ == "__main__":
    main()
