#!/usr/bin/env python3
"""Alternate isolated baseline/candidate processes; compare normalized scan times."""

import argparse, hashlib, json, os, platform, random, statistics, subprocess, tempfile
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("before", type=Path)
p.add_argument("after", type=Path)
p.add_argument("--case", required=True)
p.add_argument("--iterations", type=int, default=3)
p.add_argument("--samples", type=int, default=10)
p.add_argument("--output", required=True, type=Path)
a = p.parse_args()
assert a.iterations > 0 and a.samples > 0
binaries = {"before": a.before.resolve(), "after": a.after.resolve()}
hashes = {
    name: hashlib.sha256(path.read_bytes()).hexdigest()
    for name, path in binaries.items()
}
rows = []
with a.output.open("w") as out:
    for sample in range(-1, a.samples):
        for name in (["before", "after"] if sample % 2 == 0 else ["after", "before"]):
            with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
                proc = subprocess.Popen(
                    [str(binaries[name]), a.case, str(a.iterations)],
                    stdout=stdout,
                    stderr=stderr,
                )
                _, status, usage = os.wait4(proc.pid, 0)
                proc.returncode = os.waitstatus_to_exitcode(status)
                stdout.seek(0)
                stderr.seek(0)
                if proc.returncode:
                    raise RuntimeError(stderr.read().decode())
                row = json.loads(stdout.read())
                if sample < 0:
                    continue
                row.update(
                    variant=name,
                    sample=sample,
                    binary_sha256=hashes[name],
                    user_cpu_seconds=usage.ru_utime,
                    system_cpu_seconds=usage.ru_stime,
                    peak_rss_bytes=usage.ru_maxrss
                    * (1 if platform.system() == "Darwin" else 1024),
                )
                rows.append(row)
                out.write(json.dumps(row) + "\n")
                out.flush()
        print(f"{a.case}: completed sample {sample}", flush=True)
values = {
    name: [r["seconds"] / r["iterations"] for r in rows if r["variant"] == name]
    for name in binaries
}
rng = random.Random(42)
ratios = sorted(
    statistics.median(rng.choices(values["before"], k=a.samples))
    / statistics.median(rng.choices(values["after"], k=a.samples))
    for _ in range(5000)
)
print(
    json.dumps(
        {
            "before_ms": statistics.median(values["before"]) * 1000,
            "after_ms": statistics.median(values["after"]) * 1000,
            "speedup": statistics.median(values["before"])
            / statistics.median(values["after"]),
            "bootstrap_95": [ratios[125], ratios[4874]],
        }
    )
)
