"""Compare release binaries in fresh processes; fixture generation is excluded."""

import json
import os
import re
import statistics
import subprocess
import sys
import tempfile


def measure(binary, fixture, projection, concurrency):
    command = [binary, "read", fixture, projection, str(concurrency)]
    with tempfile.TemporaryFile() as output:
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT)
        _, status, usage = os.wait4(process.pid, 0)
        process.returncode = os.waitstatus_to_exitcode(status)
        output.seek(0)
        text = output.read().decode()
    if process.returncode:
        raise RuntimeError(text)
    elapsed = float(re.search(r"elapsed_ms=([0-9.]+)", text).group(1))
    rows = int(re.search(r"rows=([0-9]+)", text).group(1))
    rss_bytes = usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024)
    return {"rows": rows, "elapsed_ms": elapsed, "peak_rss_mib": rss_bytes / 1024**2}


if __name__ == "__main__":
    before, after, fixture = map(os.path.abspath, sys.argv[1:4])
    trials = int(sys.argv[4]) if len(sys.argv) > 4 else 5
    results = []
    for projection in ["narrow", "all"]:
        for concurrency in [1, 4]:
            binaries = [("before", before), ("after", after)]
            for _, binary in binaries:
                measure(binary, fixture, projection, concurrency)
            samples = {label: [] for label, _ in binaries}
            for trial in range(trials):
                for label, binary in binaries[::1 if trial % 2 == 0 else -1]:
                    samples[label].append(measure(binary, fixture, projection, concurrency))
            for label, values in samples.items():
                assert len({v["rows"] for v in values}) == 1
                results.append({
                    "version": label,
                    "projection": projection,
                    "concurrency": concurrency,
                    "rows": values[0]["rows"],
                    "median_elapsed_ms": statistics.median(v["elapsed_ms"] for v in values),
                    "median_peak_rss_mib": statistics.median(v["peak_rss_mib"] for v in values),
                    "samples": values,
                })
    print(json.dumps(results, indent=2))
