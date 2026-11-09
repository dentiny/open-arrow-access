#!/usr/bin/env python3
"""Run one case per child; OS CPU/RSS are whole-process, timing excludes setup."""

import argparse, hashlib, json, os, platform, subprocess, tempfile
from pathlib import Path

CASES = [
    "csv",
    "csv-projected",
    "json",
    "json-projected",
    "parquet",
    "parquet-projected",
    "xml",
    "xml-projected",
    "xml-infer",
    "atif",
    "atif-projected",
    "warc",
    "fs",
    "http",
    "hf",
    "connection",
    "fs-ranges",
    "http-ranges",
    "hf-ranges",
    "named-list",
    "named-first",
]
PROVIDERS = [
    "biorxiv",
    "zenodo",
    "dataverse",
    "internet-archive",
    "figshare",
    "bhl",
    "hathifiles",
    "federal-register",
    "govinfo",
    "library-of-congress",
    "openstax",
    "semantic-scholar",
    "regulations",
    "uspto",
    "doab",
    "oapen",
    "arxiv",
    "pmc",
    "gutenberg",
    "courtlistener",
    "meca",
    "arxiv-bulk",
    "commoncrawl",
]
p = argparse.ArgumentParser()
p.add_argument("--source-repo", type=Path, default=Path("."))
p.add_argument("--label", default="baseline")
p.add_argument("--binary", required=True, type=Path)
p.add_argument("--output", required=True, type=Path)
p.add_argument(
    "--cases", nargs="+", default=CASES + ["service-" + s for s in PROVIDERS]
)
p.add_argument("--samples", type=int, default=5)
p.add_argument("--iterations", type=int, default=10)
p.add_argument("--chunk", type=int, default=65536)
p.add_argument("--min-seconds", type=float, default=0.5)
p.add_argument("--batch", type=int, default=1024)
a = p.parse_args()
assert a.samples > 0 and a.iterations > 0
metadata = {
    "platform": platform.platform(),
    "machine": platform.machine(),
    "cpu_count": os.cpu_count(),
    "revision": subprocess.check_output(
        ["git", "-C", str(a.source_repo), "rev-parse", "HEAD"], text=True
    ).strip(),
    "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
    "dirty": bool(
        subprocess.check_output(
            ["git", "-C", str(a.source_repo), "status", "--porcelain"], text=True
        )
    ),
    "binary_sha256": hashlib.sha256(a.binary.read_bytes()).hexdigest(),
    "label": a.label,
}
a.output.parent.mkdir(parents=True, exist_ok=True)
with a.output.open("w") as out:
    for case in a.cases:
        iterations = a.iterations * 100 if case.startswith("service-") else a.iterations
        # Warm-up has its own process and is excluded from the samples.
        for sample in range(-1, a.samples):
            with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
                proc = subprocess.Popen(
                    [
                        str(a.binary.resolve()),
                        case,
                        str(iterations),
                        str(a.chunk),
                        str(a.batch),
                    ],
                    stdout=stdout,
                    stderr=stderr,
                )
                _, status, usage = os.wait4(proc.pid, 0)
                proc.returncode = os.waitstatus_to_exitcode(status)
                stdout.seek(0)
                stderr.seek(0)
                raw = stdout.read().decode()
                if proc.returncode:
                    raise RuntimeError(f"{case}: {stderr.read().decode()}")
                row = json.loads(raw)
                if sample < 0:
                    iterations = max(
                        iterations,
                        min(
                            100000,
                            int(
                                iterations
                                * a.min_seconds
                                / max(row["seconds"], 0.000001)
                            )
                            + 1,
                        ),
                    )
                    continue
                row.update(
                    sample=sample,
                    user_cpu_seconds=usage.ru_utime,
                    system_cpu_seconds=usage.ru_stime,
                    peak_rss_bytes=int(
                        usage.ru_maxrss * (1 if platform.system() == "Darwin" else 1024)
                    ),
                    minor_faults=usage.ru_minflt,
                    major_faults=usage.ru_majflt,
                    input_blocks=usage.ru_inblock,
                    output_blocks=usage.ru_oublock,
                    voluntary_switches=usage.ru_nvcsw,
                    involuntary_switches=usage.ru_nivcsw,
                    environment=metadata,
                )
                out.write(json.dumps(row) + "\n")
                out.flush()
            print(
                f'{case} sample {sample}: {row["seconds"]:.4f}s, RSS {row["peak_rss_bytes"]/1048576:.1f} MiB',
                flush=True,
            )
