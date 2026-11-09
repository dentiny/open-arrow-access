#!/usr/bin/env python3
"""Summarize JSONL samples; optionally compare identical workload dimensions."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import random
from statistics import median

DIMENSIONS = (
    "backend",
    "case",
    "chunk",
    "batch",
    "concurrency",
    "io_chunk",
    "projection",
    "iterations",
)


def load(path):
    groups = defaultdict(list)
    metadata = None
    for line in path.read_text().splitlines():
        item = json.loads(line)
        if item["type"] == "metadata":
            metadata = item
        elif not item["warmup"]:
            groups[tuple(item[k] for k in DIMENSIONS)].append(item)
    if metadata is None or not groups:
        raise ValueError(f"no metadata or measured samples: {path}")
    return metadata, groups


def speedup_interval(before, after):
    rng = random.Random(42)
    ratios = sorted(
        median(rng.choices(before, k=len(before)))
        / median(rng.choices(after, k=len(after)))
        for _ in range(2000)
    )
    return ratios[50], ratios[1949]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path, nargs="?")
    args = parser.parse_args()
    meta, baseline = load(args.baseline)
    if args.candidate:
        candidate_meta, candidate = load(args.candidate)
        for key in ("fixture_sha256", "latency_ms", "machine", "platform"):
            if meta[key] != candidate_meta[key]:
                raise ValueError(f"incompatible comparison: {key}")
        if {s["compiler"] for group in baseline.values() for s in group} != {
            s["compiler"] for group in candidate.values() for s in group
        }:
            raise ValueError("compiler versions differ")
        if baseline.keys() != candidate.keys():
            raise ValueError("workload dimensions differ; select matching input files")
        print(
            "| Workload (backend/case/chunk/batch/concurrency/I/O chunk/projection/iterations) | n | Before ms | After ms | Speedup (bootstrap 95% interval) | Before/after peak RSS MiB |"
        )
        print("| --- | ---: | ---: | ---: | ---: | ---: |")
    else:
        print(
            "| Workload (backend/case/chunk/batch/concurrency/I/O chunk/projection/iterations) | n | Median ms (min–max) | Input MiB/s | Process CPU ms | Peak RSS MiB | Logical MiB | HTTP GET/HEAD |"
        )
        print("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for key, samples in sorted(baseline.items()):
        ns = [s["elapsed_ns"] / s["iterations"] for s in samples]
        ms = median(ns) / 1e6
        rss = median(s["process"]["peak_rss_bytes"] for s in samples) / 2**20
        label = "/".join(map(str, key))
        if args.candidate:
            others = candidate[key]
            other_ns = [s["elapsed_ns"] / s["iterations"] for s in others]
            other_ms = median(other_ns) / 1e6
            other_rss = median(s["process"]["peak_rss_bytes"] for s in others) / 2**20
            low, high = speedup_interval(ns, other_ns)
            print(
                f"| {label} | {len(samples)}/{len(others)} | {ms:.3f} | {other_ms:.3f} | {ms/other_ms:.2f}× ({low:.2f}–{high:.2f}) | {rss:.1f}/{other_rss:.1f} |"
            )
        else:
            cpu_ms = (
                median(
                    (s["process"]["user_s"] + s["process"]["system_s"])
                    / s["iterations"]
                    for s in samples
                )
                * 1000
            )
            logical = (
                median(s["logical_io"]["bytes"] / s["iterations"] for s in samples)
                / 2**20
            )
            gets = median(s["http"].get("GET", 0) / s["iterations"] for s in samples)
            heads = median(s["http"].get("HEAD", 0) / s["iterations"] for s in samples)
            throughput = samples[0]["input_bytes"] / 2**20 / (ms / 1000)
            print(
                f"| {label} | {len(samples)} | {ms:.3f} ({min(ns)/1e6:.3f}–{max(ns)/1e6:.3f}) | {throughput:.1f} | {cpu_ms:.2f} | {rss:.1f} | {logical:.2f} | {gets:g}/{heads:g} |"
            )


if __name__ == "__main__":
    main()
