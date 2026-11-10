#!/usr/bin/env python3
"""Flatten repeated JSONL samples into a CSV comparison table (standard library only)."""

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import median


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    groups = defaultdict(list)
    for source in args.inputs:
        for line in Path(source).read_text().splitlines():
            row = json.loads(line)
            if row.get("type") == "environment":
                continue
            key = (
                Path(source).name,
                row.get("binary", "baseline"),
                row.get("format", row.get("provider")),
                row.get("backend", "in-process"),
                row.get("mode", "operation"),
                row.get("workers", 1),
            )
            groups[key].append(row)
    fields = [
        "source",
        "binary",
        "format_or_provider",
        "backend",
        "mode",
        "workers",
        "samples",
        "median_seconds",
        "min_seconds",
        "max_seconds",
        "median_rows_per_second",
        "median_us_per_operation",
        "median_process_cpu_seconds",
        "median_process_seconds",
        "median_peak_rss_bytes",
        "max_peak_rss_bytes",
        "median_first_batch_seconds",
        "median_http_requests",
        "median_http_payload_bytes",
        "median_batches",
        "max_batch_arrow_bytes",
        "median_input_blocks",
        "median_output_blocks",
        "median_allocation_calls",
        "median_reallocation_calls",
        "median_allocated_bytes",
        "median_deallocated_bytes",
    ]
    with output.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for key, rows in sorted(groups.items()):
            row = dict(zip(fields[:6], key))

            def med(field):
                values = [r[field] for r in rows if r.get(field) is not None]
                return median(values) if values else ""

            row.update(
                samples=len(rows),
                median_seconds=med("seconds"),
                min_seconds=min(r["seconds"] for r in rows),
                max_seconds=max(r["seconds"] for r in rows),
                median_process_cpu_seconds=median(
                    r["cpu_user_seconds"] + r["cpu_system_seconds"] for r in rows
                ),
                median_process_seconds=med("process_seconds"),
                median_peak_rss_bytes=med("peak_rss_bytes"),
                max_peak_rss_bytes=max(r["peak_rss_bytes"] for r in rows),
            )
            row["median_rows_per_second"] = (
                median(r["rows"] / r["seconds"] for r in rows)
                if "rows" in rows[0]
                else ""
            )
            row["median_us_per_operation"] = (
                median(r["seconds"] / r["iterations"] * 1e6 for r in rows)
                if "iterations" in rows[0]
                else ""
            )
            for field in [
                "first_batch_seconds",
                "http_requests",
                "http_payload_bytes",
                "batches",
                "input_blocks",
                "output_blocks",
                "allocation_calls",
                "reallocation_calls",
                "allocated_bytes",
                "deallocated_bytes",
            ]:
                row[f"median_{field}"] = med(field)
            row["max_batch_arrow_bytes"] = max(
                (r.get("max_batch_arrow_bytes", 0) for r in rows), default=0
            )
            writer.writerow(row)


if __name__ == "__main__":
    main()
