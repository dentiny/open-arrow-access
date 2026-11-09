#!/usr/bin/env python3
"""Summarize raw samples; CPU and elapsed cover the timed scan, RSS covers the process."""
import argparse
import collections
import json
import statistics
from pathlib import Path

KEYS = ('format', 'backend', 'mode', 'rows', 'chunk', 'batch')
def load(path):
    grouped = collections.defaultdict(list)
    for line in Path(path).read_text().splitlines():
        row = json.loads(line)
        grouped[tuple(row[k] for k in KEYS)].append(row)
    return grouped

def median(rows, key):
    return statistics.median(row[key] for row in rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('samples', type=Path)
    parser.add_argument('--compare', type=Path)
    args = parser.parse_args()
    other = load(args.compare) if args.compare else {}
    print('| Format/backend/mode | Rows/chunk/batch | n | Scan ms | CPU ms | First ms | Process RSS MiB | Max batch bytes | Source calls/bytes | HTTP requests/bytes | Change |')
    print('|---|---|---:|---:|---:|---:|---:|---:|---|---|---:|')
    for key, rows in sorted(load(args.samples).items()):
        fmt, backend, mode, count, chunk, batch = key
        elapsed = median(rows, 'elapsed_seconds')
        change = f'{(median(other[key], "elapsed_seconds") / elapsed - 1)*100:+.1f}% throughput' if key in other else ''
        print(f'| {fmt}/{backend}/{mode} | {count}/{chunk}/{batch} | {len(rows)} | {elapsed*1000:.3f} | {median(rows, "cpu_seconds")*1000:.3f} | {median(rows, "first_seconds")*1000:.3f} | {median(rows, "peak_rss_bytes")/2**20:.1f} | {median(rows, "max_batch_bytes"):.0f} | {median(rows,"source_calls"):.0f}/{median(rows,"source_bytes"):.0f} | {median(rows,"http_requests"):.0f}/{median(rows,"http_bytes"):.0f} | {change} |')

if __name__ == '__main__':
    main()
