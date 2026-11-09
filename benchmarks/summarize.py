#!/usr/bin/env python3
"""Summarize median-of-process-medians without treating repeated scans as independent processes."""
import json
from collections import defaultdict
from pathlib import Path
import statistics
import sys

def load(path):
    groups = defaultdict(list)
    for line in Path(path).read_text().splitlines():
        item = json.loads(line)
        if 'case' in item:
            groups[item['case'], item['backend']].append(item)
    return groups

def main():
    before = load(sys.argv[1])
    after = load(sys.argv[2]) if len(sys.argv) > 2 else None
    print('| Case | Backend | Before ms | After ms | Speedup | Peak RSS MiB (before → after) |')
    print('| --- | --- | ---: | ---: | ---: | ---: |')
    for key, items in before.items():
        median = lambda rows, column: statistics.median(r[column] for r in rows)
        seconds, rss = median(items, 'median_seconds'), median(items, 'process_peak_rss_bytes')/1048576
        if after and key in after:
            new = after[key]
            new_seconds, new_rss = median(new, 'median_seconds'), median(new, 'process_peak_rss_bytes')/1048576
            print(f'| {key[0]} | {key[1]} | {seconds*1000:.3f} | {new_seconds*1000:.3f} | {seconds/new_seconds:.2f}× | {rss:.1f} → {new_rss:.1f} |')
        else:
            print(f'| {key[0]} | {key[1]} | {seconds*1000:.3f} | — | — | {rss:.1f} |')

if __name__ == '__main__':
    main()
