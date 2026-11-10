#!/usr/bin/env python3
"""Summarize raw read_matrix CSV; medians, dispersion and warm-cache throughput."""
import csv
import statistics
import sys
from collections import defaultdict

groups = defaultdict(list)
for row in csv.DictReader(open(sys.argv[1])):
    key = tuple(row[k] for k in ('format', 'dataset', 'native', 'workers'))
    groups[key].append(row)
writer = csv.writer(sys.stdout)
writer.writerow(['format', 'dataset', 'native', 'workers', 'samples', 'median_ms',
                 'min_ms', 'max_ms', 'median_first_batch_ms', 'million_rows_per_second'])
for key, rows in groups.items():
    seconds = [float(r['seconds']) for r in rows]
    median = statistics.median(seconds)
    writer.writerow([*key, len(rows), median * 1000, min(seconds) * 1000,
                     max(seconds) * 1000,
                     statistics.median(float(r['first_batch_seconds']) for r in rows) * 1000,
                     int(rows[0]['rows']) / median / 1e6])
