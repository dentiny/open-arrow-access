#!/usr/bin/env python3
"""Run each workload in a fresh process; retain raw samples and process rusage.

Build first: cargo bench --bench performance --all-features --no-run
Usage: python3 scripts/benchmark.py --binary target/release/deps/performance-HASH --output baseline.json
"""
import argparse
import hashlib
import tempfile
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--cases', nargs='*')
    parser.add_argument('--rows', type=int, default=20000)
    parser.add_argument('--rounds', type=int, default=7)
    parser.add_argument('--chunk', type=int, default=65536)
    parser.add_argument('--batch', type=int, default=1024)
    args = parser.parse_args()
    binary = args.binary.resolve()
    env = dict(os.environ, BENCH_ROWS=str(args.rows), BENCH_ROUNDS=str(args.rounds), BENCH_CHUNK=str(args.chunk), BENCH_BATCH=str(args.batch))
    cases = args.cases or subprocess.check_output([binary, '--list'], text=True).splitlines()
    output = {'platform': platform.platform(), 'python': sys.version, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'working_tree_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(), 'dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'])), 'configuration': vars(args) | {'binary': str(binary), 'output': str(args.output)}, 'cases': {}}
    for case in cases:
        # wait4 attributes CPU/RSS to this child only (RUSAGE_CHILDREN is cumulative).
        with tempfile.TemporaryFile(mode='w+t') as stdout, tempfile.TemporaryFile(mode='w+t') as stderr:
            started = time.monotonic()
            process = subprocess.Popen([binary, case], env=env, stdout=stdout, stderr=stderr)
            _, status, usage = os.wait4(process.pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
            stdout.seek(0); stderr.seek(0)
            if process.returncode:
                raise RuntimeError(f'{case}: {stderr.read()}')
            samples = [json.loads(line) for line in stdout if line.strip()]
            median = statistics.median(sample['seconds'] for sample in samples)
            output['cases'][case] = {'median_seconds': median, 'min_seconds': min(s['seconds'] for s in samples), 'max_seconds': max(s['seconds'] for s in samples), 'process_wall_seconds': time.monotonic() - started, 'process_user_seconds': usage.ru_utime, 'process_system_seconds': usage.ru_stime, 'process_max_rss_bytes': usage.ru_maxrss if sys.platform == 'darwin' else usage.ru_maxrss * 1024, 'samples': samples}
            print(f'{case:30} {median * 1000:10.3f} ms; peak process RSS {output["cases"][case]["process_max_rss_bytes"] / 2**20:.1f} MiB', flush=True)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(output, indent=2, default=str) + '\n')


if __name__ == '__main__':
    main()
