#!/usr/bin/env python3
"""Run one benchmark per child process; preserve raw timings and OS resource counters."""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import statistics
import subprocess
import tempfile
import time


def run_child(command):
    # Files avoid pipe deadlocks while wait4 collects this specific child's usage.
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        start = time.perf_counter()
        proc = subprocess.Popen(command, stdout=out, stderr=err)
        _, status, usage = os.wait4(proc.pid, 0)
        proc.returncode = os.waitstatus_to_exitcode(status)
        wall = time.perf_counter() - start
        out.seek(0)
        err.seek(0)
        stdout, stderr = out.read().decode(), err.read().decode()
        if proc.returncode:
            raise RuntimeError(f"{command}: exit {proc.returncode}\n{stderr}\n{stdout}")
        result = json.loads(stdout)
        seconds = [s['seconds'] for s in result['samples']]
        result.update(median_seconds=statistics.median(seconds),
                      min_seconds=min(seconds), max_seconds=max(seconds),
                      process_wall_seconds=wall, process_user_seconds=usage.ru_utime,
                      process_system_seconds=usage.ru_stime,
                      process_peak_rss_bytes=usage.ru_maxrss * (1 if platform.system() == 'Darwin' else 1024),
                      process_block_inputs=usage.ru_inblock,
                      process_block_outputs=usage.ru_oublock,
                      process_voluntary_switches=usage.ru_nvcsw,
                      process_involuntary_switches=usage.ru_nivcsw)
        return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--cases', help='comma-separated; default: every case')
    p.add_argument('--backends', default='memory,fs,http,s3')
    p.add_argument('--rows', type=int, default=32768)
    p.add_argument('--iterations', type=int, default=7)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--width', type=int, default=128)
    p.add_argument('--chunk', type=int, default=65536)
    p.add_argument('--batch', type=int, default=1024)
    p.add_argument('--streaming', action='store_true')
    p.add_argument('--http-chunk', type=int, default=16384)
    p.add_argument('--latency-ms', type=int, default=0)
    p.add_argument('--concurrency', type=int, default=1)
    p.add_argument('--io-chunk', type=int, default=2097152)
    args = p.parse_args()
    binary = str(args.binary.resolve())
    cases = args.cases.split(',') if args.cases else [c for c in subprocess.check_output([binary, '--list'], text=True).splitlines() if c != 'atif-tools']
    metadata = {'binary_sha256': hashlib.sha256(Path(binary).read_bytes()).hexdigest(), 'platform': platform.platform(), 'machine': platform.machine(),
                'cpu_count': os.cpu_count(), 'rustc': subprocess.check_output(['rustc', '-Vv'], text=True),
                'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                'diff': subprocess.check_output(['git', 'diff', '--stat'], text=True),
                'options': {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('w') as output:
        output.write(json.dumps({'metadata': metadata})+'\n')
        output.flush()
        for case in cases:
            for backend in args.backends.split(','):
                for repeat in range(args.repeats):
                    command = [binary, case, backend, *map(str, [args.rows, args.iterations, args.width, args.chunk, args.batch, args.concurrency, args.io_chunk, args.http_chunk, args.latency_ms, int(args.streaming)])]
                    result = run_child(command)
                    result['repeat'] = repeat
                    output.write(json.dumps(result)+'\n')
                    output.flush()
                    print(f"{case:20} {backend:6} {repeat}: {result['median_seconds']*1000:9.3f} ms; peak RSS {result['process_peak_rss_bytes']/1048576:.1f} MiB", flush=True)


if __name__ == '__main__':
    main()
