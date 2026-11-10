#!/usr/bin/env python3
"""Run isolated release workloads; JSONL includes process CPU, peak RSS and OS block I/O.
CPU/RSS include process startup, workload generation, warmup and cleanup; Rust wall
metrics include only measured iterations. OS block counts are not network bytes.
"""
import argparse
import json
import os
import platform
import random
import subprocess
import tempfile
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', default='target/release/oaa-bench')
    parser.add_argument('--output', required=True)
    parser.add_argument('--samples', type=int, default=5)
    parser.add_argument('--seconds', type=float, default=1.0)
    parser.add_argument('cases', nargs='*')
    args = parser.parse_args()
    if args.samples < 1 or args.seconds <= 0:
        parser.error('samples and seconds must be positive')
    binary = str(Path(args.binary).resolve())
    cases = args.cases or subprocess.check_output([binary, 'list'], text=True).splitlines()
    jobs = [(sample, case) for sample in range(args.samples) for case in cases]
    random.Random(42).shuffle(jobs)
    env = {**os.environ, 'BENCH_SECONDS': str(args.seconds)}
    with open(args.output, 'w') as out:
        for sample, case in jobs:
            with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
                start = time.monotonic()
                child = subprocess.Popen([binary, case], stdout=stdout, stderr=stderr, env=env)
                _, status, usage = os.wait4(child.pid, 0)
                child.returncode = os.waitstatus_to_exitcode(status)
                process_wall = time.monotonic() - start
                stdout.seek(0); stderr.seek(0)
                if child.returncode:
                    raise RuntimeError(f'{case}: {stderr.read().decode()}')
                result = json.loads(stdout.read())
            result.update(sample=sample, platform=platform.platform(), machine=platform.machine(),
                          process_wall_seconds=process_wall, cpu_user_seconds=usage.ru_utime,
                          cpu_system_seconds=usage.ru_stime,
                          peak_rss_bytes=usage.ru_maxrss * (1 if platform.system() == 'Darwin' else 1024),
                          os_input_blocks=usage.ru_inblock, os_output_blocks=usage.ru_oublock,
                          binary=binary, chunk_size=env.get('BENCH_CHUNK_SIZE', '65536'),
                          http_fragment_size=env.get('BENCH_HTTP_FRAGMENT_SIZE', '65536'),
                          cpu_seconds_per_iteration=(usage.ru_utime + usage.ru_stime) / result['iterations'])
            out.write(json.dumps(result, sort_keys=True) + '\n'); out.flush()
            print(f'{case}: {result["ns_per_iteration"]/1e6:.3f} ms/iteration, RSS {result["peak_rss_bytes"]/1048576:.1f} MiB', flush=True)


if __name__ == '__main__':
    main()
