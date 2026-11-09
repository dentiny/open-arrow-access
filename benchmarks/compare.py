#!/usr/bin/env python3
"""Randomized paired comparison of two prebuilt binaries with identical arguments."""
import argparse
import hashlib
import platform
import http.server
import json
import os
import random
import threading
from pathlib import Path
from run import Handler, run

parser = argparse.ArgumentParser()
parser.add_argument('--before', type=Path, required=True)
parser.add_argument('--after', type=Path, required=True)
parser.add_argument('--cases', type=Path, required=True, help='run.py metadata JSON')
parser.add_argument('--filter', default='')
parser.add_argument('--samples', type=int, default=11)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
cases = [case for case in json.loads(args.cases.read_text())['cases'] if args.filter in '/'.join(map(str, case))]
if args.samples < 1 or not cases:
    parser.error('samples must be positive and at least one case must match')
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.with_suffix('.meta.json').write_text(json.dumps(dict(
    platform=platform.platform(), samples=args.samples, seed=42, cases=cases,
    before_sha256=hashlib.sha256(args.before.read_bytes()).hexdigest(),
    after_sha256=hashlib.sha256(args.after.read_bytes()).hexdigest(),
), indent=2)+'\n')
server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
env = dict(os.environ, BENCH_HTTP_URL=f'http://127.0.0.1:{server.server_port}/')
rng = random.Random(42)
try:
    for case in cases:
        for binary in [args.before, args.after]:
            run(binary.resolve(), case, env)
    jobs = [(sample, case) for sample in range(args.samples) for case in cases]
    rng.shuffle(jobs)
    with args.output.open('w') as output:
        for sample, case in jobs:
            pair = [('before', args.before), ('after', args.after)]
            rng.shuffle(pair)
            for version, binary in pair:
                before = Handler.requests, Handler.transferred
                result = run(binary.resolve(), case, env)
                result.update(sample=sample, version=version, http_requests=Handler.requests-before[0], http_bytes=Handler.transferred-before[1])
                output.write(json.dumps(result)+'\n'); output.flush()
            print(f'{sample}: {"/".join(map(str, case))}', flush=True)
finally:
    server.shutdown(); server.server_close()
