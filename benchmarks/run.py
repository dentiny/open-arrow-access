#!/usr/bin/env python3
"""Run each case in a fresh process; retain raw JSONL and process resource usage."""
import argparse
import hashlib
import http.server
import json
import os
import platform
import random
import subprocess
import tempfile
import threading
from pathlib import Path

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    data = bytes(range(251)) * (32 * 1024 * 1024 // 251 + 1)
    data = data[:32 * 1024 * 1024]
    requests = 0
    transferred = 0
    def log_message(self, *_):
        pass
    def do_HEAD(self):
        type(self).requests += 1
        self.send_response(200)
        self.send_header('Content-Length', str(len(self.data)))
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()
    def do_GET(self):
        type(self).requests += 1
        start, end = 0, len(self.data) - 1
        value = self.headers.get('Range')
        if value:
            lo, hi = value.removeprefix('bytes=').split('-')
            start = int(lo)
            end = int(hi) if hi else end
        data = self.data[start:end+1]
        type(self).transferred += len(data)
        self.send_response(206 if value else 200)
        self.send_header('Content-Length', str(len(data)))
        if value:
            self.send_header('Content-Range', f'bytes {start}-{end}/{len(self.data)}')
        self.end_headers()
        self.wfile.write(data)

def run(binary, case, env):
    # File redirection avoids pipe deadlocks while wait4 supplies per-child RSS/CPU.
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        process = subprocess.Popen([str(binary), *map(str, case)], stdout=out, stderr=err, env=env)
        _, status, usage = os.wait4(process.pid, 0)
        process.returncode = os.waitstatus_to_exitcode(status)
        out.seek(0); err.seek(0)
        if process.returncode:
            raise RuntimeError(f'{case}: {err.read().decode()}')
        result = json.loads(out.read())
        result.update(user_seconds=usage.ru_utime, system_seconds=usage.ru_stime,
                      peak_rss_bytes=usage.ru_maxrss * (1 if platform.system() == 'Darwin' else 1024),
                      input_blocks=usage.ru_inblock, output_blocks=usage.ru_oublock)
        return result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=Path('target/release/open-arrow-access-benchmarks'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--samples', type=int, default=7)
    parser.add_argument('--rows', type=int, default=16384)
    parser.add_argument('--filter', default='')
    args = parser.parse_args()
    if args.samples < 1 or args.rows < 1:
        parser.error('samples and rows must be positive')
    cases = []
    for fmt in ['csv', 'json', 'parquet', 'xml', 'xml-infer', 'atif', 'warc']:
        for mode in (['all'] if fmt == 'warc' else ['all', 'project']):
            for batch in ([1024] if fmt == 'warc' else [1, 1024]):
                cases.append((fmt, args.rows, 65536, batch, mode, 'memory'))
        cases.append((fmt, args.rows, 127, 1024, 'all', 'memory'))
    for backend in ['fs', 'http', 's3']:
        for mode in ['all', 'ranges', 'empty-ranges']:
            for concurrency in [1, 4]:
                cases.append(('storage', 16384, 65536, concurrency, mode, backend))
    cases = [case for case in cases if args.filter in '/'.join(map(str, case))]
    if not cases:
        parser.error('no cases match the filter')
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = dict(os.environ, BENCH_HTTP_URL=f'http://127.0.0.1:{server.server_port}/')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    meta = dict(platform=platform.platform(), processor=platform.processor(),
                commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                rustc=subprocess.check_output(['rustc', '-Vv'], text=True),
                binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(), samples=args.samples, cases=cases,
                dirty=bool(subprocess.check_output(['git', 'status', '--porcelain'], text=True)))
    args.output.with_suffix('.meta.json').write_text(json.dumps(meta, indent=2)+'\n')
    try:
        # Warm once per case, then shuffle samples with a fixed seed.
        for case in cases:
            run(args.binary.resolve(), case, env)
        jobs = [(sample, case) for sample in range(args.samples) for case in cases]
        random.Random(42).shuffle(jobs)
        with args.output.open('w') as output:
            for sample, case in jobs:
                before = Handler.requests, Handler.transferred
                result = run(args.binary.resolve(), case, env)
                result.update(sample=sample, http_requests=Handler.requests-before[0], http_bytes=Handler.transferred-before[1])
                output.write(json.dumps(result)+'\n'); output.flush()
                print(f'{sample}: {"/".join(map(str, case))} {result["elapsed_seconds"]:.6f}s', flush=True)
    finally:
        server.shutdown(); server.server_close()

if __name__ == '__main__':
    main()
