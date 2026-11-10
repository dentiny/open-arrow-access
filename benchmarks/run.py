#!/usr/bin/env python3
"""Release matrix runner; subprocess wait4 provides per-case CPU, RSS and block I/O."""

import argparse
import hashlib
import http.server
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import tempfile
import threading
import time
import urllib.parse


class Files(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_HEAD(self):
        self.serve(False)

    def do_GET(self):
        self.serve(True)

    def serve(self, body):
        name = urllib.parse.unquote(urllib.parse.urlsplit(self.path).path).lstrip("/")
        if name.startswith("oaa-bench/"):
            name = name.removeprefix("oaa-bench/")
        path = (self.server.root / name).resolve()
        if not path.is_relative_to(self.server.root.resolve()):
            self.send_error(403)
            return
        if not path.is_file():
            self.send_error(404)
            return
        size = path.stat().st_size
        start, end = 0, size
        if "Range" in self.headers:
            a, b = self.headers["Range"].removeprefix("bytes=").split("-")
            start, end = int(a), min(int(b) + 1, size) if b else size
        if self.server.delay:
            time.sleep(self.server.delay)
        with self.server.lock:
            self.server.count += 1
            if body:
                self.server.payload += end - start
        self.send_response(206 if "Range" in self.headers else 200)
        self.send_header("Content-Length", str(end - start))
        self.send_header("Content-Type", "application/octet-stream")
        if "Range" in self.headers:
            self.send_header("Content-Range", f"bytes {start}-{end-1}/{size}")
        self.end_headers()
        if body:
            try:
                with path.open("rb") as f:
                    f.seek(start)
                    remaining = end - start
                    while remaining:
                        chunk = f.read(min(65536, remaining))
                        self.wfile.write(chunk)
                        remaining -= len(chunk)
            except (BrokenPipeError, ConnectionResetError):
                pass


def measure(command, env):
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        start = time.monotonic()
        proc = subprocess.Popen(command, stdout=stdout, stderr=stderr, env=env)
        _, status, usage = os.wait4(proc.pid, 0)
        proc.returncode = os.waitstatus_to_exitcode(status)
        elapsed = time.monotonic() - start
        stdout.seek(0)
        stderr.seek(0)
        if proc.returncode:
            raise RuntimeError(f"{command}: {stderr.read().decode()}")
        value = json.loads(stdout.read())
    value.update(
        process_seconds=elapsed,
        cpu_user_seconds=usage.ru_utime,
        cpu_system_seconds=usage.ru_stime,
        peak_rss_bytes=(
            usage.ru_maxrss if platform.system() == "Darwin" else usage.ru_maxrss * 1024
        ),
        input_blocks=usage.ru_inblock,
        output_blocks=usage.ru_oublock,
    )
    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--binary", default="target/release/open-arrow-access-benchmarks"
    )
    parser.add_argument(
        "--compare-binary", help="interleave a second revision in every round"
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--rows", type=int, default=32768)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--files", type=int, default=4)
    parser.add_argument("--batch", type=int, default=1024)
    parser.add_argument("--formats", default="bytes,csv,json,parquet,atif,warc,xml")
    parser.add_argument("--backends", default="memory,fs,http,connection,s3-fixture")
    parser.add_argument("--modes", default="sequential,files,native")
    parser.add_argument("--workers", default="1,4")
    parser.add_argument("--http-delay-ms", type=float, default=0)
    parser.add_argument("--io-concurrency", type=int, default=1)
    parser.add_argument("--io-chunk-size", type=int)
    parser.add_argument("--projection")
    args = parser.parse_args()
    if min(args.rows, args.repeats, args.files, args.batch, args.io_concurrency) <= 0:
        parser.error("counts must be positive")
    binary = str(Path(args.binary).resolve())
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="oaa-bench-") as directory:
        subprocess.run([binary, "prepare", directory, str(args.rows)], check=True)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Files)
        server.daemon_threads = True
        server.root = Path(directory)
        server.delay = args.http_delay_ms / 1000
        server.count = 0
        server.payload = 0
        server.lock = threading.Lock()
        threading.Thread(target=server.serve_forever, daemon=True).start()
        env = os.environ.copy()
        env["OAA_BENCH_IO_CONCURRENCY"] = str(args.io_concurrency)
        env.pop("OAA_BENCH_IO_CHUNK_SIZE", None)
        if args.io_chunk_size:
            env["OAA_BENCH_IO_CHUNK_SIZE"] = str(args.io_chunk_size)
        env["OAA_BENCH_ENDPOINT"] = f"http://127.0.0.1:{server.server_port}"
        cases = []
        for name in args.formats.split(","):
            for backend in args.backends.split(","):
                if name == "parquet" and backend == "connection":
                    continue  # no range support
                for mode in (
                    ["sequential"] if name == "bytes" else args.modes.split(",")
                ):
                    for workers in (
                        [1]
                        if mode == "sequential"
                        else map(int, args.workers.split(","))
                    ):
                        command = [
                            binary,
                            "read",
                            directory,
                            name,
                            backend,
                            mode,
                            str(workers),
                            str(args.files),
                            str(args.batch),
                        ]
                        if args.projection:
                            command.append(args.projection)
                        cases.append(command)
                        if args.compare_binary:
                            cases.append(
                                [str(Path(args.compare_binary).resolve()), *command[1:]]
                            )
        # Warm each case once; randomized full rounds reduce thermal/order confounding.
        for case in cases:
            measure(case, env)
        samples = []
        rng = random.Random(0)
        with output.open("w") as stream:
            stream.write(
                json.dumps(
                    {
                        "type": "environment",
                        "platform": platform.platform(),
                        "cpu_count": os.cpu_count(),
                        "binary_sha256": {
                            p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
                            for p in sorted({case[0] for case in cases})
                        },
                        "arguments": vars(args),
                        "workspace_git_head": subprocess.check_output(
                            ["git", "rev-parse", "HEAD"], text=True
                        ).strip(),
                        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True),
                        "cache": "warm OS page cache; no eviction",
                        "http": "loopback HTTP/1.1; server CPU excluded from child rusage",
                    }
                )
                + "\n"
            )
            for repeat in range(args.repeats):
                order = cases.copy()
                rng.shuffle(order)
                for case in order:
                    with server.lock:
                        count, payload = server.count, server.payload
                    row = measure(case, env)
                    row["binary"] = Path(case[0]).name
                    with server.lock:
                        row.update(
                            http_requests=server.count - count,
                            http_payload_bytes=server.payload - payload,
                        )
                    row.update(repeat=repeat, projection=args.projection)
                    samples.append(row)
                    stream.write(json.dumps(row) + "\n")
                    stream.flush()
                print(
                    f"round {repeat+1}/{args.repeats}: {len(cases)} cases", flush=True
                )
        server.shutdown()
        server.server_close()
    grouped = {}
    for row in samples:
        key = tuple(row[x] for x in ("binary", "format", "backend", "mode", "workers"))
        grouped.setdefault(key, []).append(row)
    for key, rows in sorted(grouped.items()):
        times = [r["seconds"] for r in rows]
        print(
            *key,
            f"median={statistics.median(times):.6f}s",
            f"min={min(times):.6f}s",
            f"max={max(times):.6f}s",
            f"rss={max(r['peak_rss_bytes'] for r in rows)/2**20:.1f}MiB",
        )


if __name__ == "__main__":
    main()
