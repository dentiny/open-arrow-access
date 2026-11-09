#!/usr/bin/env python3
"""Run isolated release workloads, recording raw samples and process resources.

Uses only Python's standard library on macOS/Linux. The loopback HTTP server
implements actual HEAD/Range GET requests; it never contacts a public service.
"""

import argparse
from collections import Counter
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import itertools
import json
import os
from pathlib import Path
import platform
import random
import re
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import unquote, urlsplit


class RangeServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, root, latency_ms):
        super().__init__(("127.0.0.1", 0), RangeHandler)
        self.root = root.resolve()
        self.latency_ms = latency_ms
        self.counts = Counter()
        self.lock = threading.Lock()

    def snapshot(self):
        with self.lock:
            return self.counts.copy()


class RangeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True

    def log_message(self, *_args):
        pass

    def do_HEAD(self):
        self.serve(False)

    def do_GET(self):
        self.serve(True)

    def serve(self, body):
        server = self.server
        with server.lock:
            server.counts[self.command] += 1
        path = (server.root / unquote(urlsplit(self.path).path).lstrip("/")).resolve()
        if not path.is_relative_to(server.root) or not path.is_file():
            self.send_error(404)
            return
        size = path.stat().st_size
        start, end, status = 0, size, 200
        if header := self.headers.get("Range"):
            match = re.fullmatch(r"bytes=(\d+)-(\d*)", header)
            if not match:
                self.send_error(400)
                return
            start = int(match[1])
            end = min(size, int(match[2]) + 1 if match[2] else size)
            if start >= end:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{size}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            status = 206
        time.sleep(server.latency_ms / 1000)
        self.send_response(status)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(end - start))
        self.send_header("Content-Type", "application/octet-stream")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end-1}/{size}")
        self.end_headers()
        if body:
            sent = 0
            try:
                with path.open("rb") as stream:
                    stream.seek(start)
                    while sent < end - start:
                        data = stream.read(min(65536, end - start - sent))
                        if not data:
                            raise IOError("fixture changed during read")
                        self.wfile.write(data)
                        sent += len(data)
            except (BrokenPipeError, ConnectionResetError):
                with server.lock:
                    server.counts["disconnects"] += 1
            finally:
                with server.lock:
                    server.counts["body_bytes"] += sent


def isolated(command, env, timeout):
    # wait4 supplies this child's resources, unlike RUSAGE_CHILDREN's cumulative
    # high-water mark. Files avoid pipe deadlock while the parent waits.
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        started = time.perf_counter()
        child = subprocess.Popen(command, stdout=out, stderr=err, env=env)
        timer = threading.Timer(timeout, child.kill)
        timer.start()
        try:
            _, status, usage = os.wait4(child.pid, 0)
            child.returncode = os.waitstatus_to_exitcode(status)
        finally:
            timer.cancel()
        wall = time.perf_counter() - started
        out.seek(0)
        err.seek(0)
        if child.returncode:
            raise RuntimeError(
                f"workload failed ({child.returncode}): {err.read().decode(errors='replace')}"
            )
        result = json.loads(out.read())
    result["process"] = {
        "wall_s": wall,
        "user_s": usage.ru_utime,
        "system_s": usage.ru_stime,
        "peak_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
        "input_blocks": usage.ru_inblock,
        "output_blocks": usage.ru_oublock,
        "minor_faults": usage.ru_minflt,
        "major_faults": usage.ru_majflt,
        "voluntary_context_switches": usage.ru_nvcsw,
        "involuntary_context_switches": usage.ru_nivcsw,
    }
    return result


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def csv_list(value):
    return value.split(",")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary",
        type=Path,
        default=Path("target/release/open-arrow-access-benchmarks"),
    )
    parser.add_argument("--data", type=Path, default=Path("target/benchmark-data"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--backends", type=csv_list, default=["raw", "memory", "fs", "http"]
    )
    parser.add_argument(
        "--cases",
        type=csv_list,
        default=[
            "csv",
            "json",
            "xml",
            "xml-infer",
            "atif",
            "warc",
            "parquet",
            "lines",
            "all",
            "sequential",
            "ranges",
        ],
    )
    parser.add_argument("--chunks", type=csv_list, default=["65536"])
    parser.add_argument("--batches", type=csv_list, default=["8192"])
    parser.add_argument("--concurrency", type=csv_list, default=["1"])
    parser.add_argument("--io-chunks", type=csv_list, default=["2097152"])
    parser.add_argument(
        "--projection",
        choices=["all", "empty", "id", "text", "trajectory_id"],
        default="all",
    )
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--latency-ms", type=float, default=0)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if (
        args.samples < 1
        or args.iterations < 1
        or args.warmups < 0
        or args.latency_ms < 0
    ):
        parser.error("invalid sample, iteration, warmup, or latency value")
    manifest = json.loads((args.data / "manifest.json").read_text())
    binary = args.binary.resolve()
    server = RangeServer(args.data, args.latency_ms)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    env = os.environ.copy()
    env["OAA_BENCH_HTTP_ENDPOINT"] = f"http://127.0.0.1:{server.server_port}"
    dimensions = list(
        itertools.product(
            args.backends,
            args.cases,
            args.chunks,
            args.batches,
            args.concurrency,
            args.io_chunks,
        )
    )
    jobs = []
    for sample in range(-args.warmups, args.samples):
        group = [(sample, *dimension) for dimension in dimensions]
        random.Random(args.seed + sample).shuffle(group)
        jobs.extend(group)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with args.output.open("w") as output:
            metadata = {
                "type": "metadata",
                "platform": platform.platform(),
                "machine": platform.machine(),
                "python": platform.python_version(),
                "revision": subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], text=True
                ).strip(),
                "dirty": bool(
                    subprocess.check_output(["git", "status", "--porcelain"], text=True)
                ),
                "binary_sha256": sha256(binary),
                "fixtures": manifest,
                "fixture_sha256": {
                    p.name: sha256(p) for p in sorted(args.data.glob("data.*"))
                },
                "latency_ms": args.latency_ms,
                "seed": args.seed,
                "note": "process resources include initialization; elapsed_ns excludes fixture preload/operator setup; warm OS page cache",
            }
            output.write(json.dumps(metadata) + "\n")
            output.flush()
            for sample, backend, case, chunk, batch, concurrency, io_chunk in jobs:
                if args.projection != "all" and case in (
                    "warc",
                    "lines",
                    "all",
                    "sequential",
                    "ranges",
                ):
                    raise ValueError(f"projection is not supported for {case}")
                before = server.snapshot()
                result = isolated(
                    [
                        str(binary),
                        str(args.data.resolve()),
                        backend,
                        case,
                        chunk,
                        batch,
                        concurrency,
                        io_chunk,
                        args.projection,
                        str(args.iterations),
                    ],
                    env,
                    args.timeout,
                )
                if case in (
                    "csv",
                    "json",
                    "xml",
                    "xml-infer",
                    "atif",
                    "warc",
                    "parquet",
                    "lines",
                ):
                    assert result["rows"] == manifest["rows"] * args.iterations, result
                if case in ("lines", "all", "sequential"):
                    assert (
                        result["output_bytes"]
                        == result["input_bytes"] * args.iterations
                    ), result
                after = server.snapshot()
                result.update(
                    type="sample",
                    sample=sample,
                    warmup=sample < 0,
                    http={k: after[k] - before[k] for k in after},
                )
                output.write(json.dumps(result) + "\n")
                output.flush()
                print(
                    f"{sample:2} {backend:6} {case:10} {result['elapsed_ns']/1e6:9.2f} ms  RSS {result['process']['peak_rss_bytes']/2**20:6.1f} MiB",
                    flush=True,
                )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    main()
