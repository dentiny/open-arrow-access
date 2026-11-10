# Per-provider matrix and allocation follow-up

This supplements the existing [pipeline audit](../README.md) with an isolated
workspace executable, all 23 service feature families (plus arXiv metadata),
all six file formats, FS/HTTP sequential and scattered reads, and explicit
retained-allocation and fragmented-provider-download probes. It changes no
production code or parser versions. The earlier pipeline additionally covers
S3 protocol replay, latency/concurrency, nested/timestamp workloads and CPU stacks.

From the repository root:

```sh
cargo build --release -p oaa-bench --locked
target/release/oaa-bench list
python3 benchmarks/matrix/run.py --output work/matrix.jsonl --samples 5 --seconds 1
# Restrict or perturb individual cases:
BENCH_CHUNK_SIZE=4096 python3 benchmarks/matrix/run.py \
  --output work/formats-4k.jsonl csv json atif xml warc parquet
BENCH_HTTP_FRAGMENT_SIZE=7000 python3 benchmarks/matrix/run.py \
  --output work/http-7k.jsonl http-sequential
```

Run saved baseline/candidate binaries using `--binary` without rebuilding between
samples. Jobs run serially in a deterministic shuffled order. Every process has
one untimed warmup; timed iterations include opening and consuming the reader and
dropping batches. Fixture construction, file creation and request-log resets are
excluded from Rust wall time. Work is consumed with `black_box`; rows/bytes and
range lengths are asserted. The default format workload has 16,384 rows and a
256-byte string. ATIF has its native nested envelope. Parquet is encoded once
before timing; cross-format rates are not equivalent payload/schema comparisons.

The Python runner requires macOS or Linux and records isolated child-process CPU,
peak RSS and OS block counts through `wait4`. Process metrics include startup,
setup, warmup and cleanup, unlike Rust wall time. Block counts are OS counters,
not network traffic. `read_calls`/`range_calls` observe the in-memory format source;
HTTP range payloads are recorded separately. FS runs are warm-cache; HTTP is an
in-process protocol fixture with 64 KiB body fragments (7000 bytes for the
`http-small-fragments` case), not sockets or a cloud benchmark. The earlier test
fixture's 3-byte fragments are deliberately avoided for ordinary HTTP cases.

Provider cases resolve their committed small metadata fixtures through public
APIs. Hugging Face performs revision pinning, listing and a complete three-row
Parquet read. CourtListener decompresses its bulk fixture; PMC and Gutenberg
convert metadata to Arrow; MECA opens its archive/manifest; arXiv and Common Crawl
resolve manifests. Others resolve their metadata/file namespace without fetching
attachments. These measure local provider CPU and request shape, not catalogue
scaling or real server latency. Provider instances with transport setup are reused.

`chunks-retained` holds 128 independent 64-byte reads requested at 2 MiB each.
`retained_capacity_bytes` recovers each unique `BytesMut` allocation capacity,
then re-freezes it. This measures retained capacity, not physical RSS. Lazy zero
pages mean a 256 MiB capacity can have much smaller RSS. `http-fragmented` emits
one 16 MiB scatter buffer containing 256 independently owned 64 KiB parts; the
consumer retains the returned output. This isolates the whole-buffer copy.

See [RESULTS.md](RESULTS.md) and [results/manifest.json](results/manifest.json) for
source revisions, executable hashes, measurements, independent PRs and limits.
