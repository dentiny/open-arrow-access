# Local performance benchmarks

This non-published workspace package measures the six supported file formats
(CSV, JSON, Parquet, ATIF, XML, WARC), both explicit and inferred XML schemas,
and the OpenDAL filesystem, HTTP, and S3 adapters. It does not change production
features or defaults. S3 uses a local HTTP endpoint with fixed dummy credentials;
no cloud accounts, credentials, or public datasets are needed.

## Run

From the workspace root, on macOS or Linux with Python 3:

```sh
cargo build -p open-arrow-access-benchmarks --release --locked
python3 benchmarks/run.py --samples 7 --output benchmarks/results/local.jsonl
python3 benchmarks/summarize.py benchmarks/results/local.jsonl
```

Filter cases or increase the corpus size:

```sh
python3 benchmarks/run.py --filter atif --rows 131072 --samples 7 --output benchmarks/results/atif-large.jsonl
python3 benchmarks/run.py --filter storage --output benchmarks/results/storage-local.jsonl
# One case: format, rows, input chunk bytes, batch rows, projection, backend.
target/release/open-arrow-access-benchmarks xml 16384 65536 1024 project memory
```

The full matrix covers 50 cases: 16,384 records, full/single-column reads, 1/1024
row batches, 64 KiB/127-byte source chunks, and storage concurrency 1/4. Storage
scans use a 32 MiB object; scatter reads request 4096 non-adjacent 64-byte ranges
at 8192-byte intervals. Empty-range cases retain 4096 output positions without
payload reads. The storage `batch` argument means I/O concurrency, not row count.
Parquet uses ranged input, so sequential source chunk size does not affect it.

Format data is generated before timing. CSV, JSON, XML, and Parquet contain an
integer and Unicode text; ATIF repeats the first nonblank checked-in trajectory;
WARC repeats the checked-in resource record. This WARC case measures small-record
framing and header validation, not large HTTP bodies. Row counts and storage
byte counts are asserted, and output is consumed then dropped instead of retained.
Fixtures are synthetic and have no external download or licensing requirements.

## Metrics and boundaries

Each case warms once, then runs in a **fresh process** for every sample. Sample
order is shuffled using seed 42. Every raw sample includes:

- `elapsed_seconds`, `cpu_seconds`: scan wall time and process CPU time, starting
  before accessor open and ending after consumption. Setup/input generation,
  temporary-file creation, and backend construction are outside these timers.
- `first_seconds`: time from open to the first batch/chunk/record. Scatter cases
  report time to completion of the entire range request. It includes metadata I/O.
- `source_calls`, `source_bytes`: calls/bytes at the instrumented in-memory
  ByteReader boundary. These show Parquet projection's actual range-read savings.
  Sequential counters exclude EOF. Each call incurs two relaxed atomics.
- `http_requests`, `http_bytes`: local server observations for HTTP/S3, including
  HEAD requests. Bytes count response payload, not headers/signature overhead.
  The server handles range requests and persistent connections.
- `max_batch_bytes`: largest live Arrow batch's array memory, or largest WARC
  body. Batches are released immediately; this is not total heap usage.
- `peak_rss_bytes`, process user/system time, input/output block counts: per-child
  `wait4` measurements. **RSS and these process CPU counters include setup**,
  generated input, runtime, decoder buffers and allocator retention. They are
  not allocation counts, decoder-only peaks, or a direct measure of memory bandwidth.

The metadata sidecar stores the upstream commit, dirty flag, Rust compiler,
platform, cases, and SHA-256 of the binary. A dirty checkout is expected while
adding the harness. Preserve exact source commits/patches alongside comparisons.
The scanner uses a single-thread Tokio runtime; OpenDAL may use its blocking pool.
Process CPU can therefore exceed scan wall time for filesystem/concurrent I/O.

Local filesystem data is freshly written and warm in the OS cache. HTTP/S3
measure loopback SDK/framing/request overhead, including the Python server;
client CPU excludes server CPU. They do not establish physical disk throughput,
WAN latency, TLS cost, or production cloud-service throughput. No Azure/GCS/HDFS
accounts are configured or measured. Provider discovery APIs are outside this
format/storage matrix. CPU-bound conclusions require CPU time close to wall time;
request/byte counts and concurrency changes help identify I/O overhead. RSS alone
does not establish a memory-bandwidth bottleneck.

## Compare an optimization

Keep two release binaries built with the same compiler, lockfile, and harness.
For upstream dependency changes, use a separate Cargo configuration rather than
editing production dependency versions:

```toml
# /absolute/path/patches.toml
[patch.crates-io]
atif-arrow = { path = "/absolute/path/atif-arrow/crates/atif-arrow" }
xml-arrow = { path = "/absolute/path/xml-arrow-rs/crates/xml-arrow" }
```

```sh
# Build baseline before applying production changes.
cargo build -p open-arrow-access-benchmarks --release --locked
cp target/release/open-arrow-access-benchmarks /tmp/oaa-before
cp Cargo.lock /tmp/oaa-baseline.lock
# Apply the independent storage patch and/or upstream checkouts, then:
cargo build -p open-arrow-access-benchmarks --release --config /absolute/path/patches.toml
cp target/release/open-arrow-access-benchmarks /tmp/oaa-after
# Restore the registry lockfile after local dependency patching.
cp /tmp/oaa-baseline.lock Cargo.lock
python3 benchmarks/compare.py --before /tmp/oaa-before --after /tmp/oaa-after \
  --cases benchmarks/results/local.meta.json --samples 11 --output benchmarks/results/paired-local.jsonl
```

`compare.py` warms both versions, shuffles cases, and randomizes version order
within each pair. `--filter atif` narrows it to one format. Run measurements without
concurrent compiles, profilers, or other benchmarks; retain dispersion, not only a
single best run. See [the investigation report](REPORT.md) for observed results,
source references, validation and unresolved bottlenecks.
