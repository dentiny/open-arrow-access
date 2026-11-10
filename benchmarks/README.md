# Read performance benchmarks

Results: [2026-11-10 local findings](results/2026-11-10/README.md).

This harness exercises public readers on deterministic generated corpora, checks
row counts, drops each batch after consumption, and saves every sample as JSONL.
Use the repository's pinned compiler and lockfile. The published run records its
actual compiler separately (Homebrew 1.97.1); final validation uses pinned 1.97.0.
Build and measure separately:

```sh
rustup run 1.97.0 cargo build -p open-arrow-access-benchmarks --features full --release --locked
python3 benchmarks/run.py --output benchmarks/results/matrix.jsonl
python3 benchmarks/run_providers.py --iterations 10000 --output benchmarks/results/providers.jsonl
```

The benchmark binary is gated behind `full` so default workspace checks do not
enable every provider/format. `allocations` implies `full` and adds allocator counters.

Run from the repository root on macOS or Linux (the runner requires `wait4`). No
cloud credentials, public datasets, Docker, or Python packages are needed. The
matrix starts an HTTP/1.1 server on **127.0.0.1** and deletes its temporary corpus
when finished. It does not contact provider websites. Do not run other builds or
benchmarks concurrently when collecting publication-quality measurements.

## Coverage

- CSV, JSONL, Parquet, ATIF JSONL, physical WARC-to-Arrow, explicit-schema XML.
- Byte-only storage control; immutable memory, OpenDAL filesystem, OpenDAL HTTP,
  and the provider `Connection` HTTP body reader.
- Sequential file sets; ordered parallel file sets; composed native file planning.
  Parallel configurations use 1 and 4 resident partitions, two queued batches per
  partition, and a four-thread Tokio runtime. All cases consume the same number
  of logical input files. A file set repeats the same immutable object deliberately
  to keep bytes/cache state identical across modes.
- 24 concrete provider operations, using committed protocol fixtures and public
  service entry points. API services resolve/validate metadata and build lazy file
  namespaces; Gutenberg/PMC/HF/inventory drain Arrow batches; CourtListener drains
  decoded bytes; MECA opens and validates archive membership. Common Crawl resolves
  its already-decompressed path manifest. A metadata resolve is **not** a content
  download benchmark or a measurement of public API latency.

`Connection` is sequential-only byte storage, so Parquet combinations are omitted:
Parquet requires range reads. The other formats use file and native parallelism
on each supported backend. WARC native parsing cost remains distinct from Arrow
conversion: this matrix measures their composed path. XML inference, compressed
format matrices, skewed partitions, giant records, real S3/GCS/Azure services and
cloud RTT/bandwidth are not covered by the default run. Storage is one generic
OpenDAL adapter, not separate implementations for every upstream backend.

## Measurement contract

Generated records have two simple columns and a roughly 200-byte UTF-8 payload.
ATIF includes the mandatory envelope and a step, WARC includes mandatory headers,
and Parquet has 4,096-row groups. These are synthetic baselines, not substitutes
for representative production corpora. XML uses signed `Int64` IDs because the
XML mapper does not support unsigned integers; all generated values fit both.

Each case runs in a fresh process, with one additional warm-up process per case.
Full rounds are shuffled with seed 0. Default: 32,768 rows per object, four logical
files, 1,024-row batches, five measured rounds. OS page cache is warm; the harness
does not claim cold disk throughput. Corpus construction, memory input loading,
runtime startup and operator construction precede the timed read region.

The binary reports:

- Open-to-EOF wall time, open time, first-batch time, row and batch counts.
- Maximum Arrow memory for **one consumed batch**, not a bound on decoder/queue
  memory. The runner additionally records whole-process peak RSS.

The Python parent records `wait4` user/system CPU, elapsed process time, peak RSS
(normalized to bytes), and block input/output counters. **CPU/RSS/block counters
include process setup**, whereas binary wall time covers the read region. Do not
divide process CPU by read-region wall time and call it CPU utilization. Calculate
process CPU / process elapsed instead; small scans remain startup-sensitive.
Block counters do not measure page-cache bytes or HTTP traffic. Loopback server
CPU is excluded from the child's CPU counters. HTTP request count and response
payload bytes are recorded separately. RSS includes the input corpus for memory
cases, library/runtime overhead, active readers and queued batches; it is not an
allocation count. No allocator instrumentation is installed in production crates.

Provider operations use a no-network transport with immutable full response
bodies and an atomic request counter. The transport retains no request history.
Service construction and fixture loading precede the timed loop; one warm-up
operation precedes each sample. Provider CPU/RSS are still process-wide. Most
provider fixtures are small and measure fixed overhead, not scaling with a large
manifest. HF is pinned to a fixture commit and uses a fixture-only token.

## Experiments

```sh
# Separate I/O wait from decoding. Delay applies to every HEAD/GET response.
python3 benchmarks/run.py --http-delay-ms 10 --backends http,connection \
  --output benchmarks/results/http-10ms.jsonl

# Projection changes Arrow allocation; ATIF/WARC still validate complete records.
python3 benchmarks/run.py --formats atif --projection trajectory_id \
  --output benchmarks/results/atif-projection.jsonl
python3 benchmarks/run.py --formats warc --projection record_id \
  --output benchmarks/results/warc-projection.jsonl

# Smaller batches expose per-batch overhead and increase scheduling/queue costs.
python3 benchmarks/run.py --batch 64 --output benchmarks/results/batch-64.jsonl

# A single file separates native partition behavior from file-level overlap.
python3 benchmarks/run.py --files 1 --output benchmarks/results/single-file.jsonl
```

For comparisons build each revision into a separately named binary, pass
`--binary`, keep all parameters identical, and alternate runs to check drift.
Report medians and ranges, CPU and RSS alongside speedup. A throughput change
without a controlled before/after measurement is a hypothesis, not a finding.
Use `sample PID 5 -file profile.txt` on macOS or `perf record -g` on Linux for CPU
stacks; profile separately because sampling changes timings. Preserve symbols
and use `[profile.release] debug = 1` in a local-only config if source lines are
needed. The published JSONL results are timings without profiler overhead.

The optional `s3-fixture` backend exercises the real OpenDAL S3 adapter, including
request signing and ranged GETs, against the same local server under a path-style
bucket. The server implements HEAD/GET/range responses, ignores authentication,
and does not implement a full S3 server. Its fixture-only signing keys are embedded
in the benchmark. This measures adapter/protocol overhead on warm local files;
it is **not AWS or MinIO performance**, and cannot establish cloud/storage-server
bottlenecks. The default matrix includes it; use `--backends` to select cases.

For allocation counts, build a **separate** instrumented executable:

```sh
rustup run 1.97.0 cargo build -p open-arrow-access-benchmarks --features allocations --release --locked
cp target/release/open-arrow-access-benchmarks /tmp/oaa-allocations
```

The `allocations` feature wraps `System` with `stats_alloc`. Arrow read samples
and provider operations then include allocation/reallocation calls and cumulative
allocated/deallocated bytes for their read/operation region. These are global
counters across the runtime's threads, not peak live bytes; a few result-object
allocations are included. Do not compare instrumented timings with normal timing
runs: allocator atomics especially distort multithreaded scans. The optional
`batch-builder` provider control builds 1,024 rows with eight named `UInt64`
fields per operation, isolating the shared metadata builder from parsers/network.
It is not one of the 24 provider cases.

`--compare-binary` interleaves two binaries in the same randomized rounds; samples
record their basename and the environment records SHA-256 digests. I/O tuning is
available through `--io-concurrency` and `--io-chunk-size` (bytes). These configure
per-reader I/O, independently of `--workers` and Arrow batch size.

To regenerate aggregate CSV tables from the published samples:

```sh
python3 benchmarks/summarize.py benchmarks/results/2026-11-10/*.jsonl \
  --output /tmp/oaa-summary.csv
```

The environment's `workspace_git_head` (or `git_head` in earlier samples) identifies
the checkout when the runner executed. An explicitly supplied binary can come from
a different revision. See the result report's binary/source manifest for the
actual production revisions and flags; executable SHA-256 hashes identify variants.
Published environment paths are normalized to basenames, without changing samples.
