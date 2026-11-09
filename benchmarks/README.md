# Local performance matrix

The `pipeline` executable checks row/byte counts and reports individual timed
scans as JSON. `run.py` runs each case in a separate child process and adds
`wait4` CPU, peak RSS, block-I/O and context-switch counters. `summarize.py`
reports the median of the independent process medians.

## Run

```sh
cargo bench --bench pipeline --all-features --no-run --locked
# Use the executable path printed by Cargo, including its hash:
python3 benchmarks/run.py --binary target/release/deps/pipeline-<hash> \
  --output work/performance.jsonl --repeats 3 --iterations 7
python3 benchmarks/summarize.py work/performance.jsonl
```

`run.py` requires a Unix platform with `os.wait4`; RSS units are normalized for
macOS and Linux. The Rust executable itself can run without the Python wrapper.
Builds are excluded from all measurements. Do not compile, test or profile
concurrently with a timing run. Keep before/after binaries and use identical
options; the output records the compiler, OS, source revision, working-tree diff
summary, binary hash (newer runs), configuration and every timing sample.

The default matrix uses 32,768 rows, 128-byte text values, batches of 1,024 rows,
64-KiB emitted chunks and explicit 2-MiB I/O splitting. Each process generates its
input, initializes its runtime and warms up once before the seven scans. No
external datasets, credentials, cloud accounts or public endpoints are used.

| Area | Cases |
| --- | --- |
| Formats | CSV, JSON/JSONL, Parquet, XML explicit schema, XML inference, nested XML, ATIF full/projection, native WARC |
| Storage backends | In-memory byte source, OpenDAL filesystem, HTTP protocol replay, S3 protocol replay with synthetic signing credentials |
| Read patterns | Sequential, whole-object range, up to 4,096 scattered 32-byte ranges, 4,096 adjacent 32-byte ranges |
| Shared core | Line splitting across configurable byte chunks |
| Scaling probes | ATIF tool/result fan-out; transport fragments; artificial request latency and I/O concurrency |

HTTP/S3 replay uses an **in-process transport**, with 16-KiB response fragments
by default. Request counts and returned body bytes are measured at that transport.
S3 exercises OpenDAL's S3 adapter/signing, not a live S3 server. The memory source
uses test-support counters, and filesystem input is warm-cache. Provider discovery
APIs, real network/TLS/DNS, remote service latency, cold disks, and cloud throughput
are **not** measured by this suite. Parquet's deliberately repetitive fixture is
small and dictionary-friendly; do not compare its bytes/sec with text corpora.

## Focused probes

```sh
# Large ATIF fan-out: width means calls/results for this case. Run 8,32,256,1024.
python3 benchmarks/run.py --binary <binary> --cases atif-tools \
  --backends memory --rows 64 --width 1024 --output work/tools.jsonl

# Compare transport fragments of 3, 256 and 16384 bytes.
python3 benchmarks/run.py --binary <binary> --cases storage-sequential \
  --backends http,s3 --rows 8192 --http-chunk 3 --io-chunk 0 \
  --output work/chunks.jsonl

# Opt in to direct sequential streaming; an explicit I/O size overrides it.
python3 benchmarks/run.py --binary <binary> --cases storage-sequential \
  --backends http,s3 --rows 8192 --http-chunk 3 --io-chunk 0 --streaming \
  --output work/streaming.jsonl

# Request-latency sensitivity: compare concurrency 1 and 4.
python3 benchmarks/run.py --binary <binary> --cases storage-sequential \
  --backends http,s3 --rows 8192 --io-chunk 65536 --latency-ms 5 \
  --concurrency 4 --output work/concurrency.jsonl

python3 benchmarks/summarize.py work/before.jsonl work/after.jsonl
```

The heavy `atif-tools` case is excluded from the default matrix. Its width changes
the number of calls/results, not a text payload size. Baseline executables do not
implement streaming; compare their ordinary sequential path with a patched
executable's `--streaming --io-chunk 0` path as an explicit configuration tradeoff.

## Interpreting CPU, I/O and memory

- `samples[].seconds` includes open/metadata and pulling to EOF. Setup and warm-up
  are excluded. No decoded batches or records are accumulated by the consumer.
- CPU/RSS/block counters cover the **whole child process**, including generation,
  warm-up and all scans. RSS is an allocator/OS high-water mark, not live heap or
  bytes allocated by one scan. A second input copy during fixture setup can affect
  it. Block-I/O counters are operations, not network bytes.
- Compare memory vs FS vs HTTP/S3 to separate decoding from adapter costs. Use the
  latency/concurrency probe for I/O waiting; replay without latency isolates local
  CPU and allocation costs. No CPU-stack attribution is inferred from wall time.
- Report short-scan changes cautiously. Repeated scans in one process are
  correlated; use independent process repeats and inspect their spread. These
  generated inputs identify candidates, not production-wide speed guarantees.
- On macOS, `profile.py --binary <binary> --output work/cpu.txt` collects a 3-second
  `sample` profile from the selected long-running case and terminates the child.
  Run it separately from timing measurements. On Linux use `perf` with the same
  executable/options. Profiler permissions depend on the host.

For a baseline build, check out the recorded upstream commit in a separate clone,
then copy only `benchmarks/`, `Cargo.toml` and `Cargo.lock` from this branch. The
harness has a fallback for the baseline accessor and accepts `--streaming` only
when the actual storage method exists. For upstream ATIF/XML comparisons, build
with Cargo's `--config 'patch.crates-io.atif-arrow.path="/path/to/atif-arrow/crates/atif-arrow"'`
and `--config 'patch.crates-io.xml-arrow.path="/path/to/xml-arrow-rs/crates/xml-arrow"'`.
Keep each resulting executable; do not commit local path overrides into the lockfile.

For alternating A/B processes use:

```sh
python3 benchmarks/compare.py --before <baseline-binary> --after <patched-binary> \
  --cases xml,xml-nested --backends memory,http --output-prefix work/paired
python3 benchmarks/summarize.py work/paired-before.jsonl work/paired-after.jsonl
```

See [RESULTS.md](RESULTS.md) for the measured findings, before/after tables, exact
upstream revisions, limitations and the small optimization PRs.
