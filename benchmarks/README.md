# Local performance benchmarks

Build and run from the repository root using the pinned Rust toolchain and
Python 3.9+ on macOS or Linux. If `cargo` is a system installation rather
than a rustup shim, explicitly select both Cargo and rustc, for example
`RUSTC="$(rustup which --toolchain 1.97.0 rustc)" rustup run 1.97.0 cargo build ...`.
The compiler used is embedded in every sample; verify it before comparing:

```sh
cargo build --release -p open-arrow-access-benchmarks --locked
target/release/open-arrow-access-benchmarks generate target/benchmark-data 32768 512
python3 benchmarks/run.py --output target/baseline.jsonl
python3 benchmarks/summarize.py target/baseline.jsonl
```

The default matrix covers CSV, JSON, XML (explicit and inferred schema), ATIF
JSONL, Parquet/Snappy and native WARC records, plus sequential bytes, sparse
ranges, `read_lines` and `read_all`. It runs each through a direct shared-Bytes
control (`raw`), OpenDAL memory, filesystem, and a real loopback HTTP server.
WARC emits native records; its output is not equivalent to an Arrow scan.
Dataset-provider discovery APIs are not storage backends and are outside this
matrix. Gzip and archive decoding are also outside this initial matrix.

Fixtures are generated once, outside measurement, with ordered IDs and
deterministic per-row ASCII payloads from a fixed xorshift sequence. All formats
use the same IDs/payloads; ATIF and WARC add their required structure. Parquet has
8192-row groups and Snappy compression. These synthetic payloads are not a
representative corpus: repeat with production-shaped data before changing
defaults. Generated shards belong under `target/`, not in `testdata/`.

The checked-in [M5 Pro baseline and independent optimizations](results/2026-11-09-m5pro/REPORT.md)
include raw samples, allocation counts and reproducible comparisons.

## Separate bottlenecks

Vary one dimension at a time, using the same generated files and binary:

```sh
# Decoder overhead and batch/chunk sensitivity, without storage I/O.
python3 benchmarks/run.py --backends raw --cases csv,json,xml,atif,parquet \
  --chunks 1024,65536 --batches 256,8192 --output target/decoder-sweep.jsonl

# Narrow projection versus all columns (compare to a matching all-column run).
python3 benchmarks/run.py --cases csv,json,xml,parquet --projection id \
  --output target/projection.jsonl

# A bounded, reproducible latency model, including HTTP request/body accounting.
# Smaller fixtures keep the deliberately sparse range workload practical.
target/release/open-arrow-access-benchmarks generate target/benchmark-io 4096 512
python3 benchmarks/run.py --data target/benchmark-io --backends http \
  --cases sequential,ranges,parquet --concurrency 1,4 --io-chunks 65536,2097152 \
  --latency-ms 2 --output target/io-sweep.jsonl

# Long records expose line scanning and cross-chunk assembly costs.
target/release/open-arrow-access-benchmarks generate target/benchmark-long 4096 8192
python3 benchmarks/run.py --data target/benchmark-long --backends raw \
  --cases lines,atif --chunks 1024,65536 --batches 256 --samples 9 \
  --iterations 8 --output target/long-baseline.jsonl
```

For comparisons, preserve the baseline executable (`cp` it outside `target/release`),
build the candidate, then use `--binary PATH` to run each against identical files
and dimensions. `summarize.py BASELINE CANDIDATE` reports median speedups and
a deterministic bootstrap 95% interval; it rejects mismatched fixtures,
platforms, latency settings and workload dimensions. The interval reflects only
sample variation in this run, not uncertainty across machines or corpora. Use
several interleaved baseline/candidate rounds for small changes. Do not compile
or run other benchmarks during measurement.

## Read the measurements

Each JSONL file contains machine/revision/binary and fixture hashes, randomized
sample order, explicit warmups, and all raw samples. Workloads run serially,
each in a fresh subprocess. Failures, timeouts, row-count mismatches and byte-count
mismatches abort the run rather than becoming successful samples. The runner
does not flush the OS page cache: filesystem results represent warm-cache reads,
not physical disk bandwidth. Fixture hashing also warms the input cache.

* `elapsed_ns` includes opening the reader, schema preparation/inference and
  consumption for all iterations. It excludes process startup, input preload
  into `raw`/memory, and operator setup. Summary times divide by iterations.
* `first_item_ns` measures first batch/record latency from the beginning of the
  first open, including inference. It is null for byte collection/range cases.
* `process` contains per-child `wait4` CPU time, peak RSS, page faults, block I/O
  and context switches. These include startup and preload, so CPU/elapsed ratios
  can exceed one for very short cases. RSS is **whole-process** high-water memory,
  including input retained by raw/memory, runtime and decoder/output allocations;
  it is not an allocator count or isolated decoder memory. Block counters vary
  by OS and do not count network traffic. macOS RSS bytes and Linux KiB are
  normalized to bytes.
* `logical_io` counts byte-reader calls, requested ranges and delivered bytes,
  preserving the backend's batched `read_ranges` implementation. It does not
  measure backend requests. Projected Parquet reads should reduce these bytes.
* `http` counts actual HEAD/GET requests and body bytes at the loopback server.
  Delay is applied once per request before headers; network bandwidth, TLS,
  authentication, throttling and cloud topology are not simulated.
* Summary input-equivalent throughput uses whole file size, even when a projected
  scan reads fewer bytes; it is not physical transfer bandwidth.
* `output_bytes` sums Arrow array memory sizes across batches (or returned byte
  lengths). It is not retained/peak memory. Byte and WARC controls consume and
  discard results; the direct raw sequential control does not touch every byte,
  so its throughput must not be interpreted as memory bandwidth.

CPU saturation with stable logical I/O suggests decoder/allocation work;
latency increasing with HTTP request count and falling with concurrency suggests
I/O waits. Batch-size-dependent RSS and first-batch latency reveal buffering
tradeoffs. Confirm suspected CPU hotspots with a stack profiler, for example:

```sh
# Linux (permissions for perf are host-specific):
perf record -g -- target/release/open-arrow-access-benchmarks \
  target/benchmark-long raw lines 65536 256 1 2097152 all 100
perf report
# macOS: run the same long workload, then `sample PID 2 -file target/cpu.txt`.
```

## Allocation diagnostics

For suspected copies/allocation pressure, build an additional executable with
`cargo build --release -p open-arrow-access-benchmarks --features allocation-stats`.
Keep the ordinary executable separately and use `--binary` to select the
instrumented one. This optional global allocator counts allocation/reallocation
calls and cumulative allocated/deallocated bytes **within the timed scan**, not
preload. It adds atomic-counter overhead, so use the ordinary executable for
throughput claims. It does not report retained or peak allocator bytes.
Each sample declares its instrumentation; the comparison tool rejects comparisons
between instrumented and ordinary runs.

## Remote storage

The initial cloud entry point is S3/S3-compatible storage. Build with `--features s3`,
upload identical generated files separately, then supply the OpenDAL builder
configuration via `OAA_BENCH_STORAGE_CONFIG` (JSON string-to-string map containing
`bucket`, `root`, `region`, optional `endpoint`, and credentials as appropriate).
Run with `--backends s3` and the matching local `--data` manifest/shards. The
benchmark performs stat/read operations only; it never uploads or lists objects.
The configuration is not written to result files. Remote response errors may
contain endpoint details, so inspect logs before publishing. No real-cloud
latency, request cost or throughput result is claimed by a loopback run.

## Validate the harness

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s benchmarks -p 'test_*.py'
target/release/open-arrow-access-benchmarks generate target/benchmark-smoke 17 32
python3 benchmarks/run.py --data target/benchmark-smoke --samples 1 --warmups 0 \
  --iterations 1 --output target/benchmark-smoke.jsonl
cargo test --workspace --all-features --release --locked
```
