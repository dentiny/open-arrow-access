# Local read performance matrix — 2026-11-10

Baseline: upstream main `42673eb04fbce2c2cfcaa487293c480d1934e3ba` (validation refactor, #314), plus benchmark-only commit `09998fa`. Optimized code adds CSV record-buffer reuse and contiguous JSON partition framing. The repository was fetched again before this run because main advanced during the initial investigation.

## Method

- Apple M5 Pro, 18 logical CPUs, 24 GiB RAM; macOS 26.5.2 (25F84).
- Homebrew Rust/Cargo 1.97.1, release profile, committed Cargo.lock; 8 Tokio worker threads. This records the compiler actually used by PATH; the repository's rust-toolchain.toml requests 1.97.0.
- OpenDAL local filesystem, warm page cache, no compression. Fixture creation and compilation are outside measurement. The final runs were sequential, after tests and compilation finished.
- Six formats: CSV, JSON object streams, Parquet, XML, ATIF JSON Lines, and WARC-to-Arrow.
- Three dataset entry points: one file, explicit FileSet, sorted Directory. Eight shards, 32,768 rows per shard; dataset scans return 262,144 rows. Shards deliberately share identical bytes and schemas. ATIF repeats the first committed trajectory fixture; the other formats are generated deterministically.
- Parallel workers: 0 (ordinary sequential open), 1, 2, 4, 8. Prefetch is 2 batches/partition. Default Arrow batch size is 8,192. Per-reader byte I/O keeps its defaults: concurrency 1 and backend-default chunk size; this is a record-worker sweep, not a storage-I/O-concurrency sweep. Multi-file scans exercise both whole-file partitioning and `with_file_planning()` native partitions. Single files exercise their native planner. Parquet has four row groups/file. Sequential mode ignores the native flag and is measured only once per dataset.
- Each configuration has one discarded warmup and five measured baseline repetitions: **138 configurations, 690 samples**. The CSV/JSON before/after confirmation has nine measured repetitions each: another 828 samples. Total final data: **1,518 measured scans**, all with asserted row counts and fully consumed batches.
- Timing starts before `open` and includes discovery, schema preparation, scheduling and consumption. `first_batch_seconds` includes open and first output. Rows and batch counts are recorded; `black_box` consumes each batch. Existing format/dataset/parallel tests cover values, schema and ordering; the performance harness itself verifies counts.

These are synthetic local workloads. Cross-format times are not a codec ranking: ATIF trajectories and WARC records have different schemas and semantic work from the two-column CSV/JSON/Parquet/XML records. Network providers, compression, cold-cache I/O, wide schemas, projected scans, skewed file sizes, queue-depth sweeps and backend-global I/O contention are not measured. A shared laptop and fixed case order also introduce noise; raw samples and min/max are included. No significance test or universal speedup is claimed.

## Baseline matrix

Median milliseconds; lower is better. File P4 uses whole-file partitions; native P4/P8 uses format partitions flattened through the same executor. For a single file, file-level P4 is inapplicable. Every omitted worker setting is retained in [baseline-summary.csv](baseline-summary.csv), including first-batch latency and throughput.

| Format | Dataset | Sequential | File P4 | Native P4 | Native P8 |
|---|---|---:|---:|---:|---:|
| csv | single | 1.243 | — | 5.392 | 5.111 |
| csv | files | 9.880 | 5.421 | 38.577 | 39.502 |
| csv | directory | 10.025 | 5.523 | 44.264 | 41.320 |
| json | single | 2.406 | — | 4.928 | 4.413 |
| json | files | 17.748 | 9.979 | 30.324 | 35.223 |
| json | directory | 17.818 | 10.491 | 29.016 | 30.367 |
| parquet | single | 0.482 | — | 0.303 | 0.315 |
| parquet | files | 5.730 | 2.310 | 2.568 | 2.097 |
| parquet | directory | 4.036 | 3.161 | 2.515 | 2.088 |
| xml | single | 31.653 | — | 23.911 | 24.000 |
| xml | files | 270.690 | 206.087 | 174.653 | 180.860 |
| xml | directory | 262.136 | 179.302 | 169.833 | 180.229 |
| atif | single | 363.366 | — | 164.573 | 162.360 |
| atif | files | 2914.177 | 1922.801 | 951.830 | 779.301 |
| atif | directory | 2803.929 | 1741.768 | 877.570 | 656.814 |
| warc | single | 25.217 | — | 19.303 | 19.667 |
| warc | files | 191.814 | 136.705 | 144.655 | 145.618 |
| warc | directory | 196.939 | 140.834 | 145.894 | 145.451 |

## Measured improvements

The table compares nine-repetition confirmation runs on the same upstream baseline, using native partitions and four workers. The optimized run sits between the full baseline run and the confirmation baseline run; compilation and test tasks were not running. The patches affect native planning; sequential readers are unchanged in code.

| Format | Dataset | Before ms | After ms | Speedup | Time reduction |
|---|---|---:|---:|---:|---:|
| csv | single | 4.787 | 1.932 | 2.48× | 59.6% |
| csv | files | 37.831 | 14.381 | 2.63× | 62.0% |
| csv | directory | 37.919 | 14.383 | 2.64× | 62.1% |
| json | single | 4.272 | 1.940 | 2.20× | 54.6% |
| json | files | 26.242 | 12.451 | 2.11× | 52.6% |
| json | directory | 26.440 | 12.821 | 2.06× | 51.5% |

### CSV: allocating/cloning records in the sequential framer

`csv/planning.rs` previously consumed `into_byte_records()`, producing an owned ByteRecord for every row. That iterator clones its reusable internal record for each item. The framer immediately writes the row into canonical CSV and discards the owned record, so the ownership/allocation work buys nothing. The patch keeps one ByteRecord in CsvChunks and calls `read_byte_record(&mut record)`, retaining its buffers across rows and partition boundaries. Header ownership remains separate. Existing dialect, split BOM, escaping, multiline-field, projection, error and byte-target tests pass.

Native CSV still parses, rewrites and Arrow-decodes its input. Buffer reuse removes one bottleneck; it does not eliminate that serial multi-pass pipeline. For these small local shards, file-level parallelism can remain faster than native planning. A separate experiment replacing per-row writer flushes with exact serialized-size counting was rejected: combined with reuse it was slower than reuse alone in the initial `ca31d4e` investigation (FileSet P4 medians 14.699 ms versus 13.570 ms). It is not part of the patches.

### JSON: per-record slicing, collection and copying

`json/planning.rs` previously sliced one Bytes value per object, collected a Vec of those values and copied every object into a new concatenation buffer, inserting newlines. The patch validates the same object stream with serde_json but advances a cursor over pending input and freezes one contiguous slice per partition. It removes the record stream/Vec and concatenation copy. Incomplete tails retain their bytes; complete earlier objects are not reparsed after an input refill. Oversized records, strict row limits, soft byte targets, nested objects, adjacent objects without separators, invalid syntax, fragmented input, projection, gzip and EOF behavior remain covered.

A new regression test exercises three input chunk sizes, three row targets and four byte targets, including quotes/braces, trailing whitespace and empty streams. Long whitespace after completed records can trigger the byte target rather than growing a partition indefinitely.

JSON still has a validation/framing pass and Arrow's decode pass. On many small shards, whole-file parallel reads can remain preferable even after the improvement. Simply preallocating the old concatenation buffer was a much smaller candidate; the retained patch eliminates that buffer entirely.

## Other bottlenecks and limits

These are observations plus source-level explanations, not claims that every component has been CPU-profiled.

- **Parquet:** Native row-group partitions can scale for this workload. Each partition opens its own byte reader, validates identity and constructs a stream; tiny row groups make this overhead visible, especially with one worker. Footer metadata is already loaded once and shared. A single file has only four groups, so eight configured workers cannot decode eight independent groups. See `parquet/planning.rs` and `parquet/validation.rs`.
- **XML:** Native planning performs sequential record framing before dispatching XML conversion. Schema inference also contributes to open/first-batch latency. More workers cannot accelerate that serial discovery step and can add scheduling/buffering costs. See `xml/planning.rs`, particularly `infer_records`, `read_records` and `XmlBatchBuilder::append_xml`. An explicit schema avoids inference but is not benchmarked here.
- **ATIF:** CPU sampling during the initial investigation shows upstream `atif_arrow::TrajectoryBatchBuilder::append_json`, normalization, serde_json Value/BTreeMap work, string operations and allocator activity. This identifies upstream conversion/allocation as a substantial target, rather than establishing a storage bottleneck. Native partitions provide smaller work units; their scaling differs from whole-file workers. See [atif-sample.txt](atif-sample.txt). This dependency-level hotspot remains open; these PRs do not modify or vendor atif-arrow.
- **WARC:** Native planning serially parses and owns raw headers/bodies before workers convert them into Arrow rows. Header/date/string conversion and metadata building remain downstream work. The measurements should guide whole-file versus native selection; the source-level explanation is not a WARC CPU profile. See `warc/planning.rs` and `warc/arrow.rs`.
- **Directory/FileSet/executor:** Directory includes listing, validation, sorting and deduplication; FileSet uses caller selection directly. This eight-file matrix quantifies those end-to-end paths but does not establish behavior for very large directories. Ordered output intentionally retains resident slots until earlier queues drain. Slow-first-file head-of-line blocking and queue-depth tuning need a separate skewed-workload benchmark; they were not measured or changed here.

## Reproduction

Build the harness-only commit `09998fa` for the baseline, and this PR branch for optimized code. Both sit on upstream `42673eb`. Use the same compiler, lockfile, machine and runtime settings for comparisons.

```sh
cargo build --release --locked -p open-arrow-access-dataset --features directory --example read_matrix
BENCH_REPS=5 target/release/examples/read_matrix > baseline.csv
BENCH_FORMAT=csv,json BENCH_REPS=9 target/release/examples/read_matrix > confirmation.csv
python3 docs/benchmarks/summarize.py baseline.csv > baseline-summary.csv
```

Run the first two commands on each intended revision, changing output filenames. Environment overrides are `BENCH_ROWS`, `BENCH_SHARDS`, `BENCH_REPS`, and comma-separated `BENCH_FORMAT`. Defaults are 32768/8/5/all six formats. Keep compiler/test processes out of timed runs. An independent full comparison can run all six formats on both revisions; this report reruns changed CSV/JSON paths and retains the full upstream baseline for the other formats.

Raw samples: [baseline.csv](baseline.csv), [confirmation.csv](confirmation.csv), [optimized.csv](optimized.csv). Summaries: [baseline-summary.csv](baseline-summary.csv), [confirmation-summary.csv](confirmation-summary.csv), [optimized-summary.csv](optimized-summary.csv). Rates use logical rows rather than compressed/physical-byte throughput.

## Validation

On upstream `42673eb` with both patches: `cargo test --workspace --all-features --locked` passed **293 tests**, zero failures/ignored; `cargo clippy --workspace --all-features --all-targets --locked -- -D warnings` passed. `cargo fmt --all --check` and diff whitespace checks passed. Separate small PRs contain only their respective CSV or JSON changes; the umbrella PR includes the harness, evidence and both optimizations. Their overlapping patches should be merged through one review path.
