# Local format/storage performance report — 2026-11-09

Measured on an Apple M5 Pro with 24 GiB RAM, macOS 26.5.2, Rust 1.97.0
(`2d8144b78`), optimized release builds. The production baseline is
`open-arrow-access` main at `d04720975a76d72d5c57083bcd4f95fe586f7cf4`.
The benchmark harness is at `328a012f683893fea37d79f77eca0618c93ee30e`.

The measurements identify three avoidable costs in this repository and a larger
ATIF schema-construction cost in `atif-arrow`. Each fix is measured independently;
**speedups below must not be multiplied together**. No change to storage defaults
is proposed from these local results.

## Method and coverage

The matrix covers all six format families: CSV, JSON, XML (explicit/inferred
schema), ATIF JSONL, Parquet/Snappy and native WARC. It exercises the generic
OpenDAL adapter through memory, filesystem and real loopback HTTP Range requests,
plus a direct shared-Bytes control that removes OpenDAL I/O. S3 configuration is
supported but no real cloud service was benchmarked. Dataset-provider discovery,
compression and archives are outside this initial matrix.

The main dataset has 32,768 ordered records, each with a deterministic 512-byte
ASCII payload. Formats share IDs/payloads, but their output structures differ;
these timings are not a ranking of equivalent decoder work. Long-record runs use
4,096 records with 8,192-byte payloads; latency runs use 4,096 × 512-byte payloads.
Generation is outside measurement, and hashes of every input are in each JSONL.

Ordinary scans use a current-thread Tokio runtime. Timers include reader opening,
schema preparation/inference and consumption, excluding process startup, operator
setup and raw/memory preload. Each sample runs in its own process; `wait4` records
that child's CPU and whole-process peak RSS, including preload and allocator
retention. RSS is not isolated decoder memory or a live-buffer maximum.

Normal sweeps have five measured samples after one warmup, three scans per
sample. Long-record comparisons have nine samples × eight scans; repeated
collection has seven × ten, and schema comparisons have seven × three. Tables
show median time **per scan**, and median process peak RSS **without dividing by
iterations**. Each round randomizes workload order. The OS page cache is warm;
these are not physical disk bandwidth or real-cloud measurements. Blocks/CPU
counters include startup and preload, unlike scan time. Bootstrap intervals only
cover variation within these runs on this machine.

Allocation diagnostics use a separate executable with `allocation-stats`; its
atomic counters perturb performance, so its times are not used for speed claims.
Counts cover the timed scan, excluding preload. Row counts and collected-byte
lengths are checked; full value/schema/error behavior is checked by crate tests.

The JSONL `revision` field is the checkout at runner invocation, not necessarily
the source of a preserved executable. [binaries.json](binaries.json) maps executable
SHA-256 to source variants, and [patches/](patches/) contains the exact independent
changes applied to the harness baseline. Upstream ATIF source before the cache
change matches the published 0.4.0 `src/` tree byte-for-byte. Its integration uses
a temporary Cargo path override; the only lockfile change is removing the
registry source/checksum for that same version. No local path override is shipped.

## Format and storage baseline

The default matrix uses 64 KiB emitted chunks, 8,192-row batches, one I/O task,
and 2 MiB I/O chunks, without projection.

| Format | Direct Bytes ms | Filesystem ms | Loopback HTTP ms | Filesystem process peak RSS MiB |
| --- | ---: | ---: | ---: | ---: |
| CSV | 7.97 | 9.43 | 15.50 | 33.1 |
| JSON | 3.05 | 4.37 | 10.69 | 33.4 |
| XML, explicit schema | 55.12 | 53.31 | 60.11 | 21.7 |
| ATIF JSONL | 379.97 | 380.86 | 402.33 | 35.5 |
| Parquet/Snappy | 2.35 | 4.04 | 11.85 | 73.3 |
| WARC, native records | 19.17 | 20.83 | 28.05 | 16.5 |

[Full matrix](matrix-summary.md), [raw samples](matrix.jsonl).

ATIF's direct scan consumes approximately 381 ms process CPU for a 380 ms scan;
removing storage barely changes it. XML and WARC also spend most of the local
scan in CPU work, though their output contracts differ. XML inference adds a
bounded startup sample, not another complete-file read in these runs. A two-second
`sample` of the ATIF process shows `TrajectoryBatchBuilder::append_json →
normalize_trajectory → trajectory_schema` and nested schema allocation on the hot
path. The line workload's samples are dominated by the `read_lines` polling path.

### I/O: request granularity and concurrency

With 2 ms artificial per-request latency on the 2 MiB input:

| Workload | Configuration change | Median before → after ms | HTTP GETs before → after |
| --- | --- | ---: | ---: |
| Sequential | 64 KiB → 2 MiB I/O chunks, concurrency 1 | 106.68 → 9.60 | 33 → 2 |
| Sparse ranges | concurrency 1 → 4, 2 MiB I/O chunks | 790.55 → 215.00 | 257 → 257 |
| Parquet | 64 KiB → 2 MiB I/O chunks, concurrency 1 | 114.30 → 19.30 | 36 → 5 |

The sparse scan deliberately reads 4 KiB every 8 KiB: approximately 1 MiB of
payload in 257 nonadjacent ranges. The adapter preserves gaps instead of fetching
unrequested bytes. Concurrency overlaps those requests; increasing chunk size
alone cannot coalesce the holes. This is an explicit I/O/memory/request-count
tradeoff, not evidence that the existing 2 MiB default should change.
The main 16 MiB sparse workload makes 2,052 GETs for approximately 8 MiB of
returned data and takes 303.65 ms even without injected latency.

[Latency/concurrency sweep](io-summary.md), [raw samples](io-sweep.jsonl).

### Projection and batch memory

Projecting only Parquet `id` reduces logical bytes from 16.32 MiB to 0.18 MiB,
HTTP GETs from 18 to 6, median HTTP scan time from 11.85 to 2.20 ms, and HTTP
process peak RSS from 72.1 to 11.8 MiB. CSV, JSON and XML still consume their
whole input when projecting `id`; they must scan the serialized stream.

For the direct 64 KiB-chunk scan, reducing batch size from 8,192 to 256 lowers
CSV process peak RSS from 43.0 to 25.1 MiB and ATIF from 48.5 to 31.1 MiB.
CSV times remain about 8 ms; ATIF times remain about 382–386 ms. Smaller batches
reduce output buffering here without fixing the dominant ATIF conversion cost.
Parquet row-group/input buffering is distinct from emitted Arrow batch size.

[Projection results](projection-summary.md), [batch/chunk sweep](decoder-summary.md).
Input-equivalent throughput in generated summary tables divides whole file size
by scan time, even for projected scans; it is not physical transferred bandwidth.
The zero-copy raw sequential control also does not touch every input byte.

## Independently measured fixes

### 1. SIMD-assisted newline search in `open-arrow-access`

Replace the per-byte `position` loop with `memchr`, retaining the existing
zero-copy line slicing, binary content, LF/CRLF handling and bounded-line checks.
On the long-record dataset with 64 KiB chunks, line splitting falls from
8.432 to 0.656 ms: **12.86×** (bootstrap 95% interval 12.47–13.61×).
With 1 KiB chunks it improves 12.138 → 3.540 ms, **3.43×**. ATIF conversion of
those same long records improves 63.570 → 55.514 ms at 64 KiB chunks, **1.15×**;
peak RSS is essentially unchanged.

[Comparison](memchr-comparison.md), [baseline](long-baseline.jsonl),
[candidate](long-memchr.jsonl), [patch](patches/b9da239.patch).
Regression tests cover binary tails, CRLF, SIMD-size/chunk boundaries, exact
line limits, terminal errors and empty input. This is not a claim about all line
lengths or CPU architectures.

### 2. Borrow ATIF document input in `open-arrow-access`

Keep each `Bytes` document alive and validate/borrow its UTF-8 while appending,
instead of first converting shared line slices to an owned `Vec`/`String`.
On the main dataset with 256-row batches this removes **32,439 allocations** and
**21,366,334 cumulative allocated bytes (20.38 MiB)** per scan. Long-record
runtime changes are small and similar to unchanged line-control drift; **no
reliable throughput improvement is claimed for this change alone**. This is a
copy/allocation reduction, not the fix for ATIF's dominant CPU cost.

[Allocation baseline](alloc-baseline.jsonl), [candidate](alloc-atif-borrow.jsonl),
[timing/control comparison](borrow-comparison.md), [patch](patches/5b07659.patch).
Existing ATIF tests retain verbatim raw JSON, nested content, blank-line handling,
one-based document indices, projection and malformed UTF-8/source errors.

### 3. Bound length-hint reservation in `open-arrow-access::read_all`

The old collector geometrically grows a `Vec` from empty. Reserve from the known
length, capped at 64 MiB, after receiving the first nonempty chunk. Empty inputs
and initial errors allocate no output; unknown, understated and overstated hints
do not change the bytes collected. Larger inputs can continue to grow normally.

For 16.03 MiB input, the allocation-instrumented scan goes from nine reallocations
to zero; cumulative allocated bytes fall from 33,569,140 to 16,824,692. The number
of allocation calls remains 523, because those counts also include reader/chunk
futures and metadata.

| Filesystem collection | Median before → after ms | Process peak RSS before → after MiB |
| --- | ---: | ---: |
| One scan per process | 2.556 → 2.380 | 32.3 → 28.3 |
| Ten scans per process | 2.096 → 1.231 per scan | 178.9 → 30.5 |

Repeated collection gives **1.70×** filesystem speedup (1.63–1.75× interval).
Direct-memory repeated collection improves about 3×. The larger repeated-scan RSS
change includes allocator-retained memory across scans; it is **not** a claim
that one live 16 MiB collection required 179 MiB. A single scan has a much smaller
RSS improvement and its latency benefit is small; HTTP single-scan time is
unchanged within noise. Sequential streaming controls retain their time/memory.

[Repeated comparison](collect-comparison.md), [single-scan comparison](collect-once-comparison.md),
[allocation candidate](alloc-reserve.jsonl), [patch](patches/e52b68a.patch).
New tests include absent, zero, underestimated and `u64::MAX` hints, empty input,
empty chunks, and initial/late source errors.

### 4. Reuse the immutable schema in upstream `atif-arrow`

`normalize_trajectory` calls `trajectory_schema()` for each document. The schema
is fixed, but the original function rebuilds its nested fields, metadata maps and
reference-counted containers every time. A process-wide `LazyLock<SchemaRef>`
builds it once, with subsequent calls cloning the `Arc`. Thread sharing and
caller copy-on-write isolation are tested; schema contents are unchanged.

This is measured with **the original open-arrow-access adapter**, independently
of the three changes above. Across the four backends it improves ATIF scans by
**1.63–1.65×**. Filesystem time falls **378.796 → 229.093 ms** (1.65×,
1.64–1.66× interval), and direct Bytes time **377.257 → 229.531 ms**.

Allocation diagnostics for 32,768 trajectories, batch size 256:

| Counter | Before | Cached schema | Reduction |
| --- | ---: | ---: | ---: |
| Allocation calls | 13,277,520 | 5,908,592 | 55.5% |
| Cumulative allocated bytes | 1,209,409,852 | 466,233,725 | 61.4% |

Peak RSS is essentially unchanged: this removes repeated temporary allocations,
not the retained output batches. Schema construction remains constant regardless
of projected fields, so the cache also benefits callers selecting fewer columns.
The latter follows from the implementation; it is not a separately measured
projection speed claim.

[Comparison](schema-comparison.md), [baseline](schema-baseline.jsonl),
[candidate](schema-cached.jsonl), [allocation candidate](alloc-schema-cached.jsonl),
[upstream patch](patches/7224617.patch).
Upstream base: `a2ece1dcfdaea46a78a26682bb8a6829aefb2b56`.
Candidate: `7224617` in `dentiny/atif-arrow`.

## Reproduce and validate

Use [the benchmark guide](../../README.md) for build/generation and every sweep.
Regenerate `32768 512`, `4096 8192` and `4096 512` shards in separate directories;
compare their hashes with the recorded manifests. To reproduce one isolated fix,
start from harness revision `328a012`, apply only its archived patch, build a
separate executable, and run the exact dimensions recorded in its JSONL. Copy the
patch outside the checkout before changing revisions. Preserve the baseline
executable and run comparisons with `--binary`; `binaries.json` distinguishes
ordinary and allocation-instrumented executables.

For upstream ATIF, check out `7224617` in an adjacent clone and build the unchanged
harness baseline using a temporary Cargo config:

```toml
[patch.crates-io]
atif-arrow = { path = "/absolute/path/to/atif-arrow/crates/atif-arrow" }
```

Do not commit this local override or the corresponding temporary lockfile change.
The baseline is the published 0.4.0 dependency; only the schema source differs.

Validation performed includes the harness's HTTP/accounting/process-resource
contract tests, the 44-case smoke matrix, core/all-format regression suites,
upstream `atif-arrow` tests and Clippy, ATIF integration tests with the path
candidate, and the full combined workspace all-feature release tests and Clippy.
Changed TOML files pass Taplo 0.10.0; the original `storage/Cargo.toml` has an
unrelated pre-existing key-order formatting failure in the repository-wide check.
All PRs are intended for review only; none is merged as part of this work.
