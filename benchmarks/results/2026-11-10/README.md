# Local read performance — 2026-11-10

The main sweep measures **all six formats across five local storage paths**, with
sequential reads, file-level parallel reads and composed native file planning.
There are **150 configurations × five measured rounds = 750 baseline samples**,
plus 24 provider operations × five rounds. Separate controlled experiments cover
three optimizations, allocator traffic and simulated response latency.

## Revisions and machine

- Format/storage baseline: `6154485ad282c24f96ac2e2643cf1951779e50a7`, fetched from
  upstream main during the session. The initial exploratory sweep used `ce58e2e`;
  those superseded samples are not bundled here.
- Main subsequently advanced to `dabc4c9` with reserved-query validation sharing.
  Its format, storage, dataset, layer and core implementations are identical to
  `6154485`. The final provider sweep uses `dabc4c9`; all PRs are based on it.
- Apple M5 Pro, 18 physical/logical CPUs, 24 GiB RAM, macOS 26.5.2 arm64.
  Sampling used **Homebrew rustc 1.97.1 / LLVM 22.1.8**, recorded verbatim in the
  matrix environments. The repository pins 1.97.0; final validation uses 1.97.0.
  These published timings are 1.97.1 results, not measurements of the pinned compiler.
- Release builds, four Tokio runtime threads, 32,768 rows/object, four logical
  files, 1,024-row batches, one or four resident partitions, two queued batches
  per partition. One warm-up per case, randomized full rounds with seed 0.
- OS page cache is warm. The same immutable object is selected four times.
  Fixtures have repeated ~200-byte text; Parquet has low cardinality and is
  particularly small. These are reproducible synthetic cases, not production
  capacity estimates. Background workloads and CPU placement were not controlled.
- Normal timing builds use `full`; allocator builds use `allocations` and their
  timings are kept separate. The CSV includes medians, ranges, CPU, RSS, first
  batch latency, request counts, payload bytes and allocation counters.

The S3 path is the **real OpenDAL S3 adapter/signing code against a minimal local
HEAD/GET/range fixture**. It is not AWS or MinIO. Docker was unavailable and the
MinIO macOS download returned HTTP 410. Actual cloud/server performance, cold-cache
storage, TLS/WAN behavior and cloud credentials were not tested. The Python
loopback server's CPU is excluded from child CPU accounting. Backend defaults for
output chunks and I/O are retained; `Connection` emits different-sized chunks from
OpenDAL. Parquet + sequential-only `Connection` is unsupported and omitted.

## Baseline format/storage matrix

Times below are medians in milliseconds for **131,072 rows**. The detailed CSV
contains every mode/worker/backend combination and their ranges.

| Format | Memory seq | FS seq | HTTP seq | S3 fixture seq | Connection seq | Memory native P=4 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| csv | 18.29 | 20.58 | 34.99 | 35.91 | 20.29 | 31.38 |
| json | 10.69 | 13.31 | 28.41 | 29.04 | 13.25 | 16.98 |
| parquet | 1.26 | 1.99 | 14.63 | 15.08 | unsupported | 1.59 |
| atif | 591.29 | 589.21 | 625.31 | 612.98 | 594.57 | 300.82 |
| warc | 122.64 | 126.35 | 152.16 | 151.17 | 123.67 | 99.47 |
| xml | 152.00 | 155.98 | 171.02 | 172.67 | 153.71 | 601.50 |

CPU-heavy decoding is visible on memory input: eliminating storage does not make
ATIF/WARC/XML scans free, whereas JSON/CSV are already fast enough that framing
and task overhead matter. Native JSON frames with serde and then decodes with
Arrow; CSV parses/canonicalizes before Arrow decoding. For these short records,
native P=4 is slower than sequential. This is a composition tradeoff in these
fixtures, not evidence to replace parsers or change defaults globally. File-level
parallelism is useful when there is I/O wait; see the latency experiment below.

Parquet's tiny low-cardinality corpus makes per-reader/partition setup prominent;
its absolute milliseconds cannot be extrapolated to large production Parquet.

### CPU and memory scope

The binary's `seconds` covers open-to-EOF. `wait4` CPU and RSS cover the **entire
process**, including fixture loading/runtime/client setup. Process CPU / process
elapsed is meaningful; process CPU / read-region time is not utilization.
`max_batch_arrow_bytes` measures one consumed Arrow batch, not total decoder or
queue memory. RSS includes inputs and runtime overhead. Block I/O counters are
not page-cache bytes or HTTP traffic. HTTP payload/request counters are separate.

## Optimization 1: group XML native decode partitions

The original XML planner created **one task and one single-row Arrow batch per
record**, ignoring the requested 1,024-row batching for native partitions. That
produced 131,072 tasks/batches instead of 128. The fix uses the shared lazy
`RecordChunks::into_plan`, preserving source record indices/offsets, namespaces,
projection/inference and record order. Groups respect the row target and the
smaller of the soft partition byte target and XML's serialized batch-byte limit.
Oversized individual records retain the existing soft-target behavior.

Five interleaved measured rounds, same corpus and parameters:

| Backend | P=1 before → after ms | Speedup | P=4 before → after ms | Speedup | P=4 median RSS before → after MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| memory | 1457.61 → 166.57 | 8.75× | 607.84 → 104.11 | 5.84× | 19.3 → 22.1 |
| fs | 1449.19 → 168.35 | 8.61× | 607.56 → 111.75 | 5.44× | 18.0 → 24.0 |
| http | 1484.73 → 185.93 | 7.99× | 642.03 → 118.34 | 5.43× | 23.2 → 25.0 |
| s3-fixture | 1451.36 → 188.87 | 7.68× | 634.96 → 119.94 | 5.29× | 23.6 → 26.1 |
| connection | 1431.36 → 168.90 | 8.47× | 624.35 → 101.82 | 6.13× | 15.8 → 18.9 |

Separate instrumented memory-input/P=1 scans (three repeats) reduce median
allocation calls from **6,030,285 to 1,316,429**,
and cumulative allocated bytes from **1,073.2 MB to
383.0 MB**. These are allocation traffic, not live/peak RAM.
RSS rises modestly because multiple full batches can be resident. This is an
explicit speed/allocation-traffic versus residency tradeoff, not an RSS reduction.

## Optimization 2: bulk-copy ATIF partition frames

The planner flattened owned `Bytes` into a byte-at-a-time iterator collected into
a growing `Vec`. The fix sums complete-frame lengths, reserves once and extends
from each slice. It copies the same bytes, including original line endings and
whitespace; validation, raw JSON and global source indices are unchanged.

Seven interleaved rounds:

| Backend | P=1 before → after ms | Speedup | P=4 before → after ms | Speedup |
| --- | ---: | ---: | ---: | ---: |
| memory | 676.81 → 602.60 | 1.12× | 292.25 → 261.09 | 1.12× |
| fs | 690.38 → 601.03 | 1.15× | 301.36 → 279.52 | 1.08× |
| http | 720.88 → 612.48 | 1.18× | 307.82 → 277.73 | 1.11× |
| s3-fixture | 711.84 → 613.16 | 1.16× | 320.23 → 272.96 | 1.17× |
| connection | 687.27 → 602.36 | 1.14× | 280.95 → 262.30 | 1.07× |

## Optimization 3: inline row-field presence

The shared metadata builder allocated a `Vec<bool>` for every row to detect
missing/duplicate writes. A `u64` mask covers the first 64 output fields; a vector
retains support for arbitrary wider schemas. No public API or width limit changes.

The parser-free control builds 1,000 batches × 1,024 rows × eight UInt64 fields.
Seven normal timing repeats improve median time from **163.09 to 140.50 ms
(1.16×)**. Three independent instrumented repeats show **1,081,011
→ 57,011 allocation calls**, exactly one fewer allocation per row.
Reallocations stay unchanged; cumulative allocated bytes drop by
**8,192,000 bytes**. This does not imply
an equivalent RSS reduction.

The WARC end-to-end check has overlapping timing ranges: sequential improves
slightly and native P=4 does not show a reliable improvement. The control proves
lower builder overhead/allocation pressure; it does **not** establish a blanket
parallel-WARC speedup. See `builder-warc-comparison.jsonl`.

## Controlled I/O latency

Every local HEAD/GET response waits 10 ms before sending its body. This models
per-request wait, not WAN bandwidth, full TCP/TLS behavior or cloud service work.
Per-reader I/O concurrency is distinct from file-level resident partitions.

| Format / backend / mode | C=1 median ms | C=4 median ms | C=1 HTTP requests | C=4 HTTP requests |
| --- | ---: | ---: | ---: | ---: |
| bytes / connection / sequential | 58.98 | 59.25 | 4 | 4 |
| bytes / http / sequential | 279.63 | 114.66 | 20 | 20 |
| bytes / s3-fixture / sequential | 270.13 | 117.23 | 20 | 20 |
| json / connection / files | 40.06 | 37.89 | 4 | 4 |
| json / connection / sequential | 66.37 | 63.85 | 4 | 4 |
| json / http / files | 210.43 | 64.79 | 20 | 20 |
| json / http / sequential | 283.68 | 126.54 | 20 | 20 |
| json / s3-fixture / files | 205.15 | 57.12 | 20 | 20 |
| json / s3-fixture / sequential | 285.32 | 127.52 | 20 | 20 |
| parquet / http / files | 420.47 | 403.60 | 44 | 44 |
| parquet / http / sequential | 565.04 | 551.41 | 44 | 44 |
| parquet / s3-fixture / files | 417.86 | 406.72 | 44 | 44 |
| parquet / s3-fixture / sequential | 575.24 | 558.68 | 44 | 44 |

OpenDAL obtains a content length and splits sequential I/O into requests;
`Connection` streams a GET without the same HEAD/splitting path. Thus the latter's
request count differs. These are different defaults/capabilities, not interchangeable
Parquet backends. Existing `io_concurrency`, `io_chunk_size`, content-length hints
and file-level parallelism provide workload-specific tuning; increasing defaults
would also change residency and was not justified by these fixtures.

## Provider operations

These use public service entry points with immutable no-network response fixtures,
not production API calls. Each process constructs its service/fixtures once, warms
one operation, then measures 10,000 operations. Metadata resolvers retain lazy
attachments; HF/PMC/inventory/Gutenberg drain Arrow, CourtListener drains decoded
bytes, MECA validates membership, Common Crawl resolves paths. Operations and
fixture sizes differ, so this table is not a provider ranking.

| Provider | Median µs/operation | Median process RSS MiB | Requests/operation |
| --- | ---: | ---: | ---: |
| arxiv | 18.28 | 11.4 | 0 |
| bhl | 3.30 | 12.2 | 1 |
| biorxiv | 3.71 | 12.2 | 1 |
| commoncrawl | 0.69 | 9.8 | 0 |
| courtlistener | 29.83 | 13.7 | 0 |
| dataverse | 3.19 | 12.6 | 1 |
| doab | 13.13 | 13.5 | 1 |
| federal-register | 3.04 | 12.1 | 1 |
| figshare | 3.32 | 12.2 | 1 |
| govinfo | 3.92 | 12.2 | 1 |
| gutenberg | 38.31 | 12.0 | 0 |
| hathifiles | 4.54 | 12.2 | 1 |
| huggingface | 45.86 | 15.5 | 5 |
| internet-archive | 3.05 | 12.6 | 1 |
| library-of-congress | 5.07 | 12.3 | 1 |
| meca | 20.63 | 12.0 | 0 |
| oapen | 13.07 | 13.5 | 1 |
| openstax | 3.80 | 12.2 | 1 |
| pmc | 5.59 | 11.1 | 0 |
| pmc-inventory | 37.57 | 13.2 | 0 |
| regulations | 3.82 | 12.2 | 1 |
| semantic-scholar | 5.64 | 12.2 | 2 |
| uspto | 3.93 | 12.2 | 1 |
| zenodo | 2.22 | 12.4 | 1 |

## Validation and reproduction

On pinned Rust 1.97.0, the benchmark branch passes **287 tests across 80 suites**;
a local validation branch with all three independent fixes passes **291 tests
across 80 suites**. Clippy for all targets/features with warnings denied, no-default-feature workspace
check, locked metadata, Rust formatting, Taplo 0.10.0 formatting/lint and Python
syntax/format checks. Two existing Cargo manifests received dependency-order-only
formatting so the repository-wide Taplo check is clean.

Targeted tests: XML 14, ATIF 8, shared row builder 8, including grouped row/byte
limits, empty projection, global error offsets, raw-json/CRLF fidelity, nested
fields, duplicate writes and narrow/wide presence boundaries. Final combined
validation additionally runs the full workspace suite, Clippy across all targets
and features, Rust/TOML formatting, Taplo lint, locked metadata and no-default-feature
workspace checking. See the PR descriptions for final check status.

Use the [harness guide](../../README.md) for build/run commands and scope.
`summary.csv` is generated from every published JSONL file by
`benchmarks/summarize.py`; individual samples retain ranges and parameters.
`binary-sources.json` records executable hashes, production revisions and flags.
Environment checkout HEAD can differ from an explicitly supplied executable's
source; published environment paths are reduced to basenames only.

Unmeasured: real S3/GCS/Azure servers, cold disks, giant/heterogeneous records,
full compression × format × backend matrices, skewed partitions and provider
rate limits/pagination over live networks. All three demonstrated code bottlenecks
are in this repository's composition/shared-builder code, so their fixes belong
here rather than in the ATIF/WARC/XML parser repositories. No parser changes or
upstream PR merges are needed for these fixes.
