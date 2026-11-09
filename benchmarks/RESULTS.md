# Local benchmark findings — 2026-11-09

Measured upstream `dentiny/open-arrow-access` main at
`b2c508eaae91244b632555ed50f3325ffe882085`, with generated deterministic inputs.
The host was macOS 26.5.2, arm64, 18 logical CPUs; Rust was Homebrew 1.97.1
(LLVM 22.1.8). The repository toolchain file names 1.97.0; these measurements
record the actual compiler rather than claiming to use the pinned patch release.

The suite covers all six formats and memory/FS/HTTP/S3 adapter paths. HTTP and S3
are local protocol replay, and FS is warm-cache. These are **not live cloud or
provider API benchmarks**. See [README.md](README.md) for methodology and commands.

## Findings and review units

1. **ATIF fan-out validation is quadratic.** Each referenced observation result
   scans the step's tool calls. Build a borrowed-key hash index lazily when both
   groups exceed eight entries. Small groups retain their allocation-free scan;
   error order, optional IDs and invalid-reference paths are preserved.
2. **Nested XML scalar mappings allocate selector vectors per path segment.**
   Walk unique prefixes directly. If an intermediate prefix branches, fall back
   to the existing selector: multiple parents can still produce one final value.
   Namespace matching, missing/null values and repeated-value errors are tested.
3. **Fragmented storage output retains a temporary `Vec<Bytes>` before copying.**
   Copy directly into one final byte allocation on the fragmented fallback path;
   contiguous chunks retain their existing zero-copy path.
4. **Eager I/O-range assembly can retain huge numbers of tiny fragment descriptors.**
   Offer `with_streaming_reads(true)` for sequential reads without explicit I/O
   splitting. This is a **memory/CPU tradeoff**, disabled by default. Exact ranges,
   explicit chunk sizes and concurrency above one retain the original reader.

The umbrella PR includes the benchmark/report and storage changes. A separate
storage PR extracts the same production changes for independent review; ATIF and
XML changes belong in their own upstream repositories. No dependency release or
local Cargo path override is committed to the umbrella PR's lockfile.

## ATIF: alternating-process comparison

Each row measures 64 trajectories with N calls and N results per trajectory,
reverse-order references, projection to `trajectory_id`, one warm-up and seven
scans per process. Five independent processes per variant alternate A/B order.

| Calls/results per trajectory | Before ms | After ms | Speedup |
| --- | ---: | ---: | ---: |
| 8 | 1.210 | 1.116 | 1.08× |
| 32 | 4.326 | 4.309 | 1.00× |
| 256 | 37.014 | 24.134 | 1.53× |
| 1024 | 314.939 | 94.266 | 3.34× |

The smaller rows are controls, not claimed improvements. At 1024 calls the
end-to-end reduction is about 70%. Three-second macOS `sample` captures also
support the attribution: `validate_details` appears in 1688/2477 main-thread
samples before, versus 159/2463 after. Baseline stacks repeatedly traverse JSON
value keys and `memcmp`; the patched profile moves most work to normalization and
JSON parsing. These are sampling fractions, not exact CPU-time percentages.
The normalized evidence is in [results/cpu-samples.json](results/cpu-samples.json).

## XML: alternating-process comparison

32,768 records, two scalar fields, 128-byte text, batches of 1024. Five processes
per variant, seven scans each. The nested mapping is `a/b/id` and `a/b/text`.
The direct mapping is an unchanged-path control.

| Case | Backend | Before ms | After ms | Speedup | Peak RSS MiB (before → after) |
| --- | --- | ---: | ---: | ---: | ---: |
| xml | memory | 33.191 | 33.220 | 1.00× | 29.5 → 29.5 |
| xml | http | 33.187 | 33.817 | 0.98× | 25.7 → 25.7 |
| xml-nested | memory | 54.038 | 49.242 | 1.10× | 31.4 → 31.5 |
| xml-nested | http | 55.202 | 48.762 | 1.13× | 27.4 → 27.1 |

Nested-path improvement is approximately 10–13% end-to-end in the paired run;
separate earlier runs were 13–15%. No direct-path improvement is claimed.

## Storage: copying and bounded streaming

The fragmented-copy comparison uses 1 MiB of input, 256-byte transport fragments,
64-KiB output chunks, five alternating processes and 31 scans each:

| Case | Backend | Before ms | After ms | Speedup | Peak RSS MiB (before → after) |
| --- | --- | ---: | ---: | ---: | ---: |
| storage-sequential | http | 0.150 | 0.137 | 1.10× | 10.9 → 10.9 |

This reduces descriptor staging on the copy path. It does not eliminate the
OpenDAL range assembler's retained fragments or establish a general RSS saving.

With **three-byte transport fragments**, direct streaming changes the tradeoff:

| Case | Backend | Before ms | After ms | Speedup | Peak RSS MiB (before → after) |
| --- | --- | ---: | ---: | ---: | ---: |
| storage-sequential | http | 12.484 | 40.422 | 0.31× | 202.8 → 10.3 |
| storage-sequential | s3 | 12.579 | 38.426 | 0.33× | 203.7 → 11.1 |

RSS falls by roughly 19× in this deliberately adversarial 1-MiB workload, but
wall/CPU work increases by roughly 3×. Therefore streaming remains opt-in.
The deterministic regression test checks that requesting the first 64 KiB consumes
only sixteen 4-KiB transport fragments, rather than eagerly staging a 2-MiB range.
It also checks cancellation by dropping that reader. Default behavior, exact
range reads, explicit I/O splitting and parallel reads remain separately covered.

At ordinary 16-KiB fragments the measured RSS saving is small and extra open/poll
cost can dominate sub-millisecond scans. Do not enable the option globally based
on the tiny-fragment result.

## I/O waiting and request geometry

With 1 MiB input, 64-KiB I/O splitting and artificial 5-ms request latency,
concurrency 1 versus 4 yields:

| Backend | Concurrency 1 ms | Concurrency 4 ms | Speedup |
| --- | ---: | ---: | ---: |
| http | 114.519 | 33.756 | 3.39× |
| s3 | 114.836 | 33.985 | 3.38× |

These are simulated waits using the existing concurrency option, not a new
optimization or a prediction of cloud latency. Adjacent ranges already coalesce;
scattered ranges require many more requests. Counts and response bytes are in the
raw samples, so future over-fetch/coalescing work can be evaluated against its
extra-byte cost rather than wall time alone.

## Full matrix, controls and limits

- [default-before.jsonl](results/default-before.jsonl) and
  [default-after.jsonl](results/default-after.jsonl): 14 cases × four backends ×
  three independent processes per variant, using default byte-read options
  (`--io-chunk 0`), streaming disabled. Every scan checks row or byte counts.
- `paired-*`: alternating A/B follow-ups for the specific claims and S3 controls.
  Broad matrix differences also move on unchanged paths (including WARC). Small
  percentage changes on short scans are not treated as optimization evidence.
- `stream-*` and `latency-*`: focused probes.
  Input generation and warm-up are outside scan timers; OS resource counters
  include both, so peak RSS must not be read as live per-scan heap usage.
- WARC is benchmarked but has no demonstrated optimization in this change set.
  Parquet's small dictionary-friendly input does not characterize large row-group
  I/O, compression diversity or real corpus cardinality. Real remote storage,
  provider discovery, cold disks and cloud bandwidth remain outside these results.
- The full matrix exposes CPU-heavy decoding in XML/ATIF/WARC relative to local
  adapter overhead. CPU sampling specifically confirms ATIF's reference lookup;
  equal local timings alone are not sufficient stack-level attribution elsewhere.

## Revisions and validation

Upstream baselines:

- ATIF: `6578616e7c07951aa8fad3601a896681d0bc53d7` (0.4.4).
- XML: `9a9a9051e8a33e01e5559b104651e954dfb86221` (0.2.5).
- WARC: `d506b864d39e5f72f75a4199b6cf645f583089e9` (0.3.2).

The original workspace passed 250 tests. The combined local changes passed 252
workspace tests; upstream ATIF passed 19 tests and XML passed 39 tests. All three
changed codebases passed Clippy with warnings denied, including benchmark targets.
Rustfmt and the umbrella repository's Taplo 0.10.0 formatting/lint checks pass.
See [results/manifest.json](results/manifest.json) for binary hashes and variant
mapping. The upstream microbenchmarks isolate fan-out validation and nested path
conversion; this repository measures their end-to-end ingestion effects.

Optimization PRs:

- [ATIF reference indexing](https://github.com/dentiny/atif-arrow/pull/29).
- [XML nested selection](https://github.com/dentiny/xml-arrow-rs/pull/25).
- Storage extraction: publication pending.

