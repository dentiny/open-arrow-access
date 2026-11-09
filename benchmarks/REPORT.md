# Performance investigation — 2026-11-09

Baseline: upstream `main` at `25875d501b314f052ad4dbe6d9636d8fc1e6e05b`.
Host: macOS 26.5.2, ARM64, Rust 1.97.1/LLVM 22.1.8, release builds. The repository pins 1.97.0; the available compiler used for every compared binary was 1.97.1. Synthetic local data only. No production cloud measurements.

The investigation covers 50 format/storage cases. Each baseline case has eleven fresh-process samples from the final combined comparison: eleven randomized before/after pairs per case. Scan CPU and wall time exclude input generation. RSS includes setup and retained input. See [methodology and commands](README.md), [baseline samples](results/baseline-final.jsonl), and [paired samples](results/paired.jsonl).

## Format baselines

16,384 records, 64 KiB input chunks, 1024-row batches, full output. Values below are medians. Arrow batch memory measures output arrays, not decoder-only memory. WARC reports its largest body instead.

| Format | Scan ms | Scan CPU ms | First output ms | Process peak RSS MiB | Largest output bytes | Input read calls/bytes |
|---|---:|---:|---:|---:|---:|---:|
| csv | 0.856 | 0.859 | 0.107 | 9.1 | 78044 | 14/890010 |
| json | 1.306 | 1.309 | 0.138 | 10.4 | 65756 | 20/1250458 |
| parquet | 0.408 | 0.412 | 0.298 | 13.2 | 59612 | 4/636823 |
| xml | 22.041 | 21.946 | 1.456 | 11.9 | 45276 | 17/1086631 |
| xml-infer | 22.102 | 22.052 | 1.557 | 11.9 | 45276 | 17/1086631 |
| atif | 219.748 | 219.052 | 14.062 | 17.6 | 1262984 | 102/6651904 |
| warc | 8.637 | 8.614 | 0.029 | 9.4 | 3 | 31/1998848 |

All in-memory format scans have CPU time approximately equal to wall time. These cases are CPU-bound, rather than waiting on storage. The fixtures differ between formats; this table is not a ranking of equivalent encodings or real-world corpus complexity.

## Measured optimizations

Review PRs: [ATIF #25](https://github.com/dentiny/atif-arrow/pull/25), [XML #21](https://github.com/dentiny/xml-arrow-rs/pull/21), [empty-range planning #195](https://github.com/dentiny/open-arrow-access/pull/195), [small-range retained memory #194](https://github.com/dentiny/open-arrow-access/pull/194).

All optimization branches target their own upstream main independently; none is merged. Production dependency versions are unchanged in the benchmark PR. The upstream changes were injected only through local Cargo patches.

| Change | Before ms | After ms | Wall-time reduction |
|---|---:|---:|---:|
| ATIF full / 1024 rows | 219.748 | 190.291 | 13.4% |
| ATIF trajectory_id / 1024 rows | 125.366 | 98.467 | 21.5% |
| XML explicit full / 1024 rows | 22.041 | 21.760 | 1.3% |
| Empty ranges / filesystem / concurrency 1 | 0.202 | 0.158 | 21.8% |

### ATIF: avoid error-path allocations for nullable null fields

`normalize_object` previously formatted a nested field path before discovering that a nullable field was absent/null. Sparse trajectories touch many such schema fields per step. Check nullable nulls first, preserve explicit Arrow null insertion when materializing, and still normalize/validate every other field. Required nulls and omitted projected fields retain validation behavior.

Upstream baseline: `dentiny/atif-arrow@c04c024`. Change: `bc727ffef27f7f3fcb663b8310f4a38f21a7bbbe` on `perf/skip-null-field-paths`. Regression coverage compares sparse missing/explicit-null fields across projections and checks required-field errors and atomic appends. Existing core ATIF tests and Clippy pass.

A two-second macOS `sample` capture on the initial `f7871a7` baseline ([excerpt](results/atif-profile-excerpt.txt)) used 262,144 projected trajectories. Of 1,627 main-stack samples, one inclusive normalization branch accounted for 703, its nested normalize_field branch for 611, and a descendant format_inner branch for 287. Descendant counts overlap and must not be summed. The allocator/realloc stacks underneath formatting support the allocation hotspot; these are sampled stacks, not allocation counts. The capture is diagnostic and was not included as a timing sample.

### XML: borrow scalar text until Arrow append

Keep the parsed DOM alive through row validation and append. Internal scalar values use `Cow<str>`: attributes and a single text node borrow DOM text; text separated by comments/CDATA uses one concatenation buffer. Arrow still owns its final strings. Numeric conversion avoids a temporary String. No public API change or syntax/type validation is skipped. Tests cover comments, CDATA, empty text/numeric nodes, and atomic errors. XML core tests, doctests, and Clippy pass.

Upstream baseline: `dentiny/xml-arrow-rs@75186af`. Final change: `b55e8301547a2ea0e2e440da1e9305abbc265ab0` on `perf/borrow-scalar-text`. Final randomized pairs show small, workload-dependent XML scan-time savings; the initial numeric-only experiment was within noise and was expanded to borrowed text before publication. This is a small allocation optimization, not a solution to XML streaming/DOM parsing cost.

XML cases from the final latest-main combined comparison: eleven pairs per case. [Extracted samples](results/xml-final.jsonl) retain version labels and hashes.

| XML case | Batch | Chunk | Before ms | After ms | Wall-time reduction |
|---|---:|---:|---:|---:|---:|
| xml/all | 1 | 65536 | 29.679 | 28.822 | 2.9% |
| xml/all | 1024 | 127 | 23.029 | 22.436 | 2.6% |
| xml/all | 1024 | 65536 | 22.041 | 21.760 | 1.3% |
| xml/project | 1 | 65536 | 25.808 | 25.310 | 1.9% |
| xml/project | 1024 | 65536 | 20.748 | 20.541 | 1.0% |
| xml-infer/all | 1 | 65536 | 29.913 | 28.860 | 3.5% |
| xml-infer/all | 1024 | 127 | 22.836 | 22.322 | 2.2% |
| xml-infer/all | 1024 | 65536 | 22.102 | 21.357 | 3.4% |
| xml-infer/project | 1 | 65536 | 30.931 | 30.603 | 1.1% |
| xml-infer/project | 1024 | 65536 | 22.046 | 21.177 | 3.9% |

### Storage: skip fetch planning for all-empty range batches

Validate the complete request first, then return one empty Bytes per range without constructing an OpenDAL fetch plan. Bounds, output positions, and the sequential cursor contract remain unchanged. The filesystem case improves about 16–22%; loopback end-to-end effects are much smaller because the metadata HEAD still runs. No network bytes/requests are claimed to be saved.

Change: `2080e84` on `perf/empty-range-batches`. Storage tests cover valid empty offsets and an out-of-bounds empty offset mixed into the batch.

## Storage I/O and memory

32 MiB sequential scan versus 4096 disjoint 64-byte ranges, with 2 MiB I/O splitting. HTTP and S3 use the local server; request totals include metadata HEAD.

| Backend / request | Concurrency | Scan ms | Client CPU ms | Process RSS MiB | HTTP requests | Payload bytes |
|---|---:|---:|---:|---:|---:|---:|
| fs / all | 1 | 2.464 | 2.468 | 44.0 | 0 | 0 |
| fs / all | 4 | 1.437 | 3.359 | 50.1 | 0 | 0 |
| fs / ranges | 1 | 27.338 | 28.832 | 42.3 | 0 | 0 |
| fs / ranges | 4 | 29.251 | 83.176 | 42.6 | 0 | 0 |
| http / all | 1 | 10.773 | 6.247 | 49.2 | 17 | 33554432 |
| http / all | 4 | 9.125 | 6.205 | 57.8 | 17 | 33554432 |
| http / ranges | 1 | 317.408 | 108.700 | 75.4 | 4097 | 262144 |
| http / ranges | 4 | 204.502 | 114.242 | 75.5 | 4097 | 262144 |
| s3 / all | 1 | 11.058 | 6.439 | 50.0 | 17 | 33554432 |
| s3 / all | 4 | 9.223 | 6.448 | 57.9 | 17 | 33554432 |
| s3 / ranges | 1 | 358.199 | 128.166 | 76.1 | 4097 | 262144 |
| s3 / ranges | 4 | 217.184 | 127.357 | 76.3 | 4097 | 262144 |

Each scatter scan requests only 256 KiB but needs 4097 HTTP requests including HEAD, versus 17 requests for the 32 MiB scan. Concurrency reduces request-latency overhead; it does not reduce request count. The adapter intentionally uses gap(0) to avoid downloading unrequested bytes, so silently enabling read amplification is not proposed. Filesystem scans are warm-cache measurements, not physical disk bandwidth.

### Retained small-range backing buffers

Tiny returned network slices can retain larger receive allocations. Limiting fetch range count alone reduced RSS by only about 1.5 MiB. Compacting only uniquely owned slices did not reduce peak RSS. Copying small outputs alone cannot remove the initial peak while a complete large fetch is staged. The measured fix combines bounded scatter staging with copying small results out as each batch is consumed. Adjacent/overlapping input runs are kept together across the count threshold to preserve common coalescing. Global coalescing across separated batches is not guaranteed; unusually reordered overlapping inputs can repeat some backend reads.

Final storage change: `e4d7285` on `perf/compact-range-buffers`. Up to 512 scattered ranges are staged together in addition to the existing 2 MiB payload bound; adjacent/overlapping runs can continue to the byte bound. Results of at most 4 KiB are copied out as each staging batch is consumed. Larger outputs retain existing zero-copy/coalescing behavior. The 4096-range workload preserves 4097 total HTTP requests and 262,144 payload bytes.

The table uses eleven pairs per case from the latest-main combined comparison. The storage branches were also tested independently. [Storage samples](results/range-memory-final.jsonl) are extracted from the shared paired data; metadata identifies all source commits and before/after binary hashes.

| Backend | Concurrency | Before ms | After ms | Before RSS MiB | After RSS MiB |
|---|---:|---:|---:|---:|---:|
| fs | 1 | 27.338 | 27.426 | 42.3 | 41.1 |
| fs | 4 | 29.251 | 29.009 | 42.6 | 41.5 |
| http | 1 | 317.408 | 313.419 | 75.4 | 46.0 |
| http | 4 | 204.502 | 196.317 | 75.5 | 46.2 |
| s3 | 1 | 358.199 | 350.616 | 76.1 | 46.9 |
| s3 | 4 | 217.184 | 218.749 | 76.3 | 47.1 |

The loopback HTTP/S3 cases save roughly 29 MiB of process peak RSS (about 38–39%). Throughput is broadly similar: HTTP median latency improves 1–4%, while S3 concurrency four is about 0.7% slower in this run. This trades small-result copying for lower retained memory; it is not a claim that every range workload becomes faster. The input remains identical and included in both RSS measurements. Tests exercise 1537 mixed/duplicate/empty ranges across staging boundaries, cursor preservation, and 2048 adjacent ranges still issuing one coalesced backend read.

## Remaining opportunities

- ATIF projection still deserializes, serializes into serde_json::Value, normalizes, and validates omitted fields. The measured change removes unnecessary paths, not that whole pipeline. Avoiding redundant materialization is a larger candidate that must preserve validation and error behavior.
- XML captures records, serializes them, parses a DOM, then converts to Arrow. Inferred-schema projection currently occurs after full conversion in open-arrow-access; the measured full/project times are nearly equal despite smaller output arrays. Upstream support for selecting inferred mappings before replay/conversion is the next useful API work.
- Tiny batches strongly amplify per-batch Arrow allocation and flush overhead for CSV/JSON/Parquet/ATIF. Batching is already configurable; these measurements do not justify changing defaults globally.
- Parquet projection already works well: this case reads 160,432 versus 636,823 bytes and scans in about 0.12 versus 0.41 ms. Preserve range projection savings rather than indiscriminately coalescing gaps.
- WARC small-record parsing is CPU-bound and uses owned header/body records. No WARC code change was published without a separately validated improvement; the new benchmark provides a baseline for future header/framing work. Large-body WARC and compressed corpus workloads still need dedicated datasets.
- No memory-bandwidth saturation, memory leak, WAN throughput or cloud-account behavior was established. RSS is a process high-water mark and does not by itself establish any of those bottlenecks.

## Validation and interpretation

Local validation on the latest-main combination: workspace all-features tests, Clippy for all workspace targets, cargo fmt and Taplo 0.10.0 format/lint; upstream ATIF/XML core tests and Clippy; storage range tests including multi-batch ordering/duplicates/empty ranges, cursor preservation and long adjacent-run coalescing. Python tools use only the standard library.

Raw paired samples retain unchanged-format controls. Some very small/batch-one controls drift by several percent, so small differences are not generalized to other workloads. Only format-specific confirmed changes are claimed; the range-memory cases record both RSS savings and latency tradeoffs. Results depend on this machine, allocator, release layout and generated corpus.
