# Latest-main performance follow-up, 2026-11-09

This continues the earlier M5 Pro corpus/storage investigation in the same benchmark PR. The production baseline is upstream main **`32e68053d6801932fed3e2ed3e0f50e1eaa33f93`**, refreshed after the earlier fixes landed. The supplemental harness is recorded at **`20acbff`**; candidate production patches are included in [patches/](patches/). All measurements are local ARM64 macOS release builds with Rust 1.97.0. Exact binary hashes, toolchain/platform metadata, upstream format bases and lockfile identities accompany the [raw sources](sources.json) and JSONL samples.

## Evidence

There are **320 measured process samples**, excluding warm-up: 44 baseline cases × five samples, four adapter candidate cases × five samples, two small-batch cases × five samples per variant, and three native format comparisons × ten alternating processes per variant. [Baseline summary](baseline-summary.md), [independent comparisons](comparison.md), [validation records](validation.json), and [reproduction/coverage guide](../../../docs/performance-supplement.md) describe the exact scope. The raw files are `baseline.jsonl`, `root-after.jsonl`, `atif-small-{before,after}.jsonl`, and `{atif,xml,warc}-native.jsonl`.

The cases cover six file formats, filesystem/HTTP/Hugging Face/download byte paths, sequential and sparse range reads, full/early manifest listings, and representative paths for all 23 service features. Provider fixtures exclude public network latency. The original benchmark matrix remains the reference for loopback latency/concurrency experiments and random-payload I/O measurements.

## Findings and independent fixes

| Finding | Local evidence | Focused change |
| --- | --- | --- |
| A named manifest list eagerly copies every filename before the first poll. | For 100,000 names, first-item latency is **1.596 ms → 0.076 µs**. Whole-process peak RSS is **34.53 → 24.52 MiB**. Full-list throughput is within the measured noise interval. | Share the immutable name index and clone names only as the stream is polled. |
| The ATIF adapter reconstructs the entire nested Arrow builder on every batch and again at EOF. | At batch=1,024, 4,096-trajectory scans improve **37.252 → 26.822 ms (1.39×)**; one-column scans improve **15.908 → 11.885 ms (1.34×)**. At batch=1, full scans improve **81.568 → 67.632 ms (1.21×)**. | Keep one lazily initialized builder per reader and use its existing flush/reset operation. |
| ATIF offset validation allocates a child Vec for each struct field even for a single row. | Native full-schema conversion: **206.873 → 184.347 ms (1.12×)** for 32,768 simple trajectories. | [ATIF #23](https://github.com/dentiny/atif-arrow/pull/23): use stack slices for singleton checks while retaining aggregate checks for repeated children. |
| XML direct field lookup builds intermediate/result Vecs. | Native two-field conversion: **19.774 → 17.292 ms (1.14×)** for 32,768 records. | [XML #19](https://github.com/dentiny/xml-arrow-rs/pull/19): resolve direct elements, attributes and `.` without those intermediate allocations. |
| WARC identifies known headers through a linear walk of all known names. | Native small-body record scans: **16.205 → 14.851 ms (1.09×)** for 32,768 records. | [WARC #31](https://github.com/dentiny/warc-rs/pull/31): dispatch by byte length before case-insensitive comparison. |

The first-item ratio is deliberately not presented as a general storage throughput improvement: the baseline does work proportional to manifest size even when the caller immediately stops. The candidate removes that work. RSS is a whole-process high-water mark, including construction, fixtures and allocator retention; the 10 MiB difference is not a claim about exact live listing allocations.

These are independent changes. The ATIF adapter comparison uses the registry dependency, not the upstream ATIF candidate, and native upstream comparisons do not use the adapter patches. Their speedups must not be multiplied. Format dependency upgrades are not included; the native improvements require release/upgrade before downstream adapter users receive them.

## CPU, I/O and memory interpretation

The memory-backed format cases remove storage waits from the timed scan and retain process CPU accounting. XML and ATIF remain expensive parsing/conversion paths; reducing allocation/dispatch work produces measurable native CPU improvements, while ATIF builder reuse improves the adapter itself. This supports the identified CPU costs; it does not allocate every sampled CPU cycle to a function or replace a production profiler.

Sparse read cases expose request count and range granularity, while the original loopback latency matrix demonstrates request-latency sensitivity. This supplemental in-process HTTP transport cannot establish public-cloud bandwidth or latency, and warm filesystem scans do not establish cold-disk throughput. No production I/O chunk/concurrency defaults were changed.

Memory work with immediate benefit is avoiding eager manifest name copies and repeated nested builder initialization. The native conversion changes primarily improve CPU/allocation churn; their process RSS is essentially unchanged. Small provider metadata fixtures establish local resolver overhead, not large-manifest memory bounds. Larger XML streaming changes (including avoiding per-record serialization/reparse) remain candidates requiring a separate design and corpus validation.

## Validation

The combined candidate passes workspace all-feature tests and all-target Clippy with warnings denied. Each native format passes its format tests and all-target Clippy; WARC uses no-default features to isolate its parser and includes 48 unit tests plus doctests. New regressions cover listing lifetime/order after accessor drop, retained nested Arrow batches across ATIF flushes, XML namespace/attribute/duplicate mapping behavior, and unknown WARC header normalization. See [validation.json](validation.json).

The supplemental harness itself has no allocator instrumentation. Its process CPU, RSS, faults, I/O blocks, context switches, logical reader-call counters and executable hashes are recorded without equating RSS to allocated bytes. Fixture setup and file generation are outside the scan timer but inside process resource accounting. Repeated payloads make Parquet a highly compressible control; the formats have different output semantics and ATIF has a different row count, so this report does not rank formats against each other.
