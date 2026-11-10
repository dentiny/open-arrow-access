# Performance audit — 2026-11-09

This PR adds reproducible benchmarks and reports two independently reviewable
parser optimizations. It changes no production code or released parser versions
in Open Arrow Access. This audit issues no merge or auto-merge operations.
XML #29 was subsequently trimmed to production code and merged externally by
`dentiny`; the measured production hunk is unchanged. ATIF #33 also contains only
its six-line production change after the requested benchmark/target removal.

## Source and environment

The paired measurements use upstream main **`2dc1ac0f6e99868c67b1dcd9ac221bcc66ba8ddc`**.
The initial checkout was `bdc4893`; both comparison binaries and all paired trials
were refreshed to `2dc1ac0`. Main then advanced to
**`e08ae08d8521bee11eadf8b7f28707f3b49ccf09`** (reader wrapper removal and lifecycle
documentation). The benchmark branch was refreshed again and the complete
87-case baseline and 249-test release suite were rebuilt/rerun on that snapshot.
The complete baseline table below uses `e08ae08`; the paired comparisons and
resource probes remain pinned to `2dc1ac0`. Their raw files are kept separate.
Earlier exploratory runs are not mixed into these measurements.
The baseline lockfile uses ATIF 0.4.5, XML 0.2.6, WARC 0.3.2,
Arrow/Parquet 59.3.0 and OpenDAL 0.59.2.

- ATIF candidate: `c14f9005e1620f5752b22ee673db192a53dc04be`,
  [upstream PR #33](https://github.com/dentiny/atif-arrow/pull/33).
- XML candidate: `655d3f57871fbadcb1108441909726aaea5b0152`,
  [upstream PR #29](https://github.com/dentiny/xml-arrow-rs/pull/29).
- Apple M5 Pro, 18 logical CPUs, 24 GiB RAM; macOS 26.5.2 (25F84).
- Actual compiler: Homebrew Rust 1.97.1, release/bench profile, Tokio with two workers.
  The repository toolchain file requests Rust 1.97.0; the same installed 1.97.1
  compiler was used throughout the comparisons.

Both binaries use identical benchmark source. The optimized binary combines
temporary local ATIF/XML path patches, but every measured format case exercises
only its corresponding parser. The current-main baseline is a third binary.
Published PR heads remove only benchmark files/targets from the measured parser
commits: ATIF `bc9669f`, XML `1f1d406`. No path overrides or unreleased dependency bumps
are committed here. Binary/harness/lockfile hashes and exact source revisions
are in [the manifest](results/manifest.json). See [the methodology](README.md)
for commands, workload construction, units and coverage limits.

## Measured changes

**XML sparse nested structs:** capacity validation allocated a temporary
`Vec<&Value>` for each child even when validating one row. Keep the reference on
the stack. Multi-item lists still aggregate offset growth before appending;
null structs add no variable-length growth. The generated schema has two nested
structs and 32 missing optional attributes, deliberately exposing sparse-schema
validation overhead. All five process pairs improve by **20.3–21.0%**.
Scalar XML is effectively unchanged. This is not a universal XML speedup.

**ATIF absent optional fields:** offset validation recursed through every
builder under null structs/lists and empty candidate sets. These candidates
cannot add string bytes or list values. Return before that traversal; leave
non-null cumulative offset checks and append behavior untouched. The confirmation
run interleaves ATIF and CSV pairs and uses 31 timed scans per process. Median
paired ATIF wall-time reduction is **4.3%**, and paired process CPU reduction is
**4.4%**. Four pairs improve; one is effectively flat (-0.3%). The unchanged CSV
control's median paired reduction is -0.9%, with one noisy -10.2% pair.
Treat this as a modest local improvement requiring representative-corpus checks.
No meaningful ATIF peak-RSS improvement is claimed.

| Workload | Baseline median ms | Candidate median ms | Reduction of pooled median | Median paired reduction |
| --- | ---: | ---: | ---: | ---: |
| atif (confirmation) | 95.630 | 91.575 | 4.2% | 4.3% |
| atif-document | 60.392 | 58.334 | 3.4% | 1.8% |
| atif-project | 35.405 | 35.724 | -0.9% | -0.9% |
| xml-struct | 59.146 | 46.821 | 20.8% | 20.3% |
| xml | 20.835 | 20.885 | -0.2% | -0.4% |
| csv (confirmation) | 1.495 | 1.525 | -2.0% | -0.9% |
| warc | 10.152 | 10.153 | -0.0% | -0.7% |

The main comparison has five process pairs per case, 15 measurements per process;
the ATIF/CSV confirmation has five pairs and 31 measurements per process. Every
process warms up once. Pooled medians and pair medians are distinct statistics.
The earlier main ATIF comparison had substantial host variation (pooled reduction
9.0%, median paired reduction 3.4%); its raw data are retained, but the 4.3%
confirmation estimate is used for the ATIF claim. ATIF's single-document effect
is small/noisy; its id-only projection does not traverse the nested builders and
has no reliable improvement. WARC, CSV and scalar XML are unchanged controls.

## CPU evidence

Separate macOS `sample` runs show ATIF normalization, Arrow append paths and
allocation activity, and XML `validate_capacity` stacks containing temporary
vector construction and allocator work. Inlining prevents the sample from
isolating ATIF offset-check time reliably. The diagnosis combines source-level
work inspection with paired wall/CPU changes; these traces do not assign exact
CPU percentages to individual phases. Sampling runs are excluded from timings.

Retained call graphs: [ATIF](results/cpu-atif.txt), [XML](results/cpu-xml.txt).
Binary-image inventories were removed; the sampled call graphs are preserved.

## I/O and memory findings

1. **Scattered reads amplify requests.** From the 2,560,000-byte fixture,
   625 disjoint 32-byte ranges return only 20,000 bytes but make **626 requests**,
   including HEAD. Adjacent ranges return 131,072 bytes with only **two requests**.
   The HTTP/S3 replay has no real RTT, so its low wall time is not a cloud latency
   prediction. Reading unrequested gaps would change the byte contract and can
   defeat Parquet pruning; this audit does not silently enable gap coalescing.
2. **Concurrency already helps latency-bound scans.** For a 1 MiB object split
   into 64-KiB I/O ranges with a configured 2-ms timer per request, concurrency
   one vs four has the following medians across fresh processes:

| Replay backend | Concurrency 1 ms | Concurrency 4 ms | Requests/scan |
| --- | ---: | ---: | ---: |
| http | 63.927 | 18.685 | 17 |
| s3 | 64.732 | 19.023 | 17 |

   This demonstrates the existing I/O concurrency knob under artificial latency;
   Tokio timer granularity and scheduling also contribute. It does not justify
   changing concurrency defaults without real service/rate-limit measurements.
3. **Tiny transport fragments cause excessive staging.** A deliberately
   adversarial 1 MiB object delivered in 3-byte fragments drives process peak
   RSS to roughly 204 MiB; ordinary 16-KiB fragments use about 11 MiB. The table
   reports medians across three fresh processes, seven timed scans each:

| Replay backend | 3-byte time ms | 16-KiB time ms | 3-byte peak MiB | 16-KiB peak MiB |
| --- | ---: | ---: | ---: | ---: |
| http | 12.872 | 0.028 | 203.7 | 10.6 |
| s3 | 12.917 | 0.041 | 204.1 | 11.6 |

   OpenDAL stages fragment descriptors for a range; the adapter also assembles
   contiguous output. A direct-streaming policy has backpressure/concurrency
   tradeoffs (an earlier proposal, #220, was closed without merging). This audit
   records the remaining hotspot and does not resubmit that closed proposal.
4. **Line polling/assembly depends strongly on input chunking.** The same
   78.125-MiB, 4-KiB-line fixture takes 14.972 ms in 257-byte chunks
   vs 1.198 ms in 64-KiB chunks. Chunk count, async dispatch and buffer
   assembly all contribute. Both processes peak near 242 MiB because fixture
   construction retains full payload copies: that RSS is not the line buffer's
   memory usage. The reader's line buffer is bounded by a line in this workload.
5. **Whole-document APIs retain whole-document costs.** ATIF single-document
   input is collected before typed conversion; owned WARC records buffer their
   bodies; metadata JSON creates a full value tree. The ATIF one-document probe
   has a 3.33-MiB input with 20,000 steps and a whole-process peak near 133 MiB.
   That peak also includes generation and fixture copies and cannot be assigned
   solely to the decoder. Large-record scaling and allocation profiling are
   needed before proposing API changes. Shared streaming downloads avoid full
   body collection; metadata discovery still needs its parsed document.

## Complete baseline matrix

Each entry below is from main `e08ae08`, one fresh process with seven timed samples after warmup. All
format and storage outputs are consumed and their row/record or byte counts
checked. The table is a coverage baseline, not a ranking between different
formats/schemas. Provider paths run only against the HTTP replay transport.

| Case | Memory ms | Warm FS ms | HTTP replay ms | S3 replay ms | Memory-process peak MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| csv | 1.464 | 1.623 | 1.563 | 1.546 | 16.5 |
| csv-project | 1.364 | 1.556 | 1.442 | 1.479 | 16.3 |
| json | 1.086 | 1.293 | 1.236 | 1.217 | 17.5 |
| json-project | 1.065 | 1.166 | 1.111 | 1.196 | 17.3 |
| parquet | 0.099 | 0.167 | 0.113 | 0.134 | 17.3 |
| parquet-project | 0.019 | 0.094 | 0.033 | 0.053 | 17.1 |
| xml | 20.653 | 20.524 | 20.412 | 20.602 | 18.6 |
| xml-project | 19.234 | 19.206 | 19.044 | 19.275 | 18.3 |
| xml-infer | 20.944 | 20.781 | 20.414 | 21.050 | 18.9 |
| xml-nested | 28.448 | 29.300 | 29.106 | 28.643 | 19.4 |
| xml-struct | 56.285 | 57.519 | 55.590 | 55.707 | 20.4 |
| atif | 92.550 | 93.881 | 115.121 | 104.505 | 26.9 |
| atif-document | 55.700 | 59.345 | 60.645 | 56.085 | 136.1 |
| atif-project | 37.137 | 36.126 | 35.551 | 35.088 | 24.6 |
| atif-timestamp | 101.997 | 103.074 | 101.896 | 103.116 | 29.8 |
| warc | 9.845 | 10.417 | 9.900 | 10.098 | 24.3 |
| storage-sequential | 0.001 | 0.136 | 0.066 | 0.081 | 15.0 |
| storage-range | 0.001 | 0.174 | 0.048 | 0.066 | 15.0 |
| storage-scattered | 0.016 | 3.950 | 0.992 | 2.910 | 15.2 |
| storage-adjacent | 0.104 | 0.220 | 0.168 | 0.174 | 15.7 |
| lines | 0.361 | 0.509 | 0.427 | 0.434 | 15.2 |
| service-body | — | — | 0.106 | — | — |
| service-download | — | — | 0.066 | — | — |
| service-metadata | — | — | 3.678 | — | — |

Memory storage near zero is an in-memory slice, not a physical I/O benchmark.
Input schemas, compression/encoding, and logical complexity differ across formats.
Parquet id-only projection prunes a physical column; CSV/JSON must still traverse
input delimiters or values. Provider `service-metadata` validates its 20,000-entry
array after JSON parsing. Raw logs include per-process user/system CPU, peak RSS,
block I/O, switches, request counts and requested response payload bytes.

## Validation and scope

- Registry baseline: **249** passing workspace release tests on each main snapshot, `2dc1ac0` and `e08ae08`.
- Combined local ATIF/XML candidates: **249** passing workspace release tests.
- Native ATIF: **18** passing release tests; native XML: **38** including doctests.
- Strict release Clippy passes for the workspace/all targets/all features and
  both native parser packages/all targets. The focused parser PRs contain only production code.
- Rustfmt, Taplo 0.10.0 formatting/lint, locked Cargo metadata and the release
  no-default-features workspace check pass.

The retained evidence contains **3,074 timing samples and 294 independent
process measurements**, plus two separate CPU samples. Raw files:
[latest-main baseline](results/baseline.jsonl),
[paired-main baseline](results/baseline-paired-main.jsonl),
[main before](results/paired-before.jsonl), [main after](results/paired-after.jsonl),
[ATIF confirmation before](results/atif-confirm-before.jsonl),
[ATIF confirmation after](results/atif-confirm-after.jsonl),
[latency c1](results/latency-c1.jsonl), [latency c4](results/latency-c4.jsonl),
[small-chunk lines](results/fragmented-lines.jsonl),
[large-chunk lines](results/large-chunk-lines.jsonl),
[tiny transport fragments](results/tiny-fragments.jsonl),
[ordinary transport fragments](results/normal-fragments.jsonl).

No real cloud credentials, HF/Xet traffic, cold-disk trials, compression/archive
combinations, production corpora or individual provider catalogue resolvers are
measured. Shared provider body/metadata/download paths are covered. The sparse
ATIF/XML workloads target absent optional fields; dense-schema performance needs
separate representative inputs. CPU/RSS are whole-process measurements, including
setup, input generation and warmup; elapsed samples exclude setup. None of these
numbers is a portable throughput or memory ceiling. No merge or auto-merge operation is performed by this work. XML #29 was merged
externally as recorded above; this audit does not undo that external action.
