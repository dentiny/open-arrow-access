# Follow-up measurements

Local macOS 26.5.2 ARM64, release Rust 1.97.0. Current upstream main is
`29b5687a05104a37418fcccdf18cdbd06df39df1`; this suite is added to the existing
umbrella branch after merging that main. Five isolated processes per case, at
least 0.3 seconds of timed work each. Raw samples retain outliers. See the
[methodology](README.md) before comparing CPU/RSS with wall time.

## Format and storage baseline

| Case | Median ms/iteration | Median process RSS MiB |
| --- | ---: | ---: |
| atif | 77.8273 | 27.8 |
| atif-project | 30.0465 | 25.0 |
| chunks-retained | 0.2082 | 14.1 |
| chunks-short | 0.0014 | 11.0 |
| csv | 2.1016 | 21.2 |
| csv-project | 1.9470 | 21.1 |
| fs-ranges | 26.9938 | 44.8 |
| fs-sequential | 0.7027 | 47.8 |
| http-fragmented | 0.2529 | 75.8 |
| http-ranges | 59.6390 | 63.3 |
| http-sequential | 0.1475 | 59.9 |
| http-small-fragments | 0.5880 | 60.4 |
| json | 1.0394 | 21.9 |
| json-project | 0.9265 | 21.8 |
| metadata-builder | 2.3126 | 13.0 |
| metadata-builder-wide | 34.7423 | 39.6 |
| parquet | 0.1174 | 22.2 |
| parquet-project | 0.0189 | 22.1 |
| warc | 7.8036 | 23.2 |
| xml | 18.5279 | 22.7 |
| xml-infer | 18.6196 | 22.8 |
| xml-project | 17.6494 | 22.5 |

## Individual providers

Fixture resolution/decoding times include public API validation and wrapper setup,
with connection construction outside the timer. Tiny correctness fixtures are
not a scalable catalogue benchmark. Zero HTTP requests means a memory-backed
manifest/decoder, not zero parser work.

| Case | Median µs/iteration | HTTP requests/iteration |
| --- | ---: | ---: |
| arxiv | 17.29 | 0 |
| arxiv-metadata | 17.01 | 1 |
| bhl | 5.16 | 1 |
| biorxiv | 5.41 | 1 |
| courtlistener | 29.80 | 0 |
| dataverse | 7.28 | 1 |
| doab | 16.82 | 1 |
| federal-register | 4.80 | 1 |
| figshare | 5.62 | 1 |
| govinfo | 5.12 | 1 |
| gutenberg | 37.99 | 0 |
| hathifiles | 7.89 | 1 |
| huggingface | 396.44 | 6 |
| internet-archive | 5.46 | 1 |
| library-of-congress | 8.23 | 1 |
| meca | 19.79 | 0 |
| oapen | 16.81 | 1 |
| openstax | 5.44 | 1 |
| pmc | 5.68 | 0 |
| regulations | 6.45 | 1 |
| semantic-scholar | 8.23 | 2 |
| uspto | 7.12 | 1 |
| warc | 0.84 | 0 |
| zenodo | 3.06 | 1 |

## Findings and independent changes

1. **Retained short-read allocations.** `read_chunks` returns 64 bytes while
   retaining its requested 2 MiB Vec. Holding 128 outputs retains exactly
   268,435,456 allocation bytes. Compacting heavily underfilled large buffers
   reduces this to 8,192 bytes, a 32,768× capacity reduction. This is allocation
   capacity, not an RSS speedup. The regression checks recoverable capacity.
2. **Fragmented provider downloads.** `Connection::open` called `Buffer::to_bytes`
   before splitting output, copying all 16 MiB of a scatter buffer. Iterating its
   nonempty parts preserves ownership without the copy. The new test checks
   pointer sharing, leading/intermediate/trailing empty parts, order and maximum
   output size. Local before/after figures are listed below. Content-Length
   validation and EOF/error behavior still pass existing contract tests.
3. **ATIF raw_json.** Upstream copied the entire input to a temporary JSON string
   before writing Arrow. Appending the borrowed original removes that allocation
   and copy, with offset checks before mutation. Five alternating 1 MiB A/B pairs
   show 9.3% median paired wall reduction on full conversion (all five improve),
   and 73.5 → 58.5 MiB median full-conversion process RSS. Raw-only paired wall
   reduction is 8.6%, with two regressions; session-only control is -0.4%.
4. **XML list staging.** Direct-child list mappings staged a source-node vector
   before collecting converted values. Visiting children directly removes that
   intermediate collection. Five alternating pairs show 5.8% median paired wall
   reduction at 1024 items and 9.3% at 16 items; multi-segment control shifts 0.5%.
   One large-list pair regresses 32.8%; this is a noisy local indication.

| Follow-up | Before median ms | After median ms | Before/after median RSS MiB |
| --- | ---: | ---: | ---: |
| Provider scatter buffer | 0.2529 | 0.0293 | 75.8 / 59.8 |
| 128 retained short reads | 0.2082 | 0.2092 | 14.1 / 11.2 |

The core/provider numbers are independent-process medians, not alternating pairs;
CPU frequency, shared host load and allocator caches add noise. Raw process CPU,
wall, block counts and peak RSS are retained. Physical memory savings are not
inferred from allocation capacity. Parser samples alternate baseline/candidate
order by pair. ATIF was measured at `87345d7` then rebased onto test-only
`0a54d93`; XML was measured at `1bf4fce` then rebased onto the whitespace-helper
refactor `13572ef`. Native tests were rerun after rebasing.

## Bottleneck interpretation and rejected work

- Memory-backed format cases isolate parsing/conversion CPU and allocation from
  backend latency. ATIF's schema/validation differ from the two-column synthetic
  CSV/JSON/XML workload; absolute cross-format times are not a parser ranking.
- Small scattered reads still amplify requests. The existing pipeline audit
  measures exact backend request counts, coalescing and synthetic latency;
  real object-store performance needs credentials, network and a representative
  corpus. No S3/cloud throughput is inferred from mock results.
- CPU/RSS in tiny provider fixtures primarily describe metadata resolution. They
  do not prove the provider has a production bottleneck; these cases provide a
  repeatable baseline and regression surface.
- A separate direct-copy experiment in OpenDAL sequential staging did **not**
  show a reliable gain at realistic fragment sizes and is not submitted. It also
  overlaps the earlier closed storage proposal #220. No streaming policy or
  default storage behavior is changed by the umbrella PR.
- No measured WARC optimization was identified in this follow-up. Its baseline
  is retained; no speculative upstream WARC PR is submitted.

Validation: the current-main umbrella workspace test suite passes; independent
core/provider regressions and both native parser suites pass. No merge or
auto-merge has been requested. Links to the focused PRs are added below.

- [ATIF: append raw JSON directly](https://github.com/dentiny/atif-arrow/pull/35).
- [XML: eliminate direct-child list staging](https://github.com/dentiny/xml-arrow-rs/pull/31).
