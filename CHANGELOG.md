# Changelog

## Unreleased

- Unify CSV header handling across explicit and inferred schemas with
  `CsvHeader::{Auto, Present, Absent}`, defaulting to Auto. Resolve once per input
  before parallel partitioning, including streaming decompression sources.
- Preserve ambiguous all-string/single-row inference samples and numeric-looking
  first values as data, following arrow-csv's conservative detection approach.

Compatibility changes:
- Replace `CsvDialect::has_header: bool` with `header: CsvHeader`; migrate true/false
  to Present/Absent. Default dialects now retain Auto when setting a delimiter.
- Move `CsvInferenceOptions::has_header` to `with_header`: None becomes Auto,
  Some(true) becomes Present, and Some(false) becomes Absent. Sampling settings
  no longer change header policy.
- Explicit-schema CSV readers now inspect the first record at open by default.
  Use Absent to retain the previous default and avoid header inspection.
- All-string CSV inference no longer assumes a header. Select Present for files
  with all-string headers. Opaque `CsvFormat` boolean settings remain explicit.

## 0.2.0

- Add reusable file-source opening and typed record access through `BoundSource`.
- Add content-based ATIF JSON/JSONL framing with `AtifFormat::Auto`.
- Add `SchemaEvolutionLayer` for column changes and improve generated-column projection pushdown and memory use.
- Push dataset union projections into file reads and reuse compiled schema alignment plans.
- Provide a default record format for OAI providers.
- Align Arrow dependencies with the Arrow 58 ecosystem.
- Fix run-end encoded type handling and decimal cast validation.

Compatibility changes:
- Rename the PMC text URL output column to `text`.
- Default WARC Arrow dates to timestamps via schema evolution.

## 0.1.0

Initial release of open-arrow-access, a composable Rust library for source access
and Apache Arrow record streams.

- Shared asynchronous accessor, reader, factory, layer, and scan-planning contracts.
- OpenDAL storage integration with sequential reads, exact ranges, and discovery.
- Opt-in Parquet, CSV/TSV, JSON/JSONL, XML, ATIF, and WARC readers.
- Streaming gzip, bzip2, LZ4, and Zstandard decompression, plus ZIP/TAR member access.
- Ordered file sets and directory datasets, including opt-in schema union by name.
- Source services for research repositories, books, web archives, and government data.
- Schema remapping, generated columns, seeded sampling, bounded parallel reads,
  and opt-in unordered emission for compatible sources.
- Usage guides, architecture diagrams, extension examples, and documented fixtures.

The default Cargo feature set is empty. Enable formats, codecs, datasets, and
services explicitly. This is an initial 0.1 release; public APIs may evolve in
subsequent minor releases.
