# Changelog

## Unreleased

- Add accessor metadata inspection and schema derivation through record layers.
- Inspect CSV/JSON inference samples, Parquet footers, and fixed record schemas without executing readers.

## 0.4.0 - 2026-11-16

- Add opt-in source columns and source positions with user-configurable record framing.
- Preserve PMC metadata tolerance for unknown upstream fields and strict field types independently of generic JSON format settings.
- Adapt XML metadata parsing to the quick-xml 0.42 string interfaces.
- Replace compound return tuples with named result structs.
- Add global row limits for record scans and registered compression codecs.
- Select CSV delimiters per file, enforce strict JSON schemas, and coerce JSON scalar values into strings.
- Include object locators in decode/decompression errors, handle compression suffixes case-insensitively, and skip empty inferred inputs in union-by-name file sets.
- Require ATIF 0.5.2, XML 0.3.2, and WARC 0.3.5; release all public workspace crates at 0.4.0.

## 0.3.1

- Align OpenDAL and its HTTP/metrics components on 0.59.5, allowing compatible patch updates.
- Require atif-arrow 0.5.1, warc-rs 0.3.4, and xml-arrow 0.3.1 from the coordinated dependency release.
- Update the workspace and its internal dependency requirements together.

## 0.3.0

- Add `PreparedSource` for reusable schema binding with execution-time read options
  and native partition planning, plus `file_accessor` and `file_scan_accessor`.
- Expose `BoundSource` from core and add conversion from `FileSource`.
- Add optional `ScanStatistics` and Parquet footer row counts.
- Add the opt-in `blocking` feature and `BlockingRecordBatchReader` Arrow adapter.
- Add typed provider attachment decoding with `open_attachment_source` and
  validated HTTP range reads that honor read concurrency settings.
- Add demand-driven OAI continuation harvesting, inferred provider record formats,
  and reusable API page sets for bounded parallel scans.
- Add automatic discovery of arXiv bulk distributions.
- Add Internet Archive credentials, Dataverse API/bearer authentication, Zenodo
  bearer tokens, and custom-connection API keys for BHL, Regulations.gov,
  Semantic Scholar, and USPTO.
- Add a transport-level `HttpConcurrencyLimitLayer` shared by provider connections
  and OpenDAL storage, with admission held until response completion or drop.
- Project PMC metadata directly and streamline file-set planning, provider
  validation, JSON end-of-input handling, and MECA reader state.

Compatibility notes:
- `OpenDalAccessor::new` now limits ongoing HTTP requests to 64 by default. Use
  an explicit shared limit when composing storage operators and providers.
- Public APIs may evolve across minor releases while the project is pre-1.0.

## 0.2.2

- Add reusable binary document decoding with `BinaryDecoder`.

## 0.2.1

- Unify CSV header handling with `Auto`, `Present`, and `Absent` modes and
  consolidate header regression coverage.

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
