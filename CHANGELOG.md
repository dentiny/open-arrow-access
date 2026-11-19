# Changelog

## Unreleased

- Preserve native attachment scan planning for Parquet, CSV/TSV, JSON/JSONL and XML, enabling shared crawl execution for provider files.
- Bind attachment formats and outer compression once, forward format metadata inspection, and remove temporary per-open bound sources.
- Retain conservative provider-independent ordering in attachment plans and opened readers; binary attachments remain whole-document rows.

## 0.12.0 - 2026-11-19

- Add `crawl::CrawlAccessor` with existing-source, file-set, directory and provider-resolver entry points for integrated crawls.
- Expose default crawl options and builders for parallel reads, emission, schema evolution, coalescing and prefetch.
- Preserve native scan planning through schema evolution, sharing one immutable transformation plan across partition workers.
- Document discovery, manifest-to-file mapping, execution/emission workflows, layer boundaries and performance tuning.
- Release all 8 public workspace crates at 0.12.0 with matching internal dependency requirements.

## 0.11.0 - 2026-11-19

- Add a typed S2ORC v2 reader with a fixed Arrow schema, native scan planning, and validation before output projection.
- Reject numeric and boolean values in JSON string fields instead of coercing them (breaking behavior change).
- Use owned BinaryView bodies by default for WARC Arrow output, replacing LargeBinary bodies while retaining parser allocations (breaking schema change).
- Bound streaming-file read-ahead by estimated Arrow bytes, with an 8 MiB default soft budget per resident file.
- Read positioned byte sources sequentially by default and reserve range reads for explicit stored-offset access.
- Add shared HTTP read pacing and Retry-After cooldowns across cloned operators.
- Release all 8 public workspace crates at 0.11.0 with matching internal dependency requirements.

## 0.10.3 - 2026-11-19

- Build WARC dates directly as millisecond timestamps, avoiding string formatting and reparsing.
- Add opt-in positioned WARC BinaryView bodies that retain parser allocations, with a configurable soft body-byte batch budget.
- Simplify WARC batch construction using Arrow builders while preserving the default LargeBinary schema and source positions.
- Release all 8 public workspace crates at 0.10.3 with matching internal dependency requirements.

## 0.10.2 - 2026-11-19

- Defer positioned source opens until scan execution so a slow first file does not block later parallel reads.
- Preserve schema validation during planning, ordered output guarantees, and cancellation of lazy file opens.
- Release all 8 public workspace crates at 0.10.2 with matching internal dependency requirements.

## 0.10.1 - 2026-11-19

- Add an opt-in layer to skip missing source objects with explicit reporting.
- Reuse one streaming GET for sequential reads and separate sequential readers, ranged readers, and buffering.
- Reuse Arrow JSON decoders across PMC metadata documents.
- Release all 8 public workspace crates at 0.10.1 with matching internal dependency requirements.

## 0.10.0 - 2026-11-18

- Add `CoalesceLayer` with a strict row cap and a soft estimated-byte budget for grouping small record batches.
- Forward scan byte-read options through provider selections so downstream IO configuration reaches the underlying source.
- Start eligible whole-object reads with a Range GET instead of a preflight metadata HEAD, retaining existing handling for caller-supplied lengths.
- Parse fully qualified Hugging Face dataset split URIs.
- Add an all-strings CSV schema discovery mode.
- Release all 8 public workspace crates at 0.10.0 with matching internal dependency requirements.

## 0.9.0 - 2026-11-18

- Read PMC article metadata from a selected inventory shard, with native partitions for parallel reads.
- Select PMC PDF, XML or TXT content URLs, defaulting to PDF, through the derived `content_url` column.
- Flatten PMC metadata into top-level typed fields: remove the duplicate `metadata` struct and rename `text` to `text_url` (breaking schema changes).
- Allow explicit CSV delimiters to override file extensions.
- Release all 8 public workspace crates at 0.9.0 with matching internal dependency requirements.

## 0.8.2 - 2026-11-18

- Preserve pre-consumption source statistics through prepared sources and file selections for ingestion planning.
- Decode numeric PMC PMID values and expose complete PMC metadata as a typed Arrow struct.
- Release all 8 public workspace crates at 0.8.2 with matching internal dependency requirements.

## 0.8.1 - 2026-11-18

- Forward native scan planning through SelectionAccessor so arXiv TAR shards retain their independent partitions in downstream parallel readers.
- Preserve fixed selections, per-read options, schema and ordering when forwarding scan plans.
- Release all eight public workspace crates at 0.8.1 with matching internal dependency requirements.

## 0.8.0 - 2026-11-18

- Add a reusable attachment record accessor and a fixed-selection accessor for composing provider requests with per-read options.
- Join arXiv bulk PDFs with cached metadata in one Arrow record stream, preserving typed metadata fields and original PDF bytes.
- Normalize modern, legacy, and OAI arXiv identifiers and reject missing matches, duplicate IDs, and malformed identifiers.
- Release all eight public workspace crates at 0.8.0 with matching internal dependency requirements.

## 0.7.0 - 2026-11-17

- Read streaming files concurrently in file-set and directory scans, including compressed WARC, JSONL, and CSV.
- Use one in-memory queue per resident file for both ordered and unordered emission; concurrency limits resident files, with no byte limit on buffered output.
- Preserve native partition parallelism and unify file planning, validation, and alignment without reopening inspected inputs.
- Infer union-by-name schemas concurrently and reuse retained scan plans and inference samples.
- Make JSON schema inference linear for large records split across many input chunks.
- Release all eight public workspace crates at 0.7.0 with matching internal dependency requirements.

## 0.6.0 - 2026-11-17

- Support custom Common Crawl HTTP inventory URLs and reuse the paths manifest resolver.
- Resolve same-origin paths manifests, preserve absolute manifest URLs, and retain URL suffixes.
- Restore WARC `id`, `type`, and `warc_info_id` column names.
- Fix `Send` attachment-binding futures for asynchronous accessors.
- Honor HTTP `Retry-After` using released OpenDAL APIs, removing the Git dependency patches.
- Release all eight public workspace crates at 0.6.0 with matching internal dependency requirements.

## 0.5.0 - 2026-11-17

- Make directory and file-set dataset APIs unconditional; remove the `directory` and `dataset-directory` Cargo features.
- Add accessor, file-format, and dataset metadata inspection with optional schemas and suffix-based format detection.
- Add record prefetch with row and byte budgets, propagate source positions with decoded Arrow batches, and preserve positioned readers through identity schema evolution.
- Add projection and batch-size builders, source-aware byte streams, capability-preserving record stream helpers, and lazy scan partition helpers.
- Expose standard WARC header columns and add PMC release discovery and public-bucket storage.
- Centralize provider API key configuration, add observation and gauge exporter helpers, and simplify storage and decompression reader handling.
- Remove example directories and consolidate committed test fixtures and reader contract assertions.
- Release all public workspace crates at 0.5.0.

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
