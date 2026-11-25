# Changelog

## Unreleased

- Add `GutenbergMirror` to route advertised Gutenberg file URLs through caller-configured byte storage while preserving original metadata and sequential-open behavior.

## 0.21.9 - 2026-11-25

- Preserve historical CourtListener filesystem paths and fall back to publisher download URLs (or NULL), avoiding document-stream failures without dropping opinions.
- Validate CourtListener parallel CSV record boundaries using explicit source types; enable its UTC timezone parsing dependency independently of optional layers.
- Reconcile identical BHL OCR aliases and quarantine unmatched pages.
- Register external Parquet tables in DuckDB using shared caching.
- Make built-in layers opt-in Cargo features. Enable individual `layer-*` features, `crawl` for crawl composition, or `layers-all` for every built-in layer; direct layers-crate users must enable the corresponding unprefixed features.
- Release all eight public workspace crates at 0.21.9 with aligned internal dependency requirements.

## 0.21.8 - 2026-11-25

- Add configurable CSV preamble skipping before header detection and schema inference.
- Replace scalar Gutenberg file metadata `extent` and `modified` with `extent_values: List<UInt64>` and `modified_values: List<Utf8>`, preserving repeated declarations and validating numeric extents even when projected out. Consumers of these nested fields must update.
- Release all eight public workspace crates at 0.21.8 with aligned internal dependency requirements.

## 0.21.7 - 2026-11-24

- Normalize CourtListener bulk snapshots to one DuckDB schema, including document joins and validation.
- Build BHL lookup indexes after committing table data, and validate filter predicates against their input schema.
- Release all eight public workspace crates with aligned internal dependency requirements.

- Distinguish source blob bytes (`open-arrow-sink:blob=bytes`) from selected source URIs (`uri`) with shared annotation helpers and mark original binary/archive, arXiv, BHL, MECA, Gutenberg and WARC payloads plus selected PMC/CourtListener download URLs. Preserve filenames, metadata links, typed text and WARC BinaryView buffers; document the sink BinaryView compatibility requirement.

- Add Gutenberg whole-book Arrow rows retaining every advertised file's original bytes, MIME types and URI, plus exact RDF metadata for unmapped fields. Keep the metadata-only schema unchanged.

## 0.21.6 - 2026-11-24

- Batch wide BHL metadata joins into 8,192-request ranges after scoping source tables once, avoiding spill exhaustion on large conversion requests while retaining global page-version selection and output order.
- Keep the batch selection query in a dedicated SQL file.
- Release all eight public workspace crates at 0.21.6 with aligned internal dependency requirements.

## 0.21.5 - 2026-11-24

- Bound BHL snapshot queries to a 1 GiB DuckDB buffer budget, two threads and 1 GiB of spill, and use a 128 MiB remote memory cache with request fanout limited to four.
- Reduce scoped BHL metadata intermediates while preserving complete records, relationship order and lexical values.
- Release all eight public workspace crates at 0.21.5 with aligned internal dependency requirements.

## 0.21.4 - 2026-11-23

- Scope BHL metadata relations to selected pages and related parents before correlated joins build wide intermediate results.
- Add optional native numeric BHL lookup keys and ART indexes, preserving source values and relation order; prepare the metadata snapshot before distributing it.
- Simplify BHL database initialization with fixed SQL scripts and query-local helpers, retaining compatibility with existing snapshots.
- Clarify that remote DuckDB on-disk caches remain private and temporary until the final connection clone closes.
- Release all eight public workspace crates at 0.21.4 with aligned internal dependency requirements.

## 0.21.3 - 2026-11-23

- Add a common `FilterLayer` accepting caller-defined DataFusion predicates and scalar UDFs, with hidden projection dependencies and lazy partition support.
- Separate CSV framing from scan planning and reuse framing for bounded schema inference.
- Report the article ID when an arXiv database lookup returns no matching metadata.
- Release all eight public workspace crates at 0.21.3 with aligned internal dependency requirements.

## 0.21.2 - 2026-11-23

- Expose TAR link entries as non-file entries so arXiv source readers can skip linked attachments without following their targets. Continue rejecting file reads of links and unsupported sparse or special entries.
- Release all eight public workspace crates at 0.21.2 with aligned internal dependency requirements.

## 0.21.1 - 2026-11-23

- Reuse pooled DuckDB metadata connections and cached prepared queries for arXiv article lookups, returning connections before Arrow result processing.
- Ignore DuckDB upgrades in Dependabot so upgrades remain manual.
- Release all eight public workspace crates at 0.21.1 with aligned internal dependency requirements.

## 0.21.0 - 2026-11-23

- Add explicit `ArxivMetadataSource::{Oai, DuckDbDatabase { location, init_sql }}` selection; keep lazy OAI as the default.
- Let shared `DuckDbDatabase::connect(location, init_sql)` execute caller-provided initialization SQL before read-only local or remote attachment.
- Open caller-created BHL databases containing the 13 source tables without requiring the importer manifest.
- Query arXiv metadata from an indexed local or remote DuckDB snapshot through shared connections and `cache_httpfs`, with optional secret SQL before attachment.
- Change PDF/LaTeX/XML conversion metadata schemas to omit nine fields unavailable in the Cornell/Kaggle snapshot, rather than emitting NULL placeholders. Keep raw OAI harvesting schemas unchanged; document both contracts and the nested schema change.
- Stop retrying HTTP 406 Not Acceptable responses under the provider transport retry policy.

- Release all eight public workspace crates at 0.21.0 with aligned internal dependency requirements.

## 0.20.4 - 2026-11-22

- Select BHL OCR versions by authoritative metadata sequence, accepting zero-based filename alternatives without emitting duplicate pages or choosing mismatched content.
- Give BHL and CourtListener shared managed DuckDB connections with private writable spill storage retained across connection clones.
- Preserve arXiv source payload bytes without rejecting non-UTF-8 TeX or unrecognized single-file text; simplify content readers and reuse parsed article IDs.
- Delegate ranged and sequential TAR parsing to archive packages, retaining lazy member reads, cancellation and decoder-trailer checks.
- TAR API change: remove `TarLimits` and `ArxivService::with_tar_limits`; use `TarStream::new(input)` and `TarArchive::open(input).await`. TAR counts, member sizes and GNU/PAX metadata have no custom limits. Only ranged indexing requires Tokio.
- Release all eight public workspace crates at 0.20.4 with aligned internal dependency requirements.

## 0.20.3 - 2026-11-22

- Fix the BHL multi-directory planner's Send future compilation error by constructing directory listing futures before buffering them. Preserve bounded concurrency and selected directory order.
- Release all eight public workspace crates at 0.20.3 with aligned internal dependency requirements.

## 0.20.2 - 2026-11-22

- Read multiple complete BHL item/part directories in one native scan with `BhlShardReadRequest::from_prefixes`, sharing one metadata join and preserving selected directory order. Bound concurrent directory listings to eight by default, with a per-request override.
- Retry HTTP 406 responses through the configured transport retry policy, preserving admission control and retry limits.
- Release all eight public workspace crates at 0.20.2 with aligned internal dependency requirements.

## 0.20.1 - 2026-11-22

- Add `BhlShardAccessor` and `BhlShardReadRequest` for complete OCR item/part directories, with read-only metadata snapshot reuse, strict reconciliation and native CrawlAccessor planning.

- Release all eight public workspace crates at 0.20.1 with aligned internal dependency requirements.

## 0.20.0 - 2026-11-22

- Add persistent BHL metadata database creation and read-only reopening; stream selected-page metadata on a bounded blocking worker while preserving repeated page, part and item-title associations, relation order and missing/conflicting source reports.
- Batch DuckDB imports with reusable appenders and one transaction per source table; flatten creator-identifier and DOI membership joins to avoid large intermediate results.
- Stream explicit arXiv raw shards and bulk manifests sequentially; normalize legacy source IDs, decode standalone legacy TeX and distinguish unavailable LaTeX from corrupt source packages.
- Support GNU long names in indexed and streaming TAR reads, accept OAI `text/xml` responses, and avoid full-cell scans for never-null CSV predicates.
- Add shared per-request HTTP admission and rate-limit integration for connections.
- Make all `Connection` downloads consume the original GET response sequentially, avoiding conditional Range replays and arXiv HTTP 412 failures. `Connection` no longer provides random access; ZIP/Parquet readers require an OpenDAL-backed or custom ranged source.
- Keep DuckDB bindings on the Arrow 58-compatible 1.10506.x line.
- Release all eight public workspace crates at 0.20.0 with aligned internal dependency requirements.

## 0.19.0 - 2026-11-21

- Add CourtListener's joined Arrow schema with typed timestamps, dates, counts and nested opinion, cluster, docket, court and provenance structs.
- Add strict streaming COPY CSV table readers that preserve unquoted NULLs, quoted empty strings, multiline fields and explicit historical dialects.
- Add `CourtListenerDatePartitionAccessor`: import four same-date tables concurrently into a temporary DuckDB database, join with SQL, and stream one row per opinion with source order and provenance.
- Keep metadata inspection and scan planning free of storage I/O; import each source table in one transaction and reject duplicate keys before querying.
- Document date partitioning and caller-managed publication of one Lance dataset per date.
- Release all eight public workspace crates at 0.19.0 with aligned internal dependency requirements.

## 0.18.2 - 2026-11-21

- Use DuckDB bindings 1.10506.1 for BHL metadata joins, sharing Arrow 58 with the workspace instead of compiling a second Arrow major.
- Consume joined Arrow batches directly, removing the in-memory IPC serialization bridge and its two direct IPC dependencies; retain streaming error propagation through the fallible batch-step API.

- Release all eight public workspace crates at 0.18.2 with aligned internal dependency requirements.

## 0.18.1 - 2026-11-20

- Support newer DuckDB Rust bindings (1.20000.1 through 1.20001.x), locking to 1.20001.0 and resolving native-library dependency conflicts with downstream workspaces.
- Bridge DuckDB's internal Arrow 60 metadata batches through in-memory Arrow IPC into the unchanged public Arrow 58 schema; OCR payloads retain their existing path.
- Propagate DuckDB streaming errors directly through the binding's fallible iterator.
- Release all eight public workspace crates at 0.18.1 with aligned internal dependency requirements.

## 0.18.0 - 2026-11-20

- Connect the BHL OCR page reader to DuckDB-joined metadata for explicitly selected item/part pages, preserving source provenance and exposing missing/conflicting metadata through a caller report callback.
- Honor BHL page projections: skip OCR downloads when `data` is omitted, construct only selected columns, and preserve row counts for empty projections.
- Release all eight public workspace crates at 0.18.0 with aligned internal dependency requirements.

## 0.17.0 - 2026-11-20

- Stream arXiv bulk LaTeX package decoding from the existing Arrow buffer, filter attachments before body materialization and yield batches incrementally while retaining automatic metadata enrichment.

- Add the always-available integrated `ArchiveAccessor` for provider-independent TAR/single-file expansion, outer decompression, header filtering and incremental binary member batches. Sequential TAR completion drains the underlying input through EOF to surface decoder and source errors.

- Stream explicit arXiv content shards once and yield owned article partitions incrementally, removing upfront header-range indexing and per-article storage reopens while overlapping input reads with bounded article processing.

- Populate arXiv content metadata automatically by default from the typed OAI record schema; preserve nullable metadata output and support parallel article processing within a manifest-selected TAR.
- Expose archive batch partitions for parallel record processing.
- Support BHL AWS Open Data sources only, with a dedicated binary page reader, fixed page schema, caller-supplied typed metadata and page identity validation; remove the BHL API provider.
- Bind Semantic Scholar files from a supplied dataset manifest without additional API reads.
- Release all eight public workspace crates at 0.17.0 with aligned internal dependency requirements.

## 0.16.0 - 2026-11-20

- Append nullable `metadata: Struct` to the common arXiv content schema, reusing the full arXivRaw OAI record schema. Current content readers leave the column null; metadata fetching and joining are not part of this schema change.

## 0.15.0 - 2026-11-20

- Append non-null `data_type: LargeUtf8` to the shared arXiv PDF/LaTeX/XML content schema, tagging each row's unchanged `data: LargeBinary` payload; retain the existing `format` column. Consumers of the previous eight-field content schema must accept this ninth field.

- Add provider-independent typed attachment cells and `AttachmentFallbackLayer` for per-cell primary/fallback selection; arXiv provider wiring remains separate.
- Expose pinned Semantic Scholar dataset manifests and their native file entries.
- Release all 8 public workspace crates at 0.15.0 with matching internal dependency requirements.

## 0.14.1 - 2026-11-20

- Keep TAR indexing futures Send by owning the seek adapter across header-read awaits, fixing compilation of explicit-shard arXiv discovery streams.
- Release all 8 public workspace crates at 0.14.1 with matching internal dependency requirements.

## 0.14.0 - 2026-11-20

- Add a common, fixed eight-column arXiv content schema for PDF, LaTeX and XML, with all content emitted as `data: LargeBinary`; keep metadata as a separate data source.
- Extract LaTeX source members without attachments, preserving article identity and outer/inner source positions and rejecting unsupported non-LaTeX inputs.
- Accept one explicit relative TAR shard without manifest discovery or listing.
- Plan individual members within an explicit shard as independent scan partitions so the crawl executor can read and parse them concurrently under its existing concurrency limit. TAR header indexing remains sequential before output begins.
- Add caller-supplied byte-source length hints to avoid repeated metadata requests, and expose validated TAR member ranges. `ByteReadRequest` struct literals must include the new `content_length` field; constructor-based callers remain compatible.
- Release all 8 public workspace crates at 0.14.0 with matching internal dependency requirements.

## 0.13.0 - 2026-11-19

- Accept arXiv manifest sizes that count member payloads without TAR framing; retain lower-bound length and TAR structural/truncation validation.
- Bind arXivRaw metadata to its published string/struct/list schema, preserving modern and legacy IDs and stable optional/version fields instead of sampled numeric inference. Full-record output uses stable `arxiv:` field prefixes; protocol-header-only mapping remains unchanged.
- Add an overridable full-record mapping hook for OAI providers, keeping sampled inference as the default for other providers.

- Read original arXiv PDF and HTML articles, and expand LaTeX source packages and ancillary attachments.
- Reduce S2ORC span parsing allocations with a strict serde visitor and direct consumption of validated spans.
- Document arXiv bulk file hierarchy, sharding, and validation gaps.
- Release all 8 public workspace crates at 0.13.0 with matching internal dependency requirements.

## 0.12.1 - 2026-11-19

- Move S2ORC v2 paper body text from `body.text` to top-level `text: LargeUtf8` by default; retain body annotations and independent bibliography text (typed-output schema change).
- Add nested struct field extraction/removal through schema evolution, with parent-null propagation, shared value buffers and schema-only transformation.
- Scan and copy quoted JSON string runs in bulk while retaining JSON validation and decoding behavior.
- Align architecture/API documentation and document the seven-column S2ORC output schema and consumer migration.
- Release all 8 public workspace crates at 0.12.1 with matching internal dependency requirements.

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
