# Support matrix

These components support the source-access and decoding stages of pretraining
data preparation, from raw corpus files and provider collections to Arrow input
for downstream preprocessing.

Feature names in this page belong to the `open-arrow-access` facade. All optional
features are default-off. This page describes implemented behavior; future work
is listed in the [roadmap](roadmap.md).

## File formats

| Input | Feature | Output | Schema and input requirements |
| --- | --- | --- | --- |
| Parquet | `format-parquet` | Arrow batches | File schema; exact ranges and known byte length; uncompressed/Snappy pages; opt-in Brotli via `format-parquet-brotli`, Gzip via `format-parquet-gzip`, LZ4 via `format-parquet-lz4` |
| CSV / TSV | `format-csv` | Arrow batches | Explicit or sampled Arrow schema; configurable delimiter and header; sequential bytes |
| JSON / JSONL | `format-json` | Arrow batches | Explicit or sampled Arrow schema; whitespace-separated objects, not top-level arrays; bare NaN/Infinity/-Infinity accepted; sequential bytes |
| XML | `format-xml` | Arrow batches | Explicit record path; bounded schema inference or explicit mappings; sequential UTF-8 bytes |
| ATIF JSON / JSONL | `format-atif` | Arrow batches | Fixed trajectory schema; one complete trajectory per document or nonblank line; sequential bytes |
| WARC | `format-warc` | Native `WarcRecord` values | Sequential bytes; preserves headers and binary bodies; no built-in Arrow conversion |

Projection selects top-level fields in caller order. Record readers expose a
fixed schema at open. CSV and JSON support bounded per-input schema inference;
CSV also supports automatic header detection. Cross-file schema merging remains
planned. XML already infers schemas independently for each file; use explicit
mappings when multiple files must share a schema.

See [format behavior](formats.md) for layout, validation, and memory limits.
There is no automatic format or compression detection. PDF, HTML, images, and
other provider files can be accessed as bytes; the library does not supply
content-extraction or multimodal decoding for those representations.

## Storage

`storage::OpenDalAccessor` is always available. Configure an OpenDAL operator,
then wrap it once. Enable backend and transport features on your own OpenDAL
0.59.2 dependency; this project does not define a separate S3 accessor or facade
`storage-s3` feature.

| Backend / source | Configuration | Scope |
| --- | --- | --- |
| Local filesystem | OpenDAL `services-fs` | Root-relative objects, sequential bytes, exact ranges, and discovery |
| Amazon S3 / compatible stores | OpenDAL `services-s3` and a transport | Caller configures bucket, endpoint, region, credentials, and policies |
| HTTP | OpenDAL `services-http` and a transport, or a provider's HTTP file accessor | Generic adapter needs stat/known lengths; provider downloads may instead expose sequential GET-only readers |
| Hugging Face repositories | `service-huggingface` | Provider-owned repository, revision, root, endpoint, and authentication |
| Other OpenDAL services | Enable the corresponding upstream feature | Same adapter; usable operations depend on the configured operator |
| Custom byte sources | Implement `ByteAccessor` / `ByteReader` | Formats consume these contracts directly |

Backend availability is not a guarantee for every composition. `OpenDalAccessor`
needs upstream reads and stat support or supplied content lengths to advertise
reading; listing follows the operator's capability. Parquet and ranged archive
inspection additionally require exact ranges and known lengths. A sequential
provider download cannot satisfy them without an additional ranged representation.
The checked-in storage tests exercise filesystem behavior and generic adapter
contracts; they are not a certification of every upstream cloud service.

Backend setup, credentials, roots, and OpenDAL storage policies belong to the
caller. Object locators are interpreted by the supplying accessor. For generic
storage, locators are passed to the configured backend. See [S3 configuration](usage.md#use-s3-or-another-opendal-backend).

## Compression and archives

| Representation | Feature | API | Input / output |
| --- | --- | --- | --- |
| Gzip | `decompression-gzip` | `DecompressionLayer::new(Gzip)` | Sequential compressed bytes → decoded bytes; concatenated members |
| Bzip2 | `decompression-bz2` | `DecompressionLayer::new(Bzip2)` | Sequential compressed bytes → decoded bytes; concatenated members |
| LZ4 frames | `decompression-lz4` | `DecompressionLayer::new(Lz4)` | Standard, concatenated, and skippable frames → decoded bytes |
| Zstandard | `decompression-zstd` | `DecompressionLayer::new(Zstd)` | Standard, concatenated, and skippable frames → decoded bytes |
| ZIP | `decompression-zip` | `ZipAccessor` / `ZipArchive` | Ranged, known-length archive → selected Stored/Deflate entry bytes |
| TAR, sequential | `decompression-tar` | `TarStream` | Sequential archive → ordered entry headers and byte readers; can follow gzip for `.tar.gz` |
| TAR, ranged | `decompression-tar` | `TarArchive` | Ranged, known-length archive → cached headers and independent selected entry readers |

Streaming codecs remove range access and report an unknown decoded length.
They compose with sequential JSON, CSV, XML, ATIF, and WARC consumers. They cannot
directly feed the current Parquet reader or ranged ZIP/TAR inspection. Parquet's
internal page compression is independent of these external codecs.

TAR supports regular GNU/USTAR files and directories; links, sparse files, and
extension headers are unsupported. Archive names remain metadata; readers do
not extract paths to the filesystem. Read compressed/ZIP entries through EOF to
complete integrity validation. See [decompression](../decompression/README.md)
and the [TAR example](../examples/read_tar.rs).

## Dataset composition

| Selection | Feature | Ordering | Schema policy |
| --- | --- | --- | --- |
| `DirectoryAccessor` | `dataset-directory` | Recursive by default; suffix selection; lexicographically sorted and deduplicated paths | First file fixes the projected schema; later files must match, including metadata |
| `FileSetAccessor` | `dataset-directory` | Caller order; repeated objects retained | Same schema policy; empty selection is invalid |
| Hugging Face Parquet split | `service-huggingface` | Sorted shards from the selected config/split and pinned export commit | Reuses Parquet and FileSet |

Multi-file contents are read lazily and sequentially. Directory discovery collects
the selected paths before reading to establish order. It is not an atomic
snapshot of a mutable prefix. These record datasets require an accessor returning
`RecordReader`; the native WARC parser has a different output type.

## Dataset providers

Providers handle discovery, source identity, and metadata conventions. A provider
feature does not imply that all its files decode to Arrow. Keep licensing, rights,
and release metadata with your downstream corpus manifest.

| Provider | Feature | Available access / output | Guide |
| --- | --- | --- | --- |
| Hugging Face | `service-huggingface` | Repository file bytes; Parquet config/split as Arrow | [Guide](../service/huggingface.md) |
| Common Crawl | `service-warc` | Crawl/segment membership; Arrow reads through a caller-supplied record accessor | [Guide](../service/commoncrawl.md) |
| PMC | `service-pmc` | Article-version metadata and inventory shards as Arrow; article content is separate | [Article metadata](../service/pmc.md), [inventory](../service/pmc-inventory.md) |
| Gutenberg | `service-gutenberg` | Per-book RDF metadata as one Arrow row; content URIs remain metadata | [Guide](../service/gutenberg.md) |
| CourtListener | `service-courtlistener` | Bulk snapshot discovery and decoded CSV bytes | [Guide](../service/courtlistener.md) |
| Zenodo | `service-zenodo` | Record metadata and lazy original-file bytes | [Guide](../service/zenodo.md) |
| Internet Archive | `service-internet-archive` | Item metadata and lazy original-file bytes | [Guide](../service/internet-archive.md) |
| Dataverse | `service-dataverse` | Dataset-version metadata and lazy original-file bytes | [Guide](../service/dataverse.md) |
| arXiv | `service-arxiv` | OAI metadata pages, bulk manifests, and selected archive members | [Bulk archives](../service/arxiv.md), [metadata](../service/arxiv-metadata.md) |
| bioRxiv / medRxiv | `service-biorxiv` | Metadata pages, lazy JATS bytes, and MECA archive support | [Guide](../service/biorxiv.md) |
| MECA | `service-meca` | Transfer manifest and referenced ZIP entry bytes | [Guide](../service/meca.md) |
| DOAB | `service-doab` | OAI metadata pages and continuation tokens | [Guide](../service/doab.md) |
| OAPEN | `service-oapen` | OAI metadata pages and continuation tokens | [Guide](../service/oapen.md) |
| HathiTrust Hathifiles | `service-hathifiles` | Catalogue metadata and original gzip files | [Guide](../service/hathifiles.md) |
| OpenStax | `service-openstax` | Repository discovery and commit-pinned source archive bytes | [Guide](../service/openstax.md) |
| Figshare | `service-figshare` | Article/version metadata and original-file bytes | [Guide](../service/figshare.md) |
| Semantic Scholar | `service-semantic-scholar` | Release-pinned dataset partitions as original bytes | [Guide](../service/semantic-scholar.md) |
| Biodiversity Heritage Library | `service-bhl` | Versioned bulk OCR files; API metadata and advertised file bytes | [Guide](../service/bhl.md) |
| Library of Congress | `service-library-of-congress` | Search/item metadata and advertised resource bytes | [Guide](../service/library-of-congress.md) |
| USPTO | `service-uspto` | Bulk-product discovery, metadata, and original-file bytes | [Guide](../service/uspto.md) |
| Federal Register | `service-federal-register` | Search/document metadata and original document bytes | [Guide](../service/federal-register.md) |
| GovInfo | `service-govinfo` | Bulk directories, package metadata, and original-file bytes | [Guide](../service/govinfo.md) |
| Regulations.gov | `service-regulations` | Resource metadata and original attachment bytes | [Guide](../service/regulations.md) |

Provider paging, authentication, release selection, and download capabilities vary.
See the linked guides before composing a format reader. Many providers expose
one metadata page at a time; callers own continuation, pacing, and retry policy.

## Layers and execution

| Component | Status | Scope |
| --- | --- | --- |
| `Layer<A>` / `AccessorExt::layer` | Implemented | Type-preserving accessor wrappers with explicit capabilities |
| `DecompressionLayer<C>` | Implemented | Streaming byte transformation; range capability removed |
| Per-reader byte I/O concurrency | Implemented | `ByteReadOptions`; ordered output, default concurrency one |
| Generated columns | Implemented | Row-level SQL expressions and final projection; [source projection pushdown deferred](generated-columns.md) |
| Metrics, timeout, read budgets | Planned | Observation and resource policies |
| Partition planning / parallel scans | Planned | Source-aware work units and bounded execution |
| Unordered emission | Implemented | Explicit `UnorderedEmissionLayer`; requires a reorderable scan; independent of parallel execution |

`layers/` includes Arrow type conversion and generated-column record layers. Backend-specific OpenDAL layers can already be configured on the
operator; they are distinct from this project's accessor layers.
