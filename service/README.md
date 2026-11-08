# Dataset source providers

Provider-specific discovery and schema conventions live in this crate. Each provider adds its own default-off feature and facade exports.

See [service response schemas](response-schemas.md) for each entry point's return
type, Arrow fields and nullability, reader metadata, and example response shapes.

Providers use the shared object/reader contracts and existing byte, format,
and decompression accessors. Backend configuration stays caller-controlled.
Generic storage access and object-store prefix listing belong in `storage`,
not in a provider.

The [source-attribute proposal](../docs/source-attributes.md) defines provider
constraints and conditional planning. Sequential cursor-based file discovery
can coexist with independent file reads; dependent API page requests require
a different plan. Services retain their own ordering requirements while
delegating discovered representations to dataset and format accessors.

The default-off `huggingface` feature (`service-huggingface` on the facade)
provides `HuggingFaceService::repository` for dataset repository files.
The service owns repository ID, revision, root, endpoint and authentication.
Its byte accessor reuses `storage::OpenDalAccessor`; format accessors decode records.
Floating revisions are not snapshots.
The same service exposes `dataset(HuggingFaceDatasetRequest)` and implements
`Accessor` for that request. This opens one config/split as ordered Arrow batches
from the HF Parquet export. A floating export revision is first pinned with a
small `expand=sha` metadata request; membership and file
reads then use that same commit. Explicit commit requests skip the metadata call.
The selected directory is listed nonrecursively, Parquet paths are sorted, and
existing `ParquetAccessor`/`FileSetAccessor` perform lazy sequential reading,
projection, batching and schema validation. Only the shard list is collected.

A missing complete split fails. `with_partial_exports()` explicitly permits
fallback to `partial-<split>`; full exports take precedence, and permission or
transport errors never trigger fallback. `with_revision` refers to the Parquet
representation, not the original source branch. Native dataset-card parsing and
Viewer row APIs are outside this entry point.

## Gutenberg

Enable `gutenberg` (`service-gutenberg` on the facade) to convert one per-book
RDF/XML object into one Arrow row. `GutenbergAccessor` accepts a standard
`ReadRequest` and reuses `xml-arrow` for metadata extraction. File entries retain
RDF order; their URIs are metadata and are never fetched.

| Column | Arrow type |
| --- | --- |
| `ebook_id` | UInt64 |
| `title`, `rights` | nullable LargeUtf8 |
| `creator` | List of struct: nullable `uri`, `name` (Utf8), `alias` (List<Utf8>), nullable `birthdate`, `deathdate` (Int32 years) |
| `language`, `bookshelf` | List<Utf8> |
| `subject` | List of struct: nullable `scheme` (Utf8), `value` (Utf8) |
| `issued` | nullable Date32; Gutenberg release date |
| `files` | List of struct: `uri` (Utf8), `format` (List<Utf8>), nullable `extent` (UInt64), nullable `modified` (Utf8) |

Missing optional metadata becomes null or an empty list. Only mapped RDF fields
are emitted. Malformed RDF terminates the reader rather than dropping the book.
Projection preserves caller order and zero-column row counts; mapped fields are
validated even when projected away. Byte options are forwarded to the RDF read.

One RDF emits one row,
independently of the batch-size target. File downloading, catalogue discovery,
archive expansion and writing remain with the caller.

Run the [example](../examples/read_gutenberg.rs) against the synthetic metadata
fixture, or supply a mirror root and RDF path:

```sh
cargo run --example read_gutenberg --features service-gutenberg
```

## PMC inventory (`pmc`)

Enable `service-pmc` for `service::pmc_inventory::PmcInventoryAccessor`.
A `ReadRequest` selects one `manifest.json`; its `fileSchema` defines CSV columns
and order. The reader reuses gzip and CSV adapters, opens shards lazily in
manifest order, and forwards read options. It never fetches article objects.

Original CSV fields and values are preserved without extra columns. Bucket and
Key must be present even when projected away; Key values are not interpreted.
Schema metadata records the manifest URI, version and creation timestamp.
Empty inventories retain the declared schema and produce EOF.

Run the
`read_pmc_inventory` example against a caller-configured inventory snapshot.

## PMC article-version metadata (`pmc`)

Enable `service-pmc` for `service::pmc::PmcAccessor`. This service reads exactly
one metadata JSON object named by a standard `ReadRequest`, and emits one row.
It never lists a directory or opens a PDF, XML, text, or media URL. Inventory
enumeration is a separate service. The input byte accessor requires sequential
reads only; filesystem, SDK operators, and decompression layers are caller-owned.

```rust,no_run
use open_arrow_access::{Accessor, ReadRequest, service::pmc::PmcAccessor};
use open_arrow_access_storage::{OpenDalAccessor, opendal};

async fn read(operator: opendal::Operator) -> Result<(), Box<dyn std::error::Error>> {
    let source = PmcAccessor::new(OpenDalAccessor::new(operator));
    let mut reader = source.open(ReadRequest::new("metadata/PMC123.2.json")).await?;
    while let Some(batch) = reader.next().await? {
        assert_eq!(batch.num_rows(), 1);
    }
    Ok(())
}
```

Run `cargo run --example read_pmc_inventory --features service-pmc --
/mirror/root <dated-manifest-key>` against a mirror preserving manifest/shard keys.
See the [PMC dataset README](https://pmc-oa-opendata.s3.amazonaws.com/README.txt)
and [S3 inventory manifest format](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory-location.html).

Manifest byte collection uses core `read_object`. Shard iteration retains its
provider state and uses `StreamReader` for metadata and terminal handling.

For a local mirror, run `cargo run --example read_pmc --features service-pmc --
/data/pmc-mirror metadata/PMC123.2.json`. For multiple metadata objects, compose
with the existing file-set dataset or open the explicit keys supplied by an
inventory reader. There is no provider-specific `Files` or `Prefix` input.

### Article metadata contract

The schema uses the upstream names: `pmcid`, `version`, `pmid`, `doi`, `title`,
`citation`, `is_pmc_openaccess`, `is_manuscript`, `is_historical_ocr`,
`is_retracted`, `license_code`, `xml_url`, `pdf_url`, `media_urls`, and `text_url`.
Unknown fields are ignored; duplicate keys use their final value. Reader metadata
records the input object as its source; schema metadata records
`pmc.row_kind = article_metadata`.

`pmcid` is required and validated, but its source spelling is retained. `version`
is nullable UInt32 and must be positive when present. `pmid` is nullable Utf8
and requires a JSON string.
`title` and `citation` are LargeUtf8, the four `is_*` fields are nullable Boolean,
`media_urls` is a nullable List<Utf8>, and the other optional fields are nullable
Utf8. A missing or null PDF URL is a normal row with a null `pdf_url`. No article
is filtered by content availability, licence, language, or duplication. Content
URL strings are retained as supplied and are never interpreted as read requests.

Validation lives in `pmc/validation.rs`. Invalid JSON or mapped field types are
terminal decode errors. Empty upstream byte chunks are
I/O errors. `open` validates projection without starting a read; locator syntax
is owned by the underlying byte accessor and passed through unchanged.
`next` performs one metadata read. Errors and EOF are terminal; dropping a pending
reader releases its input. Common byte options are forwarded unchanged.

Projection supports caller field order and zero-column row counts. The schema
is fixed before reading, and every valid metadata object produces one row.
Known fields are validated even when omitted from the projection. Only selected
Arrow columns are built directly from the source fields, without serializing or
decoding an intermediate JSON object. The Arrow schema is the single field/type
definition: conversion dispatches on its data types, with special rules only for
PMCID and version. There is no intermediate article struct to maintain.
The JSON file-format feature is not required.
The row target is an upper bound; this single-object service emits at most one
batch with one row. It does not fetch an inventory, choose a snapshot, filter
licences, extract text, or write Lance datasets.

The article reader uses core `read_object` and `StreamReader`
for terminal handling. JSON conversion and article validation stay in PMC.
## Common Crawl configuration

Enable `service-warc` on the facade for `CommonCrawl`, which validates a
crawl identity and HTTP(S) origin, and `create_http_accessor`, which configures
an origin-rooted OpenDAL HTTP reader without fetching data. Gzip and record
format decoding remain explicit caller-supplied layers.

Enable `service-warc` for `CommonCrawlWarcRequest` and
`CommonCrawlWarcAccessor`: resolve all WARC paths from a crawl's decoded
`warc.paths.gz` inventory, preserving file order, then read through a supplied
single-file format accessor and the existing sequential FileSet. `resolve`
returns membership without opening record files. The caller supplies gzip for
compressed inventories and file decompression/format parsing separately.
Selected locators are held in memory.
There is no index, batch planner or parallel execution.

Enable `service-warc` for `CommonCrawlSegmentRequest` and
`CommonCrawlSegmentAccessor`. Segment resolution reuses the WARC inventory
parser and retains only exact `segments/<segment>/warc/` matches. It preserves
manifest order and forwards existing read options to the same sequential
FileSet. A neighboring segment with a longer numeric ID is a different selection.
Construct a request from crawl/segment IDs, a segment URL, or a shared
`CommonCrawl` source. No index, batch scheduler or parallel executor is added.

Manifest byte framing uses core `read_lines`;
path validation and selected-file duplicate checks remain in the provider.

`format-warc` exposes native parser records through `WarcAccessor`. This service
composes Arrow readers through FileSet, so its `format` argument requires a
caller-provided WARC-to-Arrow accessor. Manifest-only `resolve` needs no conversion.

## CourtListener bulk snapshots

Enable `courtlistener` (`service-courtlistener` on the facade) for
`service::courtlistener::{CourtListenerBulkAccessor, CourtListenerSnapshot}`.
Configure storage at the bucket/mirror root, above `bulk-data/`. For public S3,
enable OpenDAL `services-s3` and `http-transport-reqwest`, then configure
`opendal::services::S3::default().bucket("com-courtlistener-storage")
.region("us-west-2").skip_signature()`. No API token or account is required.

```rust,no_run
use open_arrow_access::{Accessor, Result, service::courtlistener::{CourtListenerBulkAccessor, CourtListenerSnapshot}};
use open_arrow_access::storage::{OpenDalAccessor, opendal};

async fn read(operator: opendal::Operator) -> Result<()> {
    let source = CourtListenerBulkAccessor::new(OpenDalAccessor::new(operator));
    let snapshots = source.snapshots("courts").await?;
    let snapshot = CourtListenerSnapshot::new("courts", "2026-09-30")?;
    let mut reader = source.open(snapshot.object().into()).await?;
    while let Some(csv_bytes) = reader.next().await? {
        // Original CSV bytes for a PostgreSQL COPY-compatible consumer.
    }
    Ok(())
}
```

`snapshots(table)` lists `bulk-data/` nonrecursively and returns sorted, unique,
valid dated CSV exports for that exact table. It reads only object names;
listing failures propagate and no match returns an empty vector. Explicit
snapshot reads require no listing, so HTTP-only storage can also be used.
`schema_object()` identifies the companion SQL object without fetching it.
Same-date tables are not an atomic database snapshot; dates do not pin versions.

`open(ByteReadRequest)` validates the locator and forwards I/O settings through
the existing `Bzip2` decoder. Output is pull-driven CSV bytes with unknown
length, no range reads, and terminal errors. No Arrow schema, joins, REST/RECAP,
or PDF access is added. CSV quoting and NULL/empty distinctions remain intact;
the generic Arrow CSV reader does not preserve those distinctions by default.

Sources: [bulk documentation](https://wiki.free.law/c/courtlistener/help/api/bulk-data/bulk-legal-data)
and the [official exporter](https://github.com/freelawproject/courtlistener/blob/main/scripts/make_bulk_data.sh).

## Zenodo (`zenodo`)

Enable `service-zenodo` for `service::zenodo::{ZenodoService, ZenodoAccessor}`.
`ZenodoService::new()?.record(record_id).await?` resolves one published record
using `GET /api/records/{id}`. The ID is a `NonZeroU64`; use a specific record
version, not a concept DOI. The returned ID must match the request. Sandbox or
mirror origins can be selected with `ZenodoService::with_endpoint`.

The resolved accessor caches attachment membership and exposes `files()` with
key, size and checksum. It implements `ByteAccessor`: list the empty root to
obtain exact attachment keys, then `open(ByteReadRequest::new(key))` to read one.
Attachments form a flat namespace; both recursive modes list the same files.
Unknown keys fail without file I/O. Readers use the canonical record content
route through the shared OpenDAL HTTP accessor, forwarding byte read options.
Each reader is independent and can outlive the resolved record. HTTP metadata
and range support are required by that storage accessor.

Empty files lists are valid; missing,
malformed or duplicate membership fails. File bodies are fetched lazily, and
existing decompression/format accessors can wrap the record accessor. Checksums
are exposed as upstream metadata, not verified. Reader source locators refer to
the HTTP content route relative to the record's files root.

This entry point reads publicly accessible published records. Search, OAI-PMH,
metadata dumps, concept DOI resolution, restricted files and deposit/upload APIs
are outside its scope. No API credentials are loaded implicitly.
See the [Zenodo Records API](https://developers.zenodo.org/#records).

Run `cargo run --example read_zenodo --features service-zenodo -- 3551612`
to list a record, or append an exact file key to stream its contents.

## arXiv (`service-arxiv`)

Enable `service-arxiv` for `service::arxiv::{ArxivService, ArxivKind, ArxivArchive}`.
Construct `ArxivService::new(storage)` with byte storage rooted above `pdf/` and
`src/`. For the official S3 bucket, the caller configures `arxiv`, the us-east-1
region, credentials and requester-pays behavior on their OpenDAL S3 operator.
A local mirror preserving the same keys works without any network or credentials.
The service does not configure S3, query paper APIs, or require bucket listing.

`manifest(kind, Some("2410"), byte_options).await` reads the selected PDF/source
XML manifest, validates all rows, and returns matching TAR metadata in manifest
order. `None` selects all months. An absent month is a valid empty result.
Metadata
includes shard filename, month, sequence, byte size, item count, first/last arXiv
IDs and MD5 checksum. Checksums and item counts are exposed, not verified.

`stream(&shard, byte_options).await` is the preferred bulk path. It opens the
shard once and returns the incremental generic TarStream. Each entry body can
be consumed before moving to the next header, avoiding thousands of serial
header-only range GETs before the first PDF. Unknown-length sequential sources
work here. The example uses this path by default.

The optional `archive(&shard, byte_options).await` checks the advertised file length against
the manifest and opens the shared TAR session. Use its entries/open_entry APIs
to stream selected PDFs; source archives return raw entries (often nested tar.gz
payloads). Both manifest and archive opens forward byte options. Archive members
are not extracted to disk. Service-specific validation is in validation.rs,
and error construction is in error.rs.

The bulk manifests can change upstream; this API supplies source-level version
guarantees, not an atomic historical snapshot or concept-version resolver.
No API key, credential, or live network access is needed by the automated tests.
See https://github.com/arXiv/arxiv-docs/blob/develop/source/help/bulk_data_s3.md.

Run `cargo run --example read_arxiv --features service-arxiv -- testdata/arxiv 2410 1 1`
to inspect the fixture manifest and TAR and stream one PDF entry.
