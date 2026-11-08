# Dataset source providers

Provider-specific discovery and schema conventions live in this crate. Each provider adds its own default-off feature and facade exports.

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
small `expand=sha` metadata request (64 KiB response limit); membership and file
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

Enable `gutenberg` (`service-gutenberg` on the facade) for one per-book RDF/XML
object and its selected files as a single Arrow row. Metadata extraction reuses
`xml-arrow`, the same engine as `XmlAccessor`; Gutenberg adds the book schema,
type conversion, file selection and byte limits.

The URI must match a `pgterms:file` entry in the RDF; the object locator points
to that file in your storage. This explicit mapping supports local or object
storage mirrors without inferring paths from HTTP download URLs. Files retain
request order. An empty selection keeps the book with an empty `files` list.
Unknown or duplicate selections fail. No licence or language filtering occurs.

| Column | Arrow type |
| --- | --- |
| `ebook_id` | UInt64 |
| `title`, `rights` | nullable LargeUtf8 |
| `creator` | List of struct: nullable `uri`, `name` (Utf8), `alias` (List<Utf8>), nullable `birthdate`, `deathdate` (Int32 years) |
| `language`, `bookshelf` | List<Utf8> |
| `subject` | List of struct: nullable `scheme` (Utf8), `value` (Utf8) |
| `issued` | nullable Date32; Gutenberg release date |
| `metadata_rdf` | LargeBinary; verbatim RDF, including unexpanded metadata |
| `files` | LargeList of struct: `uri` (Utf8), `format` (List<Utf8>), nullable `extent` (UInt64), nullable `modified` (Utf8), `data` (LargeBinary) |

`data` is the original file bytes, including compression, encoding and line
endings. `extent` and `modified` preserve metadata declarations; they do not
verify that storage and RDF belong to the same snapshot. Preserve source
manifests and snapshot identity separately. Missing optional metadata becomes
null or an empty list. Malformed RDF or failed selected-file reads terminate
the reader rather than dropping the book.

`with_options` supports top-level projection in caller order, including a
zero-column row. Omitting `files` skips all file I/O; RDF and selected URI
membership are still validated. `with_byte_options` forwards I/O settings to
every open. One RDF emits one row, independently of the batch-size target.

`with_limits(GutenbergLimits)` bounds collected RDF bytes (default 4 MiB), each
selected file (256 MiB), and combined RDF/file bytes per row (1 GiB). These
limits do not bound parser allocations, upstream chunks or Arrow copies. Reads
are sequential and pull-driven; dropping a reader stops its work.

This entry point reads caller-selected per-book UTF-8 RDF and files. Catalogue
discovery, rsync, HTTP downloading, archive expansion and Lance writing remain
with the caller. It does not use Gutendex or parse arbitrary RDF graphs. See
Gutenberg's [catalogue](https://www.gutenberg.org/ebooks/offline_catalogs.html)
and [mirroring guide](https://www.gutenberg.org/help/mirroring.html) for sources.

Run the [example](../examples/read_gutenberg.rs) against the synthetic metadata
fixture, or supply a mirror root, RDF path and pairs of file URI / local path:

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

`with_manifest_limit` configures the default 4 MiB manifest byte limit. Run the
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

Manifest byte collection uses core `read_limited`. Shard iteration retains its
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
terminal decode errors. The default hard decoded metadata limit is 4 MiB;
configure it through `PmcAccessor::with_limits(PmcLimits { metadata_bytes })`.
Known lengths and streamed chunks are checked. Empty upstream byte chunks are
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

The article reader uses core `read_limited` for byte bounds and `StreamReader`
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
Selected locators are held in memory; inventory lines have a 16 KiB limit.
There is no index, batch planner or parallel execution.

Enable `service-warc` for `CommonCrawlSegmentRequest` and
`CommonCrawlSegmentAccessor`. Segment resolution reuses the WARC inventory
parser and retains only exact `segments/<segment>/warc/` matches. It preserves
manifest order and forwards existing read options to the same sequential
FileSet. A neighboring segment with a longer numeric ID is a different selection.
Construct a request from crawl/segment IDs, a segment URL, or a shared
`CommonCrawl` source. No index, batch scheduler or parallel executor is added.

Manifest byte framing and the 16 KiB per-line bound use core `read_lines`;
path validation and selected-file duplicate checks remain in the provider.

`format-warc` exposes native parser records through `WarcAccessor`. This service
composes Arrow readers through FileSet, so its `format` argument requires a
caller-provided WARC-to-Arrow accessor. Manifest-only `resolve` needs no conversion.
