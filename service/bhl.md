# BHL formats, dataset hierarchy and sharding

## Acquisition routes and formats

BHL exposes related content through three interfaces; they are not identical
snapshots or interchangeable file layouts.

| Route | Published content | Selection and access |
| --- | --- | --- |
| Smithsonian Figshare OCR export | One monthly bzip2-compressed TAR of page TXT files in the inspected version | Pin article 21422193 and version; public, no BHL API key |
| AWS Open Data | Separate page TXT, JP2 images, scandata XML and gzip-compressed metadata TSV tables | Public S3 bucket bhl-open-data, us-east-2; no AWS account required |
| BHL API / item downloads | Item OCR text, PDF and image-download links when present; API metadata | API v3 key required for the current library item route |

The page OCR is existing source text, not a new OCR request. PDF is an item
download representation, not a file inside every OCR export or AWS page prefix.
Scandata XML describes pages; it is not full-text article XML. The .txt.gz
metadata files are TSV tables, distinct from the uncompressed page .txt objects.
Keep each representation's original bytes and identity; a byte chunk, TAR
directory, item, part and page are different units.

Sources: [BHL developer/data tools](https://about.biodiversitylibrary.org/tools-and-services/developer-and-data-tools/),
[API v3](https://www.biodiversitylibrary.org/docs/api3.html),
[AWS registry](https://registry.opendata.aws/bhl-open-data/).

## Figshare: version → one TAR.bz2 → item directories → page TXT

The public [version 24 metadata](https://api.figshare.com/v2/articles/21422193/versions/24)
was inspected on 2026-11-20:

| Attribute | Observed value |
| --- | --- |
| Article/version | 21422193 / 24 |
| Publication date | 2026-11-17 |
| File count | 1 |
| Filename | bhl-ocr-20261115.tar.bz2 |
| File ID | 71555779 |
| Encoded size | 46,698,847,626 bytes (46.70 GB; 43.49 GiB) |
| Supplied MD5 | 5f98a607cd80b23e3e0dfcab4978a7bc |

These values describe one pinned version, not a permanent current filename.
The supplied MD5 was recorded, not verified against a full download. Pin the
version and original file metadata; do not discover the latest file again on
every retry. The export describes BHL-hosted material; do not assume identical
coverage to the live AWS bucket or API.

A bounded HTTP range read of the first 1,048,576 compressed bytes returned
206 with Content-Range bytes 0-1048575/46698847626. Its decoded TAR prefix
contained these actual paths:

```text
Figshare article 21422193 / version 24
└── bhl-ocr-20261115.tar.bz2               # One downloadable compressed object
    └── bhl-ocr-20261115/                  # TAR member root
        └── item-29/                      # Observed grouping directory
            └── item-295621/              # One item in the inspected prefix
                ├── item-295621-61016244-0039.txt
                ├── item-295621-61016279-0004.txt
                └── item-295621-61016246-0037.txt
```

Only a prefix was inspected, not all archive members. Do not generalize the
item-29 grouping into an undocumented partition algorithm or claim that the
whole archive is item-only without checking its inventory. The names carry
item ID, page ID and sequence; physical TAR order in the sample is not page
sequence order. Sort/associate using preserved identifiers and sequence when
reconstructing items. Two sampled page files contained only CRLF; blank OCR is
still a source record and must not be silently dropped.

**No native multi-file sharding was found in Figshare version 24.** The item
directories are inside one compressed object. They do not allow independent
range reads of decoded pages through an ordinary sequential bzip2 reader.
Neither a TAR header index over compressed offsets nor more reader concurrency
alone creates independent input shards.

For parallel jobs on this route, decode once into a frozen page-file inventory
or explicitly repack into independently readable derived shards. Record derived
membership and original member paths. Do not make every worker re-decompress
the entire 46.70 GB object to select its own directory. Such staging/repacking
is application work, not an existing BHL service option.

## AWS: independent page objects plus image, XML and metadata prefixes

The [official bucket README](https://bhl-open-data.s3.amazonaws.com/README.html)
documents this different, unarchived layout. Names in brackets are upstream
templates, not sampled objects from the Figshare TAR:

```text
s3://bhl-open-data/                          # Public; region us-east-2
├── ocr/
│   ├── item-[ItemID]/
│   │   ├── item-[ItemID]-[PageID]-0001.txt  # Page OCR/plain text
│   │   └── item-[ItemID]-[PageID]-0002.txt
│   └── part-[PartID]/
│       ├── part-[PartID]-[PageID]-0001.txt  # Part-specific page OCR
│       └── part-[PartID]-[PageID]-0002.txt
├── images/
│   └── [BarCode]/
│       ├── [BarCode]_0001.jp2              # JPEG 2000 page image
│       └── [BarCode]_0002.jp2
├── scandata/
│   └── [BarCode]_scandata.xml              # Page order/type/eligibility
└── data/                                    # Gzip-compressed TSV tables
    ├── title.txt.gz
    ├── item.txt.gz
    ├── page.txt.gz
    ├── part.txt.gz
    ├── creator.txt.gz
    ├── subject.txt.gz
    ├── titleidentifier.txt.gz
    ├── partidentifier.txt.gz
    ├── partcreator.txt.gz
    ├── creatoridentifier.txt.gz
    ├── pagename.txt.gz
    └── doi.txt.gz
```

The official [special cases](https://github.com/gbhl/bhl-open-data/blob/main/NOTES.md)
give real examples and explain why item/part discovery must use metadata:

- Item 346951, page 65077706, sequence 14 has OCR key
  ocr/item-346951/item-346951-65077706-0014.txt and image key
  images/CAT109916943238207426/CAT109916943238207426_0014.jp2.
- Part 248 has no separate part OCR: use its parent item 21356's page objects
  (for example ocr/item-021356/item-021356-02839616-0127.txt).
- Some standalone parts instead have part-prefixed OCR and no corresponding
  item-prefixed OCR. Inspect BarCode and relationships; never request both
  guessed paths and interpret one failure as missing text.

The README describes minimum padding of six digits for item/part IDs and eight
for page IDs. Preserve actual inventory spelling; do not truncate longer IDs.
Sequence is separate from PageID. Scandata leafNum is not the object sequence:
only pages with addToAccessFormats=true participate. Use the documented mapping,
not equality of leafNum and sequence.

**AWS has native independent page objects, but no documented numbered dataset
shard manifest or immutable release.** Item/part prefixes are organization,
not balanced shards. An application must freeze object membership, group pages
into disjoint work units and retain source identity/checksums where available.
An item-grouped or byte-balanced page manifest is a proposed application plan,
not an upstream sharding guarantee. Define ownership before combining item and
part views so the same source page is not counted twice accidentally.

The README says OCR/images/scandata update weekly and metadata tables monthly.
The bucket is live: metadata and content fetched at different times can differ.
Pin the acquisition window and reconcile available IDs, pages and payloads
before treating this as a reproducible dataset. AWS coverage must be compared
against the Figshare export before substituting this route for task #3984.

## API/item downloads: logical file names are not bucket paths

```text
BHL API v3 GetItemMetadata(id=<ItemID>)
├── Result[].ItemTextUrl    → library logical file "text"    → item OCR bytes
├── Result[].ItemPDFUrl     → library logical file "pdf"     → PDF bytes, if any
└── Result[].ItemImagesUrl  → library logical file "images"  → advertised image download
```

These names are aliases created by the library resolver, not literal
/text/, /pdf/ or /images/ object keys. Follow the advertised URLs and inspect
their actual container/MIME type; the images download is not proof of one JP2
object or a particular ZIP layout. Missing optional links do not remove the
metadata record. JSON/API metadata, item text and page OCR are separate outputs;
the resolver does not automatically download every page or join them to tables.

## Parallelism and conversion boundaries

| Input | Native independent read unit | Additional planning |
| --- | --- | --- |
| Figshare v24 export | One TAR.bz2 object | Sequential decode/member discovery, then staged or derived shards for independent jobs |
| AWS OCR/images/XML | One object | Frozen inventory, ID joins, item/part ownership and application job groups |
| AWS metadata TSV.gz | One compressed table file | Explicit gzip + TSV decoding and reconciliation; not page-text shards |
| API item | One advertised attachment | Enumerate item scope and handle rate limits/missing links |
| Arrow batch / Lance fragment | Reader/output artifact | Not an upstream shard or a snapshot boundary |

Keep complete upstream metadata and rights fields. Figshare article-level
metadata/license does not replace per-item rights from BHL tables/API. Record
missing payloads, blank pages, failed reads and unresolved joins explicitly.
No new OCR, language/license filtering, normalization or indexing is implied.

## Proposed Arrow / Lance schema: one OCR page per row

**Design proposal, not implemented output.** The current binary attachment
route emits one `data: LargeBinary` value for an entire selected attachment.
The page schema below is the intended contract for a future OCR decoder and
the resulting Lance dataset; adding this documentation enables no job option
and does not migrate existing outputs.

| Column | Arrow type | Nullable | Meaning |
| --- | --- | --- | --- |
| `entity_type` | `Utf8` | No | `item` or `part`, identifying the source OCR namespace |
| `entity_id` | `UInt64` | No | ItemID for item OCR, or PartID for part OCR |
| `page_id` | `UInt64` | No | BHL PageID, distinct from page sequence |
| `sequence` | `UInt64` | No | Page sequence within the selected item/part namespace |
| `data` | `LargeBinary` | No | Complete original page TXT bytes, including empty/blank content and original line endings |
| `source_path` | `LargeUtf8` | No | Original TAR member path or full AWS object URI |
| `snapshot_id` | `Utf8` | No | Explicit source snapshot identity, e.g. `figshare-21422193-v24`; AWS uses a caller-assigned frozen-inventory ID |
| `source_entry_index` | `UInt64` | Yes | Zero-based physical TAR header-entry ordinal, including directory headers in the count; null for individual AWS objects |
| `metadata` | `Struct<...>` | Yes | Complete joined source metadata for the page and its related item/part/title, including rights fields |

The eight previously proposed content/identity columns are accompanied by
`source_entry_index` to preserve repeated member names in an archive.
The sampled page `item-295621-61016244-0039.txt` maps to `entity_type=item`,
`entity_id=295621`, `page_id=61016244`, `sequence=39` and `data=0D 0A`.
Its physical member ordinal is 3 in the inspected TAR prefix; its logical
sequence is 39. Numeric IDs are for joins; `source_path` retains original
zero-padding and spelling.

Use `(snapshot_id, entity_type, entity_id, page_id, sequence)` as a candidate
logical page key, **subject to full-inventory uniqueness validation**.
It is not a verified upstream primary key, nor a uniqueness constraint
automatically enforced by Lance. Keep physical provenance as well:
`(snapshot_id, source_path, source_entry_index)` distinguishes repeated TAR
members; AWS provenance uses the frozen inventory plus object URI.
Report repeated logical keys rather than silently dropping rows. Different
snapshots/routes can contain the same PageID and still represent separate
source records.

`data` is binary so ingestion need not decode, normalize, strip headers or
re-encode OCR. No derived `text` column is proposed here. Missing payloads
are accounted failures, not fabricated empty-byte rows. Actual empty/blank
source files remain valid non-null payloads. An unparseable identifier must
be reported explicitly rather than coerced to zero.

The exact children of `metadata` must be specified after inspecting the
selected upstream TSV/API schemas. `Struct<...>` deliberately is not a
complete Arrow schema ready to construct today. Preserve original field names
and all fields/values using typed nested structs/lists for related records;
do not replace metadata with a title/license-only projection. Keep complete
source tables/API responses and join provenance with the snapshot. A missing
join leaves the corresponding metadata nullable and produces an explicit
gap report; it must not drop the OCR page. Article-level Figshare metadata
belongs with the snapshot inventory and cannot supply missing per-item rights.

PDF, JP2 and scandata XML have different row granularity and do not become
extra OCR page rows. If ingested, use separate representation tables: item PDF
keyed by snapshot/entity identity; page JP2 keyed by snapshot plus its
validated BarCode/sequence-to-PageID mapping; scandata XML keyed by snapshot
and BarCode. Preserve original bytes and source locators there too.
Those asset schemas and any parent-item relationship for a part require
their own explicit contract; `entity_id` must not be assumed to be a parent
ItemID when `entity_type=part`.

## Current open-arrow-access support

Enable facade feature `service-bhl`. Existing BHL methods resolve metadata and
lazy original byte downloads; they do not implement a page-per-row OCR decoder.

| Capability | Current behavior |
| --- | --- |
| `bulk_export(Some(version))` | Delegates to Figshare article 21422193; returns original filenames and complete parsed article metadata, no BHL key |
| `get(operation, parameters)` | API v3 JSON metadata, key required |
| `item(id)` | Resolves optional text/pdf/images links for one item |
| AWS object access | Caller-configured OpenDAL S3/HTTP storage can read selected objects; not a BHL AWS corpus resolver |
| Generic format/planning tools | Reusable byte, decompression, archive and explicit file-set machinery; not automatic BHL inventory/joins or derived sharding |

`RemoteFiles` listing enumerates attachments, not pages inside an archive.
`bulk_export` returning one attachment therefore does not expose every TAR member
as a native scan partition. Shared TAR support does not make the original
TAR.bz2 independently range-readable after decompression.

Before claiming complete ingestion support, define a bounded page-record decoder
or a frozen AWS page inventory, preserve source path/IDs/order and complete
metadata, and integrate failure/count reconciliation. Generic original-byte
access is sufficient for preserving a selected small object, not for extracting
a large export into page rows. A whole-file binary Arrow value would buffer the
full attachment; avoid treating this 46.70 GB export as one such row.

Implementation references: [BHL service](src/bhl/mod.rs),
[item resolver](src/bhl/resolver.rs), [Figshare](figshare.md),
[archive/decompression tools](../decompression/README.md),
[parallel reading](../docs/parallel-read.md).

## Inspection evidence and limits

The version-24 public metadata and first 1 MiB of compressed payload were read
on 2026-11-20; 40 TAR header entries in the decoded prefix were inspected.
This establishes the displayed sampled paths and blank pages, not corpus-wide
counts, checksum validity, format coverage or sustained throughput. AWS hierarchy
and item/part exceptions were checked against official documentation; the AWS
corpus was not enumerated or compared to the export. No new Rust code, runtime
test, production conversion or deployment is included.

## Existing library API

## Service and data

The Biodiversity Heritage Library provides digitized biodiversity literature,
bibliographic metadata and item-level text/PDF/image links. It also publishes a
Smithsonian OCR export on Figshare.

Enable facade feature `service-bhl`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

### Request and response

| Library request | Upstream request | Library response |
| --- | --- | --- |
| `bulk_export(version: Option<NonZeroU32>)` | Figshare article `21422193`, optional explicit version | `RemoteFiles<ApiDocument>`; no BHL key needed |
| `get(operation, parameters)` | `/api3?op=…&format=json&apikey=…` | `ApiDocument` with `Status` and `Result` |
| `item(id: NonZeroU64)` | `GetItemMetadata` for one item | `RemoteFiles<ApiDocument>` |

`BhlOperation` covers title/item/page/part metadata, collections and statistics.
Use `new(Some(key))` for API v3 calls or `new(None)` for the public bulk export.
For custom endpoints or transports, use
`BhlService::with_services(connection, figshare).with_key(key)?` to reuse API key
validation and query authentication. This replaces any earlier `apikey` value
while retaining both configured connections, including the public Figshare export.
Item files are named `text`, `pdf`, `images` when their URLs are available.
Bulk-export files retain Figshare filenames. Both paths expose sequential original
bytes and complete parsed metadata; they do not produce Arrow rows.

Illustrative upstream API item shape (values are examples;
additional fields may be present):

```json
{
  "Status": "ok",
  "Result": [
    {
      "ItemID": 42,
      "ItemTextUrl": "https://files.example.org/item.txt",
      "ItemPDFUrl": null
    }
  ]
}
```

### Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{Accessor, ByteAccessor, Result, service::bhl::BhlService};

async fn inspect_ocr_export(version: std::num::NonZeroU32) -> Result<()> {
    let files = BhlService::new(None)?.bulk_export(Some(version)).await?;
    println!("export metadata: {}", files.metadata().value());
    let mut names = files.list("".into()).await?;
    if let Some(name) = names.try_next().await? {
        println!("file: {}", name.locator());
        let mut reader = files.open(name.into()).await?;
        if let Some(chunk) = reader.next().await? {
            println!("first chunk: {} bytes", chunk.len());
        }
    }
    Ok(())
}
```

The example selects a fixed export version and prints its metadata and one
file's first chunk. To read a specific API item instead, construct with a BHL key
and call `item(id)`; optional missing PDF/text links do not remove the metadata.

Sources: [BHL API v3](https://www.biodiversitylibrary.org/docs/api3.html), [Figshare guide](figshare.md).


