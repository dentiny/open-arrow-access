# BHL AWS Open Data: formats, page schema and metadata

BHL support reads the AWS Open Data source: OCR pages, images, scandata and
metadata TSV tables. It requires no BHL API key.

## AWS: independent page objects plus image, XML and metadata prefixes

The [official bucket README](https://bhl-open-data.s3.amazonaws.com/README.html)
documents the object layout below. Names in brackets are upstream templates:

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
    ├── partpage.txt.gz                       # Confirmed by live metadata listing
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
before treating this as a reproducible dataset.

## Parallelism and conversion boundaries

| Input | Native independent read unit | Additional planning |
| --- | --- | --- |
| AWS OCR/images/XML | One object | Frozen inventory, ID joins, item/part ownership and application job groups |
| AWS metadata TSV.gz | One compressed table file | Explicit gzip + TSV decoding and reconciliation; not page-text shards |
| Arrow batch / Lance fragment | Reader/output artifact | Not an upstream shard or a snapshot boundary |

Keep complete upstream metadata and item/part rights from the AWS TSV tables. Record
missing payloads, blank pages, failed reads and unresolved joins explicitly.
No new OCR, language/license filtering, normalization or indexing is implied.

## Fixed Arrow / Lance page schema and value assignment

`page_schema()` defines the fixed output types below. `BhlPageRecord::into_batch()`
assigns every caller-supplied value to its column, including the supplied
metadata StructArray. Metadata is required and must contain one populated row
matching the fixed 13-table / 104-field schema; null or mismatched metadata fails.
BHL includes metadata acquisition for the 13 gzip TSV tables
and uses embedded DuckDB SQL to populate this schema for selected pages. See
the [DuckDB metadata guide](bhl-metadata.md) for usage and the in-memory V1 assumption.
The page-record API also accepts caller-provided metadata matching this schema.

| Column | Arrow type | Nullable | Meaning |
| --- | --- | --- | --- |
| `entity_type` | `Utf8` | No | `item` or `part`, identifying the source OCR namespace |
| `entity_id` | `UInt64` | No | ItemID for item OCR, or PartID for part OCR |
| `page_id` | `UInt64` | No | BHL PageID, distinct from page sequence |
| `sequence` | `UInt64` | No | Page sequence within the selected item/part namespace |
| `data` | `LargeBinary` | No | Complete original page TXT bytes, including empty/blank content and original line endings |
| `source_path` | `LargeUtf8` | No | Full AWS object URI |
| `snapshot_id` | `Utf8` | No | Caller-assigned frozen AWS inventory ID |
| `source_entry_index` | `UInt64` | Yes | Retained schema field; null for individual AWS objects |
| `metadata` | `Struct` ([complete definition below](#metadata-child-fields-tsv-profile)) | No | 13 declared TSV-derived children covering page/item/part/title, authors, identifiers, subjects, names and rights |

Numeric IDs are for joins; `source_path` retains original zero-padding and
spelling. Use `(snapshot_id, entity_type, entity_id, page_id, sequence)` as a
candidate logical page key, subject to full-inventory uniqueness validation.
It is not a verified upstream primary key or a uniqueness constraint enforced
by Lance. Preserve the frozen inventory and original object URI as provenance.
Report repeated logical keys rather than silently dropping rows. Different
snapshots can contain the same PageID and still represent separate source records.

`data` is binary so ingestion need not decode, normalize, strip headers or
re-encode OCR. No derived `text` column is proposed here. Missing payloads
are accounted failures, not fabricated empty-byte rows. Actual empty/blank
source files remain valid non-null payloads. An unparseable identifier must
be reported explicitly rather than coerced to zero.

### Metadata child fields: TSV profile

This fixed schema uses the **13 TSV tables actually listed under AWS data/**.
All 104 source columns are enumerated below,
with their original case-sensitive names. Table names become the struct/list
children. `?` means nullable; list containers and list elements are non-null
when their enclosing `metadata` struct is present.

Every source TSV cell uses `LargeUtf8`, including IDs, dates and years.
This preserves original lexical values, empty strings and date precision.
It is an explicit string-valued source profile, not inferred numeric/date
types. The separate top-level `entity_id`, `page_id` and `sequence` are
validated `UInt64` join/ordering columns. No JSON-encoded whole-record blob
or unlisted extension struct replaces these declared columns.

```text
metadata: Struct<
  title: Struct<
    TitleID: LargeUtf8,
    MARCBibID: LargeUtf8,
    MARCLeader: LargeUtf8,
    FullTitle: LargeUtf8,
    ShortTitle: LargeUtf8,
    PublicationDetails: LargeUtf8,
    CallNumber: LargeUtf8,
    StartYear: LargeUtf8,
    EndYear: LargeUtf8,
    LanguageCode: LargeUtf8,
    TL2Author: LargeUtf8,
    TitleURL: LargeUtf8,
    CreationDate: LargeUtf8
  >?,
  item: Struct<
    ItemID: LargeUtf8,
    TitleID: LargeUtf8,
    ThumbnailPageID: LargeUtf8,
    BarCode: LargeUtf8,
    MARCItemID: LargeUtf8,
    CallNumber: LargeUtf8,
    VolumeInfo: LargeUtf8,
    ItemURL: LargeUtf8,
    ItemTextURL: LargeUtf8,
    ItemPDFURL: LargeUtf8,
    ItemImagesURL: LargeUtf8,
    LocalID: LargeUtf8,
    Year: LargeUtf8,
    InstitutionName: LargeUtf8,
    ZQuery: LargeUtf8,
    CreationDate: LargeUtf8,
    CopyrightStatus: LargeUtf8,
    RightsStatement: LargeUtf8,
    LicenseType: LargeUtf8,
    RightsHolder: LargeUtf8
  >?,
  page: Struct<
    PageID: LargeUtf8,
    ItemID: LargeUtf8,
    SequenceOrder: LargeUtf8,
    Year: LargeUtf8,
    Volume: LargeUtf8,
    Issue: LargeUtf8,
    PagePrefix: LargeUtf8,
    PageNumber: LargeUtf8,
    PageTypeName: LargeUtf8,
    CreationDate: LargeUtf8
  >?,
  part: Struct<
    PartID: LargeUtf8,
    ItemID: LargeUtf8,
    ContributorName: LargeUtf8,
    SequenceOrder: LargeUtf8,
    SegmentType: LargeUtf8,
    Title: LargeUtf8,
    ContainerTitle: LargeUtf8,
    PublicationDetails: LargeUtf8,
    Volume: LargeUtf8,
    Series: LargeUtf8,
    Issue: LargeUtf8,
    Date: LargeUtf8,
    PageRange: LargeUtf8,
    StartPageID: LargeUtf8,
    LanguageName: LargeUtf8,
    SegmentUrl: LargeUtf8,
    ExternalUrl: LargeUtf8,
    DownloadUrl: LargeUtf8,
    RightsStatus: LargeUtf8,
    RightsStatement: LargeUtf8,
    LicenseName: LargeUtf8,
    LicenseUrl: LargeUtf8,
    RightsHolder: LargeUtf8,
    BarCode: LargeUtf8
  >?,
  partpage: List<Struct<
    PartID: LargeUtf8,
    PageID: LargeUtf8,
    ItemID: LargeUtf8,
    SequenceOrder: LargeUtf8,
    CreationDate: LargeUtf8
  >>,
  creator: List<Struct<
    TitleID: LargeUtf8,
    CreatorID: LargeUtf8,
    CreatorType: LargeUtf8,
    CreatorName: LargeUtf8,
    CreationDate: LargeUtf8
  >>,
  creatoridentifier: List<Struct<
    CreatorID: LargeUtf8,
    IdentifierName: LargeUtf8,
    IdentifierValue: LargeUtf8,
    CreationDate: LargeUtf8
  >>,
  subject: List<Struct<
    TitleID: LargeUtf8,
    Subject: LargeUtf8,
    CreationDate: LargeUtf8
  >>,
  titleidentifier: List<Struct<
    TitleID: LargeUtf8,
    IdentifierName: LargeUtf8,
    IdentifierValue: LargeUtf8,
    CreationDate: LargeUtf8
  >>,
  partcreator: List<Struct<
    PartID: LargeUtf8,
    CreatorID: LargeUtf8,
    CreatorName: LargeUtf8,
    CreationDate: LargeUtf8
  >>,
  partidentifier: List<Struct<
    PartID: LargeUtf8,
    IdentifierName: LargeUtf8,
    IdentifierValue: LargeUtf8,
    CreationDate: LargeUtf8
  >>,
  pagename: List<Struct<
    NameBankID: LargeUtf8,
    NameConfirmed: LargeUtf8,
    PageID: LargeUtf8,
    CreationDate: LargeUtf8
  >>,
  doi: List<Struct<
    EntityType: LargeUtf8,
    EntityID: LargeUtf8,
    DOI: LargeUtf8,
    CreationDate: LargeUtf8
  >>
>
```

#### Join scope and cardinality

| Child | Rows attached to the OCR page |
| --- | --- |
| `page` | `page.PageID = page_id`; at most one source row |
| `item` | For item OCR, `item.ItemID = entity_id`; for part OCR, resolve the parent using `part.ItemID` and compare the page/partpage relationship |
| `title` | `title.TitleID = item.TitleID` for the resolved item |
| `part` | For part OCR only, `part.PartID = entity_id`; null for an item-namespace row |
| `partpage` | For part OCR, all rows matching the selected PartID and PageID; empty for an item-namespace row |
| `creator` | All creator rows for the resolved TitleID |
| `creatoridentifier` | All rows whose CreatorID occurs in the selected `creator` or `partcreator` rows |
| `subject` | All subject rows for the resolved TitleID |
| `titleidentifier` | All identifier rows for the resolved TitleID |
| `partcreator` | All creator rows for the selected PartID; empty for item OCR |
| `partidentifier` | All identifier rows for the selected PartID; empty for item OCR |
| `pagename` | All scientific-name rows for the PageID; `NameConfirmed` is the upstream name value |
| `doi` | All DOI rows matching the related Title, Item, Page and, where selected, Part by both EntityType and EntityID |

Use item page.SequenceOrder for the item view and the matching
partpage.SequenceOrder for a part view; part.SequenceOrder orders the part
itself and must not substitute for its page sequence. Cross-check these with
the source filename, preserving and reporting disagreements.

Preserve all matching relation rows and their source order in lists, including
duplicates. Do not use an arbitrary first creator, first subject or first
identifier. Duplicate matches for a supposedly single-row title/item/page/part
join are an explicit schema/data validation failure, not permission to pick one.
A missing join leaves that child null and is recorded in the reconciliation
report; it never drops the OCR page. A valid empty relationship yields an empty
list. Failed table acquisition must not be reported as an empty relationship.

The concrete rights fields are `item.CopyrightStatus`, `RightsStatement`,
`LicenseType`, `RightsHolder`, plus `part.RightsStatus`,
`RightsStatement`, `LicenseName`, `LicenseUrl`, `RightsHolder` when a part
is selected. Retain both levels without silently choosing or rewriting a
license. These fields, author names, publication details, identifiers and
scientific names are all present in the declared schema.

#### Metadata provenance and schema changes

The headers were read on 2026-11-20 using one bounded 65,536-byte gzip range
per table; no whole-table scan or join was run. The inspected objects reported
Last-Modified 2026-06-06. The advertised update cadence does not establish the
contents' actual freshness.

Freeze metadata objects separately with keys, ETags/checksums where available,
observed timestamps, table headers and source-row provenance. Associate that
manifest with the OCR snapshot. Preserve the complete original source tables
alongside the inventory so unmatched records remain available; successful page
joins do not constitute preservation of every row in a source table.

A changed/new header requires an explicit schema revision before ingestion;
do not silently discard extra columns or infer new types from a sample.
The page-record API requires populated metadata matching the fixed schema.
Missing metadata must be resolved before writing a page record.

JP2 images and scandata XML have different row granularity and do not become
extra OCR page rows. Separate asset schemas must preserve original bytes and
locators, with a validated BarCode/sequence-to-PageID mapping for JP2 and snapshot/
BarCode identity for scandata. A part's entity_id is not its parent ItemID.

## Current open-arrow-access support

Enable facade feature `service-bhl`. BHL source access is limited to AWS Open Data.

| Capability | Current behavior |
| --- | --- |
| `open_data_bucket_operator()` | Anonymous, bucket-root S3 storage for bhl-open-data in us-east-2 |
| `BhlOpenData::discover(ListRequest)` | Scoped object discovery, without downloading bodies |
| `BhlOpenData::partition(keys)` | Explicit independent object membership for native file-set planning |
| `BhlPageAccessor` | Read OCR pages with supplied metadata (`new`) or DuckDB-joined metadata (`from_metadata`) |
| `BhlPageRecord::into_batch()` | Assign and validate one complete page record, including metadata |

Automatic corpus inventory grouping remains separate work. BHL includes
[DuckDB TSV metadata joins](bhl-metadata.md).
See the [AWS implementation guide](bhl-open-data.md) for usage, source boundaries
and offline validation. Caller-bound storage retains the AWS key layout and lets
unit tests use local fixtures without live S3 access.

Implementation references: [AWS objects](src/bhl/open_data.rs),
[page reader](src/bhl/pages/accessor.rs), [fixed page schema](src/bhl/page/mod.rs),
and [parallel reading](../docs/parallel-read.md).
