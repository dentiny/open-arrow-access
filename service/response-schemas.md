# Service response schemas

This document describes the library's access responses, not the upstream HTTP
API responses. Services can return bytes, Arrow batches, or discovered object
references; they do not share one provider-wide row schema.

The implemented entries below are based on main at `3546e53`. Gutenberg and
CourtListener are documented separately as pending PRs. Recheck their linked
implementations when those interfaces change.

## Entry points

All fallible return types below are wrapped in the library's `Result<T>`.
Feature names are facade features on `open-arrow-access`.

| Provider / entry point | Feature | Successful response | Row schema comes from |
| --- | --- | --- | --- |
| Hugging Face `repository(...)` | `service-huggingface` | `HuggingFaceAccessor`; its `open(ByteReadRequest)` returns `Box<dyn ByteReader>` | No rows until a format decoder is applied |
| Hugging Face `dataset(...)` / `open(HuggingFaceDatasetRequest)` | `service-huggingface` | `Box<dyn RecordReader>` | Selected Parquet split |
| PMC `PmcAccessor::open(ReadRequest)` | `service-pmc` | `Box<dyn RecordReader>` | Fixed article metadata schema |
| PMC `PmcInventoryAccessor::open(ReadRequest)` | `service-pmc` | `Box<dyn RecordReader>` | Manifest `fileSchema` |
| Common Crawl `CommonCrawlWarcAccessor::open(...)` | `service-warc` | `Box<dyn RecordReader>` | Caller-supplied WARC-to-Arrow accessor |
| Common Crawl `CommonCrawlSegmentAccessor::open(...)` | `service-warc` | `Box<dyn RecordReader>` | Same as crawl-wide access |
| Gutenberg `GutenbergAccessor::open(GutenbergRequest)` — [PR #51](https://github.com/dentiny/open-arrow-access/pull/51) | `service-gutenberg` (pending) | `Box<dyn RecordReader>` | Fixed book schema, with selected file bytes |
| CourtListener `CourtListenerBulkAccessor::open(ByteReadRequest)` — [PR #64](https://github.com/dentiny/open-arrow-access/pull/64) | `service-courtlistener` (pending) | `Box<dyn ByteReader>` | No Arrow row schema; decoded CSV bytes |

`CommonCrawl` and `create_http_accessor` configure source/storage access; they
do not themselves produce a record response.

## Common reader and metadata shapes

[Core contracts](../core/src/reader.rs) define two response families:

```text
ByteReader
  metadata() -> &ByteMetadata
  next().await -> Result<Option<Bytes>>

ByteMetadata {
  source: Option<ObjectRef>,     // object locator, not an Arrow column
  content_length: Option<u64>,   // current byte representation's length
  version: Option<String>,      // optional version, not a snapshot guarantee
}

RecordReader
  metadata() -> &RecordMetadata
  next().await -> Result<Option<RecordBatch>>

RecordMetadata {
  schema: SchemaRef,             // fields, types, nullability, schema metadata
  source: Option<ObjectRef>,
}
```

`Some(...)` contains the next chunk/batch; `None` is EOF. Errors and EOF are
terminal. Byte chunks are nonempty and can split records or UTF-8 sequences.
Use a streaming decoder rather than interpreting each chunk as a CSV/JSON row.
Reader metadata is available after `open`; it is not repeated inside each row.

Every batch from a record reader has its declared schema. Projection selects
top-level columns in caller order; an empty projection preserves row counts
with zero columns. The schemas below show full output before projection.
`?` in schema sketches means an Arrow field is nullable; unmarked fields are
non-nullable. The JSON examples show logical row values, not a JSON response
or a serialization format supplied by this library.

## Hugging Face

### Repository files

`repository(HuggingFaceRepository)` returns a byte accessor. Its `open` returns
the selected file's original bytes, whether Parquet, JSON, or another format.
There are no service-defined columns such as `repository`, `split`, or `data`.

Through the current [OpenDAL adapter](../storage/src/backend.rs), byte metadata
contains the requested repository-relative object in `source`, the file length
in `content_length`, and `version = None`. A selected commit is not added as a
metadata field. `list(ListRequest)` returns an `ObjectStream`, i.e.
`Stream<Item = Result<ObjectRef>>`, rather than rows with file metadata.

Source: [repository accessor](src/huggingface/accessor.rs).

### Parquet datasets

The dataset entry point selects repository/revision/config/split, pins the
Parquet export revision, sorts shard paths, and reads them through FileSet.
Its schema is the first shard's projected Parquet schema, including schema
metadata. Later shards must match that schema exactly.

For example, a split containing `text: Utf8?` and `label: Int64?` emits batches
with those columns; a different dataset can have completely different fields.
No repository/config/split provenance columns are automatically appended.

Reader metadata has `source = None` because the response spans a file set.
Inspect `reader.metadata().schema` for the actual field names and types.

Sources: [dataset adapter](src/huggingface/dataset.rs),
[FileSet schema contract](../dataset/src/file_set/accessor.rs).

## PMC article-version metadata

One `ReadRequest` selects one metadata JSON object. Each valid object produces
one Arrow row, regardless of the batch-size target. Content URLs remain strings;
the service does not fetch their PDF, XML, text, or media content.

| Column | Arrow type | Nullable |
| --- | --- | --- |
| `pmcid` | `Utf8` | No |
| `version` | `UInt32` | Yes |
| `pmid` | `Utf8` | Yes |
| `doi` | `Utf8` | Yes |
| `title` | `LargeUtf8` | Yes |
| `citation` | `LargeUtf8` | Yes |
| `is_pmc_openaccess` | `Boolean` | Yes |
| `is_manuscript` | `Boolean` | Yes |
| `is_historical_ocr` | `Boolean` | Yes |
| `is_retracted` | `Boolean` | Yes |
| `license_code` | `Utf8` | Yes |
| `xml_url` | `Utf8` | Yes |
| `pdf_url` | `Utf8` | Yes |
| `media_urls` | `List<item: Utf8?>` | Yes |
| `text_url` | `Utf8` | Yes |

`pmcid` is required; `version`, when supplied, must be positive. Optional
missing/null values become null. Although the list item is nullable in the
Arrow schema, the JSON decoder accepts strings only inside `media_urls`.
Unknown JSON fields are ignored and duplicate keys use their last value.

Example logical row (all other columns are null):

```json
{
  "pmcid": "PMC123",
  "version": 2,
  "title": "Example article",
  "is_pmc_openaccess": true,
  "pdf_url": null,
  "media_urls": ["https://example.org/supplement.zip"]
}
```

Reader metadata: `source = Some(request.object)`; schema metadata:
`{"pmc.row_kind": "article_metadata"}`. The input object's source is not an
extra row column. `pmc::schema()` exposes the full schema programmatically.

Sources: [schema](src/pmc/mod.rs), [JSON conversion](src/pmc/metadata.rs).

## PMC inventory snapshots

The request selects a manifest; its `fileSchema` supplies column names and
order. **Every column is nullable `Utf8`**, including dates, sizes, flags, or
other fields that look numeric. Additional manifest columns are retained.
No article content or article metadata is fetched.

For a manifest declaring `Bucket, Key, LastModifiedDate, ETag`, the full schema is:

```text
Bucket: Utf8?
Key: Utf8?
LastModifiedDate: Utf8?
ETag: Utf8?
```

Example logical row:

```json
{
  "Bucket": "pmc-oa-opendata",
  "Key": "metadata%2FPMC123.2.json",
  "LastModifiedDate": "2026-11-02T00:00:00Z",
  "ETag": "example-etag"
}
```

Those four column names are required in the manifest. Each row must have
`Bucket = "pmc-oa-opendata"` and a nonempty `Key`, even when projected away.
`Key` is preserved as supplied, without URL decoding or interpreting it as a
read request. An empty inventory emits no batches but still declares its schema.

Reader metadata has `source = Some(manifest_object)`. Schema metadata contains:

| Key | Value |
| --- | --- |
| `pmc.inventory.manifest_uri` | Requested manifest locator |
| `pmc.inventory.creation_timestamp` | Manifest Unix epoch milliseconds, stored as a string |
| `pmc.inventory.version` | Manifest version string |

Sources: [manifest schema mapping](src/pmc_inventory/manifest.rs),
[reader metadata](src/pmc_inventory/accessor.rs),
[row validation](src/pmc_inventory/validation.rs).

## Common Crawl: crawl-wide and segment access

Both record entry points return the schema of the **caller-supplied record
accessor**. The service adds no fixed `url`, `payload`, `crawl`, `segment`, or
other columns. It reads selected WARC files in inventory order through FileSet;
the first file determines the projected schema, including metadata, and later
files must match it. Reader metadata has `source = None`.

For example, an application decoder might declare `url: Utf8?` and
`payload: LargeBinary?`. Those fields belong to that decoder and are not a
built-in Common Crawl response schema. The standalone `format-warc` accessor
returns native parser records and cannot directly satisfy this service's
`RecordReader` requirement.

`resolve(request)` returns membership without reading record contents:

```text
FileSetRequest {
  objects: Vec<ObjectRef>,       // WARC paths in inventory order
  options: ReadOptions,          // forwarded batch-size/projection settings
  byte_options: ByteReadOptions,
}
```

Example object: `crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/warc/example.warc.gz`.
Segment access changes membership, not the output schema.

Sources: [crawl-wide accessor](src/warc/accessor.rs),
[segment accessor](src/commoncrawl/segment/accessor.rs),
[FileSet response](../dataset/src/file_set/request.rs).

## Gutenberg — pending PR #51

This interface is not yet on main. The
[reviewed implementation](https://github.com/dentiny/open-arrow-access/blob/998cc82a757a2281a874f5276646be78a077fdcd/service/src/gutenberg/mod.rs)
declares one row per book RDF object, including selected related files:

```text
ebook_id: UInt64
title: LargeUtf8?
creator: List<item: Struct<
  uri: Utf8?, name: Utf8?, alias: List<item: Utf8>,
  birthdate: Int32?, deathdate: Int32?
>>
language: List<item: Utf8>
subject: List<item: Struct<scheme: Utf8?, value: Utf8>>
bookshelf: List<item: Utf8>
issued: Date32?
rights: LargeUtf8?
metadata_rdf: LargeBinary
files: LargeList<item: Struct<
  uri: Utf8, format: List<item: Utf8>, extent: UInt64?,
  modified: Utf8?, data: LargeBinary
>>
```

List containers, list items, and struct items above are non-nullable unless
marked `?`. Missing repeated metadata produces empty lists. Birth/death dates
are years; `issued` is a date stored as days since 1970-01-01. `extent` and
`modified` are metadata declarations, not storage verification results.

`metadata_rdf` contains original RDF bytes; `files[].data` contains original
selected file bytes, including their encoding/compression. Files follow request
order; an empty selection produces `files = []`. Projecting out `files` skips
file-content reads while still validating selected URIs against the RDF.

Reader metadata has `source = Some(request.metadata.object)`. The full schema
has no service-added schema metadata. `gutenberg::schema()` exposes it.

## CourtListener bulk snapshots — pending PR #64

This interface is not yet on main. The
[reviewed implementation](https://github.com/dentiny/open-arrow-access/tree/b436fa08f7513b960708cfb1b4de03f3a4b446cf/service/src/courtlistener)
returns **decoded CSV bytes, not Arrow batches**. Tables can have different
headers and types; the service defines no Arrow schema for `opinions`,
`opinion-clusters`, `courts`, or other tables. A compatible consumer must
interpret the CSV and its companion SQL schema.

The byte stream preserves headers, quoting, backslash escaping, multiline
values, unquoted NULL and quoted empty strings. A chunk is not a row. For example,
the following is illustrative concatenated output, not one guaranteed chunk:

```csv
id,title
"1","Example opinion"
"2",""
"3",
```

Reader metadata has `source = Some(selected_csv_bz2_object)` and
`content_length = None` because the decoded length is unknown. `version` is
forwarded from input metadata; with the current OpenDAL adapter it is `None`.

`snapshots(table)` returns `Vec<CourtListenerSnapshot>`, sorted/deduplicated by
date. The type exposes getters rather than public row fields:

| Getter | Response example |
| --- | --- |
| `table() -> &str` | `"opinion-clusters"` |
| `date() -> &str` | `"2026-09-30"` |
| `object() -> ObjectRef` | `bulk-data/opinion-clusters-2026-09-30.csv.bz2` |
| `schema_object() -> ObjectRef` | `bulk-data/schema-2026-09-30.sql` |

The SQL object is only identified, not downloaded or executed. Same-date tables
are separate exports, not an atomic cross-table snapshot. Discovery is not a
record response and adds no columns to the CSV stream.
