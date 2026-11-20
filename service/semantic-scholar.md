# Semantic Scholar bulk datasets

## Service and data

Semantic Scholar distributes scholarly datasets, including releases such as
S2ORC v2, with metadata and partition download URLs. This provider resolves a release
and dataset or retrieves an incremental update/deletion manifest.

Enable facade feature `service-semantic-scholar`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Release → dataset → shard → paper record

Semantic Scholar publishes snapshots containing several related datasets. S2ORC v2
is parsed scholarly full text; it is one dataset in a release, not the whole
Academic Graph and not a bulk archive of original PDF files.

```text
Datasets API
└── release/<release_id>                      # One dated snapshot
    ├── dataset/papers                       # Paper metadata
    ├── dataset/abstracts                    # Abstracts where available
    └── dataset/s2orc_v2                     # Parsed full-text dataset
        └── files[]                          # Signed download URL per shard
            ├── files[0] → gzip JSONL object
            │   ├── JSON object → one paper's parsed full text
            │   ├── JSON object → another paper
            │   └── ...
            └── files[1] → another gzip JSONL object
```

The API response is the manifest. Each `files[]` entry is a URL string, not a
paper record, directory, TAR member, or a metadata index that must be expanded
into one request per paper. Downloading a shard already retrieves many papers'
parsed content. By comparison, a PMC inventory CSV lists separate metadata
objects to fetch, while a Common Crawl segment groups many WARC files.

Use the published file list rather than constructing S3 paths. The API does not
document an assignment rule by paper ID, subject, journal or publication date.
Do not assume equal row counts, stable ordering, or matching shard numbers
between datasets or releases. Join related paper datasets by their corpus ID,
not by file position or row number.

## Dataset names, record columns and Arrow types

`papers`, `abstracts` and `s2orc_v2` are **separate dataset names** accepted by
`/release/<release_id>/dataset/<name>`. They are not Arrow types or fields
inside an S2ORC v2 record. Selecting `s2orc_v2` does not also download `papers`
or `abstracts`. A later metadata join would read those datasets separately.

| Dataset name (not a column/type) | Record contents |
| --- | --- |
| `papers` | One paper's graph metadata; no full-body text. It has its own record schema, outside this v2 ingestion contract. |
| `abstracts` | One paper's abstract where available; a separate record schema, outside this v2 ingestion contract. |
| `s2orc_v2` | One paper's parsed full text and metadata, with the six top-level fields listed below. |

### Evidence and storage contract

The following field inventory was checked against **all 20 records** in the
[official public v2 sample](https://s3-us-west-2.amazonaws.com/ai2-s2ag/samples/s2orc_v2/s2orc_v2-sample.jsonl.gz)
on **2026-11-19**, together with its
[README](https://s3-us-west-2.amazonaws.com/ai2-s2ag/samples/s2orc_v2/README.txt).
The compressed sample's SHA-256 is
`4833173b3dea488e71cd8450c5c2e2a86fc8cd4b63afb35d78c51dbb517d4c47`.
It is a public sample, not a verified sample from our pinned full release.
The README is descriptive rather than a complete type/nullability contract;
in particular its `externalIds` spelling differs from the sample's
`externalids`. Use exact observed field names and validate the full release.

**JSONL is the upstream transport format, not the intended Arrow storage type.**
Known scalar fields become typed columns; nested records become Arrow
`Struct` and arrays become `List`. Do not serialize a known record, struct or
annotation array into a JSON string column. The mapping below is the typed
output contract implemented by `S2orcV2Accessor`; the byte provider itself still
returns original bytes.
Nullable output fields accommodate missing upstream values; validate required
paper identity. Sample observations alone do not establish full-release
nullability or rule out additional fields.

### Top-level columns: one paper per row

| Column | Observed upstream value | Proposed Arrow type | Meaning |
| --- | --- | --- | --- |
| `corpusid` | Integer in all 20 records | `UInt64`, required | Paper identity; preserve the lowercase spelling. |
| `openaccessinfo` | Object | `Struct`, nullable | Open-access metadata; children below. |
| `title` | String | `LargeUtf8`, nullable | Paper title. |
| `authors` | Array of strings | `List<Utf8>`, nullable | Author names, not author-ID objects; retain list order. |
| `body` | Object | `Struct<text: LargeUtf8, annotations: Struct<...>>`, nullable | Full-body text and typed annotation lists. |
| `bibliography` | Object | `Struct<text: LargeUtf8, annotations: Struct<...>>`, nullable | Bibliographic text and typed annotation lists. |

All nested children below are nullable in the fixed reader schema. The default
reader keeps `openaccessinfo`, `body` and `bibliography` as typed structs.
The optional [body-text extraction profile](../README.md#s2orc-v2-output-schema)
moves `body.text` into a single top-level `text: LargeUtf8` field, removes the
original nested text, and preserves `body.annotations` and `bibliography`.
It requires the extraction API from [PR #588](https://github.com/dentiny/open-arrow-access/pull/588);
published 0.12.0 retains the default six-column schema.

After extraction the ordered top-level columns are `corpusid`, `openaccessinfo`,
`title`, `authors`, `body`, `bibliography`, `text`. The transformed `body` type is
`Struct<annotations: Struct<...>>`; the bibliography still contains its own
`text` and annotations. Null parents produce null top-level text. The text is
not rewritten, so body annotation offsets still refer to the same contents now
stored in `text`. Use the transformed accessor for both metadata/schema
inspection and reads. LZ4 belongs to the downstream writer's per-column policy;
it does not turn the Arrow string into a binary value.

### Open-access struct children

| Field path | Proposed Arrow type | Observed meaning/type |
| --- | --- | --- |
| `openaccessinfo.disclaimer` | `LargeUtf8` | Human-readable provenance/copyright statement; string. |
| `openaccessinfo.license` | `Utf8` | License label; string or null. |
| `openaccessinfo.url` | `Utf8` | Source paper URL; string, not downloaded PDF bytes. |
| `openaccessinfo.status` | `Utf8` | Open-access status such as `GOLD`; string. |
| `openaccessinfo.externalids` | `Struct<Medline: Utf8, MAG: Utf8, ACL: Utf8, DOI: Utf8, MedRxiv: Utf8, PubMedCentral: Utf8, ArXiv: Utf8>` | External catalog identifiers. |

The sample's `Medline`, `MAG`, `DOI` and `PubMedCentral` values are strings
or null. Keep identifiers as strings even when they contain only digits.
`ACL`, `MedRxiv` and `ArXiv` are present but null in every sample record:
their proposed `Utf8` type is an identifier-storage choice, not an observed
non-null type. Confirm non-null values against the full-release contract before
finalizing the reader. Do not infer Arrow `Null` as their permanent type.

### Text and annotation struct children

`body.text` and `bibliography.text` map to `LargeUtf8`. Each annotation
field listed below maps to a nullable `List<Span>` after decoding its payload.

| Field path | Public sample evidence |
| --- | --- |
| `body.annotations.sentence` | JSON-encoded array string or null; start/end are decimal strings inside populated arrays. |
| `body.annotations.paragraph` | JSON-encoded array string; integer start/end. |
| `body.annotations.section_header` | JSON-encoded array string or null; integer start/end; optional section-number attribute `n`. |
| `body.annotations.bib_ref` | JSON-encoded array string; integer start/end; optional `ref_id` and `matched_paper_id`. |
| `bibliography.annotations.bib_entry` | JSON-encoded array string; integer start/end; attributes include `id` and optional `matched_paper_id`. |
| `bibliography.annotations.bib_title` | JSON-encoded array string or null; integer start/end. |
| `bibliography.annotations.bib_venue` | JSON-encoded array string; integer start/end. |
| `bibliography.annotations.bib_id` | Null in all 20 records; non-null payload schema remains unverified. |
| `bibliography.annotations.bib_author_first_name` | Null in all 20 records; non-null payload schema remains unverified. |
| `bibliography.annotations.bib_author_last_name` | Null in all 20 records; non-null payload schema remains unverified. |

The shared target `Span` is an Arrow struct, not a JSON string:

```text
Span: Struct<
  start: UInt64,
  end: UInt64,
  attributes: Struct<
    n: Utf8,
    id: Utf8,
    ref_id: Utf8,
    matched_paper_id: UInt64
  >
>
```

`start` and `end` are required within a populated span. `attributes` and its
children are nullable; they cover the union of attribute keys observed in this
sample. Parse decimal-string offsets strictly into integers and preserve
integer offsets. Preserve null versus empty list, span order, null attributes,
and missing attribute values. The three all-null annotation fields provisionally
use `List<Span>` in this design, but their non-null payloads must be verified
before that schema is treated as complete.

This requires two decoding steps: parse the outer JSONL paper record, then parse
each non-null annotation string into the typed span list. Validate offsets
against the corresponding unchanged text using the upstream offset convention;
do not silently reinterpret character offsets as UTF-8 byte offsets.

Unknown fields or annotation attribute shapes must produce an explicit schema
validation failure until their typed mapping is added. Do not silently drop
them, infer a permanent type from all-null samples, or fall back to whole-record
`raw_json`. A complete typed schema must be validated on actual release data
before production ingestion.

## Full-text identity

Corpus ID identifies a paper in the graph; it is not a shard ID or a PMC
article-version identifier. Preserve dataset/release/shard provenance separately.
The same paper can appear in different releases and may overlap with our PMC
or arXiv inputs. Conversion alone does not deduplicate those corpora.

Parsed text contains neither original PDF bytes nor page images. Fetching a
paper URL is a separate operation and may have different availability from the
parsed text. Converting S2ORC v2 does not collect the original PDFs.

## Obtaining the shard list

1. Read `GET /datasets/v1/release/latest` (or a selected dated release).
2. Save the returned `release_id`; use that date for every task in the run.
3. With `x-api-key`, read
   `GET /datasets/v1/release/<release_id>/dataset/s2orc_v2`.
4. Enumerate the returned `files[]`. Each member is an independent download
   and a natural conversion task input.

The following Rust example uses the existing `SemanticScholarService` and
`ByteAccessor::list` API. Export a valid `S2_API_KEY` before running. It resolves
`latest` once and lists all synthetic member names from that pinned manifest;
it does not download shard objects, submit jobs, or print signed URLs.

```toml
[dependencies]
open-arrow-access = { version = "0.12.0", default-features = false, features = ["service-semantic-scholar"] }
futures = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use futures::TryStreamExt;
use open_arrow_access::{
    ByteAccessor,
    service::semantic_scholar::SemanticScholarService,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key = std::env::var("S2_API_KEY")?;
    let dataset = "s2orc_v2";
    // Resolves latest once, then fetches this release's dataset manifest.
    let files = SemanticScholarService::new(&key)?
        .dataset("latest", dataset)
        .await?;
    let release_id = &files.metadata().release_id;
    let parts = files
        .list("".into())
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    println!(
        "release={release_id} dataset={dataset} shards={}",
        parts.len()
    );
    for part in parts {
        // Synthetic member name only; never print the key or signed URLs.
        println!("{release_id}/{dataset}/{}", part.locator());
    }
    Ok(())
}
```

API access and object downloads are separate steps. The key authenticates the
dataset manifest request; the returned URLs carry the download authorization.
Signed URLs expire: fetch fresh links for the pinned release when necessary.
Do not persist expiring URLs as the only durable identity for queued tasks.
Record the exact member mapping (for example, underlying object path without
signature parameters) and verify it when refreshing URLs; the index is only a
position in that resolved manifest. Archive source objects and checksums if
long-term replay must survive upstream availability changes.

On **2026-11-19**, the public `latest` response resolved to **2026-11-10**.
Its `s2orc_v2` description advertised approximately 16M records in 30 × 6GB files.
These are upstream README descriptions, not measured counts or a guarantee
about a later release. The authenticated `files[]` is authoritative for actual
task membership. Exact per-shard rows and decoded bytes require reading the
objects; do not divide 16M by 30 to assign an exact row count to each task.
No production key or authenticated full-data manifest was used to verify these
descriptive counts for this documentation.

## Sharding choices

| Approach | How tasks are formed | Tradeoff |
| --- | --- | --- |
| Reuse upstream shards | One manifest member per job, with a distinct output destination. | First choice: independent downloads and retries without a preprocessing pass. |
| Group upstream shards | Resolve an explicit list of members into one job. | Fewer jobs, larger retry scope; requires a consumer that accepts multiple members. |
| Create smaller local shards | Stream-decompress each input and rotate outputs only between complete JSONL records. | Additional preprocessing and storage; useful when upstream files are too large or more tasks are needed. |

### File hierarchy: reuse upstream shards

The source hierarchy below is **logical manifest membership**, not a public S3
directory to construct or list. `part-00000` is our provider's synthetic name
for `files[0]`; the actual gzip object is reached through its signed URL.
Each source object contains many paper records.

```text
release/2026-11-10/
└── dataset/s2orc_v2/
    └── files[]                             # Authenticated API manifest
        ├── [0] → part-00000 → gzip object   # Task 0 input
        │   ├── JSONL paper record
        │   ├── JSONL paper record
        │   └── ...
        ├── [1] → part-00001 → gzip object   # Task 1 input
        │   └── many JSONL paper records
        └── ...

oci://pretrain-dataset/s2orc_v2/2026-11-10/
├── part-00000.lance/                        # Task 0 destination dataset
│   ├── <dataset metadata>
│   └── <writer-generated data files>
├── part-00001.lance/                        # Task 1 destination dataset
│   ├── <dataset metadata>
│   └── <writer-generated data files>
└── ...
```

One input gzip shard maps to one task and one independent Lance dataset. A
`.lance/` destination is a dataset directory/object prefix, not one physical
file. Its internal data files are controlled by the writer; they do not inherit
the source shard count.

### File hierarchy: group upstream shards

Grouping changes the task's selected membership, not the upstream files:

```text
Selected source members                     Destination datasets
├── group-00000: files[0], files[1]       →   .../group-00000.lance/
│   ├── part-00000 → gzip object             ├── <dataset metadata>
│   └── part-00001 → gzip object             └── <writer-generated data files>
└── group-00001: files[2], files[3]       →   .../group-00001.lance/
    ├── part-00002 → gzip object             ├── <dataset metadata>
    └── part-00003 → gzip object             └── <writer-generated data files>
```

`group-00000` is an illustrative planner label, not an upstream directory or
a currently supported single-file service selector. A consumer must accept an
explicit multi-member selection to execute this plan; the current conversion
URI selects only one `file_name`. Give each selected input member to exactly
one group for disjoint full coverage.

### File hierarchy: create smaller derived shards

A preprocessing pass can mirror and stream-decompress each upstream object,
then write smaller gzip JSONL files at complete record boundaries. The paths
below are a **proposed local layout**, not upstream object names or directories
automatically created by the provider.

```text
/mnt/datasets/s2orc_v2/2026-11-10/
├── original/
│   ├── part-00000.jsonl.gz                  # Mirror of source files[0]
│   └── part-00001.jsonl.gz                  # Mirror of source files[1]
└── derived/
    ├── part-00000/
    │   ├── chunk-00000.jsonl.gz             # First complete record range
    │   ├── chunk-00001.jsonl.gz             # Next disjoint record range
    │   └── ...
    └── part-00001/
        ├── chunk-00000.jsonl.gz
        └── ...

oci://pretrain-dataset/s2orc_v2/2026-11-10/derived/
├── part-00000/
│   ├── chunk-00000.lance/                  # One task per derived input file
│   ├── chunk-00001.lance/
│   └── ...
└── part-00001/
    ├── chunk-00000.lance/
    └── ...
```

Only the derived files are inputs to this conversion plan; do not also convert
their original mirror files into the same logical corpus. Record the mapping
from each derived path to its original member and complete record range.
Recompression creates independently decodable files; it does not make arbitrary
byte ranges of the original gzip independently readable.

For derived shards, persist the source release/object, record boundaries, output
membership, counts and checksums. A hash partition on corpus ID is another
possible downstream scheme, but still requires reading the source and writing
the partitions. Sampling is not disjoint sharding or complete coverage.

A gzip object cannot generally be split at arbitrary compressed byte offsets
and independently decoded. The current provider exposes sequential bytes, not
a gzip index or record-range API. Parallelize across source objects first.
Reader concurrency, number of conversion jobs and Lance output file rotation
are separate controls. An upstream input shard is not a Lance fragment.

## Request and response

| Library request | Upstream GET | Library response |
| --- | --- | --- |
| `release(id)` | `/datasets/v1/release/{id}` | Release `ApiDocument` |
| `dataset(release, dataset)` | Release lookup, then `/datasets/v1/release/{resolved_id}/dataset/{dataset}` | `RemoteFiles<DatasetMetadata>` |
| `diffs(dataset, start, end)` | `/datasets/v1/diffs/{start}/to/{end}/{dataset}` | Diff `ApiDocument` |

Configure an explicit API key with `new(key)`. `DatasetMetadata` contains
`release_id`, `release` and `dataset` documents. Dataset metadata advertises a
`files` array of signed URLs; names become `part-00000`, `part-00001`, etc.
Downloads retain signatures and emit sequential bytes in the published format.
`"latest"` is resolved to one release ID before downloading. Signed URLs can
expire; resolve the pinned release again for fresh links. Diff lists are returned
unchanged; the library does not apply updates/deletions to your dataset.

Illustrative upstream dataset manifest shape (values are examples;
additional fields may be present):

```json
{
  "name": "s2orc_v2",
  "README": "Dataset-specific schema and license information",
  "files": [
    "https://files.example.org/part?signature=example"
  ]
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, ByteAccessor, Result, service::semantic_scholar::SemanticScholarService,
};

async fn inspect_dataset(key: &str, release: &str, dataset: &str) -> Result<()> {
    let files = SemanticScholarService::new(key)?
        .dataset(release, dataset)
        .await?;
    println!("release: {}", files.metadata().release_id);
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

For dataset `s2orc_v2`, the example reports the resolved release and a partition
name, then reads its first original chunk. Apply gzip/JSON decoding according to
the selected dataset's own schema; no fixed Arrow columns are added here.

Source: [Semantic Scholar Datasets API](https://api.semanticscholar.org/api-docs/datasets).

## Custom connections

Use `SemanticScholarService::with_connection(connection).with_key(key)?` when
configuring a custom endpoint, transport or HTTP policy. This reuses the same
key validation and authentication header as `new(key)`, preserves other
connection settings, and replaces any previously configured API key.

## Typed S2ORC v2 records

`S2orcV2Accessor::new(decompressed_bytes)` reads one paper per Arrow row with
`s2orc_v2_schema()`. Nested metadata stays in structs; author names are a list,
and JSON-encoded annotation arrays are decoded to `List<Struct>`. There is no
whole-record JSON string column. The byte accessor supplied here must already
expose uncompressed JSONL; the provider download interface still returns original
bytes.

The reader validates all input columns before projection, preserves null versus
empty annotation lists, and implements `ScanPlanner` using the shared JSON
framer. `with_partition_options` configures the soft decoded-byte chunk target;
a single complete record may exceed it. Use `ParallelReadLayer` to execute
planned chunks concurrently with bounded read-ahead; opening the reader directly
remains sequential. No whole shard is buffered or reserialized to JSON.

### Schema validation

The typed reader uses a fixed schema even when early records contain only nulls.
Like the generic JSON reader, it rejects numeric/boolean values in string fields.
Unknown fields at every nested object level and unknown span or
attribute fields fail decoding before output projection. Corpus IDs must be
positive UInt64 values. Span offsets accept unsigned integers or digit-only
strings, reject overflow/reversed ranges, and require text for populated lists.
Offsets are preserved without guessing a byte/character coordinate conversion.

The same explicit span/attribute schema is used for every annotation kind,
including fields that were all-null in the public sample. Synthetic tests exercise
non-null values in every declared field; this is not evidence that every future
upstream payload has that shape. Unknown shapes fail closed and need an explicit
typed schema update, never a JSON-string fallback.

## Decoding and current limitations

`SemanticScholarService::dataset` returns a byte accessor. Its synthetic
`part-00000` names are assigned from `files[]` positions; they are not upstream
filenames and have no `.gz` suffix. The existing Rust example reads original
compressed bytes, not decoded paper rows.

When composing with an attachment JSONL reader, use an explicit gzip
decompression policy (with gzip and JSON features enabled), or an adapter that
preserves a correct compressed logical filename. Automatic suffix routing alone
does not decompress `part-00000`: it does not sniff gzip magic bytes. Adding
`format=jsonl` in a consumer is not sufficient to solve this naming mismatch.

The generic JSON reader infers a fixed schema from a bounded sample and rejects
later unknown fields, including nested ones. A consumer requiring every upstream
field must use an explicit validated Arrow schema, including typed nested
structs/lists and secondary decoding of annotation payload strings as described
above. `S2orcV2Accessor` provides that typed decoding over already-decompressed
JSONL; validating the schema and annotation semantics against full-release data
remains a separate step. This provider has no PDF downloader, all-shard job
scheduler, or automatic diff application.

For conversion-service task inputs and remaining integration work, see the
[ml-infra source guide](https://github.com/Adcock-AGI-Inc/ml-infra/blob/main/lance/lance_migration/docs/sources/semantic-scholar.md).

## Official references

- [Datasets API](https://api.semanticscholar.org/api-docs/datasets)
- [Pinned release used for the dated descriptions](https://api.semanticscholar.org/datasets/v1/release/2026-11-10)
- [S2ORC project (bulk API access instructions)](https://github.com/allenai/s2orc)
- [API tutorial](https://www.semanticscholar.org/product/api/tutorial)

Preserve dataset-specific license and attribution information from the release
README alongside the data.
