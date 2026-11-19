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
annotation array into a JSON string column. The mapping below is the proposed
typed output contract; the current byte provider does not implement it yet.
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

All nested children below are nullable in the proposed schema. Keep
`openaccessinfo`, `body` and `bibliography` as typed structs, or explicitly
normalize their children to columns such as `openaccessinfo_license`,
`body_text` and `bibliography_text`. Either representation preserves types;
do not keep both duplicate representations by default.

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
`Accessor::list` API. Export a valid `S2_API_KEY` before running. It resolves
`latest` once and lists all synthetic member names from that pinned manifest;
it does not download shard objects, submit jobs, or print signed URLs.

```toml
[dependencies]
open-arrow-access = { version = "0.10.3", default-features = false, features = ["service-semantic-scholar"] }
futures = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use futures::TryStreamExt;
use open_arrow_access::{
    Accessor,
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
above. There is no S2ORC v2-specific typed reader, PDF downloader, all-shard job
scheduler, or automatic diff application in this provider.

For conversion-service task inputs and remaining integration work, see the
[ml-infra source guide](https://github.com/Adcock-AGI-Inc/ml-infra/blob/main/lance/lance_migration/docs/sources/semantic-scholar.md).

## Official references

- [Datasets API](https://api.semanticscholar.org/api-docs/datasets)
- [Pinned release used for the dated descriptions](https://api.semanticscholar.org/datasets/v1/release/2026-11-10)
- [S2ORC project (bulk API access instructions)](https://github.com/allenai/s2orc)
- [API tutorial](https://www.semanticscholar.org/product/api/tutorial)

Preserve dataset-specific license and attribution information from the release
README alongside the data.
