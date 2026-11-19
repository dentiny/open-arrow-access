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

## Full-text formats and identity

This guide covers the `s2orc_v2` full-text dataset. Select that dataset explicitly
when obtaining the release manifest.

| Dataset | Principal documented content |
| --- | --- |
| `papers` | Core paper metadata; not full-body text. |
| `abstracts` | Abstract text where available; not full-body text. |
| `s2orc_v2` | `openaccessinfo`, `title`, `authors`, and separate `body` and `bibliography`, each with text and annotation spans. |

A decompressed shard is JSONL: each line is a JSON object. Text and annotation
spans describe the parsed document; annotations are not separate paper records.
Preserve the original text when retaining span offsets. Do not flatten away
bibliography, annotation or open-access fields when complete upstream metadata
is required. Consult the selected release's dataset README and actual records
for field names, types and optionality; this table is not a complete Arrow schema.

Corpus ID identifies a paper in the graph; it is not a shard ID or a PMC
article-version identifier. Preserve dataset/release/shard provenance separately.
The same paper can appear in different releases and may overlap with our PMC
or arXiv inputs. Conversion alone does not deduplicate those corpora.

Parsed JSON contains neither original PDF bytes nor page images. Fetching a
paper URL is a separate operation and may have different availability from the
parsed text. Do not assume that converting S2ORC v2 satisfies a raw-PDF collection
requirement.

## Obtaining the shard list

1. Read `GET /datasets/v1/release/latest` (or a selected dated release).
2. Save the returned `release_id`; use that date for every task in the run.
3. With `x-api-key`, read
   `GET /datasets/v1/release/<release_id>/dataset/s2orc_v2`.
4. Enumerate the returned `files[]`. Each member is an independent download
   and a natural conversion task input.

The following Python example uses only the standard library. It lists task
identities without printing the API key or signed download URLs. Export a valid
`S2_API_KEY` before running; no file downloads or jobs are started.

```python
import json
import os
from urllib.request import Request, urlopen

base = "https://api.semanticscholar.org/datasets/v1"
key = os.environ["S2_API_KEY"]
dataset = "s2orc_v2"

def get_json(path, authenticated=False):
    headers = {"x-api-key": key} if authenticated else {}
    with urlopen(Request(base + path, headers=headers), timeout=60) as response:
        return json.load(response)

release = get_json("/release/latest")["release_id"]
manifest = get_json(f"/release/{release}/dataset/{dataset}", authenticated=True)
files = manifest["files"]
print(f"release={release} dataset={dataset} shards={len(files)}")
for index, signed_url in enumerate(files):
    # Keep the index-to-object mapping in a controlled run manifest.
    # signed_url is the temporary download location; do not log it.
    print(f"{release}/{dataset}/part-{index:05}")
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
field should validate a complete schema or preserve each complete JSON record
alongside extracted columns. There is no S2ORC v2-specific typed Arrow schema,
automatic raw-JSON column, PDF downloader, all-shard job scheduler, or automatic
diff application in this provider.

For conversion-service task inputs and remaining integration work, see the
[ml-infra source guide](https://github.com/Adcock-AGI-Inc/ml-infra/blob/main/lance/lance_migration/docs/sources/semantic-scholar.md).

## Official references

- [Datasets API](https://api.semanticscholar.org/api-docs/datasets)
- [Pinned release used for the dated descriptions](https://api.semanticscholar.org/datasets/v1/release/2026-11-10)
- [S2ORC project (bulk API access instructions)](https://github.com/allenai/s2orc)
- [API tutorial](https://www.semanticscholar.org/product/api/tutorial)

Preserve dataset-specific license and attribution information from the release
README alongside the data.
