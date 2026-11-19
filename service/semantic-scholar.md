# Semantic Scholar bulk datasets

## Service and data

Semantic Scholar distributes scholarly datasets, including releases such as
S2ORC, with metadata and partition download URLs. This provider resolves a release
and dataset or retrieves an incremental update/deletion manifest.

Enable facade feature `service-semantic-scholar`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

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
