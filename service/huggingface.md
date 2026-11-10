# Hugging Face datasets

## Service and data

The Hugging Face Hub stores dataset repositories containing files such as
Parquet, JSON and CSV. The library provides both repository byte access and
Arrow reading of a dataset's published Parquet config/split export.

Enable facade feature `service-huggingface`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

| Library request | Input format | Library response |
| --- | --- | --- |
| `repository(HuggingFaceRepository)` | `repository: String`, `revision: String` (default `main`), `root: String` | `HuggingFaceAccessor`; repository-relative `ByteReadRequest` opens original bytes |
| `dataset(HuggingFaceDatasetRequest)` / `open(request)` | repository, config, split; export revision defaults to `refs/convert/parquet`; read/byte options | `Box<dyn RecordReader>` |

`dataset` pins the export revision to a commit, lists/sorts Parquet shard paths
and reads them lazily in order. The first projected shard defines the Arrow
schema; later shards must match it. There are no added repository/split columns;
record metadata has `source = None`. A repository file reader instead exposes
byte length/source and ranges through OpenDAL.

`plan_scan` exposes the native Parquet row groups across all selected shards.
Install `ParallelReadLayer` to execute them with one shared partition budget,
retaining shard path order and row-group order. Later shard footers are inspected
lazily during partition discovery; ordinary `dataset`/`open` stays sequential.

`with_revision` on a dataset request selects the Parquet export revision, not an
original source revision. Missing complete exports fail unless
`with_partial_exports()` is enabled; full exports take precedence. Constructor
options support explicit endpoint/token overrides. HF environment/cached-token
settings are also consulted; use `HF_HUB_DISABLE_IMPLICIT_TOKEN` when appropriate.
Repository reads of a floating revision are not pinned automatically.

## Usage and results

```rust
use open_arrow_access::{
    Result,
    service::huggingface::{HuggingFaceDatasetRequest, HuggingFaceOptions, HuggingFaceService},
};

async fn read_split() -> Result<()> {
    let service = HuggingFaceService::new(HuggingFaceOptions::default()).await?;
    let mut reader = service
        .dataset(HuggingFaceDatasetRequest::new(
            "lhoestq/demo1",
            "default",
            "train",
        ))
        .await?;
    println!("schema: {:?}", reader.metadata().schema);
    if let Some(batch) = reader.next().await? {
        println!(
            "first batch: {} rows, {} columns",
            batch.num_rows(),
            batch.num_columns()
        );
    }
    Ok(())
}
```

The example prints the actual export schema and first Arrow batch dimensions.
Field names/values depend on the dataset; inspect the schema rather than assuming
all Hub datasets have a `text` column. For raw files, use `repository` then
`open(ByteReadRequest::new("path/inside/repository"))` instead.

Source: [Hugging Face dataset repositories](https://huggingface.co/docs/hub/datasets).
