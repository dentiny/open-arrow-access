# Hugging Face storage

`storage-huggingface` is default-off. It forwards to the storage crate's
`huggingface` feature and reuses OpenDAL `0.59.2`'s Hugging Face service and the
shared byte/range adapter. There is no separate Hub HTTP client or protocol parser.

```rust,no_run
use open_arrow_access::{
    Accessor, ByteAccessor, ReadRequest, Result,
    file_format::ParquetAccessor,
    storage::{HuggingFaceAccessor, HuggingFaceBuilder},
};

async fn read() -> Result<()> {
    let builder = HuggingFaceBuilder::default()
        .repo_type("dataset")
        .repo_id("owner/example")
        .revision("main")
        .root("data")
        .download_mode("http");
    let files = HuggingFaceAccessor::new(builder).await?;
    let _objects = files.list("".into()).await?;
    let parquet = ParquetAccessor::new(files);
    let mut reader = parquet.open(ReadRequest::new("train.parquet")).await?;
    while let Some(batch) = reader.next().await? {
        // Consume ordered Arrow batches.
        let _ = batch;
    }
    Ok(())
}
```

Enable `format-parquet` as well for this example. The reexported builder is
OpenDAL's `Hf` builder: use its `token`, `endpoint`, `repo_type`, `revision`, `root`,
`download_mode` and optional resolve-cache settings directly. Its implicit token
and endpoint environment conventions apply. Initialization runs on Tokio's
blocking pool because the SDK may read a cached token file. No remote scan starts
until `open`, `list`, or reader consumption is requested.

Object locators are relative to the configured root. Empty, absolute, dot/empty
component paths, backslashes, NULs and surrounding whitespace are rejected for
file opens rather than being normalized to another object. Directory listing
accepts an empty root or a relative directory with an optional trailing slash.
It recursively streams files in the Hub's paginated discovery order and filters
directory entries. This order is not a reproducible dataset order. A missing
prefix fails during its metadata check; a file prefix is invalid input.

`ByteReadOptions` retains per-open I/O concurrency and chunk splitting. Sequential
output chunk size is independently configurable with `with_chunk_size`. Ranges
use the existing bounded/coalesced adapter and do not change the sequential
cursor. Sequential reads retain one OpenDAL stream across output chunks, so
emitting smaller chunks does not create one remote request per output chunk.
Storage I/O chunk size and concurrency control that stream's reads and staging.
Reader lengths refer to stored files; reader capabilities do not claim
listing. Inputs must remain unchanged during a read. Pin a repository commit for
stable selection; a floating branch such as `main` is not an atomic snapshot,
and the adapter does not invent an object version.

This backend accesses repository files; it does not execute Hugging Face dataset
scripts or interpret viewer configuration/split metadata. The existing
`ml-infra` dataset-viewer resolver remains a reference for that separate service
extension. Source dataset semantics can be composed above these byte contracts.
External decompression is composed separately.

Tests inject an OpenDAL `HttpTransport` and exercise the real HF SDK without
contacting the public Hub or using account credentials. HTTP fixtures cover
revision/root selection, authentication forwarding, paginated file discovery,
exact ranges, option splitting, failure categories, and Parquet composition.
Xet decoding and remote redirect behavior remain owned by OpenDAL/hf-xet; this
adapter does not independently reimplement or integration-test those protocols.
