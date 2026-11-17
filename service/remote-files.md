# Resolved provider files

## Service and data

`ResolvedFileSet<A, M>` binds an ordered logical-name namespace to a byte
accessor and provider metadata. `common::RemoteFiles<M>` is the sequential HTTP
specialization used by many providers. Neither is a separate upstream service.

Available without a provider feature. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

`Connection::files(Vec<MetadataLink>, metadata)` accepts entries shaped as
`MetadataLink { file_name: String, url: String }`. It validates URLs and nonempty,
unique logical names, then returns `RemoteFiles<M>` without file-body I/O.
`metadata() -> &M` retains the caller/provider's metadata.

`list(ListRequest::new(""))` emits cached names in insertion order for either
recursion mode. Names are opaque, not filesystem paths; only an empty listing prefix
is accepted. `open(ByteReadRequest::new(name))` selects an exact name and
forwards byte options. HTTP readers are sequential; other `ResolvedFileSet`
backends can retain range support. Unknown names fail before download I/O.

## Usage and results

```rust
use open_arrow_access::{
    Accessor, Result,
    service::common::{Connection, MetadataLink},
};

async fn read_named_file(url: &str) -> Result<()> {
    let files = Connection::new("https://example.org")?.files(
        vec![MetadataLink {
            file_name: "paper.pdf".into(),
            url: url.into(),
        }],
        "selected-version",
    )?;
    println!("metadata: {}", files.metadata());
    let mut reader = files.open("paper.pdf".into()).await?;
    if let Some(chunk) = reader.next().await? {
        println!("original PDF chunk: {} bytes", chunk.len());
    }
    Ok(())
}
```

Given an actual HTTP(S) PDF URL, the example binds a logical filename, retains
its version label and reads a byte chunk. The supplied name does not change or
infer the content's format. See [response shapes](response-schemas.md) for typed
provider metadata versus parsed `ApiDocument` metadata.

## Multi-attachment record scans

`ResolvedFileSet::records(format)` binds one explicit format to the resolved
namespace. Select logical names with `service::FileSetRequest`; the service
module also exports `FileSetAccessor` for callers that need to name the type.
These exports are available through the facade without a direct dataset-crate
dependency. Enable the chosen format, such as `format-json`.

```rust
use open_arrow_access::{
    ReadOptions, Reader, Result, ScanPlanner,
    file_format::json::JsonFormat,
    service::{FileSetRequest, common::RemoteFiles},
};

async fn scan_selected_files(files: RemoteFiles, names: Vec<String>) -> Result<usize> {
    let records = files
        .records(JsonFormat::infer())
        .with_union_by_name()
        .with_file_planning();
    let request = FileSetRequest::new(names).with_options(ReadOptions {
        projection: Some(vec!["id".into()]),
        ..Default::default()
    });
    let mut reader = records.plan_scan(request).await?.into_reader();
    let mut rows = 0;
    while let Some(batch) = reader.next().await? {
        rows += batch.num_rows();
    }
    Ok(rows)
}
```

For names `["b.json", "a.json", "b.json"]`, the scan retains that order and reads
the repeated attachment twice. Names are manifest keys, not arbitrary download
URLs. An empty selection is invalid; unknown names return `NotFound` when opened.

Exact projected-schema matching is the default. Opting into `with_union_by_name()`
inspects every selected file before emitting rows, merges compatible schemas by
name and fills absent columns with null. Projection is resolved against the union
and its requested column order is restored for each batch. Incompatible types or
metadata still fail rather than being silently coerced.

`with_file_planning()` combines a format's native partitions with the selected
file order; use it only when the format supports scan planning. It does not start
parallel work itself. Apply the existing `ParallelReadLayer` to overlap independent
partitions within one executor budget; see [parallel reading](../docs/parallel-read.md).
Without union-by-name inspection, later file opens/plans remain lazy. Each open
owns its reader cursor, and these record bindings do not re-fetch provider metadata
or discover more attachments.
