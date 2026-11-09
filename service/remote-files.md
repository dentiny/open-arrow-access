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
