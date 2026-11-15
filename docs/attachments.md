# Decode a selected attachment

`open_attachment_source(accessor, request, options)` binds one explicitly selected
attachment to a `BoundSource`. Supply any `ByteAccessor`, including a provider's
`ResolvedFileSet` or `common::RemoteFiles`. Providers retain file membership,
authentication, metadata resolution and download policy; this entry point only
selects an existing decoder and optional outer decompression.

```rust,no_run
use open_arrow_access::{
    AttachmentFormat, AttachmentOptions, BoundSource, ByteAccessor, ReadOptions,
    ReadRequest, Result, open_attachment_source,
};

async fn read_events<A: ByteAccessor + 'static>(files: A) -> Result<BoundSource> {
    let options = AttachmentOptions::new(AttachmentFormat::Jsonl);
    options.validate()?; // Can also run before resolving provider metadata.
    open_attachment_source(
        files,
        ReadRequest::new("events.jsonl.gz").with_options(ReadOptions {
            projection: Some(vec!["id".into()]),
            ..Default::default()
        }),
        options,
    ).await
}
```

Enable `format-json` and `decompression-gzip` for this example. Read the result
with `source.reader().await?` and poll its batches as with other bound sources.
The full `ReadRequest`, including projection, batch size and byte options, is
preserved. The request's locator is interpreted by the supplied accessor; it is
not an arbitrary URL to resolve or a format to infer from the filename.

## Typed configuration

| `AttachmentFormat` | Cargo feature | Schema/interpretation | Default outer compression |
| --- | --- | --- | --- |
| `Binary` | none | One non-null `data: LargeBinary` row containing unchanged bytes | `None` |
| `Csv` | `format-csv` | Infer columns after consuming the first row as a comma-separated header | `Auto` |
| `Tsv` | `format-csv` | Same header policy with a tab delimiter | `Auto` |
| `Json` / `Jsonl` | `format-json` | The same whitespace-separated JSON object/JSON-lines decoder | `Auto` |
| `Parquet` | `format-parquet` | Embedded Parquet schema; input must support ranges and have a known length | `None` |
| `Xml { record_path }` | `format-xml` | Infer fields beneath the specified absolute record path | `Auto` |

Top-level JSON arrays/scalars are rejected, matching the existing `JsonFormat`;
the entry point does not flatten them or follow metadata links.

`AttachmentOptions::default()` selects `Binary`. Binary input includes empty
objects, invalid UTF-8 and compressed bytes: a `.gz` filename does not cause
decompression. The entire object is buffered on first consumption. An empty
projection still yields one row. Configuring binary mode with outer compression
returns `InvalidInput` rather than silently changing the original bytes.

XML configuration owns its record path, so non-XML formats have no irrelevant
path option. Empty/whitespace-only paths return `InvalidInput`. Decoder-specific
path syntax and projection validation are performed during binding. The format
variants remain available in minimal builds; a disabled format returns
`Unsupported` before byte I/O, so applications can validate typed configuration
without needing their own feature-availability dispatch.

Use `with_outer_compression(OuterCompression::None)` to disable the default text
decompression policy. `Auto` delegates to the existing suffix-based layer using
the selected logical name. Enable each needed `decompression-*` codec. A known
suffix with a disabled codec returns `Unsupported`; unknown suffixes pass through.
This does not sniff compression magic or automatically select ZIP/TAR members.
Parquet defaults to no outer layer to retain native range access. Explicit outer
decompression can remove range support; it does not add buffering/seek emulation.

Binding retains the first reader for consumption. Later `reader()` calls reopen
independent readers with the same request and require the bound schema to match.
Errors from the accessor, decompressor and decoder propagate through the shared
error type. SQL/string parsing, FFI ownership and application error presentation
belong to the consumer. For custom schemas, CSV dialects, XML namespaces or other
format settings, compose the existing typed formats directly with `FileAccessor`
or use `open_file_source` for an OpenDAL operator.
