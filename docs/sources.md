# Unified source resolution

`source::{SourceRegistry, SourceRequest}` is always available on the facade.
This is an application-facing adapter over the existing typed storage, decoder,
and provider APIs. It does not change those contracts.

```toml
[dependencies]
open-arrow-access = { path = "/path/to/open-arrow-access", features = ["format-json", "service-huggingface"] }
opendal = { version = "=0.59.2", default-features = false, features = ["services-fs", "services-http", "http-transport-reqwest"] }
```

The registry requires a version containing this API; it is not available
in the published 0.1.0 release yet. Backend features remain on the application's
OpenDAL dependency, as with the typed accessors.

```rust,no_run
use open_arrow_access::{Result, source::{SourceRegistry, SourceRequest}};

async fn read() -> Result<()> {
    let mut request = SourceRequest::new("events.jsonl");
    request.options.projection = Some(vec!["text".into()]);
    let mut reader = SourceRegistry::default().open(request).await?;
    println!("{:?}", reader.metadata().schema);
    while let Some(batch) = reader.next().await? {
        println!("{} rows", batch.num_rows());
    }
    Ok(())
}
```

## Storage and formats

The default registry resolves local paths, `file://` URLs, HTTP/HTTPS file URLs,
and other enabled OpenDAL object-storage URI schemes. A URI names one object;
its directory is not silently scanned. Use `storage_options` for backend options
such as S3 region, endpoint, and credentials. An explicit file URI determines the
filesystem root; it cannot be combined with a storage `root` override.

File suffixes select enabled Parquet, CSV, TSV, JSON/JSONL/NDJSON, XML, ATIF, or
WARC readers. Set `request.format` explicitly for absent/ambiguous suffixes.
ATIF `.json` and `.jsonl` files need explicit `atif` or `atif-jsonl` selection.
JSON is a stream of objects, not a top-level array; WARC emits physical records
with unmodified binary bodies. This adapter never invents a text representation
for PDF or other undecoded bytes.

`format_options["header"]` accepts `true`/`false` for CSV/TSV; otherwise bounded
header/schema inference is used. XML requires `format_options["record_path"]`.
Other unrecognized format options fail rather than being ignored. Read and byte
options are forwarded to existing readers, preserving batching and projection.

Final `.gz`, `.bz2`, `.lz4`, `.zst`, and `.zstd` suffixes select enabled streaming
codecs. These compression suffixes are case-sensitive; externally compressed
Parquet still fails its range requirements. ZIP/TAR member selection, directory
or glob discovery, and HTTP query/signed URLs require an application handler or
the typed APIs; this initial adapter does not guess their selection semantics.

## Hugging Face

With `service-huggingface`, the `hf` handler accepts
`hf://datasets/<owner>/<repository>`:

```rust,no_run
use open_arrow_access::{Result, source::{SourceRegistry, SourceRequest}};

async fn dataset() -> Result<()> {
    let mut request = SourceRequest::new("hf://datasets/lhoestq/demo1");
    request.service_options.insert("config".into(), "default".into());
    request.service_options.insert("split".into(), "train".into());
    let mut reader = SourceRegistry::default().open(request).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} rows", batch.num_rows());
    }
    Ok(())
}
```

Dataset mode delegates to `HuggingFaceService::dataset`, including export commit
pinning, shard discovery/order, schema checks, and lazy shard reads. Optional
`revision` selects the Parquet export revision (default `refs/convert/parquet`);
`allow_partial=true` opts into partial exports. Dataset mode already supplies
Arrow records, so it rejects a file-format override.

For a repository file, set `service_options["path"]` and optionally a source
`revision` (default `main`), then select/infer the file format. File mode cannot
be combined with dataset config/split/partial settings. Shared settings include
`endpoint` and `token`; environment/cached-token resolution is unchanged.

## Register another provider or decoder

`SourceRegistry::empty()` starts without handlers. `register_source` maps a
scheme/name to an `Arc<dyn SourceHandler>`. Set `SourceRequest.source` to select a
handler explicitly when the provider uses an opaque identifier instead of a URI.
A handler returns either:

- `SourceInput::Bytes`: original bytes with an appropriate source locator in
  reader metadata; the registry selects a decoder and optional compression.
- `SourceInput::Records`: a native Arrow reader; file-format overrides fail.

`register_format` adds an `Arc<dyn FormatHandler>` plus suffix aliases. Its
`decode` method receives the opened bytes and the complete source request;
`request.read_request()` constructs the shared read request for a factory.

This boundary allows services with different typed requests or output shapes to
share one application entry point without pretending every provider is a file
or every API response is tabular. Other provider-specific handlers must be
registered explicitly. The default registry currently includes only Hugging Face
among the service providers. No existing provider is silently wrapped or fetched.
