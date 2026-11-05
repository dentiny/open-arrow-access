# Common Crawl segments

Enable the default-off `service-commoncrawl` feature. A segment is a selected set
of WARC files, not a WARC continuation record. For example:

```text
https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/
```

`CommonCrawlSegmentRequest::from_url` resolves that selection against the official
origin-root-relative inventory `crawl-data/CC-MAIN-2022-40/warc.paths.gz` and
selects paths under the exact `crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/warc/`
prefix. It does not scrape a directory listing or assume a particular file count.
`new(crawl, segment)` selects the public Common Crawl origin; `from_url` also
accepts HTTP(S) mirror origins. URL queries, fragments, credentials and path
aliases are rejected.

## Composition

`CommonCrawlSegmentAccessor<M, F>` takes:

- A `ByteAccessor` exposing **decoded manifest bytes**. For the official gzip
  inventory, explicitly compose the gzip decompression layer over storage.
- A record accessor accepting `ReadRequest`, normally a WARC format accessor
  over gzip-decoded file bytes. Storage, compression and format remain separate.

Both accessors interpret `crawl-data/...` locators from the same origin root.
`create_http_accessor(request.origin())` configures OpenDAL HTTP without reading
any objects. The `storage-opendal` feature also exposes `OpenDalAccessor::new`
and the OpenDAL SDK for callers supplying their own configured operator,
transport, retry or concurrency policies. Generic operator listing uses prefix
semantics and supports virtual directories without directory-marker objects;
listing does not require `stat`. Filesystem-specific directory validation remains
on `FsAccessor`. Opening byte readers still requires `read` and `stat`. This
read interface exposes no writes.
HTTP does not advertise directory listing; the inventory supplies discovery.

After the separate gzip and WARC PRs are merged, the complete composition is:

```rust,ignore
use open_arrow_access::{Accessor, AccessorExt, Result};
use open_arrow_access::decompression::{DecompressionLayer, Gzip};
use open_arrow_access::file_format::WarcAccessor;
use open_arrow_access::service::{
    CommonCrawlSegmentAccessor, CommonCrawlSegmentRequest, create_http_accessor,
};

async fn read_segment() -> Result<()> {
    let request = CommonCrawlSegmentRequest::from_url(
        "https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-40/segments/1664030338280.51/",
    )?;
    let bytes = create_http_accessor(request.origin())?;
    let decoded = bytes.layer(DecompressionLayer::new(Gzip));
    let records = WarcAccessor::new(decoded.clone());
    let source = CommonCrawlSegmentAccessor::new(decoded, records);
    let mut reader = source.open(request).await?;
    while let Some(batch) = reader.next().await? {
        // Consume Arrow batches.
        let _ = batch;
    }
    Ok(())
}
```

The integration requires `service-commoncrawl`, `decompression-gzip` and
`format-warc`. This PR is stacked on directory/FileSet support and does not copy
those separate codec or format implementations. `resolve(request).await` can
also return the `FileSetRequest` without opening record files; a format value
of `()` is sufficient when using resolution alone.

## Manifest contract

Manifest bytes are read incrementally with at most 16 KiB per line, including
line terminators. Selected locators are retained in memory, proportional to the
segment's file count. Blank lines are ignored. LF, CRLF and a final line without
a newline are accepted.

Entries must be canonical ASCII paths beginning `crawl-data/`; alphanumeric
characters, `/`, `-`, `_` and `.` are accepted. Absolute URLs, leading slashes,
whitespace, escaped/percent-encoded paths, empty components, dot components,
query strings and fragments are rejected rather than normalized. Recognized
suffixes are `.warc`, `.warc.gz`, `.warc.bz2`, `.warc.zst`, `.warc.zstd` and
`.warc.lz4`. The configured file accessor remains responsible for the selected
compression; suffix recognition does not automatically choose a decoder.
Invalid paths anywhere in the inventory fail resolution. Duplicate selected
paths are errors. Empty manifests and segments with no matching files are errors.

The provider preserves manifest file order and exact canonical path spelling.
It does not sort, deduplicate or filter WARC record types. Membership validation
finishes before opening any record file. It then uses the existing FileSet
reader: one file at a time, fixed projected schema, lazy subsequent opens and
terminal errors. All file requests receive the requested Arrow options and byte
I/O options; the same byte options are supplied when opening the manifest.

Files are expected to remain unchanged during a scan. A manifest does not create
an atomic snapshot. There is no continuation-record reassembly, partition index,
generated `docid`, compressed member-offset context, Lance writing or conversion
job scheduling in this provider. Those parts of the example conversion contract
belong to separate layers/applications.

The discovery rules follow the user's existing
[`ml-infra` WARC manifest provider](https://github.com/Adcock-AGI-Inc/ml-infra/blob/main/lance/lance_migration/crates/inspector/src/provider/warc.rs).
This adapter reuses OpenDAL for HTTP and FileSet for record reading, while
streaming the inventory instead of loading the complete crawl manifest.
