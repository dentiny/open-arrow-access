# Dataset source providers

Provider-specific discovery and schema conventions live in this crate. Each provider adds its own default-off feature and facade exports.

Providers use the shared object/reader contracts and existing byte, format,
and decompression accessors. Backend configuration stays caller-controlled.
Generic storage access and object-store prefix listing belong in `storage`,
not in a provider.

The [source-attribute proposal](../docs/source-attributes.md) defines provider
constraints and conditional planning. Sequential cursor-based file discovery
can coexist with independent file reads; dependent API page requests require
a different plan. Services retain their own ordering requirements while
delegating discovered representations to dataset and format accessors.

The default-off `huggingface` feature (`service-huggingface` on the facade)
provides `HuggingFaceService::repository` for dataset repository files.
The service owns repository ID, revision, root, endpoint and authentication.
Its byte accessor reuses `storage::OpenDalAccessor`; format accessors decode records.
Floating revisions are not snapshots.
The same service exposes `dataset(HuggingFaceDatasetRequest)` and implements
`Accessor` for that request. This opens one config/split as ordered Arrow batches
from the HF Parquet export. A floating export revision is first pinned with a
small `expand=sha` metadata request (64 KiB response limit); membership and file
reads then use that same commit. Explicit commit requests skip the metadata call.
The selected directory is listed nonrecursively, Parquet paths are sorted, and
existing `ParquetAccessor`/`FileSetAccessor` perform lazy sequential reading,
projection, batching and schema validation. Only the shard list is collected.

A missing complete split fails. `with_partial_exports()` explicitly permits
fallback to `partial-<split>`; full exports take precedence, and permission or
transport errors never trigger fallback. `with_revision` refers to the Parquet
representation, not the original source branch. Native dataset-card parsing and
Viewer row APIs are outside this entry point.

## Gutenberg

Enable `gutenberg` (`service-gutenberg` on the facade) for one per-book RDF
object and its selected files as a single Arrow row:

```rust,no_run
use open_arrow_access::{
    Accessor, Result,
    service::{GutenbergAccessor, GutenbergFile, GutenbergRequest},
    storage::{OpenDalAccessor, opendal},
};

async fn read() -> Result<()> {
    let operator = opendal::Operator::new(
        opendal::services::Fs::default().root("/mirror"),
    ).expect("configure filesystem");
    let books = GutenbergAccessor::new(OpenDalAccessor::new(operator));
    let request = GutenbergRequest::new("cache/epub/11/pg11.rdf", vec![
        GutenbergFile::new(
            "https://www.gutenberg.org/ebooks/11.txt.utf-8",
            "books/11.txt",
        ),
    ]);
    let mut reader = books.open(request).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} book", batch.num_rows());
    }
    Ok(())
}
```

The URI must match a `pgterms:file` entry in the RDF; the object locator points
to that file in your storage. This explicit mapping supports local or object
storage mirrors without inferring paths from HTTP download URLs. Files retain
request order. An empty selection keeps the book with an empty `files` list.
Unknown or duplicate selections fail. No licence or language filtering occurs.

| Column | Arrow type |
| --- | --- |
| `ebook_id` | UInt64 |
| `title`, `rights` | nullable LargeUtf8 |
| `creator` | List of struct: nullable `uri`, `name` (Utf8), `alias` (List<Utf8>), nullable `birthdate`, `deathdate` (Int32 years) |
| `language`, `bookshelf` | List<Utf8> |
| `subject` | List of struct: nullable `scheme` (Utf8), `value` (Utf8) |
| `issued` | nullable Date32; Gutenberg release date |
| `metadata_rdf` | LargeBinary; verbatim RDF, including unexpanded metadata |
| `files` | LargeList of struct: `uri` (Utf8), `format` (List<Utf8>), nullable `extent` (UInt64), nullable `modified` (Utf8), `data` (LargeBinary) |

`data` is the original file bytes, including compression, encoding and line
endings. `extent` and `modified` preserve metadata declarations; they do not
verify that storage and RDF belong to the same snapshot. Preserve source
manifests and snapshot identity separately. Missing optional metadata becomes
null or an empty list. Malformed RDF or failed selected-file reads terminate
the reader rather than dropping the book.

`with_options` supports top-level projection in caller order, including a
zero-column row. Omitting `files` skips all file I/O; RDF and selected URI
membership are still validated. `with_byte_options` forwards I/O settings to
every open. One RDF emits one row, independently of the batch-size target.

`with_limits(GutenbergLimits)` bounds collected RDF bytes (default 4 MiB), each
selected file (256 MiB), and combined RDF/file bytes per row (1 GiB). These
limits do not bound parser allocations, upstream chunks or Arrow copies. Reads
are sequential and pull-driven; dropping a reader stops its work.

This entry point reads caller-selected per-book UTF-8 RDF and files. Catalogue
discovery, rsync, HTTP downloading, archive expansion and Lance writing remain
with the caller. It does not use Gutendex or parse arbitrary RDF graphs. See
Gutenberg's [catalogue](https://www.gutenberg.org/ebooks/offline_catalogs.html)
and [mirroring guide](https://www.gutenberg.org/help/mirroring.html) for sources.

Run the [example](../examples/read_gutenberg.rs) against the synthetic metadata
fixture, or supply a mirror root, RDF path and pairs of file URI / local path:

```sh
cargo run --example read_gutenberg --features service-gutenberg
```
## Common Crawl configuration

Enable `service-warc` on the facade for `CommonCrawl`, which validates a
crawl identity and HTTP(S) origin, and `create_http_accessor`, which configures
an origin-rooted OpenDAL HTTP reader without fetching data. Gzip and record
format decoding remain explicit caller-supplied layers.

Enable `service-warc` for `CommonCrawlWarcRequest` and
`CommonCrawlWarcAccessor`: resolve all WARC paths from a crawl's decoded
`warc.paths.gz` inventory, preserving file order, then read through a supplied
single-file format accessor and the existing sequential FileSet. `resolve`
returns membership without opening record files. The caller supplies gzip for
compressed inventories and file decompression/format parsing separately.
Selected locators are held in memory; inventory lines have a 16 KiB limit.
There is no index, batch planner or parallel execution.

Segment selection is a separate addition.

Manifest byte framing and the 16 KiB per-line bound use core `read_lines`;
path validation and selected-file duplicate checks remain in the provider.

`format-warc` exposes native parser records through `WarcAccessor`. This service
composes Arrow readers through FileSet, so its `format` argument requires a
caller-provided WARC-to-Arrow accessor. Manifest-only `resolve` needs no conversion.
