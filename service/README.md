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

## Common Crawl configuration

Enable `service-commoncrawl` on the facade for `CommonCrawl`, which validates a
crawl identity and HTTP(S) origin, and `create_http_accessor`, which configures
an origin-rooted OpenDAL HTTP reader without fetching data. Gzip and record
format decoding remain explicit caller-supplied layers.

Crawl-wide WARC datasets and segment selection are separate additions.
