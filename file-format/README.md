### Parquet row-group planning

`ParquetAccessor` implements `ScanPlanner` when its byte accessor is cloneable
(use `Arc` for a shared non-cloneable accessor). Planning reads the footer once,
fixes the projected schema, and creates one independently opened partition per
row group. Install `ParallelReadLayer` to execute these partitions while preserving
row order. Normal `open` remains sequential. Range reads and known input length
are required after byte layers; ordinary sequential gzip input is rejected.
Known input length/version changes are rejected when partitions open; absent a
storage version, this does not provide a snapshot of a mutable object.

### JSON object partitions

`JsonAccessor::plan_scan` frames complete whitespace-separated JSON objects,
including JSONL and pretty-printed objects, and emits owned decode partitions
lazily. Each partition uses an independent Arrow decoder with the fixed projected
schema. Framing uses serde_json RawValue sequentially; Arrow decoding runs in
executor workers. Incomplete records are rescanned when more input arrives, so
large records delivered in tiny chunks can increase framing cost.
This works over sequential decompression and does not require range access.

Use `FileAccessor::with_partition_options` to configure a soft chunk-byte target
(default 4 MiB); the row target is the request's batch size. A complete record can
exceed the byte target and is never rejected for its size. The resident task window
bounds the number of retained chunks, with one pending lookahead record plus the
upstream input buffer. This is not a cap on input bytes, Arrow allocations, or decoder state. Scalar/array top-level inputs
remain unsupported. Output stays ordered and batch boundaries may change.

### CSV decode partitions

The default CSV configuration and explicit `CsvDialect` configurations support
file-internal parallel decoding. `csv-async` reads complete byte records using the
source dialect, including quoted multiline fields. The `csv` writer serializes
partition records as canonical CSV with every field quoted, retaining field bytes
and replaying headers where required. Arrow workers convert these partitions to
batches in input order. This adds sequential record parsing and re-encoding before
parallel Arrow conversion; original CSV spelling and line endings are not retained.

For opaque Arrow `CsvFormat` configurations installed through `with_format`,
planning keeps the input whole because their private dialect settings cannot be
inspected safely. Use `with_dialect` for supported parallel parsing. No storage
range access is required. Soft byte targets apply to the encoded partition bytes.

### XML inferred-schema partitions

XML scan planning infers native mappings once from the configured bounded sample,
then replays sampled records before unread input. Both inferred and explicit
schemas group complete records into independent conversion partitions, bounded by
`ReadOptions::batch_size` and the smaller of `DecodePartitionOptions::target_bytes`
and `XmlLimits::max_batch_bytes`. One record may exceed the soft partition target.
Partitions retain namespace mappings, record order and source offsets. Projection
names are resolved after inference.

### ATIF trajectory partitions

ATIF JSON Lines plans frame bounded groups of complete nonblank trajectories,
assign global source record indices before scheduling, and decode each group
independently. Output remains globally ordered, including raw_json, source URI,
projection, and zero-column row counts. The original line endings are retained.
Single-document ATIF JSON remains a whole-input partition. Framing can consume
sequential decoded bytes; it does not require random reads or parallel gzip.

### WARC Arrow partitions

`WarcArrowAccessor::plan_scan` uses the upstream WARC parser to discover complete
physical header/body records, grouping them by batch rows and the soft
`DecodePartitionOptions::target_bytes` target. Owned header and body sizes count
toward that estimate; a complete oversized record is accepted. One lookahead
record, parser scratch space, and the upstream byte chunk may also be retained.
This is not a hard memory limit or a maximum WARC body size.

`ParallelReadLayer` converts each group independently to the native Arrow schema,
preserving physical order, unmodified LargeBinary bodies, source identity and
projection. Required-header/date conversion and Arrow building run in workers;
byte framing and any upstream decompression remain sequential. No range access
or custom WARC parser is required. Ordinary `open` keeps its sequential behavior.

### Parquet page compression

Plain `parquet` / `format-parquet` supports uncompressed and Snappy pages.
Enable a codec feature to read additional compressed pages; each also enables
the Parquet reader.

| Codec | File-format feature | Facade feature |
| --- | --- | --- |
| Brotli | `parquet-brotli` | `format-parquet-brotli` |
| Gzip | `parquet-gzip` | `format-parquet-gzip` |

This uses the upstream Parquet codecs and preserves range reads, projection, and
row-group planning. It is independent of external byte decompression layers.
