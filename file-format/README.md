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
file-internal parallel decoding. Framing uses csv-core with the same delimiter,
quote, escape, comment, and record terminator settings as the Arrow decoder,
including quoted multiline fields. Headers are replayed per partition and output
order is preserved. For opaque Arrow `CsvFormat` configurations installed through
`with_format`, planning keeps the input whole because their private dialect
settings cannot be inspected safely. Use `with_dialect` for supported parallel
framing. No storage range access is required.
