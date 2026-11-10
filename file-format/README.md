
### Parquet row-group planning

`ParquetAccessor` implements `ScanPlanner` when its byte accessor is cloneable
(use `Arc` for a shared non-cloneable accessor). Planning reads the footer once,
fixes the projected schema, and creates one independently opened partition per
row group. Install `ParallelReadLayer` to execute these partitions while preserving
row order. Normal `open` remains sequential. Range reads and known input length
are required after byte layers; ordinary sequential gzip input is rejected.
Known input length/version changes are rejected when partitions open; absent a
storage version, this does not provide a snapshot of a mutable object.
