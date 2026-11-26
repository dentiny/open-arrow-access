# Dependency compatibility

The public Arrow types use the Arrow 59 family. Keep Arrow, Parquet, DataFusion
and the format adapters on compatible versions when updating this workspace.
Cargo requirements allow compatible minor/patch updates; `Cargo.lock` records the
versions validated here.

| Dependency | Requirement or pinned revision |
| --- | --- |
| Arrow / Parquet | `59.2.0` |
| DataFusion | `55.0.0` |
| OpenDAL | `0.59.5` |
| Lance baseline | `fc8f59e53233a46f0d73604d66254142802389c9` |
| Iceberg baseline | `b879b9626754501f025516ce0f0e9535fa6fd414` |
| atif-arrow | `0.6.0` |
| xml-arrow | `0.4.0` |
| DuckDB | `65e8759fd4e83dad443a6e08d0b20314c76c671d` |

Lance and Iceberg are compatibility references in this workspace's metadata;
open-arrow-sink consumes them directly. ATIF and XML use published Arrow 59
releases. DuckDB is pinned to an upstream Arrow 59 binding snapshot backed by
DuckDB 1.5.5. Iceberg is pinned immediately before its Arrow 60 upgrade. Change
each Git pin only after validating the full feature set.

Rust 1.98 is the workspace minimum, aligned with the Lance baseline. The checked-in
toolchain selects Rust 1.98.1.

When updating this baseline, update the workspace requirements, lockfile,
compatibility metadata and user-facing dependency examples together. Run the
workspace checks and tests with all features.
