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
| atif-arrow | `c8bda1773745a690df2d917a4d0ef5dc8b6de81f` (0.6.0) |
| xml-arrow | `e7b8d6b7528c59b2588f35d44a4e6f3e7713aae8` (0.4.0) |
| DuckDB | `dd37440f86101db35c86d8552b2734e1363ce43a` |

Lance and Iceberg are compatibility references in this workspace's metadata;
open-arrow-sink consumes them directly. The ATIF, XML and DuckDB Git pins are
needed because their published releases do not expose Arrow 59 types. DuckDB
uses an Arrow 59 backport on the stable 1.5.6 Rust binding release, retaining the
existing engine version. Iceberg is pinned immediately before its Arrow 60
upgrade. Change each Git pin only after validating the full feature set.

Rust 1.98 is the workspace minimum, aligned with the Lance baseline. The checked-in
toolchain selects Rust 1.98.1.

When updating this baseline, update the workspace requirements, lockfile,
compatibility metadata and user-facing dependency examples together. Run the
workspace checks and tests with all features. Replace the ATIF and XML Git pins
with their corresponding releases once those versions are published.
