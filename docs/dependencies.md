# Lance dependency alignment

DataFusion, Arrow, Parquet, and OpenDAL versions follow the latest
`lance-format/lance` default-branch commit checked when this baseline is updated.
Record one immutable commit so builds remain reproducible as upstream advances.

Baseline checked on 2026-11-04:
[`343aa21c460c5e17c67185c4b57bd3365a1735a1`](https://github.com/lance-format/lance/commit/343aa21c460c5e17c67185c4b57bd3365a1735a1).
Both its [manifest](https://github.com/lance-format/lance/blob/343aa21c460c5e17c67185c4b57bd3365a1735a1/Cargo.toml)
and [lockfile](https://github.com/lance-format/lance/blob/343aa21c460c5e17c67185c4b57bd3365a1735a1/Cargo.lock)
were inspected.

| Family | Upstream manifest requirement | Upstream resolved version / our pin |
| --- | --- | --- |
| Arrow | `59.2.0` | `59.3.0` |
| Parquet | `59.2.0` | `59.3.0` |
| DataFusion | `55.0.0` | `55.2.0` |
| OpenDAL | `0.59.2` | `0.59.2` |

Use upstream's **resolved lockfile versions**, including patch versions, rather
than copying its semver requirements and allowing Cargo to select newer releases.
Direct workspace dependencies use exact `=` requirements. The committed
`Cargo.lock` also aligns transitive Arrow and OpenDAL components. For our Fs-only
OpenDAL service, absent from upstream's selected services, use the same OpenDAL
release (`0.59.2`). Feature selection remains specific to this project's scope.

DataFusion is only an unused workspace dependency pin for the later generated
column layer. It is not built or present in the resolved graph yet. The layer
must inherit it through `workspace = true`; do not import types through Lance.

## Updating the baseline

1. Resolve the latest default-branch SHA of `lance-format/lance` and inspect
   `Cargo.toml` and `Cargo.lock` at that exact SHA, including any source patches.
2. Update the workspace pins, baseline metadata, this document, and lockfile
   together. New backend/format/layer crates inherit workspace dependencies.
   Add new family components to the same version baseline.
3. Compare resolved dependency-family versions with the upstream lockfile, then
   run feature builds, tests, Clippy and Rustdoc.

The recorded SHA is a version reference, not a Git dependency on Lance. Builds
do not query upstream or automatically change dependency versions.
