# Lance dependency alignment

DataFusion, Arrow, Parquet, and OpenDAL versions follow the latest
`lance-format/lance` default-branch commit checked when this baseline is updated.
Record one immutable commit so builds remain reproducible as upstream advances.

Baseline checked on 2026-11-04:
[`343aa21c460c5e17c67185c4b57bd3365a1735a1`](https://github.com/lance-format/lance/commit/343aa21c460c5e17c67185c4b57bd3365a1735a1).
Both its [manifest](https://github.com/lance-format/lance/blob/343aa21c460c5e17c67185c4b57bd3365a1735a1/Cargo.toml)
and [lockfile](https://github.com/lance-format/lance/blob/343aa21c460c5e17c67185c4b57bd3365a1735a1/Cargo.lock)
were inspected.

| Family | Upstream manifest requirement | Upstream resolved version / our baseline |
| --- | --- | --- |
| Arrow | `59.2.0` | `59.3.0` |
| Parquet | `59.2.0` | `59.3.0` |
| DataFusion | `55.0.0` | `55.2.0` |
| OpenDAL | `0.59.2` | `0.59.2` |

Use upstream's **resolved lockfile versions**, including patch versions, as the
workspace baseline. Direct workspace dependencies use normal Cargo semver
requirements without an `=` prefix. The committed `Cargo.lock` records the
resolved versions and aligns transitive Arrow and OpenDAL components. For
additional OpenDAL services and HTTP transports, use the same OpenDAL release
(`0.59.2`). Feature selection remains specific to this project's scope.
The CSV feature uses the same Arrow baseline (`59.3.0`) for `arrow-csv`, including
the transitive Arrow Cast and Arrow Ord components in the committed lockfile.

DataFusion is only an unused workspace dependency declaration for the later generated
column layer. It is not built or present in the resolved graph yet. The layer
must inherit it through `workspace = true`; do not import types through Lance.

## Updating the baseline

1. Resolve the latest default-branch SHA of `lance-format/lance` and inspect
   `Cargo.toml` and `Cargo.lock` at that exact SHA, including any source patches.
2. Update the workspace requirements, baseline metadata, this document, and lockfile
   together. New backend/format/layer crates inherit workspace dependencies.
   Add new family components to the same version baseline.
3. Compare resolved dependency-family versions with the upstream lockfile, then
   run feature builds, tests, Clippy and Rustdoc.

The recorded SHA is a version reference, not a Git dependency on Lance. Builds
do not query upstream or automatically change dependency versions.

## XML parser

`format-xml` uses the published [`xml-arrow 0.2.8`](https://crates.io/crates/xml-arrow/0.2.8)
parser for Arrow 59 compatibility and configuration validation before I/O.
Its companion `xml-io` storage crate is not used, so storage remains on this
workspace's OpenDAL 0.59.2 baseline. Arrow family versions remain 59.3.0 in the
lockfile. The parser uses a normal registry dependency, with the resolved version
and checksum recorded in Cargo.lock.

## ATIF parser

`format-atif` uses the published
[`atif-arrow 0.4.6`](https://crates.io/crates/atif-arrow/0.4.6) converter, including
projection-aware batch builders. The resolved registry version and checksum are
recorded in Cargo.lock.

Only the pure parser/schema/converter crate is used. The companion `atif-io`
and Harbor Hub backend are not dependencies, so ATIF does not bring its own
storage, transport, runtime or decompression policy. JSON/JSONL framing is handled
over the shared `ByteReader` contract. Arrow remains 59.3.0 and OpenDAL remains
0.59.2 in this workspace's lockfile.

`format-warc` uses published `warc-rs 0.3.3` with default features disabled,
reusing its async parser without its optional storage dependencies.

Common Crawl HTTP configuration pins `opendal-service-http` to `0.59.2`, matching
the recorded OpenDAL baseline. The optional HTTP transport uses the SDK
implementation; service configuration adds no separate HTTP client.

## ZIP parser

`decompression-zip` uses the registry `async_zip 0.0.19` parser with default
features disabled and only `deflate` enabled; Stored entries need no codec.
The adapter uses its `base::read1` seek reader for central-directory parsing,
ZIP64, data descriptors, CRC and length validation. Exact `ByteReader` ranges
provide the parser's futures I/O interface without another storage SDK.
