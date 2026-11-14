# Dependency compatibility

The Arrow baseline follows the released Lance 14.0.1 and Iceberg 0.11 SDKs so
applications can pass native Arrow batches between independent access and sink
libraries without cross-version conversion. It does not follow Lance's moving
default branch.

| Family | Manifest requirement | Current lockfile |
| --- | --- | --- |
| Arrow | `58` | `58.4.1` |
| Parquet | `58` | `58.4.1` |
| DataFusion | `54.1.0` | `54.1.0` |
| OpenDAL | `0.59.2` | `0.59.2` |

Lance 14.0.1 requires Arrow 58; Iceberg 0.11 requires Arrow 58.4. DataFusion 54.1
requires Arrow 58.3, which permits the shared 58.4.1 version. Direct workspace
dependencies use normal Cargo semver requirements. Arrow and Parquet specify only
the major version (`"58"`), allowing compatible 58.x releases. The committed `Cargo.lock`
records the exact versions; all enabled formats must resolve one Arrow family.
CSV, JSON and Parquet use this same baseline, including their transitive Arrow
Cast, Ord and IPC components. Existing Parquet fixtures remain valid regardless
of which Arrow release originally wrote them.

OpenDAL remains on 0.59.2, including its service and HTTP transport crates. This
change does not require a storage API downgrade. DataFusion remains a required
layers dependency with default features disabled and `sql` explicitly enabled.
Core does not import DataFusion or Lance types.

## Updating the baseline

1. Check the Arrow requirements of the released SDKs that exchange batches with
   this workspace, including parsers and DataFusion.
2. Update workspace requirements, compatibility metadata, this document, examples
   and lockfile together. Preserve one Arrow major across all enabled formats.
3. Run all-feature workspace tests, Clippy and Rustdoc, and verify the resolved
   Arrow dependency family before updating the compatibility baseline.

## XML parser

`format-xml` uses the immutable Arrow 58 compatibility revision from
[xml-arrow-rs PR #39](https://github.com/dentiny/xml-arrow-rs/pull/39) while its
0.3.0 release is pending. The parser retains configuration
validation before I/O and reusable inferred mappings with bounded sample replay
for parallel decoding. Its companion `xml-io` storage crate is not used, so storage
remains on this workspace's OpenDAL 0.59.2 baseline.

## ATIF parser

`format-atif` uses the immutable Arrow 58 compatibility revision from
[atif-arrow PR #45](https://github.com/dentiny/atif-arrow/pull/45), including the
existing projection-aware batch builders for the planned 0.5.0 release. The parser
API is unchanged.

The version requirements already target `xml-arrow = "0.3"` and
`atif-arrow = "0.5"`. Both parser dependencies are pinned by Git revision in `Cargo.toml` and
`Cargo.lock`; they do not track a moving branch. Their compatibility PRs do not
need to be merged to build this branch. Switch back to normal registry requirements
when Arrow 58-compatible releases are published, keeping the workspace on one
Arrow major.

Only the pure parser/schema/converter crate is used. The companion `atif-io`
and Harbor Hub backend are not dependencies, so ATIF does not bring its own
storage, transport, runtime or decompression policy. JSON/JSONL framing is handled
over the shared `ByteReader` contract. Arrow remains 58.4.1 and OpenDAL remains
0.59.2 in this workspace's lockfile.

`format-warc` uses published `warc-rs 0.3.3` with default features disabled,
reusing its async parser without its optional storage dependencies. WARC itself
does not depend on Arrow and needs no version change for this baseline.

Common Crawl HTTP configuration pins `opendal-service-http` to `0.59.2`, matching
the recorded OpenDAL baseline. The optional HTTP transport uses the SDK
implementation; service configuration adds no separate HTTP client.

## ZIP parser

`decompression-zip` uses the registry `async_zip 0.0.19` parser with default
features disabled and only `deflate` enabled; Stored entries need no codec.
The adapter uses its `base::read1` seek reader for central-directory parsing,
ZIP64, data descriptors, CRC and length validation. Exact `ByteReader` ranges
provide the parser's futures I/O interface without another storage SDK.
