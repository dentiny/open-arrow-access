# arXiv bulk PDF and source archives

**Upstream fix status:** the two baseline failures documented below are fixed in
[PR #594](https://github.com/dentiny/open-arrow-access/pull/594): manifest size
can count member payloads without TAR framing, and arXivRaw uses its published
string/struct/list schema. With that patch, the native reader returned the
first real 5,706,452-byte PDF without a size override and decoded the complete
1,300-record metadata page. The conversion inspector also passed both real
source URIs when linked to the patch. These fixes require merge/release and a
consumer upgrade; the baseline evidence below describes unpatched 0.12.1.

## Service and data

This bulk provider reads PDF/source TAR shards, with XML
manifests describing each shard. This provider selects shards and opens archive
entries over caller-configured storage. Bibliographic harvesting is a separate
[OAI-PMH metadata entry point](arxiv-metadata.md).

See [native article formats, source assets and Lance storage](arxiv-articles.md)
for explicit PDF/source/HTML reads, source-package expansion and `anc/` attachments.

## Article representations: PDF, source and HTML

For ingestion, distinguish **three main full-text representations of the same
article**. They are alternatives at the article level; availability depends
on the article/version.

| Full-text representation | What it contains | How arXiv distributes it |
| --- | --- | --- |
| **PDF** | The rendered paper, with its layout, equations and figures | Website `/pdf/<id>`; bulk `pdf/` TARs and PDF manifest |
| **Source (usually TeX/LaTeX)** | Author-submitted manuscript source and supporting files, often packaged together | Website `/src/<id>`; bulk `src/` TARs and source manifest |
| **HTML** | A browser-readable full-text rendering | Website `/html/<id>`, where available; no HTML bulk manifest is documented in the linked S3 guide |

Source is commonly LaTeX, but is not guaranteed to be LaTeX for every submission.
HTML is not available for every paper. Use an explicit article version when
comparing representations; matching TAR sequence numbers does not identify the
same papers. The table describes documented distribution routes, not a
successful authenticated enumeration of every S3 prefix.

**XML is not a fourth full-text counterpart in these interfaces.** The
API/OAI-PMH XML describes the article (identifier, title, authors, abstract,
dates, etc.). Separately, the bulk XML manifests describe TAR files (keys,
months, sizes and checksums). Neither is an XML encoding of the paper's full
body. A pipeline producing full-text XML or TXT would be an additional
conversion step, not another format selected by the existing bulk provider.

```text
One article/version
├── PDF representation
├── Source representation (usually LaTeX)
│   ├── main.tex, references.bib, style.sty
│   ├── figures/plot.png, figures/diagram.pdf
│   └── anc/demo.mp4, anc/data.csv           # Optional attachments
├── HTML representation                    # Where available
└── Metadata record (XML)                  # Describes the paper; not full text
```

Figures and ancillary files are **members of the source representation**, not
additional full-text counterpart formats. The following hierarchy describes
how the PDF/source representations are packaged into bulk TAR shards.

Sources: [official bulk distribution](https://info.arxiv.org/help/bulk_data_s3.html),
[HTML full text](https://info.arxiv.org/about/accessible_HTML.html),
[API metadata XML](https://info.arxiv.org/help/api/user-manual.html) and
[OAI-PMH metadata](https://info.arxiv.org/help/oa/index.html).

Enable facade feature `service-arxiv`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

Storage is rooted above `pdf/` and `src/`. Configure credentials/requester-pays
on the OpenDAL operator when using upstream S3; the provider does not create a
paid bucket client. It also works with a local mirror of the same keys.

| Library request | Response |
| --- | --- |
| `manifest(ArxivKind::Auto, Some(yymm), byte_options)` | Available PDF shards followed by source shards; each shard has a concrete `kind` |
| `manifest(ArxivKind::Pdf, Some(yymm), byte_options)` | `Vec<ArxivArchive>` from `pdf/arXiv_pdf_manifest.xml`; `Source` selects `src/arXiv_src_manifest.xml` |
| `stream(&shard, byte_options)` | `TarStream`, sequential headers/bodies, no initial archive-wide scan |
| `archive(&shard, byte_options)` | Indexed `TarArchive`; requires known length and ranges |

`ArxivArchive` fields are `kind`, `filename`, `month: u16` (YYMM),
`sequence: u32`, `size: u64`, `num_items: u64`, `checksum: String`.
`None` selects all months; an absent month yields an empty vector. Manifest
order is retained. `Auto` probes the two known manifest keys without listing. It
skips only `NotFound` manifests and returns `NotFound` if neither exists. An
existing empty manifest or a month with no matches returns an empty vector.
Permission, authentication, I/O and malformed-manifest errors are propagated,
even if the other distribution is available. If both exist, both are included;
use `Pdf` or `Source` to select only one representation. MD5 and item counts
are metadata, not verified content. The manifest size may be the sum of member
payload bytes, excluding TAR framing; it is not an exact object length. A known
object length smaller than this size is rejected, while TAR headers, alignment,
end markers and stream truncation are checked by the TAR reader.
`TarStream::next_entry()` returns `TarEntryReader { entry, reader }`; the body
contains original PDF/source-member bytes, not Arrow rows.

## Usage and results

```rust
use open_arrow_access::{
    ByteReadOptions, Result,
    service::arxiv::{ArxivKind, ArxivService},
    storage::{OpenDalAccessor, opendal},
};

async fn read_bulk(operator: opendal::Operator, month: u16) -> Result<()> {
    let service = ArxivService::new(OpenDalAccessor::new(operator));
    let shards = service
        .manifest(ArxivKind::Auto, Some(month), ByteReadOptions::default())
        .await?;
    if let Some(shard) = shards.first() {
        println!("shard: {}, {} bytes", shard.filename, shard.size);
        let mut archive = service.stream(shard, ByteReadOptions::default()).await?;
        if let Some(open_arrow_access::decompression::tar::TarEntryReader { entry, reader: mut body }) = archive.next_entry().await? {
            println!("member: {}", String::from_utf8_lossy(&entry.name));
            if let Some(chunk) = body.next().await? {
                println!("first member chunk: {} bytes", chunk.len());
            }
        }
    }
    Ok(())
}
```

For month `2410` (October 2024), the example discovers available PDF/source
shards and reads the first entry's first chunk. To advance a live sequential archive, finish or drop
the current entry reader before requesting another entry.

Source: [arXiv bulk access](https://info.arxiv.org/help/bulk_data.html).

For native Arrow scans, `ArxivReadRequest::default().with_month(2410)` uses
`Auto`; `ArxivReadRequest::new(ArxivKind::Pdf)` still selects PDF only. Returned
`ArxivArchive.kind` is always `Pdf` or `Source`; manually constructed archives
with `Auto` are rejected before storage is opened.

API note: `ArxivKind::manifest_key()` now returns `Option<&str>` because `Auto`
has no single manifest key. Existing direct callers should handle `None` (or
unwrap only when the kind is known to be `Pdf` or `Source`). Exhaustive matches
on `ArxivKind` must also handle the new `Auto` variant.

## PDF and metadata in one table

`records().with_metadata(Arc<ArxivMetadataIndex>)` joins a cached metadata
snapshot to each PDF before emitting Arrow batches. Pass
`ArxivReadRequest::new(ArxivKind::Pdf)`; source/automatic requests are rejected.
The output retains the four bulk columns listed below and adds a non-null `metadata`
struct containing **every column** of the snapshot with its original Arrow type.
Write these batches to one Lance dataset with the original binary `data` column.
Lance Blob V2 storage is a separate sink configuration contract; a URL-download
blob option is not a selector for already-materialized PDF bytes.
No second metadata table or metadata fetch per PDF is required.

```rust
use std::sync::Arc;
use open_arrow_access::{Accessor, Result, RecordBatch};
use open_arrow_access::service::arxiv::{ArxivKind, ArxivMetadataIndex, ArxivReadRequest, ArxivService};
use open_arrow_access::storage::OpenDalAccessor;

async fn convert_rows(storage: OpenDalAccessor, metadata: RecordBatch) -> Result<()> {
    // metadata is a complete cached snapshot for the selected PDF month.
    // Its identifier column may use canonical IDs or oai:arXiv.org:<id>.
    let index = Arc::new(ArxivMetadataIndex::try_new(metadata, "identifier")?);
    let source = ArxivService::new(storage).records().with_metadata(index);
    let mut reader = source.open(ArxivReadRequest::new(ArxivKind::Pdf).with_month(2410)).await?;
    while let Some(batch) = reader.next().await? {
        // Send this batch directly to the same Lance writer.
        assert!(batch.column_by_name("metadata").is_some());
        assert!(batch.column_by_name("data").is_some());
    }
    Ok(())
}
```

Snapshot acquisition is separate: harvest OAI-PMH through one coordinator,
respect its rate limit, and persist/reuse the result. The caller must select a
stable mapping retaining all required upstream fields; the default OAI header
mapping alone is insufficient. The index makes no network requests. It retains
metadata in memory, not PDFs; monthly snapshots bound worker memory usage.

Joining is by **article ID**. A PDF filename's version suffix is retained in
`entry_name`, while its unversioned ID matches the snapshot. Metadata describes
the snapshot and may include version history; this does not assert that current
metadata describes an individual historical PDF version. Invalid filenames,
null/duplicate metadata IDs and PDFs without metadata fail rather than silently
producing incomplete rows. Metadata-only records (for example deleted articles
without a PDF) do not generate PDF rows. Projection applies after matching;
shards remain independent scan partitions and share the immutable index.

## File hierarchy and task boundaries

```text
s3://arxiv/                              # Or a mirror rooted here
├── pdf/
│   ├── arXiv_pdf_manifest.xml           # Global XML inventory, not PDF bytes
│   ├── arXiv_pdf_2410_001.tar           # October 2024, sequence 1
│   │   ├── 2410/<article-id>.pdf        # Illustrative member name
│   │   └── ...                         # Many original PDFs
│   ├── arXiv_pdf_2410_002.tar           # Same month, another shard
│   └── arXiv_pdf_2411_001.tar           # November, same directory
└── src/
    ├── arXiv_src_manifest.xml           # Independent source inventory
    └── arXiv_src_YYMM_NNN.tar
        └── <article source package>         # Inner package, often .tar.gz
            ├── main.tex                     # TeX/LaTeX manuscript
            ├── references.bib
            ├── figures/plot.png             # Submitted paper figure
            └── anc/demo.mp4                 # Optional multimedia attachment
```

**Month selection filters manifest records; it does not select a month
directory.** TARs from different months sit alongside each other under `pdf/`
(or `src/`). For example, `arXiv_pdf_2410_001.tar` encodes October 2024
(`2410`) and sequence 1 (`001`), but discovery uses the manifest's `yymm`
field rather than guessing filenames or listing a `pdf/2410/` directory.
The `2410/` member path shown inside a TAR is separate from this storage layout.

For illustration, a PDF manifest could contain the following records
(simplified from XML; these are not the complete upstream inventory):

| filename/key | yymm | seq_num |
| --- | --- | --- |
| `pdf/arXiv_pdf_2410_001.tar` | `2410` | 1 |
| `pdf/arXiv_pdf_2410_002.tar` | `2410` | 2 |
| `pdf/arXiv_pdf_2411_001.tar` | `2411` | 1 |

`manifest(ArxivKind::Pdf, Some(2410), byte_options)` reads the PDF manifest
and retains records with `yymm == 2410`: the first two TARs in this
illustration. `Some(2411)` selects the third TAR. The native Arrow equivalent
is `ArxivReadRequest::new(ArxivKind::Pdf).with_month(2410)`, whose scan plan
contains one partition per selected TAR. The storage root is unchanged.

A scheduler can assign these disjoint month selections to separate jobs and
destinations. This groups existing TARs into monthly tasks; it does not repack
files or split a TAR. Months can have different total byte sizes and TAR counts,
so monthly tasks are not necessarily balanced. Explicit subsets within a month
have the separate API limitations described under sharding below.

The outer bulk container is an uncompressed TAR. Source members can contain
compressed TeX/LaTeX packages; reading the outer TAR does not recursively unpack
those packages, compile TeX, extract PDF text, or render page images. Preserve
member names as bytes instead of inventing paths or extracting them to disk.
PDF and source are different representations: matching month/sequence numbers
are not a paper join, and `Auto` can ingest both representations.

`YYMM` groups article submission months, with a sequence within each month and
distribution. It is not a dated immutable release or an OAI update window. For
example, numeric month `1` selects `0001` (January 2000); retained filenames keep
the zero padding. Always enumerate actual manifest membership instead of
assuming contiguous sequences or fixed shard sizes/counts. Upstream describes
roughly 500 MB TAR chunks; that is an approximate packaging target, not a limit.

The PDF manifest additionally documents `first_item`, `last_item`, `timestamp`
and `content_md5sum`. `ArxivArchive` currently exposes only the six fields listed
above, plus the resolved kind; the additional fields are not an index exposed
by this API. Save the original manifest as provenance. Manifest MD5/item counts
are not automatically checked against streamed content. Known archive length
is checked, but equal length is not a content identity guarantee. Pin a mirror
snapshot and verify its checksums separately when reproducibility is required.

### Files inside one source representation

The source package can contain the LaTeX manuscript (`.tex`), bibliography and
style files (`.bib`, `.bbl`, `.sty`), submitted paper figures (PNG/JPEG/PDF/EPS),
and optional `anc/` attachments such as movies, extra images, data or code.
A figure stored as PDF here is an asset inside the source package, not the
whole-paper PDF representation in `pdf/`. Contents vary by article/version.

```text
arXiv_src_2410_001.tar              # Bulk shard containing many articles
└── <one article source package>   # Often .tar.gz
    ├── main.tex
    ├── references.bib
    ├── figures/architecture.pdf
    └── anc/demo.mp4               # Optional
```

The existing bulk reader emits one row per regular member of the **outer**
TAR. An article package remains intact in `data`, including files within it.
It does not automatically unpack that package, emit separate asset rows,
compile LaTeX, associate captions, or fetch external links.
[open-arrow-access #598](https://github.com/dentiny/open-arrow-access/pull/598)
proposes opt-in one-level expansion; it is separate from this bulk-reader
contract. Retaining a package does not establish that every possible external
attachment has been acquired.

See the official [source packaging](https://info.arxiv.org/help/config_browser.html#source)
and [ancillary-file inclusion rules](https://info.arxiv.org/help/ancillary_files.html).

### Arrow row contract

The native `records()` schema, before an optional metadata join, is:

| Column | Arrow type | Meaning |
| --- | --- | --- |
| `shard_uri` | Non-null `LargeUtf8` | Manifest filename/key, such as `pdf/arXiv_pdf_2410_001.tar`; not necessarily an absolute URI. |
| `entry_index` | Non-null `UInt64` | Physical TAR entry index; directory entries can leave gaps. |
| `entry_name` | Non-null `LargeBinary` | Exact member-name bytes. |
| `data` | Non-null `LargeBinary` | Complete original regular-file payload. |

One row represents a regular TAR member, not a shard, page, parsed paragraph,
or guaranteed unique article. Directories do not emit rows. Only the metadata
join validates PDF filenames and derives article IDs; the raw bulk accessor is
not a PDF validator. Retain mirror/manifest identity with shard and member index
for provenance; an article ID alone does not identify an input version.

## Sharding and performance assessment

The abstraction is reusable: caller-owned byte storage → manifest discovery →
TAR reader → Arrow accessor → shared scan execution. No per-paper HTTP download
is needed for bulk scans. Compared with PMC inventory rows naming metadata
objects or Common Crawl segments grouping WARC objects, the natural arXiv unit
is one manifest-selected TAR; a month is a convenient group of those units.

- `plan_scan` produces one independent partition per selected TAR. Wrap
  `records()` (or its metadata-joined accessor) in `ParallelReadLayer` to schedule
  several TARs concurrently. Plain `open` consumes the scan without installing
  that parallel layer. Entry order inside a TAR remains sequential.
- Prefer `stream` for complete bulk ingestion: it can return the first member
  without scanning the whole archive. `archive` first issues ranged header
  reads across the TAR to build its entry index; use it for selected member
  reads, not as the default all-PDF path. The index is not persisted/reused
  automatically across jobs.
- Arrow batches target 4 MiB of payload by default, with a separate row limit.
  This is a soft target: each member is buffered in full and can exceed it.
  Concurrency, queued batches, metadata index, copies during the join, and writer
  buffers all affect peak memory. Projection without `data` still drains the
  sequential member bodies; it saves materialization, not download bandwidth.
- Default TAR limits allow 65,536 headers and 1 GiB per member; callers can use
  `with_tar_limits`. GNU/USTAR regular files and directories are supported;
  extension headers, links and sparse files are rejected. A signed sample confirmed the first shard uses GNU/USTAR headers; this is
  not full-corpus format validation.

Current `ArxivReadRequest` selects kind/month, not an explicit set of filenames
or sequence range. Low-level callers can enumerate the manifest and `stream`
chosen `ArxivArchive` values, but exact-subset **Arrow scans** need an additional
selection API (or a caller-prepared mirror manifest). There is no persisted
per-TAR/member checkpoint or automatic resume from the last emitted row. Row
sampling does not create disjoint shards. Separate writers/jobs need separate
destinations and an explicit later assembly policy.

This is a sound bulk-ingestion composition, but not yet evidence of sufficient
multi-TB throughput. Benchmark a pinned real PDF month with read concurrency
1/4/8, measuring time to first batch, source bytes/s, PDFs/s, peak RSS, request
counts/retries and end-to-end sink throughput. Choose concurrency from those
measurements, including large-member outliers and retry behavior.

## Live validation and metadata readiness (2026-11-19)

At repository commit `300bae8d9b63fb1bc75e8bd2bb8ad549c3c6ba91`, a real
`ArxivMetadataService::records(Some("2026-11-02"), Some("2026-11-02"), None)`
request returned **1,300 records**, **3,383,024 XML bytes**, and a continuation
token. The first curl fetch took 4.56 s; a subsequent library fetch took 0.613 s.
These are individual metadata requests, potentially cached, not bulk PDF or
conversion throughput measurements. A November 1 window instead returned
`noRecordsMatch` with HTTP 200: always inspect the OAI body and continuation.

Reproduction of a metadata decoding failure with that populated page:

```rust
let page = service.records(Some("2026-11-02"), Some("2026-11-02"), None).await?;
let mut rows = page.records(service.inferred_record_format())
    .open(open_arrow_access::ReadRequest::new("records")).await?;
while let Some(_batch) = rows.next().await? {}
```

With `Accessor` in scope, this failed at record index 1299: inference selected
`Float64` for `ns0:id`, then encountered `cond-mat/0604352`. Modern IDs that look
numeric must remain strings, too. Increasing a sample is not a stable schema
contract. Use an explicit provider metadata mapping, including string article
IDs and all required nested fields, and validate across pages. The fixed
`service.record_format()` decoded all 1,300 protocol-header rows successfully;
it is not a complete bibliographic snapshot. The inferred envelope also has nested header IDs, not
the top-level `identifier` expected by the default metadata-index setup.

Metadata acquisition therefore still needs a coordinator that follows tokens,
paces requests, retains deleted headers/version history, reconciles repeated
updates, normalizes IDs into a top-level string column and persists a complete,
stable typed snapshot. An OAI date selects update datestamps, not PDF submission
month; a single update-day page cannot populate a month's PDF join. The library
has a continuation harvester, but snapshot construction remains caller-owned.

An anonymous GET of the official PDF manifest returned HTTP 403 with
`AccessDenied` explicitly requiring authentication for Requester Pays. This is
not evidence that signed access is broken, nor a successful PDF validation.
Use signed S3 access with requester billing and `us-east-1`. For a PDF-only role,
select `Pdf`: `Auto` probes `src/` too and deliberately propagates its access
denial. Subsequent signed validation succeeded for the manifest and a 1 MiB range of
`pdf/arXiv_pdf_2410_001.tar`, with `RequestCharged: requester`. The manifest was
5,497,601 bytes (ETag `7c7208757bb0657a354ca04a241a8729`) and listed 12,902 PDF
shards; month 2410 selected 167 shards with a declared total of 24,129 items.
These are manifest observations, not a verified count of downloaded PDFs.

**A second live-data blocker was reproduced:** the first selected archive's
manifest `size` is **484,224,805**, but signed S3 HEAD reports **484,352,000**
bytes. The object's ETag equals manifest MD5 `4914b04c8ae73f8a8f0d7e82749ac3aa`.
`records().open(ArxivReadRequest::new(ArxivKind::Pdf).with_month(2410))` fails on
its first batch with `archive length differs from manifest metadata`. The range
GET itself succeeded and showed a `2410/` directory with `ustar ` TAR magic.
Thus AWS access works while the current native Arrow path rejects this real
archive. Establish the upstream size field's meaning and use authoritative
object identity/integrity metadata before changing validation; do not silently
disable integrity checks or treat this as proven corruption. No signed bulk
PDF throughput or full-corpus coverage is claimed here.

References: [official S3 hierarchy and manifest](https://info.arxiv.org/help/bulk_data_s3.html),
[metadata harvesting](https://info.arxiv.org/help/oa/index.html),
[parallel read semantics](../docs/parallel-read.md).

For diagnosis only, a low-level `service.stream(&archive, ...)` request with
`archive.size` replaced by that signed HEAD length read the complete first
regular member, `2410/2410.00001.pdf`: **5,706,452 bytes**, starting with `%PDF-`.
The probe finished in 2.46 s including manifest discovery. No production code
was changed; native `records()` still uses manifest size and still fails. This
establishes real PDF-byte access, not a repaired conversion path or sustained
throughput benchmark.

## Common PDF / LaTeX / XML content schema

Use `records().content(ArxivContentFormat::Pdf | Latex | Xml)` for the
conversion-facing content contract. All three formats emit exactly the same
field names, order, Arrow types and nullability:

| Field | Arrow type | Meaning |
| --- | --- | --- |
| `article_id` | `LargeUtf8`, non-null | Normalized unversioned article ID |
| `format` | `LargeUtf8`, non-null | `pdf`, `latex`, or `xml` |
| `shard_uri` | `LargeUtf8`, non-null | Selected outer TAR locator |
| `entry_index` | `UInt64`, non-null | Physical outer member index |
| `entry_name` | `LargeBinary`, non-null | Original outer filename including version spelling |
| `source_index` | `UInt64`, non-null | Inner TeX member index; zero for whole PDF/XML documents |
| `source_name` | `LargeBinary`, non-null | Inner TeX filename; empty for whole PDF/XML documents |
| `data` | `LargeBinary`, non-null | PDF bytes, decoded UTF-8 TeX bytes, or original XML bytes |

`.with_metadata(index)` adds the same non-null `metadata: Struct` for all
formats when using the same snapshot. Matching precedes projection. Without a
snapshot, all three omit that column. There is no format-specific `text` column.
`content_schema()` is the single definition; `latex_schema()` aliases it.

PDF uses `kind=pdf`; LaTeX uses `kind=source` and filters attachments. XML reads
a caller-owned source inventory whose TAR article members end in `.xml`: arXiv does
not expose an official XML full-text bulk manifest, and this reader does not
synthesize XML from PDF/TeX or treat OAI bibliographic metadata as full text.
The XML mirror uses the existing source-inventory layout; no new provider
manifest is introduced. XML bytes are retained without projecting document-specific XML fields into
the common schema. PDF/XML bytes are whole outer members; LaTeX files are
expanded from their source package. Both identities remain explicit.

The lower-level `records()` retains raw archive rows, and `article_records()` /
`open_source_package()` retain their artifact-byte contracts. Those transport
APIs are distinct from the common content output; conversion integrations
should select `content(format)` rather than emitting those raw schemas.

## LaTeX-only bulk output

`service.records().latex()` accepts `ArxivKind::Source` and retains one lazy
partition per source TAR. It decodes gzip source payloads, detects TAR packages,
and emits each UTF-8 `.tex` member outside any `anc/` directory. Images,
ancillary TeX, videos, bibliography/style files and the original package are
excluded. Multi-file manuscripts remain separate files; includes are not
expanded and TeX is not compiled or concatenated. Single files must contain a
LaTeX document marker. Non-LaTeX sources, packages without eligible TeX,
invalid UTF-8 and corrupt archives fail the scan; no PDF fallback or silent
row skipping occurs.

The schema is `article_id: LargeUtf8`, `format: LargeUtf8` (`latex`),
`shard_uri: LargeUtf8`, `entry_index: UInt64`, `entry_name: LargeBinary`,
`source_index: UInt64`, `source_name: LargeBinary`, `data: LargeBinary` (all
non-null). Outer and inner physical indices preserve duplicate-name identity.
`article_id` is the normalized unversioned metadata key; original outer names
retain version spelling. Modern and legacy bulk IDs are supported.

Call `.latex().with_metadata(index)` to attach all cached snapshot fields as a
non-null `metadata` struct, matching by article ID before projection. Missing
matches fail. This does not build snapshots or establish historical-version
metadata matching. Projection, including a zero-column projection, still
validates source contents and metadata matches.

Each article package is buffered while decoding and selecting files. Batches
respect the row limit and a soft 4 MiB source-payload target; one complete file can exceed
that target. Bound active TAR partitions to control resident package memory.
Existing raw `records()` and general `open_source_package()` output are unchanged.


## Explicit single-shard jobs

`ArxivReadRequest::new(kind).with_shard("src/arXiv_src_2410_001.tar")`
selects exactly one uncompressed TAR key relative to the service storage root.
Planning creates one lazy partition, performs no manifest requests or listing,
and execution opens only that object. This also works through `.latex()` and
metadata-enriched readers because the same request is forwarded. Month and shard
selectors cannot be combined; invalid/absolute/traversing keys are rejected
before I/O. A caller-owned XML TAR can use this path without inventing an
upstream XML distribution manifest. Keep storage credentials on the service,
not in the shard key. TAR structural/length checks still apply; manifest size,
MD5 and membership cannot be checked when the manifest is intentionally bypassed.
