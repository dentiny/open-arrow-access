# arXiv native representations, source assets and Lance storage

**Implementation:** [PR #597](https://github.com/dentiny/open-arrow-access/pull/597)
adds PDF/HTML originals and the shared article accessor.
[PR #598](https://github.com/dentiny/open-arrow-access/pull/598), stacked on #597,
adds LaTeX/source packages, `anc/` attachments and related fixtures.
[PR #599](https://github.com/dentiny/open-arrow-access/pull/599) independently tests
the existing XML reader's original and structured attachment paths.
Examples require the corresponding implementation; this documentation PR adds no runtime code.

Enable `service-arxiv`. The existing [bulk reader](arxiv.md) selects PDF/source
TARs from S3 manifests. The APIs here add explicit article PDF, source and HTML
routes, plus opt-in expansion of an individual source package. They do not add
HTML/XML/TXT S3 manifests, scrape a corpus, or generate TXT/XML full text.
[OAI-PMH metadata](arxiv-metadata.md) remains a separate API; this change does
not fix inferred bibliographic schemas or build a complete metadata snapshot.

## Explicit native article formats

Configure caller-owned HTTP storage at `https://arxiv.org/`, or a local/object
mirror with the same relative routes. For HTTP, configure the backend transport,
redirects, retry/rate policy and credentials as usual. **This root is different
from the bulk S3 layout**: website `pdf/<id>` routes are not bulk `pdf/*.tar` keys.

```rust
use open_arrow_access::{Accessor, Result};
use open_arrow_access::service::arxiv::{
    ArxivArticle, ArxivArticleFormat, ArxivArticleReadRequest, ArxivService,
};
use open_arrow_access::storage::OpenDalAccessor;

async fn read_articles(storage: OpenDalAccessor) -> Result<()> {
    let source = ArxivService::new(storage).article_records();
    let request = ArxivArticleReadRequest::new(vec![
        ArxivArticle::new("2401.04088v1", ArxivArticleFormat::Pdf),
        ArxivArticle::new("2401.04088v1", ArxivArticleFormat::Latex),
        ArxivArticle::new("2401.04088v1", ArxivArticleFormat::Html),
    ]);
    let mut reader = source.open(request).await?;
    while let Some(batch) = reader.next().await? {
        // Original bytes; HTML is not reduced to text and source stays packaged.
        assert!(batch.column_by_name("data").is_some());
    }
    Ok(())
}
```

| Format | Relative route | Output |
| --- | --- | --- |
| `Pdf` | `pdf/<article-id>` | Original PDF bytes |
| `Latex` | `src/<article-id>` | Original source response; may be a package or a single file |
| `Html` | `html/<article-id>` | Original HTML bytes, including references to assets |

IDs use modern `YYMM.NNNN` or `YYMM.NNNNN` syntax, optionally versioned.
Legacy category/number IDs are rejected. Version suffixes are retained; use explicit versions for reproducible article identity.
Unversioned routes can change as authors update papers. Missing HTML propagates
the backend error; it does not silently fall back to PDF. There is no implicit
format probing, deduplication, metadata join, or HTML asset/link downloading.
Not every article has HTML or LaTeX source. Historical source formats remain
original bytes; they are not converted to LaTeX.

All selections and projections are validated before storage I/O. Each selected
representation is one lazy scan partition, emitting one row. Install the usual
`ParallelReadLayer` for bounded concurrent partitions; plain `open` is sequential.
An empty selection emits no rows, and duplicate selections remain distinct.
Complete responses are buffered, even for projections omitting `data`; this
is not a cheap remote listing API. Source access and request pacing remain caller responsibilities.
Use the official bulk route for whole-corpus acquisition.

## Expand source packages, including images and multimedia

A multi-file LaTeX submission contains `.tex`, submitted figures, and possibly
bibliography/style files. Optional `anc/` material can include additional images,
movies, data and code. arXiv documents that ancillary files are included in the
complete article source download. Availability depends on article/version.
See the official [source packaging](https://info.arxiv.org/help/config_browser.html#source)
and [ancillary-file rules](https://info.arxiv.org/help/ancillary_files.html).

```text
bulk source TAR                    # Existing reader: one row per regular member
└── one article package            # Pass this member's ByteReader to the API below
    ├── main.tex
    ├── figures/architecture.pdf
    ├── figures/results.png
    └── anc/demo.mp4               # Optional; preserved without a media allowlist
```

```rust
use open_arrow_access::{ByteReader, Result};
use open_arrow_access::service::arxiv::{
    ArxivSourceOptions, ArxivSourcePackaging, open_source_package,
};

async fn read_source(input: Box<dyn ByteReader>) -> Result<()> {
    let mut reader = open_source_package(
        input,
        "2401.04088v1",
        ArxivSourcePackaging::GzipTar,
        ArxivSourceOptions::default(),
    ).await?;
    while let Some(batch) = reader.next().await? {
        // One regular file per row: TeX, figure, video, data, etc.
        assert!(batch.column_by_name("entry_name").is_some());
    }
    Ok(())
}
```

Choose packaging explicitly: `Tar`, `GzipTar`, `Single { name }` or
`GzipSingle { name }`. A gzipped lone TeX file is not assumed to be a TAR.
`Single` also preserves native PDF/PostScript/etc. fallbacks. The decoded package
is buffered in full; gzip checks run through EOF before rows are emitted. This catches CRC errors, truncated
trailers and trailing garbage. Budget decoder/package memory per active task.

For TARs, regular files emit rows and directories are skipped; byte names,
physical header indices and duplicate names are retained. Source members have
no configured byte-size cap. The existing parser rejects unsupported links,
sparse files and extension headers. Nothing is extracted to disk, so
member names are metadata rather than filesystem paths. Expansion is one level:
a ZIP/TAR inside `anc/` remains a file. No compilation, figure-reference/caption
resolution, recursive archive expansion or external resource fetching occurs.
A batch has a soft 4 MiB payload target and a row limit; one whole member may
exceed that byte target. This API does not establish S3 ancillary completeness.

## Arrow contract and proposed Lance table

Both new APIs return the same schema, independently of any sink:

| Column | Arrow type | Meaning |
| --- | --- | --- |
| `article_id` | `LargeUtf8`, non-null | Requested article ID, retaining an explicit version suffix |
| `format` | `LargeUtf8`, non-null | `pdf`, `latex`, `html`, or `source_member` |
| `source_uri` | `LargeUtf8`, non-null | Website-relative locator, or input reader locator for expansion; empty if unknown |
| `entry_index` | `UInt64`, non-null | Physical inner TAR header index; zero for whole responses/single files |
| `entry_name` | `LargeBinary`, non-null | Exact inner filename bytes; empty for whole responses |
| `data` | `LargeBinary`, non-null | Complete original response or regular-member payload |

Recommended application layout: **one artifact per row**, with paper metadata
in a separate table keyed by normalized article ID plus a metadata snapshot ID.
Retain explicit article versions and the source snapshot/checksum in scheduling
or provenance columns. Whole source-package rows and expanded asset rows can
coexist via `format`; storing both intentionally duplicates some bytes. This
avoids a wide table with one column for every possible format and attachment.

For example, one paper may have PDF, HTML, source-package, `main.tex`, figure
and video rows. A metadata scan can select a paper/format/member before reading
payloads. Filenames alone are not unique: retain `entry_index`. Article IDs
alone do not identify immutable upstream bytes. Include the source snapshot,
`source_uri`, format and member index in an ingestion identity, and define a
retry/deduplication policy; the reader does not enforce primary keys. For bulk
expansion, preserve both outer-shard and article-member identity in the input
locator/provenance, rather than supplying only the outer TAR URI.

### Lance Blob V2 is a writer decision

The reader emits ordinary Arrow `LargeBinary`. A downstream sink must explicitly
convert that column to Lance Blob V2 if desired; plain binary writes do not
become Blob V2 automatically. Identity/path columns remain ordinary typed
columns. This Rust reader has no Lance dependency and does not download blob
URLs or choose the sink's physical layout.

Conceptually, the Blob V2 writer maps payload bytes to compact descriptors.
Small payloads can remain inline; larger payloads can use Lance-owned sidecars.
Scans can filter identity columns without materializing payloads, then fetch
selected bytes through blob read APIs. Exact placement is controlled by the
writer/version; this reader does not prescribe thresholds or physical paths.
External URI references are another option, but their owners must retain the
referenced objects. Do not describe a plain binary write as Blob V2.
See the authoritative [Blob V2 specification](https://lance.org/format/table/blob/)
and [writer/read APIs](https://lance.org/guide/blob/).

The outer arXiv TAR, an inner compressed article package, a scan partition, a
Lance data fragment and a blob sidecar are different boundaries. A Lance blob
containing `source.tar.gz` does not expose figures within it as independent
blobs. Expand the source and write member rows to obtain that granularity;
compressed member offsets are not directly addressable external blob ranges.

## Verification

Deterministic tests cover native routes, lazy partitions, error propagation,
projection/row counts, IDs and versions, original bytes, source single files,
gzip integrity, duplicate names, binary names and image/video bytes.
Inputs live in [`testdata/arxiv-articles/`](https://github.com/dentiny/open-arrow-access/blob/feat/arxiv-source-packages/testdata/arxiv-articles/README.md),
including `anc/` XML, CSV, ZIP and opaque video payloads. Rust tests compare the
original bytes and preserve nested archives without recursively expanding them.
The facade integration test also reads PDF/source/HTML through `CrawlAccessor`.

```sh
cargo test -p open-arrow-access-service --features arxiv --test arxiv_articles --test arxiv_source_package
cargo test --features service-arxiv --test arxiv_documents
cargo test --features format-xml --test arxiv_xml
```

A small live probe of `2401.04088v1` returned HTML HTTP 200 (192,348 bytes),
source HTTP 200 (`application/gzip`, 2,878,415 bytes), and PDF HTTP 206 for a
128-byte range. Running the new source reader over that downloaded package
emitted **17 regular files**, including `main.tex`, PNG/JPEG/PDF figures and
bibliography/style files; its directory entry was skipped. This sample does
not establish whole-corpus format support, S3 permissions or bulk throughput.
