# Project Gutenberg formats, file hierarchy and sharding

## Published representations

Book content is required for this ingestion; a catalogue-only conversion is not
completion. The target library support covers all available formats, with explicit format
selection. Which representations to include in the first production conversion
remains a separate requester decision.

| Representation | Content and coverage |
| --- | --- |
| Plain text (.txt) | Nearly every ebook; prefer advertised UTF-8, record other encodings explicitly. No embedded images. |
| HTML / XHTML / HTML5 | Text with document structure and references to images/assets. Standalone HTML is not necessarily a self-contained book. |
| HTML ZIP | Packaged HTML plus images and other files needed to display the book. |
| EPUB / EPUB3 | Generated ebook packages; image/no-image variants can coexist. Inspect each book's advertised files. |
| Kindle KF8 / legacy MOBI | Generated reader-specific alternatives, not additional books. |
| PDF / TeX and other legacy formats | Available only for some items, especially mathematical works. Do not assume corpus-wide PDF or full-text XML coverage. |

RDF/XML is catalogue metadata, not the book's full text. Availability varies by
ebook; never derive every download filename from the ID. Use the inventory and
advertised file MIME types/URLs. A ZIP is a container, not a separate textual
representation. Preserve original bytes and encoding; text extraction,
header/footer removal, normalization and OCR are separate decisions.

Suggested requester question: **Is UTF-8 TXT plus metadata sufficient initially,
or should we also retain HTML ZIP with images and structure?** This question concerns the initial conversion scope, not whether the library
should support other formats. No alternate representation is silently discarded.

Sources: [published formats](https://www.gutenberg.org/help/file_formats.html)
and [download options](https://www.gutenberg.org/help/bibliographic_record.html).

## File hierarchy

The main and generated collections are separate rsync modules. A mirror can
mount the generated module at cache/epub/ below the main root:

```text
<caller-owned mirror>/                       # Combined main + generated layout
├── 1/2/3/4/12345/                           # Main: ebook ID 12345; illustrative
│   ├── <basename>.txt                      # Plain text: UTF-8 or older charset
│   ├── <basename>.zip                      # Compressed text/other source files
│   ├── <html-directory>/                   # Submitted HTML; name varies
│   │   ├── <basename>.htm                  # Or .html / .xhtml
│   │   ├── <stylesheet>.css                # If stored separately
│   │   └── images/
│   │       ├── <illustration>.jpg          # Or .jpeg
│   │       ├── <illustration>.png
│   │       ├── <illustration>.gif
│   │       └── <illustration>.svg
│   ├── <html-package>.zip                  # HTML plus referenced local assets
│   ├── <basename>.tex                      # TeX/LaTeX, selected books only
│   ├── <basename>.pdf                      # PDF, selected books only
│   ├── <basename>.dvi                      # Legacy typeset output
│   ├── <basename>.ps                       # PostScript
│   ├── <basename>.eps                      # Encapsulated PostScript
│   ├── <basename>.rtf                      # Rich Text Format
│   ├── <basename>.xml                      # Rare full-text XML, not RDF metadata
│   ├── <basename>.lit                      # Legacy Microsoft Reader
│   ├── <basename>.pdb                      # Legacy Palm / Plucker family
│   ├── <basename>.prc                      # Legacy Mobipocket
│   ├── <audio>.mp3                         # Audio/non-text item or HTML asset
│   ├── <music>.mid                         # MIDI; .midi also possible
│   ├── <score>.mus                         # Legacy sheet-music data
│   ├── <video>.avi                         # Legacy multimedia
│   ├── <video>.mpg                         # MPEG; .mpeg also possible
│   └── <disc-image>.iso                    # Legacy CD/DVD collection image
└── cache/epub/                              # Generated module mounted here
    ├── 1342/                               # Indexed real example: ebook 1342
    │   ├── pg1342.rdf                      # Catalogue metadata
    │   ├── pg1342.txt                      # UTF-8 text
    │   ├── pg1342-images.html              # Generated HTML
    │   ├── images/                         # HTML image dependencies
    │   ├── pg1342-h.zip                    # Complete HTML download package
    │   ├── pg1342-images-3.epub             # EPUB3 with images
    │   ├── pg1342-images.epub               # EPUB2 with images
    │   ├── pg1342.epub                      # EPUB2 without images
    │   ├── pg1342-images-kf8.mobi           # Kindle KF8, despite .mobi suffix
    │   ├── pg1342-images.mobi               # Legacy Kindle/MOBI with images
    │   ├── pg1342.mobi                      # Legacy MOBI without images
    │   ├── pg1342.cover.medium.jpg         # Generated cover asset
    │   └── pg1342.cover.small.jpg
    └── feeds/                              # Cross-book catalogue/bulk exports
        ├── rdf-files.tar.bz2               # Catalogue archive, not full books
        ├── pg_catalog.csv.gz               # Alternate catalogue export
        └── txt-files.tar.zip               # Separate bulk plain-text package
```

**The directory name `cache/epub/` does not restrict it to EPUB.** It is the
generated collection's historical mount point and also contains TXT, HTML,
HTML ZIP, Kindle/MOBI, covers and metadata. These are sibling representation
files under a book ID, not separate top-level `txt/`, `pdf/` or `mobi/`
collections.

The main-tree branch is a format-location illustration, not a claim that book
12345 has every listed file or that those files have standardized names.
PDF/TeX, XML and legacy/non-text formats occur only in some items; actual
names, nesting and archive membership must come from the selected inventory.
The catalogue's format list is open-ended: keep unrecognized advertised
formats as identifiable original files rather than assuming this list is exhaustive.
Plucker/PDB and QiOO are legacy download families; discover their actual
packaging and MIME type instead of assigning a universal suffix or directory.

The generated branch uses concrete filenames from the
[official ebook 1342 directory listing](https://www.gutenberg.org/cache/epub/1342/?C=D%3BO%3DA)
as indexed during this documentation review, not a fresh live snapshot.
It deliberately omits converter logs, QR codes, debug output and historical
intermediate files. Their presence in a directory does not make them current
advertised book representations. KF8 and old MOBI can both end in `.mobi`;
retain the advertised format and filename, not just the extension.

HTML ZIP stores the HTML and its local dependencies inside one archive;
the HTML directory above shows the logical payload layout, not a guaranteed
ZIP root. EPUB/EPUB3 similarly package content and assets into one `.epub`
object. KF8/MOBI and PDF are individual file objects, not directories of
chapters. TeX may be a single source file or accompanied by assets/archive
members; keep actual membership. No archive expansion, rendering or
text extraction is implied by this hierarchy.

The documented main-directory example is `1/2/3/4/12345/`; these are digits
of the ID, not dates or balanced shard numbers. Older items can have different
layout/name conventions.

Do not confuse the mirror paths with website /ebooks/<id> landing/download
routes or /files/<id>/ aliases. The generated module has a different root from
the main collection; record the actual module-relative keys after sync.
Historical catalogue URLs can use cache/generated/ where the combined mirror
serves cache/epub/. Resolve that documented alias explicitly against the chosen
mirror and retain the original URL for provenance.

The official mirroring guide recommends rsync and distinguishes the main
gutenberg module from gutenberg-epub. Select a mirror from the official list,
verify its module roots, sync only the agreed scope to caller-owned storage,
then freeze that copy for conversion. The upstream tree changes; a book ID or
issued date alone does not pin file bytes.

Sources: [mirroring](https://www.gutenberg.org/help/mirroring.html),
[mirror list](https://www.gutenberg.org/MIRRORS.ALL),
[catalogues and bulk TXT](https://www.gutenberg.org/ebooks/offline_catalogs.html),
[feed directory](https://www.gutenberg.org/cache/epub/feeds/).

## Sharding: supported by independent books, not official numbered shards

There is no Gutenberg release/shard manifest equivalent to S2ORC's numbered
gzip JSONL files. Books and their representation files are independently
addressable. Main-tree digit prefixes are useful acquisition groups, but
neither equal-size partitions nor a complete inventory of generated content.

| Boundary | Parallelism and retry meaning |
| --- | --- |
| Distinct ebook IDs in a frozen inventory | Independent application shards; keep all selected representations of a book together. |
| Separate RDF/content files | Independent file reads, subject to backend limits. |
| One RDF document or one ebook payload | Whole-object unit; do not split arbitrary byte ranges into book rows. |
| One compressed corpus archive | A container, not independently seekable book shards by default; catalogue/member discovery and decompression remain necessary. |
| Arrow batch / Lance fragment | Execution/output boundaries, not upstream dataset shards. |

Recommended application plan (not an existing Gutenberg request option):

1. Pin a catalogue and mirror inventory with acquisition time, source mirror,
   ebook ID, representation, original URL, relative key, size and checksum where
   available. Record missing or unsupported representations.
2. Assign each ebook ID exactly once to a persisted shard manifest. A specified
   stable hash of the ID modulo a fixed N works; balancing groups by measured
   bytes works too. Record the algorithm, N, manifest digest and membership.
3. Include the RDF and all selected content for each assigned book. Check for
   duplicate/missing IDs and files across shards; do not use directory prefix
   counts as book counts.
4. Give each conversion shard its own destination and failure accounting.
   Freeze membership for retries; re-running against a changing mirror is not
   resuming the same snapshot.

One large ZIP/TAR catalogue is convenient for acquisition, but it does not make
each member an independently compressed/range-readable source partition.
Extract/stage independent files or construct a validated member inventory before
claiming parallel book reads. Per-book HTML ZIP/EPUB packages stay whole unless
an explicit member-extraction contract is implemented.

## Current library support and remaining work

The existing `service-gutenberg` feature reads one caller-selected RDF document.
It does not discover the corpus, synchronize rsync mirrors or follow book links.
Its typed metadata schema below contains selected mapped fields; it is not a
lossless encoding of all upstream RDF. For example, the repository fixture has
an unmapped `pgterms:marc520` element. Retain the complete RDF separately, or a
complete parsed representation retaining unknown fields, alongside typed columns.

| Layer | Available now | Still needed for corpus ingestion |
| --- | --- | --- |
| Metadata | `GutenbergAccessor`: one RDF → one metadata row | Frozen corpus membership; complete metadata retention |
| Original files | Shared byte access and [binary attachments](../docs/attachments.md) can preserve a caller-selected TXT, HTML, EPUB, Kindle, PDF or ZIP object | Gutenberg-aware selection/resolution and metadata/content association |
| Parsing | Shared XML/archive machinery can be composed explicitly | Each format's documented book-content contract; raw-byte access alone is not text extraction |
| Sharding | Explicit file sets can wrap independent RDF documents and use native file partitions | Gutenberg corpus planner and persisted book-grouped manifests |
| Recovery | Shared storage retry/reader contracts | Corpus checkpoints, missing-file policy and count reconciliation |

`GutenbergAccessor` aliases `DocumentAccessor`; its native scan plan is one
whole-input partition, with no within-document parallelism or provider listing.
A caller can wrap it in `FileSetAccessor` and execute independent RDF partitions
through `ParallelReadLayer` or the shared crawl executor. The generic machinery
does not automatically create Gutenberg shard membership or download content.
The [parallel-reading guide](../docs/parallel-read.md) describes execution.

Generic binary document reads buffer the complete selected object before
emitting its `data: LargeBinary` value. Memory depends on active file sizes and
concurrency; byte/batch targets do not make one huge book a streaming value.
Supplying a frozen Fs/S3/HTTP mirror uses existing storage contracts; rsync can
remain an acquisition step rather than a new Rust storage backend.

Relevant implementation: [Gutenberg decoder](src/gutenberg/mod.rs),
[whole-document planning](../core/src/document.rs) and
[file-set planning](../dataset/src/file_set/accessor.rs).

## Format-specific implementation follow-ups

Keep support for each format in a separate implementation PR, after this
documentation. Each PR should specify selection, original-byte output versus
decoded records, encoding/container behavior, book identity and missing/corrupt
input policy. TXT, HTML/HTML ZIP, EPUB/EPUB3, Kindle KF8/MOBI and PDF/TeX are
separate representation families; uncommon advertised formats should remain
explicitly identifiable and preservable through original-byte access.
A shared binary reader is useful infrastructure, not proof that all of those
Gutenberg format routes have already been implemented.

## Evidence and limits

This guide follows the hierarchy/sharding structure of [arXiv](arxiv.md).
The current implementation and existing synthetic Gutenberg RDF fixtures were
inspected. No live full-corpus inventory, book-payload read, sharding benchmark,
production conversion or deployment was performed for this documentation.
The displayed tree is illustrative; official documentation establishes the two
collection layouts, not availability of every format for every ID.

## Single-book metadata API

### Service and data

Project Gutenberg distributes public-domain ebooks and per-book RDF/XML
catalogue metadata. This provider converts one RDF document into one Arrow book
metadata row, including authors, subjects and published file links.

Enable facade feature `service-gutenberg`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

### Request and response

`GutenbergAccessor::with_default_decoder(storage).open(ReadRequest)` selects one
RDF object relative to caller-configured storage. It returns a `RecordReader` with
one row and this fixed schema (`?` means nullable):

```text
ebook_id: UInt64
title, rights: LargeUtf8?
creator: List<Struct<uri: Utf8?, name: Utf8?, alias: List<Utf8>, birthdate: Int32?, deathdate: Int32?>>
language, bookshelf: List<Utf8>
subject: List<Struct<scheme: Utf8?, value: Utf8>>
issued: Date32?
files: List<Struct<uri: Utf8, format: List<Utf8>, extent: UInt64?, modified: Utf8?>>
```

Missing repeated values become empty lists; birth/death dates are years and
`issued` is an Arrow date. File entries keep RDF order. URLs are retained as
metadata, without downloading books or verifying extents. Record source is the
RDF locator. Projection/order and byte options use standard `ReadRequest`
settings; known mapped fields remain validated even when projected away.

### Usage and results

```rust
use open_arrow_access::{
    Accessor, ReadRequest, Result,
    service::gutenberg::GutenbergAccessor,
    storage::{OpenDalAccessor, opendal},
};

async fn read_book_metadata(operator: opendal::Operator, rdf_key: &str) -> Result<()> {
    let books = GutenbergAccessor::with_default_decoder(OpenDalAccessor::new(operator));
    let mut reader = books.open(ReadRequest::new(rdf_key)).await?;
    if let Some(batch) = reader.next().await? {
        println!(
            "rows: {}, title: {:?}",
            batch.num_rows(),
            batch.column_by_name("title")
        );
        println!("available book files: {:?}", batch.column_by_name("files"));
    }
    Ok(())
}
```

Given one catalogue RDF object, the example obtains a metadata row and prints
its title/file columns. Ebook text is a separate download selected from those
links; there is no `files[].data` column.

Source: [Project Gutenberg machine-readable catalogue](https://www.gutenberg.org/ebooks/offline_catalogs.html).


