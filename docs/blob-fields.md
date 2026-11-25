# Source blob fields

Source-owned payload schemas use Arrow field metadata
`open-arrow-sink:blob = "true"`. The core `blob_field(Field)` helper preserves
types, nullability and existing metadata; it does not depend on a sink crate.
Sink backends own logical blob conversion, downloading and physical layout.

| Source/view | Marked field paths |
| --- | --- |
| BinaryDecoder / binary attachment | `data` |
| ArchiveAccessor | `data` |
| arXiv article downloads / source packages | `data` |
| arXiv bulk TAR records / metadata-joined PDFs | `data` |
| arXiv content / LaTeX records | `data` |
| BHL page records | `data` |
| MECA selected package members | `data` |
| Gutenberg whole-book rows | `files[].data`, `raw_rdf` |
| WARC Arrow / Common Crawl native Arrow view | `body` |
| PMC selected content representation | `content_url` |
| CourtListener explicit document-URL view | `documents.opinion_file_url`, `documents.cluster_pdf_url` |

Payload bytes remain their original Arrow binary type. WARC retains BinaryView
and its parser-owned buffers; the Lance sink needs BinaryView blob-input support
(or an explicit cast to LargeBinary at the sink boundary). open-arrow-sink 0.6.4
accepts only LargeBinary blob bytes, so it needs the companion binary-input fix
before writing annotated WARC bodies. Binary/BinaryView normalization belongs to
the sink, not the WARC parser.

Only selected content URLs are annotated: PMC's content_url chooses one format;
CourtListener document URLs require an explicit media root/accessor. Their text
values remain URLs until a blob-aware sink consumes them. Source metadata links
such as PMC's other format URLs, Gutenberg file URIs, DOI/website links and
original CourtListener paths stay ordinary metadata. Annotating a text field
means *download this URL*, not *store this text as bytes*.

Binary filenames (`entry_name`, `source_name`) remain ordinary fields. Typed text,
JSON, XML, ATIF conversation text and media path references are not implicitly
converted to blobs. Provider JSON-to-Arrow bindings and CSV/JSON/XML/Parquet or
Hugging Face schemas remain caller/upstream-owned; there is no global rule that
marks every binary column or follows every URL.

Preserve field metadata through projections, joins and nested array rebuilds.
A nested field is part of its array's DataType, so changing only the outer schema
is insufficient. Pass the source schema and matching batches directly to a
blob-aware sink; explicit DownloadBlobLayer selection is unnecessary for fields
already marked by the source. Callers needing ordinary byte/URL storage can
remove the marker consistently from both schema and nested array types.

The schema contract test in `tests/blob_fields.rs` enumerates the marked paths
and checks that unrelated fields remain unmarked. Enable the corresponding
`service-*` and `format-warc` features to cover each provider.
