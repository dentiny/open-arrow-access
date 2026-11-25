# Source blob fields

Source-owned payload schemas use Arrow field metadata `open-arrow-sink:blob`:

- `"bytes"`: existing binary payload, marked with `blob_bytes_field(Field)`.
- `"uri"`: selected source URI, marked with `blob_uri_field(Field)`.

Both core helpers preserve types, nullability and existing metadata without a
sink dependency. Sink backends validate the kind against the Arrow type and own
logical blob conversion and physical layout. URI inputs follow the configured
ingest (default download) or reference policy; byte inputs need no download.
Legacy `"true"` remains recognized by the companion sink with type inference.

| Source/view | Marked field paths | Kind |
| --- | --- | --- |
| BinaryDecoder / binary attachment | `data` | bytes |
| ArchiveAccessor | `data` | bytes |
| arXiv article downloads / source packages | `data` | bytes |
| arXiv bulk TAR records / metadata-joined PDFs | `data` | bytes |
| arXiv content / LaTeX records | `data` | bytes |
| BHL page records | `data` | bytes |
| MECA selected package members | `data` | bytes |
| Gutenberg whole-book rows | `files[].data`, `raw_rdf` | bytes |
| WARC Arrow / Common Crawl native Arrow view | `body` | bytes |
| PMC selected content representation | `content_url` | uri |
| CourtListener explicit document-URL view | `documents.opinion_file_url`, `documents.cluster_pdf_url` | uri |

Payload bytes remain their original Arrow binary type. WARC retains BinaryView
and its parser-owned buffers. These explicit markers require the companion
open-arrow-sink change (#259); version 0.6.4 does not recognize `bytes`/`uri`.
Binary/BinaryView normalization belongs to the sink, not the WARC parser.

Only selected content URLs are annotated: PMC's content_url chooses one format;
CourtListener document URLs require an explicit media root/accessor. Their text
values remain URLs until a blob-aware sink consumes them. Source metadata links
such as PMC's other format URLs, Gutenberg file URIs, DOI/website links and
original CourtListener paths stay ordinary metadata. URI markers declare external
content rather than inline text; the sink controls whether to ingest that content or retain its reference.

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
