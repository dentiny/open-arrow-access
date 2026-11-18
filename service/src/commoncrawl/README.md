# Common Crawl data hierarchy and formats

Common Crawl collects web captures and publishes named crawl releases. It makes
original captures and derived datasets available through `s3://commoncrawl/`
and `https://data.commoncrawl.org/`. A release is identified by a name such as
`CC-MAIN-2026-43`; the same URL can be captured in multiple releases.

## Crawl → segment → archive file → record

```text
s3://commoncrawl/crawl-data/
└── CC-MAIN-2026-43/                       # One crawl release
    ├── warc.paths.gz                     # Listing of WARC object keys
    ├── wat.paths.gz                      # Listing of WAT object keys
    ├── wet.paths.gz                      # Listing of WET object keys
    └── segments/
        ├── 1791170053823.21/              # One upstream crawl segment
        │   ├── warc/
        │   │   ├── CC-MAIN-...-00000.warc.gz
        │   │   │   ├── WARC record
        │   │   │   ├── WARC record
        │   │   │   └── ...
        │   │   ├── CC-MAIN-...-00001.warc.gz
        │   │   └── ...
        │   ├── wat/                      # Derived record metadata
        │   └── wet/                      # Derived extracted text
        └── ...                           # Other segments in this release
```

A segment is an upstream grouping identified in the object paths, not a single
archive file or a single web page. It contains many archive files. Do not infer
one domain, language, topic, date range or fixed page count from its ID. Select
its exact membership using the crawl's published file list and segment prefix.

For comparison, a PMC inventory shard is one compressed CSV index file naming
article metadata objects. A Common Crawl segment is a named group of archive
objects, each of which contains many records. The natural task units differ.

## What is in a paths file?

`warc.paths.gz` is a gzip-compressed text list, with one bucket-relative WARC
object key per nonempty line. It contains neither archive records nor page
bodies. An actual entry from the example release is:

```text
crawl-data/CC-MAIN-2026-43/segments/1791170053823.21/warc/CC-MAIN-20261005105623-20261005135623-00000.warc.gz
```

Prefix the key with `s3://commoncrawl/` or
`https://data.commoncrawl.org/` to retrieve the object. A segment selector filters
this inventory by the exact prefix:

```text
crawl-data/CC-MAIN-2026-43/segments/1791170053823.21/warc/
```

It does not require a separate `warc.paths.gz` inside each segment directory.
The path list is crawl-wide; the prefix chooses one segment's WARC objects.
WAT and WET have their own path lists and files.

## What is inside a WARC file?

A `.warc.gz` object is a compressed archive containing many physical WARC
records. Each record has WARC headers and a body. Important headers include
`WARC-Type`, `WARC-Record-ID`, `WARC-Date`, `WARC-Target-URI` and `Content-Length`.
The target URI identifies the captured resource; the record ID identifies the
archive record.

A `response` record body commonly contains an HTTP response, including HTTP
headers followed by the captured payload, often HTML. Other record types include
`request`, `metadata`, `warcinfo` and `revisit`. A revisit can refer to an earlier
capture rather than repeat its full payload. Therefore **one record is not
necessarily one page**, and one request/response/crawl-metadata group can contain
several records for a capture. File count, record count and page count differ.

WARC bodies are not rendered page screenshots or already-cleaned training text.
Linked images, CSS and other external resources are not guaranteed to be bundled
with each HTML capture.

## WARC, WAT and WET

| Dataset | Contents | Interpretation |
| --- | --- | --- |
| WARC (`warc/`, `warc.paths.gz`) | Original crawl records, such as HTTP requests/responses and capture metadata. | Suitable for preserving raw captures; text extraction is a later step. |
| WAT (`wat/`, `wat.paths.gz`) | Computed metadata, including JSON descriptions of HTTP/HTML information and links. | Not an interchangeable copy of the full page body, and not simply a generic JSONL file; records use WARC containers. |
| WET (`wet/`, `wet.paths.gz`) | Extracted plaintext in WARC conversion records. | Omits HTML structure and visual content; it is not automatically deduplicated or ready for training. |

These are related original/derived datasets. They need not contain identical
record types or equal record counts. Do not count all representations as
independent training documents without a deliberate data-processing policy.

## Sizes: what the inventory can tell us

Reading the published **CC-MAIN-2026-43** `warc.paths.gz` on **2026-11-18** gave:

| Level | Observed count |
| --- | ---: |
| WARC segments in this release | 100 |
| WARC objects across these segments | 100,000 |
| WARC objects in each segment of this example | 1,000 |

These are observed file-list counts, not permanent constants or page counts.
The example segment above has 1,000 WARC files; each file still contains many
records. The paths file does not report per-file record counts or compressed
byte sizes. Count records by reading the archives, or use an upstream statistic
whose definition matches the required count. Do not derive a record count from
1,000 files or from a release's advertised web-page count.

Use a pinned crawl ID and resolved file membership for repeatable work. New
crawl IDs represent further captures, not merely additional shards of the same
release. A resource may appear multiple times within/across releases; downstream
normalization, text extraction, filtering and deduplication remain separate steps.

## open-arrow-access boundaries

`CommonCrawlWarcRequest` selects the release's WARC inventory.
`CommonCrawlSegmentRequest::new(crawl, segment)` selects one exact segment from
that inventory. The corresponding accessors' `resolve` method returns selected
file paths; `open` reads the native physical-record Arrow view, or a supplied
record accessor. The native view preserves record headers and body bytes; it
does not automatically extract HTTP payloads or convert HTML to text.

The Common Crawl selectors target **WARC** membership. Their existence does not
add WAT metadata normalization or WET text-cleaning pipelines. Install gzip
handling for the paths inventory and archive bytes as described in the API guide.
Native scan planning can be composed with the existing execution layers; callers
choose task boundaries, output storage and record processing.

For one task per segment, keep the crawl ID and exact segment ID as task inputs.
A task can resolve all selected archive files and process their records. Archive
files can also be chosen as smaller task units. These are caller scheduling
choices; a segment does not imply a fixed output-file count.

See [Common Crawl API and examples](../../commoncrawl.md) and
[synthetic inventory fixtures](../../../testdata/commoncrawl/README.md).

## Official references

- [Common Crawl data access and crawl list](https://commoncrawl.org/get-started)
- [WARC/WAT/WET explanation](https://commoncrawl.org/blog/navigating-the-warc-file-format)
- [Example release's WARC paths](https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-43/warc.paths.gz)
