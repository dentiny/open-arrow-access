# PMC data model and formats

PMC (PubMed Central) is NIH/NLM's archive of biomedical and life-sciences
articles, including publisher-supplied articles and author manuscripts. The
public `pmc-oa-opendata` S3 bucket distributes article metadata and available
content. It supports anonymous access in `us-east-1`; source access does not
require our destination-storage credentials.

## Article identity and content hierarchy

The storage unit is an **article version**, not a content format. For example,
`PMC10009416.1` means article `PMC10009416`, version `1`. Another version of the
same article has a different suffix.

```text
s3://pmc-oa-opendata/
├── PMC10009416.1/
│   ├── PMC10009416.1.json   # Article metadata and content URLs
│   ├── PMC10009416.1.pdf    # PDF, when available
│   ├── PMC10009416.1.xml    # Structured full text (JATS)
│   ├── PMC10009416.1.txt    # Plain text extracted from XML
│   └── ...                 # Available figures and supplementary files
├── metadata/
│   ├── PMC10009416.1.json   # Metadata also collected under this prefix
│   └── ...
└── inventory-reports/pmc-oa-opendata/metadata/
    ├── <inventory-date>/manifest.json
    └── data/<shard-id>.csv.gz
```

Always use the content URLs in metadata rather than constructing filenames:
content availability and filenames can differ between article versions.

## Inventory → shard → record → article

### How PMC/S3 produces shards

PMC publishes an Amazon S3 Inventory report for the metadata objects. S3 writes
that listing into gzip CSV files, often called inventory **shards** or **parts**.
A dated `manifest.json` enumerates the files belonging to that report in `files[]`.
The partitioning is applied to the **listing records**, not to the PDFs or article
content themselves.

The documented interface is the manifest's explicit file list. PMC does not
publish a rule assigning article IDs, dates, journals, or topics to particular
shards, or guarantee a fixed row count/file size. AWS does not guarantee inventory
record sort order. Do not infer a PMCID interval from the first/last record,
construct shard names, or assume a shard's membership is stable across daily
reports. Select `(manifest key, shard key)` as one task input.

Each daily report lists the metadata objects covered by that inventory, not just
that day's newly added articles. A new daily report does **not** mean shard
boundaries must change every day: file count, shard keys and record distribution
may change or remain similar. There is no cross-day stability guarantee. In
particular, "shard 0" is only a position in one manifest, not a permanent group
of articles. Pin the dated manifest and the exact member key for each task.

These are listing-file boundaries, unlike a Common Crawl crawl segment's own
identity. They are still useful independent work units: read just the selected
CSV, then fetch the metadata objects named by its rows.

### What is inside each shard?

A shard is one `.csv.gz` object, **not a directory, TAR archive or set of embedded
article files**. Decompress it to get CSV records. A record lists one metadata
object for one article version:

| CSV field | Meaning |
| --- | --- |
| `Bucket` | The source bucket, `pmc-oa-opendata`. |
| `Key` | The metadata object's key, such as `metadata/PMC5000000.1.json`. CSV keys are URL-encoded; decode once before fetching. |
| `LastModifiedDate` | The metadata object's last-modified timestamp, not the article publication date. |
| `ETag` | The metadata object's S3 ETag, not the PDF's checksum. |

Use the manifest's `fileSchema` for column order. The raw inventory record has
no article title, full text, PDF bytes or content URL. Those are reached by fetching
the metadata JSON and then following the selected content URL.

```text
2026-11-18T01-00Z/manifest.json
└── files[] lists 11 gzip CSV objects
    ├── data/13cbabe4-91e8-4d22-977f-adc2fc7eb6cb.csv.gz
    │   ├── CSV record → metadata/PMC5000000.1.json
    │   │                ├── Typed article metadata
    │   │                └── pdf_url / xml_url / text_url → separate content object
    │   ├── CSV record → metadata/PMC5000001.1.json
    │   ├── CSV record → metadata/PMC5000002.1.json
    │   └── ... 840,670 records in this shard
    ├── data/35a3e3a9-aab9-4c3d-9867-ea68bf400a6b.csv.gz
    │   └── ... 907,869 records
    └── ... nine other CSV objects
```

Here `data/...` abbreviates the bucket-relative prefix
`inventory-reports/pmc-oa-opendata/metadata/data/`. The three article keys above
were read from the first shard. For the **2026-11-18** report, reading all CSV
files yielded **11 shards / 9,509,797 metadata records**. This is a dated
observation, not a fixed dataset size or a count advertised in the manifest.
Per-shard record counts require reading the CSV files; `files[].size` describes
the compressed inventory object size, not PDF volume.

An article version's metadata JSON is available in both the article folder and
`metadata/`; these are two locations for the same metadata, not two articles to
convert. This inventory lists the `metadata/` copy. PDF/XML/TXT are representations
of the same article version, not separate inventory items in this report.

For conversion, a shard task reads its inventory records, fetches the referenced
JSON objects, selects a content URL, and writes rows to its own destination.
It must not scan all of `metadata/` for every task. PDF availability varies, so
metadata record count is not PDF count.

See [AWS S3 Inventory's file/record contract](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html)
and the [dated PMC manifest](https://pmc-oa-opendata.s3.amazonaws.com/inventory-reports/pmc-oa-opendata/metadata/2026-11-18T01-00Z/manifest.json).

## PDF, XML, TXT and JSON

These describe the same article version in different ways, not four articles.
They need not be byte-for-byte or textually identical.

| Format | What it contains | What it does not preserve by itself |
| --- | --- | --- |
| PDF | Usually text, figures and tables together with page layout; useful for visual/document processing. Scanned PDFs may require OCR. | A reliable semantic structure for sections, tables and references. |
| XML (JATS) | Structured full text: titles, sections, paragraphs, tables, formulas, captions and references. Figures/media are commonly referenced as separate files. | The PDF's exact page layout, or every referenced image as embedded bytes. |
| TXT | Plain text extracted from XML; may include captions and text representations of tables/formulas. | Images, page layout and XML structure/markup. |
| JSON metadata | Article identity, descriptive fields, status/license fields and content URLs. | The article's full text or PDF bytes. |

Do not automatically ingest every format or count each format as a separate
training example. PDF is useful when layout and figures matter; XML is useful
for structured text processing; TXT is derived from XML. Preserve metadata and
content URLs so additional formats can be obtained later when needed.

## Arrow output schema

The schema proposed in [open-arrow-access #554](https://github.com/dentiny/open-arrow-access/pull/554)
has **17 top-level columns**: all 16 upstream metadata fields, each stored once,
and the derived `content_url`. There is no duplicate `metadata` struct or JSON
string column. This schema requires upgrading beyond released 0.8.2, which still
has the nested struct and spells the top-level TXT URL column `text`.

| Field | Arrow type | Nullable | Value |
| --- | --- | --- | --- |
| `pmcid` | `Utf8` | No | PMC article identifier. |
| `version` | `UInt32` | Yes | Article version; positive when provided. |
| `pmid` | `UInt64` | Yes | Numeric PubMed identifier. |
| `doi` | `Utf8` | Yes | DOI string. |
| `title` | `LargeUtf8` | Yes | Article title. |
| `citation` | `LargeUtf8` | Yes | Upstream formatted bibliographic citation string. |
| `is_pmc_openaccess` | `Boolean` | Yes | Open Access Subset flag. |
| `is_manuscript` | `Boolean` | Yes | Author-manuscript flag. |
| `is_historical_ocr` | `Boolean` | Yes | Historical back-scanning/OCR flag. |
| `is_retracted` | `Boolean` | Yes | Retraction flag. |
| `license_code` | `Utf8` | Yes | Upstream license code. |
| `xml_url` | `Utf8` | Yes | URL of the article's JATS XML. |
| `pdf_url` | `Utf8` | Yes | PDF URL, if available. |
| `media_urls` | `List<Utf8>` | Yes | URLs of figures and supplementary files. |
| `text_url` | `Utf8` | Yes | URL of the TXT representation, not its text content. |
| `mid` | `Utf8` | Yes | Manuscript identifier, when provided. |
| `content_url` | `Utf8` | Yes | Selected PDF/XML/TXT URL; PDF by default. |

For example, `citation` may be `Edinb Med J. 1868 May;13(11):1015–42.`.
It is the upstream bibliographic string, not an array of cited papers. Both
`Utf8` and `LargeUtf8` are string types.

Missing optional fields become null. `media_urls` may be null or empty; provided
elements must be strings. Its Arrow item field is nullable, but the decoder
rejects null list elements. Unknown upstream fields fail decoding rather than
being silently dropped.

A sink can ingest `content_url` as a blob while preserving `pdf_url`, `xml_url`
and `text_url` as the original URLs. The reader itself does not fetch content.

## Updates and reproducibility

PMC is **not static**. Article versions are added and updated continuously, and
occasionally removed. Inventories are regenerated daily and can lag live bucket
contents. Reports are retained upstream for 30 days.

Pinning a manifest and shard fixes the selected object keys, not immutable
article bytes: a referenced JSON/content object can change afterward. Archive
the inventory inputs when tasks must be replayed after the retention window.

## How open-arrow-access reads PMC

- `PmcAccessor` reads one metadata JSON into one typed Arrow row.
- `PmcDatasetAccessor` accepts an exact JSON key or a metadata directory prefix
  such as `metadata/`, streaming article discovery.
The inventory-shard additions below are proposed in [#553](https://github.com/dentiny/open-arrow-access/pull/553)
and are not part of the released 0.8.2 API yet.

- `PmcInventoryAccessor` reads inventory rows. `shards` discovers shard keys
  without reading CSV files; `with_shard` selects one exact manifest member.
- `PmcInventoryShardAccessor` takes a `PmcInventoryShardRequest` containing a
  fixed manifest and shard key. It streams the referenced metadata JSON objects,
  exposes one native scan partition per metadata object, and works with
  `ParallelReadLayer`.

Article output follows the schema described above after #554 is integrated.
The reader does not download PDFs, XML, text or media. URL encoding and query
parameters are preserved in content URLs.

Format selection (`PmcFormat`, default PDF, selecting a shared `content_url`)
is proposed separately in [#554](https://github.com/dentiny/open-arrow-access/pull/554);
this is also not in released 0.8.2 yet.

A downstream sink can ingest selected URL columns as blobs. Keeping metadata
and a chosen content format in the same row preserves article identity without
creating separate datasets for PDF/XML/TXT variants. Saving these originals is
an ingestion step; text extraction, cleaning, deduplication and tokenization are
subsequent processing.

See [PMC API and examples](../../pmc.md),
[inventory API](../../pmc-inventory.md), and
[fixture documentation](../../../testdata/pmc/README.md).

## Official references

- [Bucket README: structure, access, inventory and update policy](https://pmc-oa-opendata.s3.amazonaws.com/README.txt)
- [PMC cloud service and available content](https://pmc.ncbi.nlm.nih.gov/tools/cloud/)
- [Example metadata JSON](https://pmc-oa-opendata.s3.amazonaws.com/metadata/PMC10009416.1.json)
- [2026-11-18 inventory manifest](https://pmc-oa-opendata.s3.amazonaws.com/inventory-reports/pmc-oa-opendata/metadata/2026-11-18T01-00Z/manifest.json)
