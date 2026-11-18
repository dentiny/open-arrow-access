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

An inventory is a file listing, separate from the article metadata and content:

```text
A dated inventory manifest
└── files[]: gzip CSV shard keys
    └── Each CSV record: Bucket, Key, LastModifiedDate, ETag
        └── Key → metadata/PMC10009416.1.json
            ├── Article metadata: IDs, title, citation, license/status fields, ...
            └── Content URLs: pdf_url, xml_url, text_url, media_urls
```

The manifest declares CSV column order in `fileSchema`; readers must use that
schema rather than assume one fixed order. Inventory `Key` values can be
URL-encoded. They identify metadata objects, not PDF objects.

The **2026-11-18** inventory contained **11 CSV shards and 9,509,797 metadata
records** when counted. This is a dated observation, not a fixed dataset size.
Record counts were obtained by reading CSV shards; the manifest does not supply
per-shard record counts. Eleven shards means eleven compressed index files,
not eleven PDF archives. The article JSON and content remain individual objects.
Metadata record count is not PDF count, because PDF availability varies.

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
- `PmcInventoryAccessor` reads inventory rows. `shards` discovers shard keys
  without reading CSV files; `with_shard` selects one exact manifest member.
- `PmcInventoryShardAccessor` takes a `PmcInventoryShardRequest` containing a
  fixed manifest and shard key. It streams the referenced metadata JSON objects,
  exposes one native scan partition per metadata object, and works with
  `ParallelReadLayer`.

Article output includes mapped columns and a complete typed `metadata` struct.
The reader does not download PDFs, XML, text or media. The top-level Arrow `text`
column is the source `text_url` value, not extracted article text; the nested
`metadata.text_url` retains the original source field name. URL encoding/query
parameters are preserved in content URLs.

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
