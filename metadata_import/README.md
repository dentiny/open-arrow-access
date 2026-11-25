# Metadata imports

Standalone DuckDB SQL used to prepare the BHL and arXiv metadata tables.
The scripts were prepared for DuckDB v1.5.6. Run each against a **new** database
with `-bail`; they create tables and are not rerunnable on an existing import.
Run these commands from the repository root.

## BHL

Install the `httpfs` extension once, then import the 13 public gzip-compressed
TSV files directly from the BHL AWS bucket:

```sh
duckdb -bail -c 'INSTALL httpfs;'
duckdb -bail /path/to/BHL-metadata.duckdb < metadata_import/BHL/dump-bhl-metadata.sql
```

All source columns remain `VARCHAR`, including IDs and empty strings; TSV quote
parsing is disabled. The script also builds the version-1 `__bhl_lookup_*`
derived tables and indexes. To import only the raw tables, stop after the first
`CHECKPOINT`, before the optional lookup-profile section. The lookup profile
requires additional disk space and memory; rebuild it if the raw data changes.
Raw import preserves source order before lookup tables capture row ordinals.
The lookup rows are clustered by numeric key and retain lexical values and duplicates.
Each derived table commits before its ART index is bulk-built; the version marker
is committed last. A failed build can leave partial objects: discard the unpublished
file before retrying. The defaults are an 8 GiB DuckDB memory budget and two threads;
allow additional process memory and disk for sorting.
The public source URLs are live and do not pin an atomic snapshot.

This standalone SQL does not create the `__bhl_metadata` provenance header
written by `BhlMetadata::create`, so its output is not a replacement for that
API when using `BhlMetadata::open` with header validation.

## arXiv

Download and extract the Cornell/Kaggle arXiv JSON Lines metadata snapshot
(`arxiv-metadata-oai-snapshot.json`). The original import used dataset version
313. Replace `/path/to/arxiv-metadata-oai-snapshot.json` in the SQL with the
local extracted file path, then run:

```sh
duckdb -bail /path/to/arxiv.duckdb < metadata_import/arxiv/ingest-arxiv-metadata.sql
```

The script creates `arxiv_metadata_raw` (complete original JSON),
`arxiv_metadata` (normalized lookup IDs and nested OAI-shaped metadata), and
`arxiv_oai_records` (the two OAI record columns). It preserves lexical IDs and
normalizes legacy slashes only in the lookup key. The input is a partial
OAI-shaped snapshot: unavailable header values, deletion coverage, and version
size/source type are not reconstructed. No OAI requests are made. Arrow readers
must bind the resulting columns to the library schema, including nullability
and list element names.

Only SQL and documentation belong here; keep downloaded snapshots and generated
DuckDB databases outside the repository.
