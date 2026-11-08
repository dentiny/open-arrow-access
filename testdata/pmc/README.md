# Synthetic PMC article metadata

`metadata.json` is an invented article-version object with upstream fields,
content URLs, a string PMID, Unicode, unknown large numbers, and duplicate keys.
There are no content fixtures: the article service reads only the supplied JSON.
Tests also cover missing/null PDF URLs, manuscript and non-open-access flags,
optional fields, arbitrary URL metadata, and duplicate-field handling.

`metadata.json.gz` is the same JSON compressed with `gzip.compress(data, mtime=0)`
for composition and decoded-limit tests. All fixtures are synthetic and local.

Additional committed inputs replace the inline documents in the metadata tests:

| Fixture | Purpose |
| --- | --- |
| `metadata_only.json` | Manuscript metadata with no PDF URL. |
| `media_null.json`, `media_empty.json`, `media_populated.json` | Distinct null, empty, and populated URL lists. |
| `duplicate_fields.json` | Final duplicate PMID wins; empty DOI is preserved. |
| `bad_empty.json`, `bad_array.json`, `bad_truncated.json`, `bad_missing_pmcid.json`, `bad_utf8.json` | Empty input, wrong root, incomplete object, missing identifier, and invalid UTF-8 byte `ff`. |
| `bad_pmcid.json`, `bad_duplicate_pmcid.json` | Invalid PMC ID and a final duplicate null ID. |
| `bad_zero_version.json`, `bad_overflow_version.json` | Versions outside the supported positive 32-bit range. |
| `bad_doi.json`, `bad_manuscript.json`, `bad_media.json` | Wrong field or list-element types. |

`inventory/` contains the manifest and precompressed shards for inventory tests:

- `manifest.json` declares the five columns ETag, Key, Bucket, LastModifiedDate,
  FutureField and the first/second shards. The first shard repeats PMC1 twice;
  the second contains PMC2. Keys retain their original percent encoding.
- `bad_bucket.json`, `bad_timestamp.json`, `bad_schema.json`, and
  `bad_shard_path.json` change the source bucket, timestamp, schema (duplicate Key),
  and shard path (`../escape.csv.gz`), respectively.
- `empty_manifest.json` declares the same schema with no shards.
- `bad_bucket.csv.gz` and `bad_empty_key.csv.gz` contain a row with the wrong
  bucket or an empty key, even when all columns are projected away.
- `empty.csv.gz` is a valid gzip member with zero decoded bytes.

All inventory shards use `gzip.compress(csv_bytes, mtime=0)`. They are loaded
directly; tests never encode or alter their CSV/JSON contents at runtime.
