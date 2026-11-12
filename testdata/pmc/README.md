# PMC metadata fixtures

Synthetic article-version documents and inventory manifests; no article
content or live downloads. `metadata.json` includes content URL fields, a string
PMID, Unicode, unknown large numbers, and duplicate keys.
`metadata.json.gz` is the same document compressed with a zero gzip timestamp.

| Fixture | Purpose |
| --- | --- |
| `metadata_only.json` | Manuscript metadata with no PDF URL. |
| `media_null.json`, `media_empty.json`, `media_populated.json` | Distinct null, empty, and populated URL lists. |
| `duplicate_fields.json` | Final duplicate PMID wins; empty DOI is preserved. |
| `bad_empty.json`, `bad_array.json`, `bad_truncated.json`, `bad_missing_pmcid.json`, `bad_utf8.json` | Empty input, wrong root, incomplete object, missing identifier, and invalid UTF-8 byte `ff`. |
| `bad_pmcid.json`, `bad_duplicate_pmcid.json` | Invalid PMC ID and a final duplicate null ID. |
| `bad_zero_version.json`, `bad_overflow_version.json` | Versions outside the supported positive 32-bit range. |
| `bad_doi.json`, `bad_manuscript.json`, `bad_media.json` | Wrong field or list-element types. |

## Inventory inputs

`inventory/manifest.json` declares ETag, Key, Bucket, LastModifiedDate, and
FutureField. The first shard repeats PMC1 twice; the second contains PMC2.
Keys retain their original percent encoding.

| Input under `inventory/` | Case |
| --- | --- |
| `bad_bucket.json`, `bad_timestamp.json` | Incorrect source bucket or timestamp. |
| `bad_schema.json`, `bad_shard_path.json` | Duplicate Key column or `../escape.csv.gz` shard. |
| `empty_manifest.json` | Valid declared schema with no shards. |
| `bad_bucket.csv.gz`, `bad_empty_key.csv.gz` | Invalid row bucket/key even with zero-column projection. |
| `empty.csv.gz` | Valid empty gzip member. |

Inventory shards use `gzip.compress(csv_bytes, mtime=0)`.

## Use these inputs

Facade feature: `service-pmc`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-service --no-default-features --features pmc
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
