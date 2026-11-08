# Hugging Face export fixtures

Project-authored local API response examples, using the pinned commit of forty
`a` characters and the Parquet files already committed under `../parquet/`.
No credentials or upstream dataset content are included.

| Fixture | Purpose |
| --- | --- |
| `revision.json` | Valid revision response with a commit SHA. |
| `bad_truncated_revision.json` | Incomplete JSON object. |
| `bad_revision_sha.json` | A branch name instead of an immutable commit SHA. |
| `bad_oversized_revision.json` | Exactly 65,537 spaces with no JSON value. |
| `exports/train.json` | One complete training shard. |
| `exports/paginated.json` | Two training shards, a README, and an unrelated test split. |
| `exports/partial.json` | Only a partial export. |
| `exports/complete.json` | Complete and partial exports with different row values. |
| `exports/empty.json` | No exported files. |

Export entries pair a `fixture` path relative to `testdata/parquet/` with the
`metadata` returned by the API. The test transport verifies the stored size against
the file, serves only the metadata object, and reverses/paginates the selected
entries to exercise ordering. HTTP status and I/O behavior remain test controls.
