# Hugging Face fixtures

Project-authored API examples using a pinned revision of forty `a` characters
and the committed [Parquet files](../parquet/README.md). No credentials or upstream
dataset bodies are included.

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

Export entries pair a `fixture` path relative to `testdata/parquet/` with API
`metadata`. The transport verifies sizes, serves metadata, and reverses/paginates
entries to exercise ordering. Status and I/O faults are test controls.

## Use these inputs

Facade feature: `service-huggingface`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-service --no-default-features --features huggingface
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
