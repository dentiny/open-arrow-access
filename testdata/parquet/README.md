# Parquet fixtures

Generated with Arrow/Parquet 59.3.0 `ArrowWriter`. The `ordered-*` schema is
non-nullable `id: Int64`, nullable `title: Utf8`, with metadata `fixture=ordered`.
Both files have row-group sizes `2, 2, 2, 1` using a maximum row-group count of 2.
Directory/storage fixtures use uncompressed columns.

The `codec-*` fixtures were generated from the expected batch in
`file-format/tests/parquet_compression.rs` with Arrow/Parquet 58.4.1 `ArrowWriter`.
They use non-nullable `id: Int64`, nullable `text: Utf8`, no schema metadata, and
the same values listed below. `set_max_row_group_row_count(Some(3))` produces
row-group sizes `3, 3, 1`. Codec levels use the writer defaults.

| File | Contents |
| --- | --- |
| `ordered-uncompressed.parquet` | Seven ordered records in four uncompressed row groups. |
| `ordered-snappy.parquet` | The same records and schema, with Snappy compression. |
| `codec-brotli.parquet` | Seven records in three row groups with Brotli compression. |
| `codec-gzip.parquet` | The same codec records with Gzip compression. |
| `codec-lz4.parquet` | The same codec records with LZ4_RAW compression. |
| `codec-zstd.parquet` | The same codec records with Zstd compression. |
| `bad-magic.parquet` | Plain text without Parquet magic bytes or metadata; decoding must fail. |
| `directory/a.parquet` | Non-nullable `id: Int64` values `1, 2`; schema metadata `label=same`. |
| `directory/b.parquet` | The same schema with value `3`. |
| `directory/c.parquet` | The same schema with value `4`. |
| `directory/hidden.parquet` | The same schema with value `99`, for dot-prefixed directory entries. |
| `ids.parquet` | Non-nullable `id: Int64` values `1, 2, 3`, with no schema metadata, for storage adapter tests. |

| id | title |
| --- | --- |
| 0 | zero |
| 1 | null |
| 2 | two |
| 3 | three |
| 4 | null |
| 5 | five |
| 6 | six |

## Use these inputs

Facade feature: `format-parquet`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-file-format --no-default-features --features parquet
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
