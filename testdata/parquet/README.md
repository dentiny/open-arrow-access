# Parquet test fixtures

These checked-in files are read by the integration tests. Tests may copy them
into a temporary storage root, but never write or encode Parquet records.

| File | Contents |
| --- | --- |
| `ordered-uncompressed.parquet` | Seven ordered records in four uncompressed row groups. |
| `ordered-snappy.parquet` | The same records and schema, with Snappy compression. |
| `bad-magic.parquet` | Plain text without Parquet magic bytes or metadata; decoding must fail. |
| `directory/a.parquet` | Non-nullable `id: Int64` values `1, 2`; schema metadata `label=same`. |
| `directory/b.parquet` | The same schema with value `3`. |
| `directory/c.parquet` | The same schema with value `4`. |
| `directory/hidden.parquet` | The same schema with value `99`, for dot-prefixed directory entries. |
| `ids.parquet` | Non-nullable `id: Int64` values `1, 2, 3`, with no schema metadata, for storage adapter tests. |

Both `ordered-*` files have schema `id: Int64` (non-nullable), `title: Utf8`
(nullable), and schema metadata `fixture=ordered`. The records are:

| id | title |
| --- | --- |
| 0 | zero |
| 1 | null |
| 2 | two |
| 3 | three |
| 4 | null |
| 5 | five |
| 6 | six |

The row-group sizes are `2, 2, 2, 1`. These files were generated once with
Arrow/Parquet 59.3.0 `ArrowWriter`, using
`WriterProperties::builder().set_max_row_group_row_count(Some(2))` and the
compression named in each file. Preserve the bytes when editing the repository;
the local `.gitattributes` disables text conversion for Parquet files.
The directory and storage adapter files were also generated once with
Arrow/Parquet 59.3.0 `ArrowWriter`, using uncompressed columns.

Run the filesystem and Parquet tests with:

```sh
cargo test --locked --features format-parquet --test reading
```
