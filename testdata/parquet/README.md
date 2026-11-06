# Parquet test fixtures

These checked-in files are read by the integration tests. Tests may copy them
into a temporary storage root, but never write or encode Parquet records.

| File | Contents |
| --- | --- |
| `ordered-uncompressed.parquet` | Seven ordered records in four uncompressed row groups. |
| `ordered-snappy.parquet` | The same records and schema, with Snappy compression. |
| `bad-magic.parquet` | Plain text without Parquet magic bytes or metadata; decoding must fail. |

Both valid files have schema `id: Int64` (non-nullable), `title: Utf8`
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

Run the filesystem and Parquet tests with:

```sh
cargo test --locked --features storage-fs,format-parquet --test reading
```
