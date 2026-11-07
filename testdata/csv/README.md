# CSV test fixtures

Small, checked-in files exercise the reader without downloading data during tests.
All fixtures use an `id: Int64, title: Utf8` schema.

| File | Case |
| --- | --- |
| `good.csv` | Header, CRLF record endings, quoted comma, null title, embedded LF, escaped quotes, and no final newline. |
| `good_no_header.csv` | Three valid records without a header or final newline. |
| `good.tsv` | Tab delimiter, header, UTF-8 text, quoted tab, and no final newline. |
| `empty.csv` | Zero-byte input. |
| `header_only.csv` | Header followed by LF, with no records. |
| `header_only_no_newline.csv` | Header without a final newline or records. |
| `good_truncated_rows.csv` | Missing nullable title, accepted when truncated rows are enabled. |
| `bad_missing_column.csv` | Too few columns under the default strict policy. |
| `bad_extra_column.csv` | Too many columns. |
| `bad_integer.csv` | Non-integer value in the `id` column. |
| `bad_utf8.csv` | Literal invalid UTF-8 byte `0xff` in the title. |
| `bad_header.csv` | Header name does not match the schema. |

Preserve the exact bytes, including line endings, missing final newlines, and
invalid UTF-8. The local `.gitattributes` disables line-ending conversion.

`file-format/src/csv/tests.rs` loads these files into the shared memory accessor
to control chunk sizes and inject I/O failures. Its filesystem composition tests
use a dev-dependency on `FsAccessor` to read the valid CSV and strict invalid
files through the filesystem-to-decoder path.

```sh
cargo test --locked -p open-arrow-access-file-format --no-default-features --features csv csv::tests::
```
