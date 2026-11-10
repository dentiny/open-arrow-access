# JSON and JSONL test fixtures

Tests read these checked-in files directly or pass their bytes to the memory
accessor to control chunk sizes and inject read failures. No JSON fixture is
constructed or written during a test run.

The input schema is `id: Int64` (non-nullable), `title: Utf8` (nullable).

| File | Case |
| --- | --- |
| `good.json` | Four whitespace-separated objects; UTF-8 text, escaped newline, CRLF, blank lines, a pretty-printed object, unknown nested fields, null/missing titles, and no final newline. |
| `projection.jsonl` | Two records with a missing nullable title, for column and zero-column projections. |
| `many.jsonl` | One hundred ordered records, for verifying bounded incremental reads. |
| `io-failure.jsonl` | Two records; the memory accessor injects an I/O failure after the first nine bytes. |
| `empty.json` | Zero-byte input. |
| `whitespace.json` | Only CRLF, space and tab bytes. |
| `bad-empty-array.json` | An empty top-level array. |
| `bad-null.json` | A top-level null. |
| `bad-truncated-object.json` | An object missing its closing brace. |
| `bad-trailing.json` | A valid object followed by invalid trailing text. |
| `bad-type.json` | A string where the schema requires an integer. |
| `bad-utf8.json` | Literal invalid UTF-8 byte `0xff` in the title string. |

Syntax errors are rejected even with zero-column projections; selected fields
also validate types and UTF-8. Preserve the fixture bytes,
including invalid UTF-8, line endings, and missing final newlines. The local
`.gitattributes` disables text conversion and permits intentional whitespace.

```sh
cargo test --locked --features format-json --test json
cargo test -p open-arrow-access-file-format --locked --features json --test json
```
