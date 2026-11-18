# JSON / JSONL fixtures

Inputs use non-nullable `id: Int64` and nullable `title: Utf8`. They cover
whitespace-separated objects, incremental reading, projection, and invalid
syntax/types. Syntax is validated even with zero output columns.

| File | Case |
| --- | --- |
| `good.json` | Four whitespace-separated objects; UTF-8 text, escaped newline, CRLF, blank lines, a pretty-printed object, nested fields (declared in the test source schema), null/missing titles, and no final newline. |
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
| `pretty-objects.json.gz` | Three whitespace-separated objects, including pretty printing, an escaped quote/brace in a title, and a null title, for projected parallel gzip reading. |
| `union-id.json`, `union-id.json.gz` | One `{"id":1}` object, uncompressed and gzip-compressed, for inferred file-set union. |
| `union-empty-object.json`, `union-empty-object.json.gz` | One empty object, uncompressed and gzip-compressed; it contributes a row without declaring a field. |

These project-authored gzip inputs were prepared offline using Python
`gzip.compress(data, mtime=0)` (default compression level 9). Tests read the
committed bytes rather than encoding or corrupting inputs at runtime.

## Use these inputs

Facade feature: `format-json`. Run the owning component tests from the repository root:

```sh
cargo test --locked -p open-arrow-access-file-format --no-default-features --features json
```

Read fixtures directly or through the shared memory accessor. Preserve exact
bytes; the [fixture policy](../README.md) covers new cases and fault injection.
