# JSON parsing semantics

The default-off `format-json` feature uses
[Arrow JSON 59.3.0's incremental decoder](https://docs.rs/arrow-json/59.3.0/arrow_json/reader/struct.Decoder.html).
Its parsing semantics are permissive: for example, `{"id":1,,}` is accepted.
This adapter does not add a strict JSON validator.

The supported input is a stream of JSON objects, including JSONL, with a
caller-provided fixed schema. Top-level arrays, null and scalar records are
rejected. Decoder-reported syntax/type errors and truncated records such as
`{"id":1` terminate reading with `ErrorKind::Decode`; subsequent reads return EOF.
Byte I/O errors retain their original category.

See [JSON and JSONL usage](../README.md#json-and-jsonl) for schema, projection,
batching and memory behavior.
