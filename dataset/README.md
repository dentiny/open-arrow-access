# Multi-file datasets

Enable `dataset-directory` on the facade (`directory` on this crate) to combine
files through a supplied single-file Arrow accessor.

| Accessor | Selection | Order |
| --- | --- | --- |
| `DirectoryAccessor` | Prefix, optional case-sensitive suffix, recursion setting | Lexicographic paths, deduplicated |
| `FileSetAccessor` | Explicit nonempty object list | Caller order, duplicates retained |

Both expose the shared async accessor/reader contracts and read files lazily and
sequentially. The first file fixes the projected schema; every later file must
match exactly, including schema metadata. They do not merge schemas. Directory
discovery collects selected paths to establish order before opening the first file.
Recursion defaults to true; repeated/trailing prefix separators are normalized.

Use `with_options` for batch rows/projection and `with_byte_options` for per-file
I/O settings. Empty file selections are invalid; an empty file with a known schema
is supported. File-set output metadata has no single source object.

See [multi-shard examples](../docs/usage.md#read-multiple-shards) and
[architecture](../docs/architecture.md). Partition parallelism and unordered
emission are [planned](../docs/roadmap.md#planning-parallelism-and-order).
