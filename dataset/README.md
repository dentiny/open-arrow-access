# Multi-file datasets

Enable `dataset-directory` on the facade (`directory` on this crate) to combine
files through a supplied single-file Arrow accessor.

| Accessor | Selection | Order |
| --- | --- | --- |
| `DirectoryAccessor` | Prefix, optional case-sensitive suffix, recursion setting | Lexicographic paths, deduplicated |
| `FileSetAccessor` | Explicit nonempty object list | Caller order, duplicates retained |

Both expose the shared async accessor/reader contracts and read files lazily and
sequentially. The first file fixes the projected schema; every later file must
match exactly, including schema metadata. Directory
discovery collects selected paths to establish order before opening the first file.
Recursion defaults to true. Prefixes and listed locators are backend-defined and
passed through unchanged.

Use `with_options` for batch rows/projection and `with_byte_options` for per-file
I/O settings. Empty file selections are invalid; an empty file with a known schema
is supported. File-set output metadata has no single source object.

See [multi-shard examples](../docs/usage.md#read-multiple-shards) and
[architecture](../docs/architecture.md). Install
[`ParallelReadLayer`](../docs/parallel-read.md) for bounded file-level concurrency
with the same output order. Unordered emission remains planned.

## Union by name

Call `.with_union_by_name()` on `FileSetAccessor` or `DirectoryAccessor` to union
compatible file schemas before emitting any rows. This works with inferred JSONL
schemas and Parquet schemas, and composes with `.with_file_planning()` and the
existing parallel/unordered execution layers.

```rust,ignore
let files = FileSetAccessor::new(JsonAccessor::infer(storage))
    .with_union_by_name();
let reader = files.open(FileSetRequest::new(["a.jsonl", "b.jsonl"])).await?;
```

Fields retain first-seen order. Struct children are combined by name, recursively
through List, LargeList, list views and equal-size FixedSizeList elements. Missing
fields become nullable and receive null arrays; existing parent/list validity,
offsets and row counts are retained. Fields present everywhere keep their original
nullability unless a source already permits null. Null-only types may acquire a
concrete type from another file. Other differing types, list element names, and
conflicting field/schema metadata fail preflight; there is no implicit numeric or
string cast. Map, dictionary and other compound types must match exactly.

Union-by-name mode opens/plans each file in sequence to inspect its full schema,
then drops the reader/plan before inspecting the next. Actual reads reopen files lazily; JSON
inference may sample each file twice. This bounds retained preflight resources
independently of file count, but still stores the selected requests and schemas.
Sources must remain stable throughout the operation. Reopened reader/partition
metadata and every emitted batch must match the inspected source schema; drift
fails the stream instead of changing its fixed output schema.

Projection is resolved against the complete union, so a selected column may be
absent from individual files. The union-by-name path reads complete file schemas
and columns before alignment/projection; conflicts in unselected columns still
fail preflight. An empty projection retains row counts. Duplicate file selections
and declared file/record order are preserved. Schema inference failures, including
empty JSON inputs without an explicit schema, remain the format reader's policy.

Union by name combines decoded file outputs; each format retains its
reading-schema and unknown-field policy. It does not recover JSON keys omitted by that file's
inference sample, nor widen its decoder schema using samples from other files.
