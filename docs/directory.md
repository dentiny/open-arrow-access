# Directory and file-set datasets

Enable `dataset-directory` on the facade (`directory` on the dataset crate).
The dataset crate depends only on core contracts and futures. Its readers do
not select formats or inspect file extensions to dispatch a parser.

```rust,no_run
use open_arrow_access::{
    Accessor, Result,
    dataset::{DirectoryAccessor, DirectoryRequest},
    file_format::ParquetAccessor,
    storage::FsAccessor,
};

async fn read_directory() -> Result<()> {
    let fs = FsAccessor::new("/absolute/data/root").await?;
    let dataset = DirectoryAccessor::new(fs.clone(), ParquetAccessor::new(fs));
    let mut request = DirectoryRequest::new("train");
    request.suffix = Some(".parquet".into());
    let mut reader = dataset.open(request).await?;
    while let Some(batch) = reader.next().await? {
        // Consume the batch.
        let _ = batch;
    }
    Ok(())
}
```

`FileSetAccessor::new(format)` accepts `FileSetRequest::new(objects)`. It preserves
the explicit object order and any duplicate selections. This is the reusable
boundary for providers that already resolve manifests into file references.
Both request types implement `ReadOptionsProvider` and forward the same Arrow
projection/batch options and per-open byte I/O settings to every child file.

`DirectoryAccessor::new(discovery, format)` expects root-relative `/`-separated
locators. It consumes `ByteAccessor::list`, filters entries and sorts selected
locators lexicographically, removing duplicates. Selection defaults to recursive,
excludes dot-prefixed components beneath the prefix, and accepts all suffixes.
Set `recursive = false`, `include_hidden = true`, or a case-sensitive `suffix`
explicitly as needed. No filename implies a file format. Invalid/out-of-prefix
listing references are errors, including dot/traversal components even when
hidden files are enabled.

Fs discovery uses OpenDAL's recursive lister and emits files only. Empty prefix
means the configured root. Directory prefix validation follows Fs path rules;
missing prefixes, file prefixes and listing failures produce errors. Fs listing
order is unspecified; the dataset's explicit lexicographic sort establishes its
scan order. Other backends can provide listing without changing the dataset.

Discovery completes before opening the first file. Locator memory is therefore
proportional to the selected file count. The listing does not establish an atomic
snapshot; inputs must stay unchanged while read. Listing errors fail opening
before any child files or rows are read.

The first file is opened to obtain output metadata. Every later file is opened
lazily, after the previous reader is dropped. Its projected schema must equal
the first schema exactly, including field and schema metadata. Every emitted
batch is checked against that fixed schema. Empty files are skipped; an empty
selection is `InvalidInput` because no schema can be established. No union-schema
inference, implicit casts, nullable widening or schema preflight is performed.
A later mismatch can terminate an otherwise partially consumed scan.

Only one child reader is retained at a time. Rows remain in file order and in
each file's native order; file boundaries need not align with batch targets.
EOF and errors are terminal. Dropping the reader drops the current child; no
producer tasks or prefetch workers are created. Reader source metadata is `None`
for the multi-file composition rather than claiming a single source object.

This implementation follows the provider/discovery separation in the user's
[`ml-infra` directory adapter](https://github.com/Adcock-AGI-Inc/ml-infra/blob/main/lance/lance_migration/crates/inspector/src/directory.rs).
It reuses OpenDAL for filesystem traversal and accepts existing record adapters;
it does not copy Lance-specific schema union or format-priority rules.
