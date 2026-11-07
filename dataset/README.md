# Dataset composition

The default-off `directory` crate feature supplies `DirectoryAccessor` and
`FileSetAccessor`. Both use the shared async accessor/reader contracts and
emit sequential, ordered batches with a fixed projected schema. They accept
any file format implementing the common single-file `ReadRequest` contract.

File sets use the shared `StreamReader` for metadata and terminal behavior;
their local state retains lazy file opening and per-file/per-batch schema checks.


The [source-attribute proposal](../docs/source-attributes.md) remains a future
planning extension; this implementation does not enable partition parallelism
or unordered emission.
