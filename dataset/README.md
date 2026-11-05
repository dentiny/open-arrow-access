# Dataset composition

The default-off `directory` crate feature supplies `DirectoryAccessor` and
`FileSetAccessor`. Both use the shared async accessor/reader contracts and
emit sequential, ordered batches with a fixed projected schema. They accept
any file format implementing the common single-file `ReadRequest` contract.

See [directory datasets](../docs/directory.md) for selection and schema rules.
The [source-attribute proposal](../docs/source-attributes.md) remains a future
planning extension; this implementation does not enable partition parallelism
or unordered emission.
