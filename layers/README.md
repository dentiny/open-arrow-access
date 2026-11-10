# Accessor layers

`core::Layer<A>` and `AccessorExt::layer` compose wrappers while preserving an
accessor's request and reader types. The newest wrapper is outermost. Wrappers
must forward optional operations or remove their advertised capabilities.

## Available today

- The public layer contract, verified by test wrappers for byte and record accessors.
- `TypeConversionLayer` for explicit Arrow column casts; its facade feature is enabled by default.
- `decompression::DecompressionLayer<C>` for gzip, bzip2, LZ4, and Zstandard byte
  transformations. See the [decompression guide](../decompression/README.md).
- Caller-configured OpenDAL policies on the storage operator. These belong to
  OpenDAL's layer system, independently of this library's accessor layers.

The `TypeConversionLayer` is the production record layer in this crate.

## Type conversion

`TypeConversionLayer` is available through the default-enabled
`layer-type-conversion` feature. It wraps any
`Accessor<Reader = dyn RecordReader>` whose request implements
`ReadOptionsProvider`. An empty configuration and same-type conversions pass
through without converting arrays. Building a layer starts no I/O; casts are
planned once per `open`, then evaluated batch by batch. Unchanged arrays share
their existing buffers.

```rust,ignore
use arrow_schema::DataType;
use open_arrow_access::{AccessorExt, layers::{ColumnTypeSpec, TypeConversionLayer}};

let typed = source.layer(TypeConversionLayer::try_new(vec![ColumnTypeSpec {
    column: "count".into(),
    data_type: DataType::Int32,
}])?);
```

Specifications address top-level columns by exact name. Target types use Arrow's
`DataType`; a caller accepting strings can parse them with `str::parse`.
Malformed target type parameters fail at configuration; run-end encoded targets
are rejected because Arrow can panic when a null batch exceeds the run-end key
range. Decimal rescaling pairs that overflow Arrow 59.3's internal i8 arithmetic
are rejected at opening, including inside nested fields and dictionaries. This
restriction applies to the type pair even if particular values would fit.
Unsupported type pairs fail at opening, unknown/duplicate/empty column names are
errors, and invalid values or overflows terminate reading rather than becoming
null. Errors retain their source chains. Already-emitted batches are not rolled
back. Nested casts follow Arrow kernel behavior, including child-field mapping;
the supplied nested target type defines its child fields and metadata. Casts
that change extension fields, including nested ones, are rejected.

Conversion preserves row counts, order, top-level field names/nullability and
ordinary field/schema metadata. Arrow semantics are not a losslessness guarantee:
numeric casts and reduced timestamp precision may lose information without an
error. String-to-timestamp parsing uses Arrow's parser, with named timezones
enabled; the custom ISO-minute fallback from ml-infra is not included.

Casts run before the caller's final projection, including casts on hidden columns.
The inner projection includes only requested output columns and declared cast
columns; no unrelated fields are added. An empty final projection retains row
counts. Decoder/source schema overrides, cross-file schema reconciliation,
renaming, generated columns and writer-specific restrictions remain separate.

## Planned

| Layer | Purpose | Design / dependency |
| --- | --- | --- |
| Generated columns | Derived Arrow fields with dependency-aware projection | [Proposal](../docs/generated-columns.md); core already exposes `ReadOptionsProvider` |
| Metrics | Count byte operations, batches, rows, failures, and latency | Preserve values, metadata, and ordering |
| Timeout / read budgets | Bound operations and resource use | Cover sequential and ranged paths |
| Parallel reads | Concurrent files/partitions with bounded ordered merging | Validated source/partition plans |
| Unordered emission | Emit ready results when source order permits | Independent opt-in policy; ATIF requires global order |

See the [roadmap](../docs/roadmap.md) and
[source-attribute proposal](../docs/source-attributes.md). Planning-aware layers
must also wrap partition accessors so observation, generated columns, and budgets
remain active during execution.
