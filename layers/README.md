# Accessor layers

`core::Layer<A>` and `AccessorExt::layer` compose wrappers while preserving an
accessor's request and reader types. The newest wrapper is outermost. Wrappers
must forward optional operations or remove their advertised capabilities.

## Available today

- The public layer contract, verified by test wrappers for byte and record accessors.
- `decompression::DecompressionLayer<C>` for gzip, bzip2, LZ4, and Zstandard byte
  transformations. See the [decompression guide](../decompression/README.md).
- Caller-configured OpenDAL policies on the storage operator. These belong to
  OpenDAL's layer system, independently of this library's accessor layers.

This directory contains no production record or execution layer.

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
