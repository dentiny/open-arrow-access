# Generated columns

Status: required feature, designed here for a separate follow-up PR. The initial
framework only supplies the reusable contracts. This change does not implement
expression evaluation or enable DataFusion. The workspace only records an
unused dependency pin for the follow-up layer.

## Semantics

Implement `GeneratedColumnsLayer` on record accessors. It appends one column per
declaration using a row-level SQL expression, independently of storage and file
format. For example:

```json
[
  {"column": "text_length", "expression": "character_length(text)"},
  {"column": "doubled", "expression": "n * 2"},
  {"column": "quadrupled", "expression": "doubled * 2"}
]
```

- Keep the configuration shape `column` + `expression`. Names must be nonempty,
  distinct, and must not collide with any physical source field, including a
  field omitted from the requested output projection. Blank expressions fail.
- Evaluate declarations in order. Each sees source columns and earlier generated
  columns. Forward references and cycles fail rather than requiring reordering.
- Use DataFusion SQL identifier rules: unquoted identifiers are normalized;
  mixed-case or otherwise special names require quoting.
- Restrict evaluation to row-level expressions. Reject aggregates, windows,
  subqueries and volatile functions such as `random()`.
- Compile/type-check expressions once when opening a logical read, infer Arrow
  type/nullability, and expose the final schema before emitting any batch. Runtime
  data-dependent errors can still terminate the stream after earlier batches.
- Evaluate Arrow arrays batch by batch, preserving row counts, null semantics,
  logical order and existing schema/field metadata. Broadcast scalar expressions
  to the batch row count, including zero-row and zero-column inputs.
- Attach expression provenance to generated fields under the proposed
  `open_arrow_access:generated_expression` metadata key.

One evaluation context belongs to the logical scan. If stable functions that
depend on query time/configuration are supported, their context must be shared
across batches and future parallel partitions. Merely rejecting volatile
functions does not establish reproducibility across separate scans.

## Common contracts

The layer uses the existing `Layer<A>` / `Accessor` / `Reader` protocol:

```text
A::Reader = dyn RecordReader
A::Request: ReadOptionsProvider

open(request)
  -> prepare source-column selection
  -> inner.open(inner_request)
  -> compile expressions against the source schema
  -> return a RecordReader with the final output schema

next()
  -> inner.next()
  -> evaluate each expression in declaration order
  -> remove private inputs / apply final projection
  -> emit one batch with the declared output schema
```

`ReadOptionsProvider`, added in the framework PR, exposes immutable/mutable
common read options. `ReadRequest` implements it. Future dataset and service
requests can implement it while retaining their own selectors/configuration;
generated-column layers do not have to depend on file-only request types.

The existing `Layer` trait preserves request/reader *types*, not a fixed set of
Arrow fields. An appended schema remains `RecordMetadata`; transformed batches
remain `RecordBatch`. The wrapper must update metadata and emitted batches
together. No new method is needed on storage or byte readers for SQL evaluation.

## Projection and dependencies

Projection on a generated-column accessor selects the final augmented schema.
For `projection = ["text_length"]`, the inner reader must still supply `text`.
Generated field names must not be sent directly to a physical format reader.

Start the feature PR with a correctness-first path: retain the caller's final
projection, clear the inner projection, inspect the complete source schema,
compile/evaluate declarations, then project the final output. This validates
collisions against all physical fields and avoids premature dependency pruning.
The cost is reading unneeded physical columns; document that limitation.

An optimized follow-up may push down the union of visible source fields and
transitive expression dependencies. It also needs full-source schema knowledge
for collision/type validation. Do not infer dependencies with regexes or collect
SQL literals/comments as field references. Earlier generated names are computed
dependencies, not additional physical columns to read. AST parsing alone does
not supply the source schema or prove field availability.

## Source context and private fields

The reference implementation also allows expressions to use reader-provided
WARC locators, such as `__warc_path` and `__warc_offset`, while excluding those
inputs from public output. Preserve this ability as a separate optional source
context extension when the corresponding readers are implemented.

Required contract for that extension:

- Parse source-context dependencies before opening the reader, then explicitly
  request supported virtual fields. Unsupported/missing fields fail; do not
  substitute null or guess locator semantics.
- Mark reader-owned private fields and remove them after expression evaluation,
  unless an explicit public mapping exposes them. Unrelated source fields must
  not be hidden merely because imported schema metadata contains a reserved tag.
- Document locator identity and offset coordinates. A compressed frame offset
  shared by several records does not uniquely identify each record.

The current framework does not yet expose a virtual-field request API or WARC
reader. Add that optional extension together with a real source and tests;
`ReadOptionsProvider` alone does not supply private locator fields.

## Dependency and feature isolation

The planned public feature is `layer-generated-columns`, default-off. Register
it only when the follow-up implementation is usable. Keep expression types and
DataFusion dependencies inside the layer package; core remains Arrow-facing.
Use DataFusion directly through the workspace pin, aligned with Arrow and
OpenDAL to the same upstream Lance commit. Follow the
[dependency alignment policy](dependencies.md); importing through Lance would
not replace this requirement. No Lance writer, sink, catalog, or SQL query
service is needed.

This is a useful first production layer for the PR-6 workstream. Metrics can be
added separately. Parallel read and unordered emission remain independent
enhancements, and row-local generation must not depend on arrival order.

## Feature PR acceptance

Cover source expressions, earlier-generated dependencies, constants, nulls,
quoted identifiers, schema/field metadata, zero rows/columns, name collisions,
unknown/forward references, invalid expressions and rejected non-row/volatile
constructs. Include projections that select only generated fields and projections
that omit a colliding source name. Verify the same layer with multiple record
accessors, fixed-schema opening, terminal runtime errors, cancellation/drop and
unchanged output ordering. Test optional source-context fields when that
extension is actually introduced.
