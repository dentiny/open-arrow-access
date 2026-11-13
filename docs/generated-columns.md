# Generated columns

`GeneratedColumnsLayer` appends Arrow fields using row-level DataFusion SQL
expressions. It is always available through `open_arrow_access::layers` and
`open-arrow-access-layers`, including with `default-features = false`.
Core remains Arrow-only; the layers crate depends on DataFusion.

```rust,ignore
use open_arrow_access::{AccessorExt, layers::{GeneratedColumnSpec, GeneratedColumnsLayer}};

let generated = source.layer(GeneratedColumnsLayer::try_new(vec![
    GeneratedColumnSpec {
        column: "text_length".into(),
        expression: "character_length(text)".into(),
    },
    GeneratedColumnSpec {
        column: "doubled".into(),
        expression: "text_length * 2".into(),
    },
])?);
```

## Semantics

Declarations run in order. Each sees source fields and earlier generated fields;
forward references and cycles are errors. Names are exact, nonempty and distinct,
and may not collide with any source field, even one hidden by output projection.
Blank expressions fail at construction; SQL/schema errors fail at `open`.
Unquoted SQL identifiers are lowercased; quote mixed-case names: `"Count" * 2`.

Aggregates, windows, subqueries, `unnest`, and volatile functions such as
`random()` are rejected before simplification, including inside dead branches.
The enabled built-ins include core, string, Unicode, datetime and math functions.
Type coercion, expression simplification, casts, and inferred types/nullability
follow DataFusion semantics.
Each logical open uses one query context, so stable functions such as `now()` agree across declarations and
batches; separate opens need not produce identical values.

Output preserves row count, batch boundaries, logical order, source identity,
source field metadata and schema metadata. Scalar results broadcast to the batch
row count, including zero-row and zero-column inputs. Generated fields retain
inferred metadata (including extension metadata for identity expressions) and add
`open_arrow_access:generated_expression` with the original expression text.

Runtime data errors retain their source chain and column name. An error ends the
reader and releases its input immediately; previously emitted batches remain
emitted. EOF is also terminal. The layer starts no producer tasks. After cancelling
a pending `next`, drop the reader, following the core reader contract.

## Design and projection

The wrapper works with any `Accessor<Reader = dyn RecordReader>` whose request
implements `ReadOptionsProvider`; it does not depend on formats or storage.

1. Empty configuration delegates directly, without an expression context.
2. Retain the final projection. With a supplied complete source schema and an
   output projection, select the source columns needed by all declarations and
   the requested output; otherwise, clear the inner projection before opening.
3. Compile declarations once against source fields plus earlier outputs, using
   each expression's dependency schema. Coerce and simplify before physical
   planning. Pushdown reuses this plan and remaps only source-column slots to
   the selected reader; generated slots and physical expressions stay unchanged.
4. Expose the fixed augmented/projected schema before reading a batch.
5. Evaluate physical expressions on Arrow arrays, then apply the final projection.

Source arrays share their buffers. Each declaration evaluates a compact view of
its dependencies and stores its result in a column slot. Hidden results are
validated as they are produced; the final projected batch is assembled once.
Memory is bounded by one input batch, its generated arrays and expression/kernel
state. There is no DataFrame execution plan, materialized table or per-row SQL.

For dependency projection pushdown, supply the complete source schema with
`with_source_schema` and request an output projection. Dependencies come from the
parsed expressions, including references through earlier generated columns.
Declarations are checked against the complete schema before opening, and the
selected reader schema is checked at open. Without both settings, a nonempty
configuration reads every source column.

Every declaration still runs, including outputs omitted by the final projection.
Pushdown reduces source I/O and decoding, not the set of expressions evaluated.

For internal parallel reads, compose in this order:
`source.layer(ParallelReadLayer::new(...)).layer(generated_layer)`.
Generation runs once over the ordered merged batches, sharing one query context.
The generated wrapper forwards internal read concurrency but does not implement
`ScanPlanner`, so placing `ParallelReadLayer` outside it is not supported.
