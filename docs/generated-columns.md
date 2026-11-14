# Generated columns

`SchemaEvolutionLayer` adds computed Arrow fields using row-level DataFusion
SQL or `Expr` inputs alongside ordered remove, rename and cast operations. It is
always available through `open_arrow_access::layers` and
`open-arrow-access-layers`, including with `default-features = false`.
Core remains Arrow-only; the layers crate depends on DataFusion.

```rust,ignore
use open_arrow_access::{AccessorExt, layers::{ColumnEvolution, SchemaEvolutionLayer}};

let generated = source.layer(SchemaEvolutionLayer::try_new(vec![
    ColumnEvolution::add_column_sql("text_length", "character_length(text)"),
    ColumnEvolution::add_column_sql("doubled", "text_length * 2"),
])?);
```

Use `add_column_expr(name, expr)` to infer the output field from a DataFusion
expression, including custom scalar UDFs. `add_column(name, data_type, expr)`
checks an explicit output type and declares the added field nullable.
All three forms use the same dependency planning and batch execution.

## Semantics

Changes run in order. Each sees the schema left by preceding additions, renames,
removals and casts; forward references and cycles are errors. Names are exact and
must not be blank. Additions may not collide with any currently live field, even
one hidden by output projection. Removed names may be reused.
Blank expressions fail at construction; SQL/schema errors fail at `open`.
Unquoted SQL identifiers are lowercased; quote mixed-case names: `"Count" * 2`.

Aggregates, windows, subqueries, `unnest`, and volatile functions such as
`random()` are rejected before simplification, including inside dead branches.
The enabled built-ins include core, string, Unicode, datetime and math functions.
Type coercion, expression simplification, casts, and inferred types/nullability
follow DataFusion semantics. Decimal binary arithmetic is checked for unsafe
precision/scale calculations and scale-alignment powers before type coercion
or constant folding, including hidden outputs.
Each logical open uses one query context, so stable functions such as `now()` agree across declarations and
batches; separate opens need not produce identical values.

Output preserves row count, batch boundaries, logical order, source identity,
source field metadata and schema metadata. Scalar results broadcast to the batch
row count, including zero-row and zero-column inputs. Inferred fields retain metadata
(including extension metadata for identity expressions). SQL additions also add
`open_arrow_access:generated_expression` with the original expression text.

Runtime data errors retain their source chain and column name. An error ends the
reader and releases its input immediately; previously emitted batches remain
emitted. EOF is also terminal. The layer starts no producer tasks. After cancelling
a pending `next`, drop the reader, following the core reader contract.

## Design and projection

The wrapper works with any `Accessor<Reader = dyn RecordReader>` whose request
implements `ReadOptionsProvider`; it does not depend on formats or storage.

1. Empty configuration delegates directly, without an expression context.
2. Retain the final projection. With a supplied complete source schema, select
   the source columns needed by all changes and the requested output; otherwise,
   additions require clearing the inner projection before opening.
3. Compile declarations once against source fields plus earlier outputs, using
   each expression's dependency schema. Coerce and simplify before physical
   planning. Source slots are bound to projected reader positions on first use;
   computed slots retain their indices. SQL identifiers resolve against the
   current namespace, including earlier renames and removals.
4. Expose the fixed augmented/projected schema before reading a batch.
5. Evaluate physical expressions on Arrow arrays, then apply the final projection.

Source arrays share their buffers. Each declaration evaluates a compact view of
its dependencies and stores its result in a column slot. Hidden results are
validated as they are produced; the final projected batch is assembled once.
Hidden arrays are released after their last dependency use; selected generated
outputs remain live until final assembly. Every declaration still executes.
Memory is bounded by one input batch, its generated arrays and expression/kernel
state. There is no DataFrame execution plan, materialized table or per-row SQL.

For dependency projection pushdown, supply the complete source schema with
`with_source_schema`. Dependencies come from parsed expressions, including references through earlier generated columns.
Declarations are checked against the complete schema before opening, and the
selected reader schema is checked at open. Without a supplied schema, adding
columns reads every source column.

Every declaration still runs, including outputs omitted by the final projection.
Pushdown reduces source I/O and decoding, not the set of expressions evaluated.

For internal parallel reads, compose in this order:
`source.layer(ParallelReadLayer::new(...)).layer(generated_layer)`.
Generation runs once over the ordered merged batches, sharing one query context.
The schema evolution wrapper forwards internal read concurrency but does not
implement `ScanPlanner`, so placing `ParallelReadLayer` outside it is not supported.
