# Generated columns

`SchemaEvolutionLayer` adds computed Arrow fields using row-level DataFusion
SQL or `Expr` inputs alongside ordered remove, rename and cast operations. It is
available through `open_arrow_access::layers` with `layer-schema-evolution`, or
through `open-arrow-access-layers` with `schema-evolution`. Both crates have empty
default features. This layer enables DataFusion and SQL; layers such as limits and
sampling do not require them.

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
follow DataFusion semantics.
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

## Move a nested field to a top-level column

For parsed corpora such as S2ORC v2, `body.text` is the paper's body text,
while `bibliography.text` is a separate reference list. Move the body text
without retaining a duplicate in the struct:

```rust,ignore
let records = source.layer(SchemaEvolutionLayer::try_new(vec![
    ColumnEvolution::extract_nested_field(["body", "text"], "text"),
])?);
```

The resulting schema has a top-level `text` field, `body` retains its other
children (such as annotations), and `bibliography` is unchanged. Compression
belongs to the writer; send these Arrow batches to open-arrow-sink and configure
`CompressionSpec::Lz4Raw` for `text`. Do not compress the strings into binary
values before passing them to the writer.

`extract_nested_field(path, name)` appends the moved field to the top-level
schema. It retains its type and metadata, widening nullability if any parent is
nullable. Each null parent masks the extracted value, even if its child array
contains a non-null hidden value. Retained value buffers are shared; extraction
may allocate a combined validity bitmap. The original field is removed from
its parent, without changing row counts, sibling fields, parent validity or
schema metadata. Empty structs are retained, including in zero-row batches.

Use `remove_nested_field(path)` when only removal is needed. Both operations
run in declaration order alongside casts, renames and generated columns;
subsequent expressions see the new schema. Paths contain literal name segments
(including dots), require at least a column and a child, and traverse structs
only. Missing/ambiguous fields, list traversal, changes inside extension
structs, and output-name collisions fail at open. Extraction of union or
run-end encoded fields through a nullable path is rejected because those Arrow
types cannot carry the required top-level validity bitmap. Existing exact-name
`remove_column("body.text")` semantics are unchanged: that names a literal
top-level column, not a nested path.
