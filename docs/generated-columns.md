# Generated columns

`GeneratedColumnsLayer` appends Arrow fields using row-level DataFusion SQL
expressions. Enable the facade's default-off `layer-generated-columns` feature,
or `generated-columns` on `open-arrow-access-layers`. Core remains Arrow-only;
DataFusion is absent from the default dependency graph.

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
2. Retain the final projection and clear the inner projection before opening.
3. Compile declarations against the complete source schema plus earlier outputs.
   Coerce and simplify each logical expression before physical planning.
4. Expose the fixed augmented/projected schema before reading a batch.
5. Evaluate physical expressions on Arrow arrays, then apply the final projection.

Source arrays share their buffers. Only later declarations need intermediate
batches; a single generated column avoids that allocation. Memory is bounded by
one input batch, its generated arrays and expression/kernel state. There is no
DataFrame execution plan, materialized table, dependency graph or per-row SQL.

**Projection limitation:** a nonempty configuration reads every source column and
evaluates every declaration, including outputs omitted by the final projection.
This can add substantial I/O/decoding cost for wide columnar inputs. It ensures
full-schema collision/type checking with one open and the current accessor API.
Dependency-aware pushdown is deferred until accessors can supply a complete source
schema independently of the selected data. SQL literals/comments must never be
inferred as dependencies by regex. Timing the in-memory kernel path does not
measure this source-I/O cost.

For internal parallel reads, compose in this order:
`source.layer(ParallelReadLayer::new(...)).layer(generated_layer)`.
Generation runs once over the ordered merged batches, sharing one query context.
The generated wrapper forwards internal read concurrency but does not implement
`ScanPlanner`, so placing `ParallelReadLayer` outside it is not supported.

## Reference implementations and scope

The design follows `ml-infra/lance/lance_migration/crates/converter/src/generated.rs`:
ordered declarations, DataFusion physical expressions and field provenance.
It adds the accessor/request protocol, final projection, explicit zero-column row
counts, pre-simplification row-local validation and terminal input cleanup.
The available older `duckdb_lance_conversion` checkout contains DuckDB's own
generated table-column machinery but no analogous converter feature.

Reader-provided private context fields (for example WARC path/offset) remain a
separate extension: the current API has no virtual-field request contract.
Imported source metadata never silently hides a field. Add explicit source-owned
virtual-field requests together with a real reader, coordinate semantics and tests.

## Validation and performance

Integration tests cover expressions, dependent columns, constants, nulls, quoting,
metadata/buffer sharing, zero rows/columns, projection, invalid and hidden
expressions, runtime failures, drop, type conversion, and CSV with parallel reads.

Run the manual release timing check on an otherwise idle machine:

```sh
cargo test -p open-arrow-access-layers --features generated-columns --release \
  --test generated_columns generated_columns_throughput -- --ignored --nocapture
```

It reports median scan time across five runs over 8,388,608 rows in 8192-row
batches sharing the same cached input buffers, with SQL/open time reported separately: direct, empty configuration,
single arithmetic expression, and Unicode length plus an earlier-column dependency.
This is a diagnostic benchmark without a flaky wall-clock pass/fail threshold.
