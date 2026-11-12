# Layers

Layers wrap an accessor with `AccessorExt::layer`. They preserve its Rust request
and reader interfaces; record layers may change the Arrow schema and values.
All layers in this crate are always available through `open_arrow_access::layers`.
Byte decompression layers use the same contract but require [codec features](../decompression/README.md).

## Choose a layer

| Layer | Use it for | Key behavior |
| --- | --- | --- |
| `SchemaEvolutionLayer` | Add, remove, rename, or cast top-level columns | Fixed output schema; added columns use batch functors |
| `GeneratedColumnsLayer` | Append row-level SQL expressions | Declarations see earlier generated fields; uses DataFusion |
| `SamplingLayer` | Seeded Bernoulli row sampling | Approximate count; preserves selected row order |
| `ParallelReadLayer` | Execute independent scan partitions concurrently | Requires `ScanPlanner`; preserves output order by default |
| `UnorderedEmissionLayer` | Emit ready batches from a reorderable scan | Use with parallel execution; global-order sources reject it |

Enabling Cargo features does not install a layer. Construct it and call `.layer`.
OpenDAL layers belong on the OpenDAL operator and configure a separate I/O boundary.

## Compose layers

```mermaid
flowchart LR
    Storage["Storage"] --> Format["Format / dataset accessor"]
    Format --> Parallel["ParallelReadLayer: execute partitions"]
    Parallel --> Generated["GeneratedColumnsLayer: evaluate after merge"]
    Generated --> Sampling["SamplingLayer: one seeded stream"]
    Sampling --> App["Application"]
```

`source.layer(a).layer(b)` makes `b` outermost: opening goes through `b`, then
`a`, then the source. Batches flow back through `a` and then `b`.

Enable `format-json` for this complete composition function:

```rust,no_run
use open_arrow_access::{
    Accessor, AccessorExt, ReadRequest,
    file_format::json::JsonAccessor,
    layers::{GeneratedColumnSpec, GeneratedColumnsLayer, ParallelReadLayer, SamplingLayer},
    storage::{OpenDalAccessor, opendal},
};
use std::num::NonZeroUsize;

async fn read(operator: opendal::Operator) -> open_arrow_access::Result<()> {
    let source = JsonAccessor::infer(OpenDalAccessor::new(operator));
    let generated = GeneratedColumnsLayer::try_new(vec![GeneratedColumnSpec {
        column: "title_length".into(),
        expression: "character_length(title)".into(),
    }])?;
    let records = source
        .layer(ParallelReadLayer::new(NonZeroUsize::new(4).unwrap(), NonZeroUsize::MIN))
        .layer(generated)
        .layer(SamplingLayer::try_new(0.1, 42)?);
    let mut reader = records.open(ReadRequest::new("records.jsonl")).await?;
    while let Some(batch) = reader.next().await? {
        println!("{} sampled rows", batch.num_rows());
    }
    Ok(())
}
```

This expects a string `title` column. Sampling keeps each row with probability
0.1; it does not request an exact 10% count. The same seed and ordered input
produce the same selection across different batch boundaries. Unordered input
can change the sample.

Keep generated columns and sampling **outside** `ParallelReadLayer` so they run
over one merged scan. They do not implement `ScanPlanner`; putting the parallel
layer outside either is unsupported. Generated columns currently read all source
columns and evaluate all declarations before final projection.

## Evolve a schema

Add matching `arrow-array = "59.3.0"` and `arrow-schema = "59.3.0"`
dependencies to work with Arrow values directly. For a record accessor `source`
with `count` and `obsolete` columns:

```rust,ignore
use std::sync::Arc;
use arrow_array::StringArray;
use arrow_schema::DataType;
use open_arrow_access::{AccessorExt, layers::{ColumnEvolution, SchemaEvolutionLayer}};

let records = source.layer(SchemaEvolutionLayer::try_new(vec![
    ColumnEvolution::add_column("source", DataType::Utf8, |batch| {
        Ok(Arc::new(StringArray::from(vec!["import"; batch.num_rows()])))
    }),
    ColumnEvolution::remove_column("obsolete"),
    ColumnEvolution::rename_column("count", "row_count"),
    ColumnEvolution::change_data_type("count", DataType::Int64),
])?);
```

Changes refer to original source names; renames are simultaneous. A column can
be renamed and cast together, but cannot also be removed. Output names must be
unique; removed or renamed source names can be reused by additions.

Added columns are nullable and appended in declaration order. Their functors
(`Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync + 'static`) see the original
batch, including removed fields, and must return the declared type and row count.
They cannot reference other additions. Empty batches are supported.

Projection uses evolved names. Functors read all source columns and, like casts,
run even when omitted from the final projection. Unchanged arrays share buffers;
existing field/schema metadata and zero-column row counts are preserved.
Unsupported casts fail at open; invalid values, overflows, and callback failures
terminate reading with error context. Arrow casts can lose precision and do not
reconcile schemas across files.

See the [runnable example](../examples/schema_evolution.rs),
[generated columns](../docs/generated-columns.md), and
[parallel reading](../docs/parallel-read.md).

## Add a layer

```mermaid
flowchart LR
    Config["CustomLayer: validated settings"] --> Trait["impl Layer<A>: build wrapper, no I/O"]
    Trait --> Accessor["CustomAccessor<A>: delegate request, intercept open"]
    Accessor --> Reader["Optional custom Reader: metadata + next / ranges"]
```

Implement `Layer<A>` and a wrapping `Accessor` retaining `A::Request` and
`A::Reader`. Wrap each returned reader if transforming batches or observing
reads. Preserve EOF/error lifecycle, source identity, and explicit ordering;
update metadata when changing the schema or byte representation. Forward optional
listing/range/planning operations, or remove the associated advertised abilities.
Planning-aware wrappers must also wrap partition accessors.

See [adding a layer](../docs/extending.md#add-a-layer) and the
[`SamplingLayer` implementation](src/sampling.rs) for a compact complete example.
