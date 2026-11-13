//! Add, remove, rename, and cast columns, then project using output names.
//! Run: cargo run --locked --example schema_evolution --features format-json

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::{col, lit};
use open_arrow_access::{
    Accessor, AccessorExt, ReadOptions, ReadRequest,
    file_format::json::JsonAccessor,
    layers::{ColumnEvolution, SchemaEvolutionLayer},
    storage::{OpenDalAccessor, opendal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a checked-in fixture; no network access or external dataset is needed.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/json");
    let operator = opendal::Operator::new(
        opendal::services::Fs::default().root(root.to_str().ok_or("root must be valid UTF-8")?),
    )?;
    let source_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("title", DataType::Utf8, true),
    ]));
    let source = JsonAccessor::new(OpenDalAccessor::new(operator), source_schema.clone());
    // Layers are always available. Enabling format-json only enables the decoder.
    let records = source.layer(
        SchemaEvolutionLayer::try_new(vec![
            ColumnEvolution::rename_column("id", "record_id"),
            ColumnEvolution::change_data_type("record_id", DataType::Int32),
            ColumnEvolution::add_column("next_id", DataType::Int32, col("record_id") + lit(1i32)),
            ColumnEvolution::remove_column("title"),
        ])?
        .with_source_schema(source_schema),
    );
    // Unlisted fields are preserved. Projection uses evolved names and order.
    let request = ReadRequest::new("projection.jsonl").with_options(ReadOptions {
        projection: Some(vec!["next_id".into(), "record_id".into()]),
        ..Default::default()
    });
    let mut reader = records.open(request).await?;
    println!("Output schema: {:?}", reader.metadata().schema);
    while let Some(batch) = reader.next().await? {
        println!("{batch:?}");
    }
    // Unsupported casts fail at open; invalid values/overflow fail during next().
    Ok(())
}
