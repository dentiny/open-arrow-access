//! Add, remove, rename, and cast columns, then project using output names.
//! Run: cargo run --locked --example schema_evolution --features format-json

use arrow_schema::DataType;
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
    let source = JsonAccessor::infer(OpenDalAccessor::new(operator));
    // Layers are always available. Enabling format-json only enables the decoder.
    let records = source.layer(SchemaEvolutionLayer::try_new(vec![
        ColumnEvolution::rename_column("id", "record_id"),
        ColumnEvolution::change_data_type("id", DataType::Int32),
        ColumnEvolution::add_column("batch_rows", DataType::UInt64, |batch| {
            Ok(std::sync::Arc::new(arrow_array::UInt64Array::from(
                vec![batch.num_rows() as u64; batch.num_rows()],
            )))
        }),
        ColumnEvolution::remove_column("title"),
    ])?);
    // Unlisted fields are preserved. Projection uses evolved names and order.
    let request = ReadRequest::new("projection.jsonl").with_options(ReadOptions {
        projection: Some(vec!["batch_rows".into(), "record_id".into()]),
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
