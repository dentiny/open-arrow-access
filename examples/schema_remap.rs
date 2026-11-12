//! Rename and cast columns, then project using the output names.
//! Run: cargo run --locked --example schema_remap --features format-json

use arrow_schema::DataType;
use open_arrow_access::{
    Accessor, AccessorExt, ReadOptions, ReadRequest,
    file_format::json::JsonAccessor,
    layers::{FieldRemap, SchemaRemapLayer},
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
    let records = source.layer(SchemaRemapLayer::try_new(vec![
        FieldRemap {
            source_name: "id".into(),
            target_name: Some("record_id".into()),
            target_data_type: Some(DataType::Int32),
        },
        FieldRemap {
            source_name: "title".into(),
            target_name: Some("text".into()),
            target_data_type: None, // Rename only; preserve the source type.
        },
    ])?);
    // Unlisted fields are preserved. Projection uses remapped names and order.
    let request = ReadRequest::new("projection.jsonl").with_options(ReadOptions {
        projection: Some(vec!["text".into(), "record_id".into()]),
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
