//! Generate SQL columns in declaration order, then select the output columns.
//! Run: cargo run --locked --example generated_columns --features format-json

use open_arrow_access::{
    Accessor, AccessorExt, ReadOptions, ReadRequest,
    file_format::json::JsonAccessor,
    layers::{GeneratedColumnSpec, GeneratedColumnsLayer},
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
    let records = source.layer(GeneratedColumnsLayer::try_new(vec![
        GeneratedColumnSpec {
            column: "next_id".into(),
            expression: "id + 1".into(),
        },
        GeneratedColumnSpec {
            column: "doubled_next_id".into(),
            expression: "next_id * 2".into(), // Can use an earlier generated field.
        },
    ])?);
    // The layer reads expression dependencies even when they are not projected.
    let request = ReadRequest::new("projection.jsonl").with_options(ReadOptions {
        projection: Some(vec!["next_id".into(), "doubled_next_id".into()]),
        ..Default::default()
    });
    let mut reader = records.open(request).await?;
    println!("Output schema: {:?}", reader.metadata().schema);
    while let Some(batch) = reader.next().await? {
        println!("{batch:?}");
    }
    Ok(())
}
