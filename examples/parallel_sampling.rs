//! Decode JSON partitions concurrently, then sample the merged ordered stream.
//! Run: cargo run --locked --example parallel_sampling --features format-json

use std::num::NonZeroUsize;

use open_arrow_access::{
    Accessor, AccessorExt, ReadOptions, ReadRequest,
    file_format::json::JsonAccessor,
    layers::{ParallelReadLayer, SamplingLayer},
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
    let records = source
        // Four resident partitions, at most one queued batch per partition.
        .layer(ParallelReadLayer::new(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::MIN,
        ))
        // Sampling must be outside parallel execution to use one random stream.
        .layer(SamplingLayer::try_new(0.25, 42)?);
    let request = ReadRequest::new("many.jsonl").with_options(ReadOptions {
        // Small batches make this 100-row fixture exercise multiple partitions.
        batch_size: NonZeroUsize::new(16).unwrap(),
        ..Default::default()
    });
    let mut reader = records.open(request).await?;
    println!("Output schema: {:?}", reader.metadata().schema);
    let mut rows = 0;
    while let Some(batch) = reader.next().await? {
        rows += batch.num_rows();
        println!("{batch:?}");
    }
    println!("Sampled {rows} of 100 rows (probability 0.25, seed 42)");
    // Bernoulli sampling gives an approximate count, not exactly 25 rows.
    // The same ordered input and seed select the same rows across batch sizes.
    Ok(())
}
