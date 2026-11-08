use open_arrow_access::{
    ReadOptions,
    service::huggingface::{HuggingFaceDatasetRequest, HuggingFaceOptions, HuggingFaceService},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repository = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "lhoestq/demo1".into());
    let service = HuggingFaceService::new(HuggingFaceOptions::default()).await?;
    let mut reader = service
        .dataset(
            HuggingFaceDatasetRequest::new(repository, "default", "train")
                .with_options(ReadOptions::default()),
        )
        .await?;
    println!("schema: {:?}", reader.metadata().schema);
    while let Some(batch) = reader.next().await? {
        println!("{} rows", batch.num_rows());
    }
    Ok(())
}
