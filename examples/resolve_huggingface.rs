use open_arrow_access::{
    Accessor, ByteReadRequest,
    service::{HuggingFaceOptions, HuggingFaceSelection, HuggingFaceService},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service =
        HuggingFaceService::new(HuggingFaceOptions::default().with_http_downloads()).await?;
    let dataset = service
        .resolve(HuggingFaceSelection::new(
            "datasets-examples/doc-yaml-3",
            "default",
            "train",
        ))
        .await?;
    println!("commit: {}", dataset.commit);
    for object in &dataset.objects {
        println!("file: {}", object.locator());
    }
    let mut reader = dataset
        .accessor
        .open(ByteReadRequest::new(dataset.objects[0].clone()))
        .await?;
    let bytes = reader
        .read_range(0..reader.metadata().content_length.unwrap().min(16))
        .await?;
    println!("read {} bytes from the pinned first file", bytes.len());
    Ok(())
}
