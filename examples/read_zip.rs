use open_arrow_access::{
    Accessor, ByteReadRequest,
    decompression::zip::ZipAccessor,
    storage::{OpenDalAccessor, opendal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: read_zip <root> <archive> <entry-index>".into());
    }
    let operator = opendal::Operator::new(opendal::services::Fs::default().root(&args[1]))?;
    let zip = ZipAccessor::new(OpenDalAccessor::new(operator), args[3].parse()?);
    let request = ByteReadRequest::new(args[2].as_str());
    for entry in zip.entries(request.clone()).await? {
        println!(
            "{}: {:?} ({} bytes)",
            entry.index,
            String::from_utf8_lossy(&entry.name),
            entry.uncompressed_size
        );
    }
    let mut reader = zip.open(request).await?;
    let mut count = 0;
    while let Some(chunk) = reader.next().await? {
        count += chunk.len();
    }
    println!("Decoded {count} bytes from entry {}", args[3]);
    Ok(())
}
