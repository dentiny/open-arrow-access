use std::num::NonZeroU64;

use open_arrow_access::{Accessor, service::zenodo::ZenodoService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let id: NonZeroU64 = args
        .next()
        .ok_or("usage: read_zenodo <record-id> [file-key]")?
        .parse()?;
    let record = ZenodoService::new()?.record(id).await?;
    for file in record.files() {
        println!("{}\t{} bytes\t{}", file.key, file.size, file.checksum);
    }
    if let Some(key) = args.next() {
        let mut reader = record.open(key.into()).await?;
        let mut bytes = 0;
        while let Some(chunk) = reader.next().await? {
            bytes += chunk.len();
        }
        println!("Read {bytes} bytes");
    }
    Ok(())
}
