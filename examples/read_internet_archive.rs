use open_arrow_access::{Accessor, service::internet_archive::InternetArchiveService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let identifier = args
        .next()
        .ok_or("usage: read_internet_archive <item-id> [filename]")?;
    let item = InternetArchiveService::new()?.item(&identifier).await?;
    for file in item.files() {
        println!("{}\t{:?} bytes\t{:?}", file.name, file.size, file.format);
    }
    if let Some(name) = args.next() {
        let mut reader = item.open(name.into()).await?;
        let mut count = 0;
        while let Some(bytes) = reader.next().await? {
            count += bytes.len();
        }
        println!("Read {count} bytes");
    }
    Ok(())
}
