use open_arrow_access::{Accessor, service::dataverse::DataverseService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let endpoint = args
        .next()
        .ok_or("usage: read_dataverse <origin> <doi:...> <major.minor> [file-name]")?;
    let persistent_id = args.next().ok_or("missing persistent identifier")?;
    let version = args.next().ok_or("missing version")?;
    let dataset = DataverseService::new(&endpoint)?
        .dataset(&persistent_id, &version)
        .await?;
    for file in dataset.files() {
        println!("{}\tfile ID {}\t{} bytes", file.name, file.id, file.size);
    }
    if let Some(name) = args.next() {
        let mut reader = dataset.open(name.into()).await?;
        let mut bytes = 0;
        while let Some(chunk) = reader.next().await? {
            bytes += chunk.len();
        }
        println!("Read {bytes} original bytes");
    }
    Ok(())
}
