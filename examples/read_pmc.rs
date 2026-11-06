use open_arrow_access::{
    Accessor, Result,
    service::pmc::{PmcAccessor, PmcRequest},
    storage::FsAccessor,
};

/// cargo run --example read_pmc --features service-pmc,storage-fs -- /path/to/mirror metadata/PMC123.2.json
#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let root = args.next().expect("expected PMC mirror root");
    let request = match args.next() {
        Some(path) => PmcRequest::new(path),
        None => PmcRequest::prefix("metadata/"),
    };
    let source = PmcAccessor::new(FsAccessor::new(root).await?);
    let mut reader = source.open(request).await?;
    println!("{:?}", reader.metadata().schema);
    while let Some(batch) = reader.next().await? {
        println!("{} PMC PDF snapshots", batch.num_rows());
    }
    Ok(())
}
