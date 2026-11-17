use open_arrow_access::{
    Accessor,
    service::pmc::{PmcDatasetAccessor, PmcRequest},
};

/// cargo run --example read_pmc_release --features service-pmc -- pmc://oa/metadata/
/// This streams the whole selection. Use a subset URI for a small scan.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let uri = std::env::args().nth(1).unwrap_or_else(|| "pmc://oa".into());
    let source = PmcDatasetAccessor::public()?;
    let mut reader = source.open(PmcRequest::from_uri(&uri)?).await?;
    println!("{:?}", reader.metadata().schema);
    while let Some(batch) = reader.next().await? {
        println!("{} PMC metadata rows", batch.num_rows());
    }
    Ok(())
}
