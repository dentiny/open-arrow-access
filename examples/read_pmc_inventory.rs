use open_arrow_access::{
    Accessor, ReadRequest,
    service::pmc_inventory::PmcInventoryAccessor,
    storage::{OpenDalAccessor, opendal},
};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args.next().expect("expected mirror root");
    let manifest = args.next().expect("expected dated inventory manifest key");
    let operator = opendal::Operator::new(opendal::services::Fs::default().root(&root))?;
    let mut reader = PmcInventoryAccessor::new(OpenDalAccessor::new(operator))
        .open(ReadRequest::new(manifest))
        .await?;
    println!("{:?}", reader.metadata().schema);
    let mut rows = 0;
    while let Some(batch) = reader.next().await? {
        rows += batch.num_rows();
    }
    println!("{rows} inventory rows");
    Ok(())
}
