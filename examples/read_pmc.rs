use open_arrow_access::{
    Accessor, ReadRequest,
    service::pmc::PmcAccessor,
    storage::{OpenDalAccessor, opendal},
};

/// cargo run --example read_pmc --features service-pmc -- /path/to/mirror metadata/PMC123.2.json
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args.next().expect("expected PMC mirror root");
    let request = ReadRequest::new(args.next().expect("expected metadata object"));
    let operator = opendal::Operator::new(opendal::services::Fs::default().root(&root))?;
    let source = PmcAccessor::with_default_decoder(OpenDalAccessor::new(operator));
    let mut reader = source.open(request).await?;
    println!("{:?}", reader.metadata().schema);
    while let Some(batch) = reader.next().await? {
        println!("{} PMC metadata rows", batch.num_rows());
    }
    Ok(())
}
