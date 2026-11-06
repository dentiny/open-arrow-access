use open_arrow_access::{Accessor, file_format::XmlAccessor, storage::FsAccessor};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bytes = FsAccessor::new(std::env::current_dir()?).await?;
    let accessor = XmlAccessor::new(bytes, "/inventory/warehouse");
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "testdata/xml/inventory.xml".into());
    let mut reader = accessor
        .open(open_arrow_access::ReadRequest::new(path))
        .await?;
    while let Some(batch) = reader.next().await? {
        println!("{batch:?}");
    }
    Ok(())
}
