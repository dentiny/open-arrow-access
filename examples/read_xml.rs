mod common;

use open_arrow_access::{Accessor, file_format::xml::XmlAccessor};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::current_dir()?;
    let bytes = common::fs_accessor(root.to_str().ok_or("root must be valid UTF-8")?)?;
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
