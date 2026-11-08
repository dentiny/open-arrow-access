use open_arrow_access::{
    Accessor,
    file_format::xml::XmlAccessor,
    storage::{OpenDalAccessor, opendal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::current_dir()?;
    let operator = opendal::Operator::new(
        opendal::services::Fs::default().root(root.to_str().ok_or("root must be valid UTF-8")?),
    )?;
    let bytes = OpenDalAccessor::new(operator);
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
