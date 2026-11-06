use arrow_schema::DataType::{Int64, Utf8};
use open_arrow_access::{
    Accessor,
    file_format::{XmlAccessor, XmlField, XmlSchema},
    storage::FsAccessor,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = XmlSchema::new(vec![
        XmlField::new("id", "@id", Int64, false),
        XmlField::new("title", "title", Utf8, true),
    ])?;
    let bytes = FsAccessor::new(".").await?;
    let accessor = XmlAccessor::new(bytes, schema, "/catalog/book");
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "catalog.xml".into());
    let mut reader = accessor
        .open(open_arrow_access::ReadRequest::new(path))
        .await?;
    while let Some(batch) = reader.next().await? {
        println!("{batch:?}");
    }
    Ok(())
}
