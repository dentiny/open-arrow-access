use open_arrow_access::{
    Accessor, ReadRequest,
    file_format::ParquetAccessor,
    storage::{OpenDalAccessor, opendal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args.next().unwrap_or_else(|| ".".into());
    let path = args.next().unwrap_or_else(|| "data.parquet".into());
    let root = std::path::absolute(root)?;
    let operator = opendal::Operator::new(
        opendal::services::Fs::default().root(root.to_str().ok_or("root must be valid UTF-8")?),
    )?;
    let accessor = ParquetAccessor::new(OpenDalAccessor::new(operator));
    let mut reader = accessor.open(ReadRequest::new(path)).await?;
    println!("Schema: {:?}", reader.metadata().schema);
    let mut rows = 0;
    let mut batches = 0;
    while let Some(batch) = reader.next().await? {
        rows += batch.num_rows();
        batches += 1;
    }
    println!("Read {rows} rows in {batches} ordered batches");
    Ok(())
}
