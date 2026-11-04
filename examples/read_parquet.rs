use open_arrow_access::{
    Accessor, ReadRequest, Result, file_format::ParquetAccessor, storage::FsAccessor,
};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let root = args.next().unwrap_or_else(|| ".".into());
    let path = args.next().unwrap_or_else(|| "data.parquet".into());
    let root = std::path::absolute(root).map_err(|error| {
        open_arrow_access::Error::new(
            open_arrow_access::ErrorKind::Io,
            "resolve_root",
            error.to_string(),
        )
        .with_source(error)
    })?;
    let accessor = ParquetAccessor::new(FsAccessor::new(root)?);
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
