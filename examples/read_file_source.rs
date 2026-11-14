use open_arrow_access::{
    OuterCompression, ReadRequest, file_format::json::JsonFormat, open_file_source,
    storage::opendal,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = std::path::absolute(args.next().unwrap_or_else(|| ".".into()))?;
    let path = args.next().unwrap_or_else(|| "events.jsonl".into());
    let operator = opendal::Operator::new(
        opendal::services::Fs::default().root(root.to_str().ok_or("root must be valid UTF-8")?),
    )?;
    let source = open_file_source(
        operator,
        ReadRequest::new(path),
        JsonFormat::infer(),
        OuterCompression::Auto,
    )
    .await?;
    println!("Schema: {:?}", source.schema());
    let mut reader = source.reader().await?;
    let mut rows = 0;
    while let Some(batch) = reader.next().await? {
        rows += batch.num_rows();
    }
    println!("Read {rows} rows");
    Ok(())
}
