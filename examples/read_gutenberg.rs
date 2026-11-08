use open_arrow_access::{
    Accessor, ReadRequest,
    service::gutenberg::GutenbergAccessor,
    storage::{OpenDalAccessor, opendal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args.next().unwrap_or_else(|| ".".into());
    let metadata = args
        .next()
        .unwrap_or_else(|| "testdata/gutenberg/book.rdf".into());
    let operator = opendal::Operator::new(opendal::services::Fs::default().root(&root))?;
    let books = GutenbergAccessor::new(OpenDalAccessor::new(operator));
    let mut reader = books.open(ReadRequest::new(metadata)).await?;
    println!("schema: {:?}", reader.metadata().schema);
    while let Some(batch) = reader.next().await? {
        println!("{} book, {} columns", batch.num_rows(), batch.num_columns());
    }
    Ok(())
}
