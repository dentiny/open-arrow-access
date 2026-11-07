use open_arrow_access::{
    Accessor,
    service::{GutenbergAccessor, GutenbergFile, GutenbergRequest},
    storage::{OpenDalAccessor, opendal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args.next().unwrap_or_else(|| ".".into());
    let metadata = args
        .next()
        .unwrap_or_else(|| "testdata/gutenberg/book.rdf".into());
    let mut files = Vec::new();
    while let Some(uri) = args.next() {
        let object = args
            .next()
            .ok_or("usage: read_gutenberg [root] [rdf] [file-uri file-path]...")?;
        files.push(GutenbergFile::new(uri, object));
    }
    let operator = opendal::Operator::new(opendal::services::Fs::default().root(&root))?;
    let books = GutenbergAccessor::new(OpenDalAccessor::new(operator));
    let mut reader = books.open(GutenbergRequest::new(metadata, files)).await?;
    println!("schema: {:?}", reader.metadata().schema);
    while let Some(batch) = reader.next().await? {
        println!("{} book, {} columns", batch.num_rows(), batch.num_columns());
    }
    Ok(())
}
