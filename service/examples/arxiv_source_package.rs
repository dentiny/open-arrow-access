//! Inspect one downloaded gzip-TAR source package without extracting files to disk.
use arrow_array::cast::AsArray;
use open_arrow_access_core::{Bytes, ObjectRef, bytes_reader};
use open_arrow_access_service::arxiv::{
    ArxivSourceOptions, ArxivSourcePackaging, open_source_package,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: arxiv_source_package <downloaded-source.tar.gz> <article-id>".into());
    }
    let options = ArxivSourceOptions::default();
    if std::fs::metadata(&args[1])?.len() > options.max_decoded_bytes.get() as u64 {
        return Err("compressed input exceeds example size limit".into());
    }
    let bytes = Bytes::from(std::fs::read(&args[1])?);
    let input = Box::new(bytes_reader(bytes, Some(ObjectRef::new(args[1].clone()))));
    let mut reader =
        open_source_package(input, &args[2], ArxivSourcePackaging::GzipTar, options).await?;
    let mut count = 0;
    while let Some(batch) = reader.next().await? {
        let names = batch
            .column_by_name("entry_name")
            .unwrap()
            .as_binary::<i64>();
        let data = batch.column_by_name("data").unwrap().as_binary::<i64>();
        for row in 0..batch.num_rows() {
            println!(
                "{}\t{} bytes",
                String::from_utf8_lossy(names.value(row)),
                data.value(row).len()
            );
            count += 1;
        }
    }
    println!("{count} regular source members");
    Ok(())
}
