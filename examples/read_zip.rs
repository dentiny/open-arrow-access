mod common;

use std::num::NonZeroUsize;

use open_arrow_access::{
    Accessor, ByteReadRequest,
    decompression::zip::{ZipArchive, ZipLimits},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: read_zip <root> <archive> <entry-index>".into());
    }
    let bytes = common::fs_accessor(&args[1])?;
    let request = ByteReadRequest::new(args[2].as_str());
    let input = bytes.open(request).await?;
    let archive = ZipArchive::open(input, ZipLimits::default()).await?;
    for entry in archive.entries() {
        println!(
            "{}: {:?} ({} bytes)",
            entry.index,
            String::from_utf8_lossy(&entry.name),
            entry.uncompressed_size
        );
    }
    let mut reader = archive
        .open_entry(args[3].parse()?, NonZeroUsize::new(64 * 1024).unwrap())
        .await?;
    let count = common::count_bytes(reader.as_mut()).await?;
    println!("Decoded {count} bytes from entry {}", args[3]);
    Ok(())
}
