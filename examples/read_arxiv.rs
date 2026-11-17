mod common;

use open_arrow_access::{
    ByteReadOptions,
    service::arxiv::{ArxivKind, ArxivService},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .ok_or("usage: read_arxiv <local-mirror-root> <YYMM> [archive-index] [entry-index]")?;
    let month: u16 = args.next().ok_or("missing YYMM month")?.parse()?;
    let bytes = common::fs_accessor(&root)?;
    let service = ArxivService::new(bytes);
    let archives = service
        .manifest(ArxivKind::Auto, Some(month), ByteReadOptions::default())
        .await?;
    for (index, archive) in archives.iter().enumerate() {
        println!("{index}\t{}\t{} bytes", archive.filename, archive.size);
    }
    if let Some(index) = args.next() {
        let index: usize = index.parse()?;
        let mut archive = service
            .stream(
                archives.get(index).ok_or("archive index out of bounds")?,
                ByteReadOptions::default(),
            )
            .await?;
        let selected = args
            .next()
            .map(|index| index.parse::<usize>())
            .transpose()?;
        common::print_tar_entries(&mut archive, selected).await?;
    }
    Ok(())
}
