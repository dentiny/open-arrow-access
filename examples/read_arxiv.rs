use open_arrow_access::{
    ByteReadOptions,
    service::arxiv::{ArxivKind, ArxivService},
    storage::{OpenDalAccessor, opendal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .ok_or("usage: read_arxiv <local-mirror-root> <YYMM> [archive-index] [entry-index]")?;
    let month: u16 = args.next().ok_or("missing YYMM month")?.parse()?;
    let operator = opendal::Operator::new(opendal::services::Fs::default().root(&root))?;
    let service = ArxivService::new(OpenDalAccessor::new(operator));
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
        while let Some((entry, mut reader)) = archive.next_entry().await? {
            println!(
                "{}\t{}\t{} bytes",
                entry.index,
                String::from_utf8_lossy(&entry.name),
                entry.size
            );
            let mut bytes = 0;
            while let Some(chunk) = reader.next().await? {
                bytes += chunk.len();
            }
            if selected.is_none_or(|index| entry.index == index) {
                println!("Read {bytes} entry bytes");
            }
            if selected == Some(entry.index) {
                break;
            }
        }
    }
    Ok(())
}
