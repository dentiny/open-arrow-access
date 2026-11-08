use open_arrow_access::{
    Accessor,
    decompression::tar::{TarLimits, TarStream},
    storage::{OpenDalAccessor, opendal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .ok_or("usage: read_tar <local-root> <archive-path> [entry-index]")?;
    let path = args.next().ok_or("missing archive path")?;
    let source = OpenDalAccessor::new(opendal::Operator::new(
        opendal::services::Fs::default().root(&root),
    )?);
    let mut archive = TarStream::new(source.open(path.into()).await?, TarLimits::default());
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
    Ok(())
}
