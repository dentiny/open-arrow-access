mod common;

use open_arrow_access::{
    Accessor,
    decompression::tar::{TarLimits, TarStream},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .ok_or("usage: read_tar <local-root> <archive-path> [entry-index]")?;
    let path = args.next().ok_or("missing archive path")?;
    let source = common::fs_accessor(&root)?;
    let mut archive = TarStream::new(source.open(path.into()).await?, TarLimits::default());
    let selected = args
        .next()
        .map(|index| index.parse::<usize>())
        .transpose()?;
    common::print_tar_entries(&mut archive, selected).await?;
    Ok(())
}
