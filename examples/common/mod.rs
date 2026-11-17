//! Shared setup and consumption helpers for the standalone example binaries.

// Each example compiles its own copy and uses only the helpers it needs.
#![allow(dead_code)]

use open_arrow_access::{
    ByteReader, Result,
    storage::{OpenDalAccessor, opendal},
};

pub fn fs_accessor(root: &str) -> opendal::Result<OpenDalAccessor> {
    let operator = opendal::Operator::new(opendal::services::Fs::default().root(root))?;
    Ok(OpenDalAccessor::new(operator))
}

pub async fn count_bytes(reader: &mut dyn ByteReader) -> Result<u64> {
    let mut bytes = 0;
    while let Some(chunk) = reader.next().await? {
        bytes += chunk.len() as u64;
    }
    Ok(bytes)
}

#[cfg(any(feature = "decompression-tar", feature = "service-arxiv"))]
pub async fn print_tar_entries(
    archive: &mut open_arrow_access::decompression::tar::TarStream,
    selected: Option<usize>,
) -> Result<()> {
    use open_arrow_access::decompression::tar::TarEntryReader;

    while let Some(TarEntryReader { entry, mut reader }) = archive.next_entry().await? {
        println!(
            "{}\t{}\t{} bytes",
            entry.index,
            String::from_utf8_lossy(&entry.name),
            entry.size
        );
        let bytes = count_bytes(reader.as_mut()).await?;
        if selected.is_none_or(|index| entry.index == index) {
            println!("Read {bytes} entry bytes");
        }
        if selected == Some(entry.index) {
            break;
        }
    }
    Ok(())
}
