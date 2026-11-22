//! Application limits; archive format decoding belongs to tar::Archive.
use super::{TarEntry, TarLimits, error::unsupported};
use open_arrow_access_core::Result;

pub(super) fn entry<R: std::io::Read>(
    entry: &tar::Entry<'_, R>,
    index: usize,
    limits: &TarLimits,
) -> Result<TarEntry> {
    if index >= limits.max_entries {
        return Err(unsupported("TAR archive exceeds configured entry count"));
    }
    if entry.size() > limits.max_entry_bytes {
        return Err(unsupported("TAR entry exceeds configured byte limit"));
    }
    let kind = entry.header().entry_type();
    if !kind.is_file() && !kind.is_dir() {
        return Err(unsupported(
            "TAR byte entries must be regular files or directories; links and sparse files are not exposed",
        ));
    }
    Ok(TarEntry {
        index,
        name: entry.path_bytes().into_owned(),
        size: entry.size(),
        is_file: kind.is_file(),
    })
}

pub(super) fn validate_regular_entry(entry: &TarEntry) -> Result<()> {
    if !entry.is_file {
        return Err(unsupported("TAR directory entries have no file reader"));
    }
    Ok(())
}
