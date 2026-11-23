//! Application limits; archive format decoding belongs to the TAR packages.
use std::borrow::Cow;

use super::{TarEntry, TarLimits, error::unsupported};
use open_arrow_access_core::Result;

pub(super) fn entry(
    index: usize,
    name: Cow<'_, [u8]>,
    size: u64,
    is_file: bool,
    is_dir: bool,
    limits: &TarLimits,
) -> Result<TarEntry> {
    if index >= limits.max_entries {
        return Err(unsupported("TAR archive exceeds configured entry count"));
    }
    if size > limits.max_entry_bytes {
        return Err(unsupported("TAR entry exceeds configured byte limit"));
    }
    if !is_file && !is_dir {
        return Err(unsupported(
            "TAR byte entries must be regular files or directories; links and sparse files are not exposed",
        ));
    }
    Ok(TarEntry {
        index,
        name: name.into_owned(),
        size,
        is_file,
    })
}

pub(super) fn validate_regular_entry(entry: &TarEntry) -> Result<()> {
    if !entry.is_file {
        return Err(unsupported("TAR directory entries have no file reader"));
    }
    Ok(())
}
