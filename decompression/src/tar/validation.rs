//! Validate entry kinds exposed by the byte-reader API.
use std::borrow::Cow;

use super::{TarEntry, error::unsupported};
use open_arrow_access_core::Result;

pub(super) fn entry(
    index: usize,
    name: Cow<'_, [u8]>,
    size: u64,
    is_file: bool,
    is_dir: bool,
) -> Result<TarEntry> {
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
