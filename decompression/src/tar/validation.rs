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
    is_link: bool,
) -> Result<TarEntry> {
    if !is_file && !is_dir && !is_link {
        return Err(unsupported(
            "TAR byte entries must be regular files, directories, or links; sparse files and special entries are not exposed",
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
        return Err(unsupported("TAR non-file entries have no file reader"));
    }
    Ok(())
}
