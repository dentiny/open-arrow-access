use open_arrow_access_core::Result;

use super::error::{decode, header_error, unsupported};

pub(super) const BLOCK_SIZE: u64 = 512;

pub(super) fn validate_header(header: &tar::Header) -> Result<()> {
    // | name + metadata (148 B) | checksum (8 B) | metadata (356 B) |
    // 0                        148              156               512
    let sum: u32 = header.as_bytes()[..148]
        .iter()
        .chain(&header.as_bytes()[156..])
        .map(|byte| u32::from(*byte))
        .sum::<u32>()
        + 8 * 32;
    if header.cksum().map_err(header_error)? != sum {
        return Err(decode("TAR header checksum mismatch"));
    }
    let kind = header.entry_type();
    if !kind.is_file() && !kind.is_dir() {
        return Err(unsupported(
            "only regular TAR files and directories are supported; links, sparse files and extension headers are unsupported",
        ));
    }
    Ok(())
}

pub(super) fn entry_end(start: u64, size: u64, archive_len: u64) -> Result<u64> {
    start
        .checked_add(size)
        .and_then(|end| end.checked_add(BLOCK_SIZE - 1))
        .map(|end| end / BLOCK_SIZE * BLOCK_SIZE)
        .filter(|end| *end <= archive_len)
        .ok_or_else(|| decode("TAR entry exceeds archive length"))
}

pub(super) fn parse_entry(
    bytes: &[u8],
    index: usize,
    limits: &super::TarLimits,
) -> Result<super::TarEntry> {
    if index == limits.max_entries {
        return Err(unsupported("TAR archive exceeds configured entry count"));
    }
    let header = tar::Header::from_byte_slice(bytes);
    validate_header(header)?;
    let size = header.size().map_err(header_error)?;
    if size > limits.max_entry_bytes {
        return Err(unsupported("TAR entry exceeds configured byte limit"));
    }
    Ok(super::TarEntry {
        index,
        name: header.path_bytes().into_owned(),
        size,
        is_file: header.entry_type().is_file(),
    })
}
