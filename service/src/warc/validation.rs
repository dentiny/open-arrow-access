use open_arrow_access_core::Result;

use super::error::invalid_line;

pub(super) fn validate_manifest_path(path: &str, number: usize) -> Result<()> {
    if !path.starts_with("crawl-data/")
        || path
            .bytes()
            .any(|b| !b.is_ascii_alphanumeric() && !matches!(b, b'/' | b'-' | b'_' | b'.'))
        || path
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
        || ![
            ".warc",
            ".warc.gz",
            ".warc.bz2",
            ".warc.zst",
            ".warc.zstd",
            ".warc.lz4",
        ]
        .iter()
        .any(|suffix| path.ends_with(suffix))
    {
        return Err(invalid_line(
            number,
            "expected a canonical manifest-relative WARC path",
        ));
    }
    Ok(())
}
