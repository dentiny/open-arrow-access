use std::collections::HashSet;

use futures::TryStreamExt;
use open_arrow_access_core::{ByteReader, Error, ErrorKind, ObjectRef, Result, read_lines};

const MAX_LINE_BYTES: usize = 16 * 1024;

pub(super) async fn read_manifest(
    input: Box<dyn ByteReader>,
    prefix: &str,
) -> Result<Vec<ObjectRef>> {
    let lines = read_lines(input, Some(MAX_LINE_BYTES), "read_commoncrawl_manifest");
    futures::pin_mut!(lines);
    let mut line_number = 1;
    let mut selected = Vec::new();
    let mut seen = HashSet::new();
    while let Some(line) = lines.try_next().await? {
        append_line(&line, line_number, prefix, &mut selected, &mut seen)?;
        line_number += 1;
    }
    if selected.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "resolve_commoncrawl_warc",
            format!("manifest contains no files under {prefix:?}"),
        ));
    }
    Ok(selected)
}

fn append_line(
    bytes: &[u8],
    number: usize,
    prefix: &str,
    selected: &mut Vec<ObjectRef>,
    seen: &mut HashSet<String>,
) -> Result<()> {
    let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    let bytes = bytes.strip_suffix(b"\r").unwrap_or(bytes);
    let path = std::str::from_utf8(bytes)
        .map_err(|e| invalid_line(number, "manifest is not UTF-8").with_source(e))?;
    if path.is_empty() {
        return Ok(());
    }
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
    if path.starts_with(prefix) {
        if !seen.insert(path.to_owned()) {
            return Err(invalid_line(number, "duplicate selected file"));
        }
        selected.push(ObjectRef::new(path));
    }
    Ok(())
}

fn invalid_line(number: usize, message: &str) -> Error {
    Error::new(
        ErrorKind::Decode,
        "read_commoncrawl_manifest",
        format!("line {number}: {message}"),
    )
}
