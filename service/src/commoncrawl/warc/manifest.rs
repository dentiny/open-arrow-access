use std::collections::HashSet;

use open_arrow_access_core::{ByteReader, Error, ErrorKind, ObjectRef, Result};

const MAX_LINE_BYTES: usize = 16 * 1024;

pub(super) async fn read_manifest(
    mut input: Box<dyn ByteReader>,
    prefix: &str,
) -> Result<Vec<ObjectRef>> {
    let mut line = Vec::new();
    let mut line_number = 1;
    let mut selected = Vec::new();
    let mut seen = HashSet::new();
    while let Some(chunk) = input.next().await? {
        if chunk.is_empty() {
            return Err(Error::new(
                ErrorKind::Io,
                "read_commoncrawl_manifest",
                "byte reader emitted an empty chunk",
            ));
        }
        for part in chunk.split_inclusive(|b| *b == b'\n') {
            if line.len() + part.len() > MAX_LINE_BYTES {
                return Err(invalid_line(line_number, "manifest line exceeds 16 KiB"));
            }
            line.extend_from_slice(part);
            if part.ends_with(b"\n") {
                append_line(&line, line_number, prefix, &mut selected, &mut seen)?;
                line.clear();
                line_number += 1;
            }
        }
    }
    if !line.is_empty() {
        append_line(&line, line_number, prefix, &mut selected, &mut seen)?;
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
