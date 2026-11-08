use std::collections::HashSet;

use futures::TryStreamExt;
use open_arrow_access_core::{ByteReader, Error, ErrorKind, ObjectRef, Result, read_lines};

use super::{error::invalid_line, validation::validate_manifest_path};

pub(super) async fn read_manifest(
    input: Box<dyn ByteReader>,
    prefix: &str,
) -> Result<Vec<ObjectRef>> {
    let lines = read_lines(input, None, "read_commoncrawl_manifest");
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
    validate_manifest_path(path, number)?;
    if path.starts_with(prefix) {
        if !seen.insert(path.to_owned()) {
            return Err(invalid_line(number, "duplicate selected file"));
        }
        selected.push(ObjectRef::new(path));
    }
    Ok(())
}
