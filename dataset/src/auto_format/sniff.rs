use super::{ResolvedKind, error::resolve as error};
use open_arrow_access_core::{
    ByteAccessor, ByteReadOptions, ByteReadRequest, ErrorKind, ObjectRef, Result,
};

/// Match the inspector convention: a partial first line is an ATIF document.
fn classify(line: &[u8]) -> ResolvedKind {
    let atif = match serde_json::from_slice::<serde_json::Value>(line) {
        Ok(value)
            if value
                .get("schema_version")
                .and_then(|v| v.as_str())
                .is_some_and(|v| v.starts_with("ATIF-")) =>
        {
            Some(true)
        }
        Err(error) if error.is_eof() => Some(false),
        _ => None,
    };
    match atif {
        None => ResolvedKind::Json,
        Some(lines) => {
            use open_arrow_access_file_format::atif::AtifFormat;
            ResolvedKind::Atif(if lines {
                AtifFormat::JsonLines
            } else {
                AtifFormat::Json
            })
        }
    }
}

pub(super) async fn probe(
    storage: &dyn ByteAccessor,
    object: &ObjectRef,
    options: ByteReadOptions,
    limit: usize,
) -> Result<Option<ResolvedKind>> {
    let mut input = storage
        .open(ByteReadRequest::new(object.clone()).with_options(options))
        .await
        .map_err(|error| error.with_object(Some(object)))?;
    let mut line = Vec::new();
    let mut consumed = 0;
    while let Some(chunk) = input
        .next()
        .await
        .map_err(|error| error.with_object(Some(object)))?
    {
        if chunk.is_empty() {
            return Err(error(
                ErrorKind::Io,
                "byte reader returned an empty chunk",
                Some(object),
            ));
        }
        for &byte in chunk.iter() {
            if consumed == limit {
                return Err(error(
                    ErrorKind::InvalidInput,
                    format!("JSON detection exceeds {limit}-byte limit"),
                    Some(object),
                ));
            }
            consumed += 1;
            if byte == b'\n' {
                if line.iter().any(|b: &u8| !b.is_ascii_whitespace()) {
                    return Ok(Some(classify(&line)));
                }
                line.clear();
            } else {
                line.push(byte);
            }
        }
    }
    if line.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(None);
    }
    Ok(Some(classify(&line)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use open_arrow_access_file_format::atif::AtifFormat;

    #[test]
    fn classifies_json_and_atif_without_reader_features() {
        for (line, expected) in [
            (b"{\"id\":1}".as_slice(), ResolvedKind::Json),
            (
                b"{\"schema_version\":\"ATIF-1.0\"}".as_slice(),
                ResolvedKind::Atif(AtifFormat::JsonLines),
            ),
            (b"{".as_slice(), ResolvedKind::Atif(AtifFormat::Json)),
        ] {
            assert_eq!(classify(line), expected);
        }
    }
}
