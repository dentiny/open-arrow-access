use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid_line(number: usize, message: &str) -> Error {
    Error::new(
        ErrorKind::Decode,
        "read_commoncrawl_manifest",
        format!("line {number}: {message}"),
    )
}

pub(super) fn empty_manifest_selection(prefix: &str) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "resolve_commoncrawl_warc",
        format!("manifest contains no files under {prefix:?}"),
    )
}

pub(super) fn manifest_utf8(number: usize, source: std::str::Utf8Error) -> Error {
    invalid_line(number, "manifest is not UTF-8").with_source(source)
}
