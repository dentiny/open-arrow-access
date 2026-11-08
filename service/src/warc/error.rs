use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn invalid_line(number: usize, message: &str) -> Error {
    Error::new(
        ErrorKind::Decode,
        "read_commoncrawl_manifest",
        format!("line {number}: {message}"),
    )
}
