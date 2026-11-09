use open_arrow_access_core::{Error, ErrorKind};
const OP: &str = "validate_xml_root";
pub(super) fn parse(error: quick_xml::Error) -> Error {
    Error::new(ErrorKind::Decode, OP, "invalid XML metadata").with_source(error)
}
pub(super) fn root(expected: &str) -> Error {
    Error::new(
        ErrorKind::Decode,
        OP,
        format!("expected XML root {expected}"),
    )
}
