use open_arrow_access_core::{Error, ErrorKind};
const OP: &str = "read_xml_metadata";

pub(super) fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::Decode, OP, message)
}

pub(super) fn parse(error: quick_xml::Error) -> Error {
    invalid("invalid XML metadata").with_source(error)
}

pub(super) fn xml(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    invalid("invalid XML metadata").with_source(error)
}

pub(super) fn utf8(error: std::str::Utf8Error) -> Error {
    invalid("invalid XML namespace").with_source(error)
}

pub(super) fn root(expected: &str) -> Error {
    invalid(&format!("expected XML root {expected}"))
}
