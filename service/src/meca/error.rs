use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn decode(message: &str) -> Error {
    Error::new(ErrorKind::Decode, "meca_manifest", message)
}

pub(super) fn xml(error: quick_xml::DeError) -> Error {
    decode("invalid MECA manifest").with_source(error)
}

pub(super) fn utf8(error: std::str::Utf8Error) -> Error {
    decode("MECA href is not UTF-8").with_source(error)
}
