use open_arrow_access_core::{Error, ErrorKind};

pub(super) fn xml_error(error: xml_arrow::ReadError) -> open_arrow_access_core::Error {
    use open_arrow_access_core::ErrorKind;
    let fallback = match &error {
        xml_arrow::ReadError::Configuration(_) => ErrorKind::InvalidInput,
        _ => ErrorKind::Decode,
    };
    crate::error::source_error("read_xml", fallback, error)
}

pub(super) fn framing(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    crate::error::source_error("frame_xml", ErrorKind::Decode, source)
}

pub(super) fn invalid(message: &'static str) -> Error {
    Error::new(ErrorKind::Decode, "frame_xml", message)
}

pub(super) fn limit(kind: xml_arrow::Limit, limit: usize, byte_offset: u64) -> Error {
    xml_error(xml_arrow::ReadError::LimitExceeded {
        kind,
        limit,
        byte_offset,
    })
}
