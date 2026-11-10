use open_arrow_access_core::Error;

pub(super) fn xml_error(error: xml_arrow::ReadError) -> open_arrow_access_core::Error {
    use open_arrow_access_core::ErrorKind;
    let fallback = match &error {
        xml_arrow::ReadError::Configuration(_) => ErrorKind::InvalidInput,
        _ => ErrorKind::Decode,
    };
    crate::error::source_error("read_xml", fallback, error)
}

pub(super) fn conversion(source: xml_arrow::Error, record_index: u64, byte_offset: u64) -> Error {
    xml_error(match source {
        xml_arrow::Error::Configuration(message) => xml_arrow::ReadError::Configuration(message),
        source => xml_arrow::ReadError::Conversion {
            record_index,
            byte_offset,
            source,
        },
    })
}
