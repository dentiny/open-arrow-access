mod accessor;
mod format;
mod reader;

pub use accessor::XmlAccessor;
pub use format::{XmlFormat, XmlLimits};
pub use xml_arrow::{
    InferenceOptions as XmlInferenceOptions, NamespaceBinding, XmlField, XmlSchema,
};

fn xml_error(error: xml_arrow::ReadError) -> open_arrow_access_core::Error {
    use open_arrow_access_core::{Error, ErrorKind};
    let kind = ErrorKind::from_source(&error).unwrap_or(match &error {
        xml_arrow::ReadError::Configuration(_) => ErrorKind::InvalidInput,
        _ => ErrorKind::Decode,
    });
    Error::new(kind, "read_xml", error.to_string()).with_source(error)
}
