mod accessor;
mod format;
mod reader;

pub use accessor::XmlAccessor;
pub use format::{XmlFormat, XmlLimits};
pub use xml_arrow::{
    InferenceOptions as XmlInferenceOptions, NamespaceBinding, XmlField, XmlSchema,
};

fn xml_error(error: xml_arrow::ReadError) -> open_arrow_access_core::Error {
    use open_arrow_access_core::ErrorKind;
    let fallback = match &error {
        xml_arrow::ReadError::Configuration(_) => ErrorKind::InvalidInput,
        _ => ErrorKind::Decode,
    };
    crate::error::source_error("read_xml", fallback, error)
}
