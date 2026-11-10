//! XML record access with explicit or inferred schemas.
//!
//! Explicit-schema scans use xml-arrow's native record stream and independently
//! convert each selected record to Arrow. Namespace matching, input limits and
//! source offsets come from the ordinary XML parser. Inference uses one reader.

mod accessor;
mod error;
mod format;
mod planning;
use error::xml_error;

pub use accessor::XmlAccessor;
pub use format::{XmlFormat, XmlLimits};
pub use xml_arrow::{
    InferenceOptions as XmlInferenceOptions, NamespaceBinding, XmlField, XmlSchema,
};
