//! XML record access with explicit or inferred schemas.
//!
//! Explicit and inferred scans use xml-arrow's native record streams, preserving
//! namespace matching, input limits and source offsets for independent conversion.

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
