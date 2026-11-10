//! XML record access with explicit or inferred schemas.
//!
//! Explicit-schema scans decode complete record candidates in parallel, preserving
//! ancestor namespaces and native parser limits. Inferred schemas use one reader.
//! Partition counts are bounded by the executor; ancestor envelopes may exceed
//! byte targets. Decoder offsets are relative to each framed document.

mod accessor;
mod error;
mod format;
mod planning;
mod validation;
use error::xml_error;

pub use accessor::XmlAccessor;
pub use format::{XmlFormat, XmlLimits};
pub use xml_arrow::{
    InferenceOptions as XmlInferenceOptions, NamespaceBinding, XmlField, XmlSchema,
};
