//! XML record access with explicit or inferred schemas.
//!
//! `ScanPlanner` frames explicit-schema documents into independently decoded
//! record candidates. Each candidate retains its original ancestor envelope;
//! the ordinary namespace-aware reader decides whether it matches the record
//! path. This preserves namespace aliases and default-namespace overrides.
//! Inferred schemas retain one bounded sample/replay reader in a whole-input plan.
//!
//! Parallel planning bounds retained ancestor tags by `max_record_bytes` and each
//! serialized envelope/record by `max_batch_bytes`. These framing limits exclude
//! upstream buffers and Arrow allocations. Decoder offsets are relative to the
//! framed document; planning does not provide random storage reads.

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
