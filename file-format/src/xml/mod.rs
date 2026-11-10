//! XML record access with explicit or inferred schemas.
//!
//! `ScanPlanner` frames explicit-schema documents into independently decoded
//! record candidates. Each candidate retains its original ancestor envelope;
//! the ordinary namespace-aware reader decides whether it matches the record
//! path. This preserves namespace aliases and default-namespace overrides.
//! Inferred schemas retain one bounded sample/replay reader in a whole-input plan.
//!
//! Framing preserves the sequential reader's event and depth checks; the
//! authoritative decoder retains the ordinary record and batch limits. Complete
//! ancestor envelopes are retained with their records and may exceed those byte
//! targets: the parallel task window bounds partition counts, not total memory.
//! Decoder offsets are relative to the framed document; planning does not provide
//! random storage reads.

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
