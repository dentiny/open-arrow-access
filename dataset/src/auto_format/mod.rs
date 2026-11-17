//! Pure suffix detection and policy-driven selection of dataset files.

mod policy;
pub use policy::{DetectedFormat, FormatPolicy, detect_format};
