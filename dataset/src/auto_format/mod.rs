//! Pure suffix detection and selection of dataset files.

mod detection;
pub use detection::{DetectedFormat, detect_format, select_format};
