//! Storage adapters use OpenDAL and expose only the shared read contracts.

#[cfg(any(feature = "fs", feature = "huggingface"))]
mod backend;
#[cfg(feature = "fs")]
mod fs;
#[cfg(feature = "fs")]
pub use fs::FsAccessor;

#[cfg(all(test, feature = "fs"))]
mod tests;

#[cfg(feature = "huggingface")]
mod huggingface;
#[cfg(feature = "huggingface")]
pub use huggingface::{HuggingFaceAccessor, HuggingFaceBuilder};
