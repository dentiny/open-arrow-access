//! Provider services resolve source semantics into shared object/reader contracts.

#[cfg(feature = "huggingface")]
pub mod huggingface;

#[cfg(feature = "pmc")]
pub mod pmc;
#[cfg(feature = "commoncrawl")]
pub mod commoncrawl;
