//! Provider services resolve source semantics into shared object/reader contracts.

#[cfg(feature = "huggingface")]
pub mod huggingface;

#[cfg(feature = "commoncrawl")]
pub mod commoncrawl;

#[cfg(feature = "commoncrawl-warc")]
pub mod warc;
