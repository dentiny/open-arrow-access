//! Provider services resolve source semantics into shared object/reader contracts.

#[cfg(feature = "gutenberg")]
pub mod gutenberg;

#[cfg(feature = "huggingface")]
pub mod huggingface;
