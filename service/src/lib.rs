//! Provider services resolve source semantics into shared object/reader contracts.

#[cfg(feature = "courtlistener")]
pub mod courtlistener;

#[cfg(feature = "gutenberg")]
pub mod gutenberg;

#[cfg(feature = "huggingface")]
pub mod huggingface;

#[cfg(feature = "warc")]
pub mod commoncrawl;

#[cfg(feature = "warc")]
pub mod warc;

#[cfg(feature = "pmc")]
pub mod pmc_inventory;

#[cfg(feature = "pmc")]
pub mod pmc;

#[cfg(feature = "zenodo")]
pub mod zenodo;

// Individual providers may use only part of the shared HTTP utilities.
#[allow(dead_code)]
mod http_client;

#[cfg(feature = "internet-archive")]
pub mod internet_archive;
