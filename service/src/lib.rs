//! Provider services resolve source semantics into shared object/reader contracts.

#[cfg(feature = "courtlistener")]
pub mod courtlistener;

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
