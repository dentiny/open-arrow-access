//! Provider services resolve source semantics into shared object/reader contracts.

mod opendal_error;
mod resolved_files;
#[cfg(any(feature = "internet-archive", feature = "pmc"))]
mod validation;
pub use resolved_files::ResolvedFileSet;

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

mod filename;

// Individual providers may use only part of the shared HTTP utilities.
#[allow(dead_code)]
mod http_client;

#[cfg(feature = "arxiv")]
pub mod arxiv;
#[cfg(feature = "dataverse")]
pub mod dataverse;
#[cfg(feature = "internet-archive")]
pub mod internet_archive;

/// Common utilities shared by provider services.
pub mod common;

#[cfg(feature = "format-xml")]
pub mod xml_metadata;

#[cfg(feature = "uspto")]
pub mod uspto;

#[cfg(feature = "oai")]
pub mod oai;

#[cfg(feature = "library-of-congress")]
pub mod library_of_congress;
