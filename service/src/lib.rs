//! Provider services resolve source semantics into shared object/reader contracts.
//!
//! Decode resolved files and plan scans with [`ResolvedFileSet::records`], or bind
//! selected JSON records with [`common::ApiDocument::records_at`]. Formats are
//! explicit; metadata links are never followed automatically.

mod file_manifest;
mod opendal_error;
mod resolved_files;
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

// Individual providers may use only part of the shared HTTP utilities.
mod http_body;
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

#[cfg(feature = "doab")]
pub mod doab;

#[cfg(feature = "arxiv")]
pub mod arxiv_metadata;

#[cfg(feature = "hathifiles")]
pub mod hathifiles;

#[cfg(feature = "federal-register")]
pub mod federal_register;

#[cfg(feature = "govinfo")]
pub mod govinfo;

#[cfg(feature = "oapen")]
pub mod oapen;

#[cfg(feature = "figshare")]
pub mod figshare;

#[cfg(feature = "library-of-congress")]
pub mod library_of_congress;

#[cfg(feature = "meca")]
pub mod meca;

#[cfg(feature = "biorxiv")]
pub mod biorxiv;

#[cfg(feature = "openstax")]
pub mod openstax;

#[cfg(feature = "semantic-scholar")]
pub mod semantic_scholar;

#[cfg(feature = "bhl")]
pub mod bhl;

#[cfg(feature = "regulations")]
pub mod regulations;
