//! Provider services resolve source semantics into shared object/reader contracts.

#[cfg(feature = "huggingface")]
pub mod huggingface;

#[cfg(feature = "commoncrawl")]
mod commoncrawl;
#[cfg(feature = "commoncrawl")]
pub use commoncrawl::{CommonCrawl, create_http_accessor};

#[cfg(feature = "commoncrawl-warc")]
pub use commoncrawl::{CommonCrawlWarcAccessor, CommonCrawlWarcRequest};
