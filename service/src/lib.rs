//! Dataset source providers over shared accessor contracts.

#[cfg(feature = "commoncrawl")]
mod commoncrawl;
#[cfg(feature = "commoncrawl")]
pub use commoncrawl::{CommonCrawl, create_http_accessor};

#[cfg(feature = "commoncrawl-warc")]
pub use commoncrawl::{CommonCrawlWarcAccessor, CommonCrawlWarcRequest};
