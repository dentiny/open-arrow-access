//! Dataset providers composed from shared byte and record accessors.

#[cfg(feature = "commoncrawl")]
mod commoncrawl;
#[cfg(feature = "commoncrawl")]
pub use commoncrawl::{
    CommonCrawlSegmentAccessor, CommonCrawlSegmentRequest, create_http_accessor,
};
