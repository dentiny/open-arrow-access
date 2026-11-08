mod error;
mod http;
mod source;
mod url;
mod validation;

pub use http::create_http_accessor;
pub use source::CommonCrawl;

#[cfg(feature = "commoncrawl-segment")]
mod segment;
#[cfg(feature = "commoncrawl-segment")]
pub use segment::{CommonCrawlSegmentAccessor, CommonCrawlSegmentRequest};
