mod http;
mod source;
mod url;

pub use http::create_http_accessor;
pub use source::CommonCrawl;

#[cfg(feature = "commoncrawl-warc")]
mod warc;
#[cfg(feature = "commoncrawl-warc")]
pub use warc::{CommonCrawlWarcAccessor, CommonCrawlWarcRequest};

#[cfg(feature = "commoncrawl-segment")]
mod segment;
#[cfg(feature = "commoncrawl-segment")]
pub use segment::{CommonCrawlSegmentAccessor, CommonCrawlSegmentRequest};
