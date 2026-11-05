mod accessor;
mod http;
mod manifest;
mod request;

pub use accessor::CommonCrawlSegmentAccessor;
pub use http::create_http_accessor;
pub use request::CommonCrawlSegmentRequest;
