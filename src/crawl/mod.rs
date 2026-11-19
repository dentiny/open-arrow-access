//! Discover service selections and execute bounded, parallel Arrow crawls.
//!
//! See the [crawl guide](https://github.com/dentiny/open-arrow-access/blob/main/docs/crawl.md)
//! for manifest, streaming discovery, normalization and tuning examples.

mod accessor;
mod discovery;
mod options;

pub use accessor::CrawlAccessor;
pub use discovery::{CrawlResolver, ResolvedCrawl};
pub use options::CrawlOptions;
