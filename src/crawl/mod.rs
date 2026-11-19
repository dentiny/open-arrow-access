//! Discover service selections and execute bounded, parallel Arrow crawls.
//!
//! See the [crawl guide](https://github.com/dentiny/open-arrow-access/blob/main/docs/crawl.md)
//! for abstraction boundaries, workflow diagrams and stage-by-stage tuning.
//!
//! Workflow:
//! 1. Resolve the service selection, or use an existing dataset/service planner.
//! 2. Bind selected file identities to storage/codecs/format and build a lazy scan plan.
//! 3. Share one evolution plan across bounded partition workers.
//! 4. Merge ordered or ready batches through the existing executor's queues.
//! 5. Optionally coalesce and prefetch the merged stream before consumption.
//!
//! [`CrawlOptions`] exposes the layers' configurations. File/partition concurrency,
//! per-reader byte I/O and shared HTTP limits control different stages; the guide
//! explains their interaction and the resulting memory/backpressure boundaries.

mod accessor;
mod discovery;
mod options;

pub use accessor::CrawlAccessor;
pub use discovery::{CrawlResolver, ResolvedCrawl};
pub use options::CrawlOptions;
