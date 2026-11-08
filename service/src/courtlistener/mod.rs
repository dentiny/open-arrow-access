//! CourtListener bulk table discovery and lossless decoded CSV byte access.
//!
//! Storage is rooted at the bucket/mirror root, above `bulk-data/`. The public
//! bucket is `com-courtlistener-storage`, in `us-west-2`. Configure OpenDAL S3
//! for anonymous access to discover snapshots; HTTP can read explicit objects.
//! No API key is required. Table schemas and record conversion remain separate.

mod accessor;
mod snapshot;

pub use accessor::CourtListenerBulkAccessor;
pub use snapshot::CourtListenerSnapshot;
