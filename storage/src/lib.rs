//! Storage adapters use OpenDAL and expose only the shared read contracts.
//!
//! Clone one HTTP limiter across operators to share a total request budget:
//! ```no_run
//! use open_arrow_access_storage::{HttpConcurrencyLimitLayer, OpenDalAccessor, opendal};
//!
//! fn limited(operator: opendal::Operator, limit: HttpConcurrencyLimitLayer)
//!     -> OpenDalAccessor
//! {
//!     OpenDalAccessor::new(operator.layer(limit))
//! }
//! # fn main() -> open_arrow_access_core::Result<()> {
//! let limit = HttpConcurrencyLimitLayer::new(8)?;
//! // Pass limit.clone() to each operator, or use limit.wrap(transport).
//! # Ok(())
//! # }
//! ```

mod backend;
mod config;
mod error;
mod http_limit;
pub use config::{RetryConfig, StorageConfig, TimeoutConfig};
pub use http_limit::HttpConcurrencyLimitLayer;
mod validation;

pub use backend::OpenDalAccessor;

/// OpenDAL SDK used by the generic adapter.
pub use opendal;

mod named_files;
pub use named_files::NamedFileAccessor;
pub use validation::validate_file_names;

pub use error::opendal_error_kind;
