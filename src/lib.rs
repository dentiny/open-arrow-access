//! Composable, asynchronous read access to Arrow record batches.
//!
//! Enable a storage feature and `format-parquet`, `format-csv`, or `format-warc` to read files.
//! The default build exposes only the shared extension contracts.

pub use open_arrow_access_core::*;

#[cfg(feature = "storage-fs")]
pub mod storage {
    pub use open_arrow_access_storage::FsAccessor;
}

#[cfg(any(
    feature = "format-parquet",
    feature = "format-csv",
    feature = "format-warc"
))]
pub mod file_format {
    #[cfg(feature = "format-parquet")]
    pub use open_arrow_access_file_format::ParquetAccessor;
    #[cfg(feature = "format-csv")]
    pub use open_arrow_access_file_format::{CsvAccessor, CsvFormat};
    #[cfg(feature = "format-warc")]
    pub use open_arrow_access_file_format::{WarcAccessor, warc_schema};
}
