//! Composable, asynchronous read access to Arrow record batches.
//!
//! Enable a storage feature and a `format-parquet`, `format-json`, or `format-csv` reader.
//! The default build exposes only the shared extension contracts.

pub use open_arrow_access_core::*;

#[cfg(any(feature = "storage-fs", feature = "storage-huggingface"))]
pub mod storage {
    #[cfg(feature = "storage-fs")]
    pub use open_arrow_access_storage::FsAccessor;
    #[cfg(feature = "storage-huggingface")]
    pub use open_arrow_access_storage::{HuggingFaceAccessor, HuggingFaceBuilder};
}

#[cfg(any(
    feature = "format-parquet",
    feature = "format-json",
    feature = "format-csv"
))]
pub mod file_format {
    #[cfg(feature = "format-json")]
    pub use open_arrow_access_file_format::JsonAccessor;
    #[cfg(feature = "format-parquet")]
    pub use open_arrow_access_file_format::ParquetAccessor;
    #[cfg(feature = "format-csv")]
    pub use open_arrow_access_file_format::{CsvAccessor, CsvFormat};
}
