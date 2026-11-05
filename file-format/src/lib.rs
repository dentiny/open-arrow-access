//! File-format adapters consume a ByteReader, never a storage-specific handle.

#[cfg(feature = "parquet")]
mod parquet;
#[cfg(feature = "parquet")]
pub use parquet::ParquetAccessor;

#[cfg(feature = "warc")]
mod warc;
#[cfg(feature = "warc")]
pub use warc::{WarcAccessor, warc_schema};

#[cfg(feature = "csv")]
mod csv;
#[cfg(feature = "csv")]
pub use csv::{CsvAccessor, CsvFormat};
