//! File-format adapters consume a ByteReader, never a storage-specific handle.

mod accessor;
mod scan;
pub use scan::DecodePartitionOptions;
#[cfg(any(
    feature = "parquet",
    feature = "warc",
    feature = "xml",
    feature = "json",
    feature = "csv"
))]
mod error;

pub use accessor::FileAccessor;
/// Previous name for the generalized reader factory protocol.
pub use open_arrow_access_core::ReaderFactory as FileFormat;

#[cfg(feature = "parquet")]
pub mod parquet;

#[cfg(feature = "warc")]
pub mod warc;

#[cfg(feature = "json")]
pub mod json;

#[cfg(feature = "csv")]
pub mod csv;

#[cfg(feature = "xml")]
pub mod xml;

#[cfg(feature = "atif")]
pub mod atif;

#[cfg(any(feature = "xml", feature = "json", feature = "csv"))]
mod document;
#[cfg(any(feature = "xml", feature = "json", feature = "csv"))]
pub use document::read_document;
