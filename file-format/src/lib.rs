//! File-format adapters consume a ByteReader, never a storage-specific handle.

#[cfg(feature = "parquet")]
mod parquet;
#[cfg(feature = "parquet")]
pub use parquet::ParquetAccessor;

#[cfg(feature = "warc")]
mod warc;
#[cfg(feature = "warc")]
pub use warc::{WarcAccessor, WarcHeader, WarcRecord};

#[cfg(feature = "json")]
mod json;
#[cfg(feature = "json")]
pub use json::JsonAccessor;

#[cfg(feature = "csv")]
mod csv;
#[cfg(feature = "csv")]
pub use csv::{CsvAccessor, CsvFormat};

#[cfg(feature = "xml")]
mod xml;
#[cfg(feature = "xml")]
pub use xml::{NamespaceBinding, XmlAccessor, XmlField, XmlInferenceOptions, XmlLimits, XmlSchema};

#[cfg(feature = "atif")]
mod atif;
#[cfg(feature = "atif")]
pub use atif::{AtifAccessor, AtifFormat};
