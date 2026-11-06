//! File-format adapters consume a ByteReader, never a storage-specific handle.

#[cfg(feature = "parquet")]
mod parquet;
#[cfg(feature = "parquet")]
pub use parquet::ParquetAccessor;

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
