//! File-format adapters consume a ByteReader, never a storage-specific handle.

mod accessor;
mod format;

pub use accessor::FileAccessor;
pub use format::FileFormat;

#[cfg(feature = "parquet")]
mod parquet;
#[cfg(feature = "parquet")]
pub use parquet::{ParquetAccessor, ParquetFormat};

#[cfg(feature = "json")]
mod json;
#[cfg(feature = "json")]
pub use json::{JsonAccessor, JsonFormat};

#[cfg(feature = "csv")]
mod csv;
#[cfg(feature = "csv")]
pub use csv::{CsvAccessor, CsvFileFormat, CsvFormat};

#[cfg(feature = "xml")]
mod xml;
#[cfg(feature = "xml")]
pub use xml::{
    NamespaceBinding, XmlAccessor, XmlField, XmlFormat, XmlInferenceOptions, XmlLimits, XmlSchema,
};

#[cfg(feature = "atif")]
mod atif;
#[cfg(feature = "atif")]
pub use atif::{AtifAccessor, AtifFormat};
