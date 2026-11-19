//! Explicit decoding of one selected provider attachment.

mod accessor;
mod error;
mod format;
mod options;
mod source;
mod validation;

pub use accessor::AttachmentAccessor;
pub use options::AttachmentOptions;
pub use source::open_attachment_source;

/// Interpretation of a selected attachment, independent of its provider or name.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AttachmentFormat {
    /// One unchanged object as one `data: LargeBinary` row.
    Binary,
    /// Infer comma-separated columns, consuming the first record as a header.
    Csv,
    /// Infer tab-separated columns, consuming the first record as a header.
    Tsv,
    /// Infer whitespace-separated JSON objects, including JSON lines.
    Json,
    /// Alias for the same object-stream decoder as `Json`.
    Jsonl,
    /// Read Parquet using its embedded schema and storage range access.
    Parquet,
    /// Infer fields below an explicit absolute XML record path.
    Xml { record_path: String },
}
