//! Explicit decoding of one selected provider attachment.

use std::sync::Arc;

use crate::{
    AccessorExt, BinaryDecoder, BoundSource, ByteAccessor, DocumentAccessor, Error, ErrorKind,
    OuterCompression, ReadRequest, Result, decompression::AutoDecompressionLayer,
};

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

/// Format selection and compression surrounding the entire attachment.
#[derive(Clone, Debug)]
pub struct AttachmentOptions {
    pub format: AttachmentFormat,
    pub outer_compression: OuterCompression,
}

impl AttachmentOptions {
    /// Binary and Parquet default to unchanged bytes/ranges; text formats use
    /// suffix-based outer decompression. Enable the required format/codec features.
    pub fn new(format: AttachmentFormat) -> Self {
        let outer_compression = match &format {
            AttachmentFormat::Binary | AttachmentFormat::Parquet => OuterCompression::None,
            _ => OuterCompression::Auto,
        };
        Self {
            format,
            outer_compression,
        }
    }

    pub fn with_outer_compression(mut self, compression: OuterCompression) -> Self {
        self.outer_compression = compression;
        self
    }

    /// Validate configuration and format availability without storage/provider I/O.
    /// Decoder-specific options and projections are validated during binding.
    pub fn validate(&self) -> Result<()> {
        if self.format == AttachmentFormat::Binary
            && self.outer_compression != OuterCompression::None
        {
            return Err(invalid("binary attachments require OuterCompression::None"));
        }
        if let AttachmentFormat::Xml { record_path } = &self.format
            && record_path.trim().is_empty()
        {
            return Err(invalid("XML attachments require a nonempty record path"));
        }
        let (feature, enabled) = match &self.format {
            AttachmentFormat::Binary => return Ok(()),
            AttachmentFormat::Csv | AttachmentFormat::Tsv => {
                ("format-csv", cfg!(feature = "format-csv"))
            }
            AttachmentFormat::Json | AttachmentFormat::Jsonl => {
                ("format-json", cfg!(feature = "format-json"))
            }
            AttachmentFormat::Parquet => ("format-parquet", cfg!(feature = "format-parquet")),
            AttachmentFormat::Xml { .. } => ("format-xml", cfg!(feature = "format-xml")),
        };
        if enabled {
            Ok(())
        } else {
            Err(missing_format(feature))
        }
    }
}

impl Default for AttachmentOptions {
    fn default() -> Self {
        Self::new(AttachmentFormat::Binary)
    }
}

fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, "open_attachment_source", message)
}

fn missing_format(feature: &str) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        "open_attachment_source",
        format!("attachment decoding requires Cargo feature '{feature}'"),
    )
}

/// Bind one logical attachment name and complete read request to an Arrow source.
///
/// The supplied byte accessor owns file membership, authentication and storage.
/// No provider URLs are resolved here. Configuration is checked before opening
/// bytes; binding retains the first reader and subsequent readers reopen independently.
/// Binary mode buffers the whole object on first consumption, without decompression.
/// More specialized schemas/dialects can use the existing format APIs directly.
///
/// ```no_run
/// use open_arrow_access::{
///     AttachmentFormat, AttachmentOptions, BoundSource, ByteAccessor, ReadRequest,
///     Result, open_attachment_source,
/// };
///
/// // Enable format-json. `files` can be any provider's resolved byte accessor.
/// async fn selected_json<A: ByteAccessor + 'static>(files: A) -> Result<BoundSource> {
///     open_attachment_source(
///         files,
///         ReadRequest::new("events.jsonl.gz"),
///         AttachmentOptions::new(AttachmentFormat::Jsonl),
///     ).await
/// }
/// ```
pub async fn open_attachment_source<A>(
    accessor: A,
    request: ReadRequest,
    options: AttachmentOptions,
) -> Result<BoundSource>
where
    A: ByteAccessor + 'static,
{
    options.validate()?;
    let accessor: Arc<dyn ByteAccessor> = match options.outer_compression {
        OuterCompression::None => Arc::new(accessor),
        OuterCompression::Auto => Arc::new(accessor.layer(AutoDecompressionLayer::new())),
    };
    match options.format {
        AttachmentFormat::Binary => {
            BoundSource::bind(DocumentAccessor::new(accessor, BinaryDecoder), request).await
        }
        AttachmentFormat::Csv | AttachmentFormat::Tsv => {
            #[cfg(feature = "format-csv")]
            {
                use crate::file_format::{
                    FileAccessor,
                    csv::{CsvDialect, CsvFileFormat, CsvHeader},
                };
                let format = CsvFileFormat::infer().with_dialect(CsvDialect {
                    header: CsvHeader::Present,
                    delimiter: if options.format == AttachmentFormat::Tsv {
                        b'\t'
                    } else {
                        b','
                    },
                    ..Default::default()
                });
                BoundSource::bind(FileAccessor::from_format(accessor, format), request).await
            }
            #[cfg(not(feature = "format-csv"))]
            {
                Err(missing_format("format-csv"))
            }
        }
        AttachmentFormat::Json | AttachmentFormat::Jsonl => {
            #[cfg(feature = "format-json")]
            {
                use crate::file_format::{FileAccessor, json::JsonFormat};
                BoundSource::bind(
                    FileAccessor::from_format(accessor, JsonFormat::infer()),
                    request,
                )
                .await
            }
            #[cfg(not(feature = "format-json"))]
            {
                Err(missing_format("format-json"))
            }
        }
        AttachmentFormat::Parquet => {
            #[cfg(feature = "format-parquet")]
            {
                use crate::file_format::parquet::ParquetAccessor;
                BoundSource::bind(ParquetAccessor::new(accessor), request).await
            }
            #[cfg(not(feature = "format-parquet"))]
            {
                Err(missing_format("format-parquet"))
            }
        }
        AttachmentFormat::Xml { record_path } => {
            #[cfg(feature = "format-xml")]
            {
                use crate::file_format::{FileAccessor, xml::XmlFormat};
                BoundSource::bind(
                    FileAccessor::from_format(accessor, XmlFormat::new(record_path)),
                    request,
                )
                .await
            }
            #[cfg(not(feature = "format-xml"))]
            {
                let _ = record_path;
                Err(missing_format("format-xml"))
            }
        }
    }
}
