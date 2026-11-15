use std::sync::Arc;

use super::{AttachmentFormat, AttachmentOptions};
use crate::{
    AccessorExt, BinaryDecoder, BoundSource, ByteAccessor, DocumentAccessor, OuterCompression,
    ReadRequest, Result, decompression::AutoDecompressionLayer,
};

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
                Err(super::error::missing_format("format-csv"))
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
                Err(super::error::missing_format("format-json"))
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
                Err(super::error::missing_format("format-parquet"))
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
                Err(super::error::missing_format("format-xml"))
            }
        }
    }
}
