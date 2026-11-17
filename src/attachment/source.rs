use super::{AttachmentFormat, AttachmentOptions};
use crate::{
    Accessor, BinaryDecoder, BoundSource, ByteAccessor, DocumentAccessor, ReadRequest,
    RecordReader, Result,
};
use std::sync::Arc;

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
    let accessor = options.outer_compression.apply(accessor);
    match options.format {
        AttachmentFormat::Binary => {
            bind_records(DocumentAccessor::new(accessor, BinaryDecoder), request).await
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
                bind_records(FileAccessor::from_format(accessor, format), request).await
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
                bind_records(
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
                bind_records(ParquetAccessor::new(accessor), request).await
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
                bind_records(
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

// Erase the record accessor before binding. Besides keeping the async state small,
// this gives callers a Send future when using the helper from async-trait methods.
async fn bind_records(
    accessor: impl Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
    request: ReadRequest,
) -> Result<BoundSource> {
    let accessor: Arc<dyn Accessor<Request = ReadRequest, Reader = dyn RecordReader>> =
        Arc::new(accessor);
    BoundSource::bind(accessor, request).await
}
