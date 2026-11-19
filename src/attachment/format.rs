use std::sync::Arc;

use super::{AttachmentFormat, AttachmentOptions};
use crate::{BinaryDecoder, ByteAccessor, DocumentAccessor, ReadRequest, Result, ScanPlanner};

pub(super) type AttachmentRecords = dyn ScanPlanner<Request = ReadRequest>;

/// Bind format and compression once without inspecting or opening the selected file.
pub(super) fn records<A: ByteAccessor + 'static>(
    accessor: A,
    options: AttachmentOptions,
) -> Result<Arc<AttachmentRecords>> {
    options.validate()?;
    let accessor = options.outer_compression.apply(accessor);
    let records: Arc<AttachmentRecords> = match options.format {
        AttachmentFormat::Binary => Arc::new(DocumentAccessor::new(accessor, BinaryDecoder)),
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
                Arc::new(FileAccessor::from_format(accessor, format))
            }
            #[cfg(not(feature = "format-csv"))]
            {
                return Err(super::error::missing_format("format-csv"));
            }
        }
        AttachmentFormat::Json | AttachmentFormat::Jsonl => {
            #[cfg(feature = "format-json")]
            {
                use crate::file_format::{FileAccessor, json::JsonFormat};
                Arc::new(FileAccessor::from_format(accessor, JsonFormat::infer()))
            }
            #[cfg(not(feature = "format-json"))]
            {
                return Err(super::error::missing_format("format-json"));
            }
        }
        AttachmentFormat::Parquet => {
            #[cfg(feature = "format-parquet")]
            {
                use crate::file_format::parquet::ParquetAccessor;
                Arc::new(ParquetAccessor::new(accessor))
            }
            #[cfg(not(feature = "format-parquet"))]
            {
                return Err(super::error::missing_format("format-parquet"));
            }
        }
        AttachmentFormat::Xml { record_path } => {
            #[cfg(feature = "format-xml")]
            {
                use crate::file_format::{FileAccessor, xml::XmlFormat};
                Arc::new(FileAccessor::from_format(
                    accessor,
                    XmlFormat::new(record_path),
                ))
            }
            #[cfg(not(feature = "format-xml"))]
            {
                let _ = record_path;
                return Err(super::error::missing_format("format-xml"));
            }
        }
    };
    Ok(records)
}
