use std::pin::Pin;

use futures::{
    StreamExt,
    io::AsyncBufRead,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{
    BytePosition, ByteReader, Result, SourceColumn, into_async_read, into_positioned_async_read,
};

use super::decode_error;

/// A complete parser unit, retaining its origin through parallel decode planning.
pub(super) struct RawRecord {
    pub headers: warc::RawRecordHeader,
    pub body: Vec<u8>,
    pub position: Option<BytePosition>,
}
/// Share framing between sequential batches and independently decoded partitions.
/// Ordinary reads use the existing unpositioned adapter.
pub(super) fn read_records(
    input: Box<dyn ByteReader>,
    columns: &[SourceColumn],
) -> Result<BoxStream<'static, Result<RawRecord>>> {
    let track = columns.iter().any(|column| *column != SourceColumn::Object);
    let (input, observer): (Pin<Box<dyn AsyncBufRead + Send>>, _) = if track {
        crate::validation::validate_positioned_source(input.as_ref())?;
        let input = into_positioned_async_read(input, "read_warc");
        let observer = input.observer();
        (Box::pin(input), Some(observer))
    } else {
        (Box::pin(into_async_read(input, "read_warc")), None)
    };
    let records = warc::WarcReader::new(input).iter_raw_records();
    Ok(
        stream::try_unfold((records, observer), |(mut records, observer)| async move {
            if let Some(observer) = &observer {
                observer.mark();
            }
            let Some((headers, body)) = records.next().await.transpose().map_err(decode_error)?
            else {
                return Ok(None);
            };
            let position = observer
                .as_ref()
                .map(|observer| {
                    crate::validation::require_source_position(observer.marked_position())
                })
                .transpose()?;
            Ok(Some((
                RawRecord {
                    headers,
                    body,
                    position,
                },
                (records, observer),
            )))
        })
        .boxed(),
    )
}
