use super::decode_error;
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use open_arrow_access_core::{
    BytePosition, ByteReader, Error, ErrorKind, Result, into_positioned_async_read,
};
pub(super) type RawRecord = (warc::RawRecordHeader, Vec<u8>);
pub(super) fn positioned_records(
    input: Box<dyn ByteReader>,
) -> Result<BoxStream<'static, Result<(RawRecord, BytePosition)>>> {
    if input.position().is_none() {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "read_warc",
            "byte input does not provide source positions",
        )
        .with_object(input.metadata().source.as_ref()));
    }
    let input = into_positioned_async_read(input, "read_warc");
    let observer = input.observer();
    let records = warc::WarcReader::new(input).iter_raw_records();
    Ok(
        stream::try_unfold((records, observer), |(mut records, observer)| async move {
            observer.mark();
            let Some(record) = records.next().await.transpose().map_err(decode_error)? else {
                return Ok(None);
            };
            let position = observer.marked_position().ok_or_else(|| {
                Error::new(
                    ErrorKind::Unsupported,
                    "read_warc",
                    "byte input lost source positions",
                )
            })?;
            Ok(Some(((record, position), (records, observer))))
        })
        .boxed(),
    )
}
