use futures::stream;
use open_arrow_access_core::{ByteReader, ObjectRef, Reader, StreamReader, into_async_read};

use super::{WarcRecord, decode_error};

/// Adapt parsed records with shared input and terminal-state handling.
pub(super) fn create_reader(
    input: Box<dyn ByteReader>,
    source: ObjectRef,
) -> impl Reader<Item = WarcRecord, Metadata = ObjectRef> {
    let input = into_async_read(input, "read_warc");
    let records = warc::WarcReader::new(input).iter_records();
    let stream = stream::try_unfold(records, |mut records| async move {
        Ok(records
            .next()
            .await
            .transpose()
            .map_err(decode_error)?
            .map(|record| (record, records)))
    });
    StreamReader::new(stream, source)
}
