use futures::stream;
use open_arrow_access_core::{RecordMetadata, RecordReader, record_stream_reader};

use super::xml_error;

/// Adapt native XML batches while retaining format-specific projection and errors.
pub(super) fn xml_reader(
    reader: xml_arrow::XmlReader<'static>,
    metadata: RecordMetadata,
    projection: Option<Vec<usize>>,
) -> impl RecordReader {
    let stream = stream::try_unfold(reader, |mut reader| async move {
        let Some(batch) = reader.next_batch().await.map_err(xml_error)? else {
            return Ok(None);
        };
        Ok(Some((batch, reader)))
    });
    record_stream_reader(stream, metadata, projection)
}
