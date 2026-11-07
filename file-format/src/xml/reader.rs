use futures::stream;
use open_arrow_access_core::{Error, ErrorKind, RecordMetadata, RecordReader, StreamReader};

use super::xml_error;

/// Adapt native XML batches while retaining format-specific projection and errors.
pub(super) fn xml_reader(
    reader: xml_arrow::XmlReader<'static>,
    metadata: RecordMetadata,
    projection: Option<Vec<usize>>,
) -> impl RecordReader {
    let stream = stream::try_unfold(
        (reader, projection),
        |(mut reader, projection)| async move {
            let Some(batch) = reader.next_batch().await.map_err(xml_error)? else {
                return Ok(None);
            };
            let batch = match &projection {
                None => batch,
                Some(indices) => batch.project(indices).map_err(|error| {
                    Error::new(ErrorKind::Decode, "read_xml", error.to_string()).with_source(error)
                })?,
            };
            Ok(Some((batch, (reader, projection))))
        },
    );
    StreamReader::new(stream, metadata)
}
