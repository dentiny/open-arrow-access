use futures::stream;
use open_arrow_access_core::{RecordMetadata, RecordReader, StreamReader, project_batch};

use super::xml_error;

/// Adapt native XML batches while retaining format-specific projection and errors.
pub(super) fn xml_reader(
    reader: xml_arrow::XmlReader<'static>,
    metadata: RecordMetadata,
    projection: Option<Vec<usize>>,
) -> impl RecordReader {
    let stream = stream::try_unfold(
        (reader, projection, metadata.schema.clone()),
        |(mut reader, projection, schema)| async move {
            let Some(batch) = reader.next_batch().await.map_err(xml_error)? else {
                return Ok(None);
            };
            let batch = project_batch(batch, projection.as_deref(), schema.clone())?;
            Ok(Some((batch, (reader, projection, schema))))
        },
    );
    StreamReader::new(stream, metadata)
}
