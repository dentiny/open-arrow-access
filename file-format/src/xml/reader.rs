use futures::TryStreamExt;
use open_arrow_access_core::{RecordMetadata, RecordReader, record_stream_reader};

use super::xml_error;

pub(super) fn xml_reader(
    reader: xml_arrow::XmlReader<'static>,
    metadata: RecordMetadata,
    projection: Option<Vec<usize>>,
) -> impl RecordReader {
    let stream = reader.into_stream().map_err(xml_error);
    record_stream_reader(stream, metadata, projection)
}
