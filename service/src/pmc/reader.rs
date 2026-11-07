use std::sync::Arc;

use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadRequest, ByteReader, ReadRequest, RecordMetadata, RecordReader, StreamReader,
    read_limited,
};

use super::{PmcLimits, metadata};

/// Read one bounded metadata object with shared terminal-state handling.
pub(super) fn create_reader<A>(
    accessor: Arc<A>,
    request: ReadRequest,
    limits: PmcLimits,
    metadata: RecordMetadata,
) -> impl RecordReader
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader> + 'static,
{
    let schema = metadata.schema.clone();
    let stream = stream::once(async move {
        let input = accessor
            .open(ByteReadRequest::new(request.object.clone()).with_options(request.byte_options))
            .await?;
        let raw = read_limited(input, limits.metadata_bytes.get(), "read_pmc").await?;
        metadata::decode_batch(&raw, request.object.locator(), schema)
    });
    StreamReader::new(stream, metadata)
}
