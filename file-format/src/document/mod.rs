mod error;

use futures::stream;
use open_arrow_access_core::{
    ByteMetadata, Bytes, ReadRequest, ReaderFactory, ReaderFactoryExt, RecordBatch, RecordReader,
    Result, StreamReader,
};

/// Decode one buffered document through a configured file format. Exactly one
/// row is required, and the complete input is consumed to detect trailing errors.
/// Providers can then validate domain fields and project the resulting batch.
pub async fn read_document<F>(bytes: Bytes, format: F, request: ReadRequest) -> Result<RecordBatch>
where
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
{
    let metadata = ByteMetadata {
        source: Some(request.object.clone()),
        content_length: Some(bytes.len() as u64),
        version: None,
    };
    let input = StreamReader::new(
        stream::iter((!bytes.is_empty()).then_some(Ok(bytes))),
        metadata,
    );
    let mut reader = format.open_input(Box::new(input), request).await?;
    let batch = reader.next().await?.ok_or_else(error::row_count)?;
    if batch.num_rows() != 1 || reader.next().await?.is_some() {
        return Err(error::row_count());
    }
    Ok(batch)
}
