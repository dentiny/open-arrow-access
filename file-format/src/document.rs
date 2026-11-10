use crate::error::document_row_count;
use open_arrow_access_core::{
    Bytes, ReadRequest, ReaderFactory, ReaderFactoryExt, RecordBatch, RecordReader, Result,
    bytes_reader,
};

/// Decode one buffered document through a configured file format. Exactly one
/// row is required, and the complete input is consumed to detect trailing errors.
/// Providers can then validate domain fields and project the resulting batch.
pub async fn read_document<F>(bytes: Bytes, format: F, request: ReadRequest) -> Result<RecordBatch>
where
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader>,
{
    let input = bytes_reader(bytes, Some(request.object.clone()));
    let mut reader = format.open_input(Box::new(input), request).await?;
    let batch = reader.next().await?.ok_or_else(document_row_count)?;
    if batch.num_rows() != 1 || reader.next().await?.is_some() {
        return Err(document_row_count());
    }
    Ok(batch)
}
