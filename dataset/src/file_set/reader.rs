use std::sync::Arc;

use futures::stream;
use open_arrow_access_core::{
    Accessor, ReadRequest, RecordBatch, RecordMetadata, RecordReader, Result, SchemaRef,
    StreamReader,
};

use super::error::schema_mismatch;

struct FileSetState<A, I> {
    accessor: Arc<A>,
    current: Option<Box<dyn RecordReader>>,
    remaining: I,
    schema: SchemaRef,
}

impl<A, I> FileSetState<A, I>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader>,
    I: Iterator<Item = ReadRequest>,
{
    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(reader) = self.current.as_mut() {
                if let Some(batch) = reader.next().await? {
                    if batch.schema() != self.schema {
                        return Err(schema_mismatch());
                    }
                    return Ok(Some(batch));
                }
                self.current = None;
            }
            let Some(request) = self.remaining.next() else {
                return Ok(None);
            };
            let next = self.accessor.open(request).await?;
            if next.metadata().schema != self.schema {
                return Err(schema_mismatch());
            }
            self.current = Some(next);
        }
    }
}

/// Create an ordered file-set reader with shared lifecycle handling.
pub(super) fn create_reader<A, I>(
    accessor: Arc<A>,
    current: Box<dyn RecordReader>,
    remaining: I,
    metadata: RecordMetadata,
) -> impl RecordReader
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
    I: Iterator<Item = ReadRequest> + Send + 'static,
{
    let state = FileSetState {
        accessor,
        current: Some(current),
        remaining,
        schema: metadata.schema.clone(),
    };
    let stream = stream::try_unfold(state, |mut state| async move {
        Ok(state.read_batch().await?.map(|batch| (batch, state)))
    });
    StreamReader::new(stream, metadata)
}
