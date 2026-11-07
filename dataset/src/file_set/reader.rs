use std::{sync::Arc, vec::IntoIter};

use futures::stream;
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ObjectRef, ReadOptions, ReadRequest, RecordBatch, RecordMetadata,
    RecordReader, Result, SchemaRef, StreamReader,
};

use super::error::schema_mismatch;

struct FileSetState<A> {
    accessor: Arc<A>,
    current: Option<Box<dyn RecordReader>>,
    remaining: IntoIter<ObjectRef>,
    options: ReadOptions,
    byte_options: ByteReadOptions,
    schema: SchemaRef,
}

impl<A> FileSetState<A>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader>,
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
            let Some(object) = self.remaining.next() else {
                return Ok(None);
            };
            let next = self
                .accessor
                .open(
                    ReadRequest::new(object)
                        .with_options(self.options.clone())
                        .with_byte_options(self.byte_options),
                )
                .await?;
            if next.metadata().schema != self.schema {
                return Err(schema_mismatch());
            }
            self.current = Some(next);
        }
    }
}

/// Create an ordered file-set reader with shared lifecycle handling.
pub(super) fn create_reader<A>(
    accessor: Arc<A>,
    current: Box<dyn RecordReader>,
    remaining: IntoIter<ObjectRef>,
    options: ReadOptions,
    byte_options: ByteReadOptions,
    metadata: RecordMetadata,
) -> impl RecordReader
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    let state = FileSetState {
        accessor,
        current: Some(current),
        remaining,
        options,
        byte_options,
        schema: metadata.schema.clone(),
    };
    let stream = stream::try_unfold(state, |mut state| async move {
        Ok(state.read_batch().await?.map(|batch| (batch, state)))
    });
    StreamReader::new(stream, metadata)
}
