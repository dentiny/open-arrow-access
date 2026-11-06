use std::{sync::Arc, vec::IntoIter};

use open_arrow_access_core::{
    Accessor, ByteReadOptions, Capability, Error, ErrorKind, ObjectRef, ReadOptions, ReadRequest,
    Reader, RecordBatch, RecordMetadata, RecordReader, Result, async_trait,
};

pub(super) struct FileSetReader<A> {
    accessor: Arc<A>,
    current: Option<Box<dyn RecordReader>>,
    remaining: IntoIter<ObjectRef>,
    options: ReadOptions,
    byte_options: ByteReadOptions,
    metadata: RecordMetadata,
}

impl<A> FileSetReader<A> {
    pub(super) fn new(
        accessor: Arc<A>,
        current: Box<dyn RecordReader>,
        remaining: IntoIter<ObjectRef>,
        options: ReadOptions,
        byte_options: ByteReadOptions,
        metadata: RecordMetadata,
    ) -> Self {
        Self {
            accessor,
            current: Some(current),
            remaining,
            options,
            byte_options,
            metadata,
        }
    }
}

impl<A> FileSetReader<A>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader>,
{
    async fn read_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(batch) = self.current.as_mut().unwrap().next().await? {
                if batch.schema() != self.metadata.schema {
                    return Err(schema_mismatch());
                }
                return Ok(Some(batch));
            }
            self.current = None;
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
            if next.metadata().schema != self.metadata.schema {
                return Err(schema_mismatch());
            }
            self.current = Some(next);
        }
    }
}

#[async_trait]
impl<A> Reader for FileSetReader<A>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader>,
{
    type Item = RecordBatch;
    type Metadata = RecordMetadata;

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_range: false,
            list: false,
        }
    }

    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.current.is_none() {
            return Ok(None);
        }
        let result = self.read_batch().await;
        if !matches!(result, Ok(Some(_))) {
            self.current = None;
        }
        result
    }
}

fn schema_mismatch() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "read_file_set",
        "file or batch schema differs from the fixed dataset schema",
    )
}
