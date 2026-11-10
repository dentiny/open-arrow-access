use std::sync::Arc;

use open_arrow_access_core::{
    Accessor, Capability, Error, ErrorKind, ReadRequest, RecordMetadata, RecordReader, Result,
    ScanPartition, ScanPlan, ScanPlanner, async_trait,
};

use super::{FileSetRequest, reader::create_reader};

/// Read files sequentially through one supplied format accessor.
///
/// The first file determines the fixed output schema. Later files must have
/// exactly the same projected schema, including metadata. An empty selection
/// is invalid; an empty file with a known schema is supported.
#[derive(Debug)]
pub struct FileSetAccessor<A> {
    inner: Arc<A>,
}

impl<A> Clone for FileSetAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<A> FileSetAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[async_trait]
impl<A> Accessor for FileSetAccessor<A>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    type Request = FileSetRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: FileSetRequest) -> Result<Box<dyn RecordReader>> {
        let mut objects = request.objects.into_iter();
        let first = objects.next().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "open_file_set",
                "file selection is empty",
            )
        })?;
        let reader = self
            .inner
            .open(
                ReadRequest::new(first)
                    .with_options(request.options.clone())
                    .with_byte_options(request.byte_options),
            )
            .await?;
        let metadata = RecordMetadata {
            schema: reader.metadata().schema.clone(),
            source: None,
        };
        Ok(Box::new(create_reader(
            self.inner.clone(),
            reader,
            objects,
            request.options,
            request.byte_options,
            metadata,
        )))
    }
}

#[async_trait]
impl<A> ScanPlanner for FileSetAccessor<A>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    async fn plan_scan(&self, request: FileSetRequest) -> Result<ScanPlan> {
        let mut objects = request.objects.into_iter();
        let first = objects.next().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "plan_file_set",
                "file selection is empty",
            )
        })?;
        let first = self
            .inner
            .open(
                ReadRequest::new(first)
                    .with_options(request.options.clone())
                    .with_byte_options(request.byte_options),
            )
            .await?;
        let metadata = RecordMetadata {
            schema: first.metadata().schema.clone(),
            source: None,
        };
        let mut partitions: Vec<ScanPartition> = vec![Box::pin(async move { Ok(first) })];
        for object in objects {
            let inner = self.inner.clone();
            let read_request = ReadRequest::new(object)
                .with_options(request.options.clone())
                .with_byte_options(request.byte_options);
            partitions.push(Box::pin(async move { inner.open(read_request).await }));
        }
        Ok(ScanPlan {
            metadata,
            partitions,
        })
    }
}
