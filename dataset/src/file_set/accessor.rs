use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, future::BoxFuture, stream};

use open_arrow_access_core::{
    Accessor, Capability, Error, ErrorKind, ReadRequest, RecordMetadata, RecordReader, Result,
    ScanPartition, ScanPlan, ScanPlanner, async_trait,
};

use super::{FileSetRequest, error::schema_mismatch, reader::create_reader};

type FilePlanner<A> = fn(Arc<A>, ReadRequest) -> BoxFuture<'static, Result<ScanPlan>>;

/// Read files sequentially through one supplied format accessor.
///
/// The first file determines the fixed output schema. Later files must have
/// exactly the same projected schema, including metadata. An empty selection
/// is invalid; an empty file with a known schema is supported.
#[derive(Debug)]
pub struct FileSetAccessor<A> {
    inner: Arc<A>,
    file_planner: Option<FilePlanner<A>>,
}

impl<A> Clone for FileSetAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            file_planner: self.file_planner,
        }
    }
}

impl<A> FileSetAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
            file_planner: None,
        }
    }

    /// Flatten native file partitions into one ordered scan and executor budget.
    /// Later files are planned lazily; ordinary `open` remains sequential.
    pub fn with_file_planning(mut self) -> Self
    where
        A: ScanPlanner<Request = ReadRequest> + 'static,
    {
        self.file_planner =
            Some(|inner, request| Box::pin(async move { inner.plan_scan(request).await }));
        self
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
        if let Some(plan_file) = self.file_planner {
            let first = plan_file(
                self.inner.clone(),
                ReadRequest::new(first)
                    .with_options(request.options.clone())
                    .with_byte_options(request.byte_options),
            )
            .await?;
            let metadata = RecordMetadata {
                schema: first.metadata.schema,
                source: None,
            };
            let schema = metadata.schema.clone();
            let inner = self.inner.clone();
            let later = stream::iter(objects).then(move |object| {
                let inner = inner.clone();
                let schema = schema.clone();
                let read_request = ReadRequest::new(object)
                    .with_options(request.options.clone())
                    .with_byte_options(request.byte_options);
                async move {
                    let plan = plan_file(inner, read_request).await?;
                    if plan.metadata.schema != schema {
                        return Err(schema_mismatch());
                    }
                    Ok(plan.partitions)
                }
            });
            return Ok(ScanPlan::from_stream(
                metadata,
                first.partitions.chain(later.try_flatten()),
            ));
        }
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
        Ok(ScanPlan::new(metadata, partitions))
    }
}
