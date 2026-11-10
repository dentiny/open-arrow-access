use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, future::BoxFuture, stream};

use open_arrow_access_core::{
    Accessor, Capability, OrderRequirement, ReadRequest, RecordMetadata, RecordReader, Result,
    ScanPartition, ScanPlan, ScanPlanner, async_trait,
};

use super::{
    FileSetRequest,
    error::{empty_selection, schema_mismatch},
    reader::create_reader,
    validation::{validate_file_ordering, validate_file_policy},
};

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
    ordering: OrderRequirement,
}

impl<A> Clone for FileSetAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            file_planner: self.file_planner,
            ordering: self.ordering,
        }
    }
}

impl<A> FileSetAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
            file_planner: None,
            ordering: OrderRequirement::PreserveGlobal,
        }
    }

    /// Flatten native file partitions into one ordered scan and executor budget.
    /// Later files are planned lazily; ordinary `open` remains sequential.
    pub fn with_file_planning(mut self) -> Self
    where
        A: ScanPlanner<Request = ReadRequest> + 'static,
    {
        self.ordering = self.inner.order_requirement();
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
        let mut requests = request.into_requests();
        let first = requests
            .next()
            .ok_or_else(|| empty_selection("open_file_set"))?;
        let reader = self.inner.open(first).await?;
        let metadata = RecordMetadata {
            schema: reader.metadata().schema.clone(),
            source: None,
        };
        Ok(Box::new(create_reader(
            self.inner.clone(),
            reader,
            requests,
            metadata,
        )))
    }
}

#[async_trait]
impl<A> ScanPlanner for FileSetAccessor<A>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    fn order_requirement(&self) -> OrderRequirement {
        self.ordering
    }

    async fn plan_scan(&self, request: FileSetRequest) -> Result<ScanPlan> {
        let mut requests = request.into_requests();
        let first = requests
            .next()
            .ok_or_else(|| empty_selection("plan_file_set"))?;
        let first = match self.file_planner {
            Some(plan_file) => plan_file(self.inner.clone(), first).await?,
            None => ScanPlan::whole_input(self.inner.open(first).await?),
        };
        validate_file_policy(&first)?;
        let ordering = self.ordering.combine(first.ordering);
        let metadata = RecordMetadata {
            schema: first.metadata.schema,
            source: None,
        };
        let inner = self.inner.clone();
        if let Some(plan_file) = self.file_planner {
            let schema = metadata.schema.clone();
            let later = stream::iter(requests).then(move |request| {
                let inner = inner.clone();
                let schema = schema.clone();
                async move {
                    let plan = plan_file(inner, request).await?;
                    validate_file_policy(&plan)?;
                    validate_file_ordering(ordering, plan.ordering)?;
                    if plan.metadata.schema != schema {
                        return Err(schema_mismatch());
                    }
                    Ok(plan.partitions)
                }
            });
            Ok(
                ScanPlan::from_stream(metadata, first.partitions.chain(later.try_flatten()))
                    .with_ordering(ordering),
            )
        } else {
            let later = requests.map(move |request| {
                let inner = inner.clone();
                let partition: ScanPartition = Box::pin(async move { inner.open(request).await });
                Ok(partition)
            });
            Ok(
                ScanPlan::from_stream(metadata, first.partitions.chain(stream::iter(later)))
                    .with_ordering(ordering),
            )
        }
    }
}
