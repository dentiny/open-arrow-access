use std::{pin::Pin, sync::Arc};

use futures::{
    FutureExt, StreamExt, TryStreamExt,
    future::{BoxFuture, try_maybe_done},
    stream,
};

use open_arrow_access_core::{
    Accessor, Capability, ReadRequest, RecordCapability, RecordMetadata, RecordReader, Result,
    ScanPlan, ScanPlanner, ScanStatistics, Statistic, StatisticsProvider, async_trait,
    scan_partition,
};

use super::{
    FileSetRequest,
    error::empty_selection,
    validation::{validate_file_ordering, validate_file_policy},
};

pub(super) const METADATA_CONCURRENCY: usize = 16;

pub(super) type FilePlanner<A> = fn(Arc<A>, ReadRequest) -> BoxFuture<'static, Result<ScanPlan>>;

/// Read files sequentially through one supplied format accessor.
///
/// Every file must match one fixed projected schema, including metadata, unless
/// union by name is explicitly enabled. An empty selection is invalid; an empty file with a
/// known schema is supported.
#[derive(Debug)]
pub struct FileSetAccessor<A> {
    inner: Arc<A>,
    file_planner: Option<FilePlanner<A>>,
    union_by_name: bool,
}

impl<A> Clone for FileSetAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            file_planner: self.file_planner,
            union_by_name: self.union_by_name,
        }
    }
}

impl<A> FileSetAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
            file_planner: None,
            union_by_name: false,
        }
    }

    /// Union compatible schemas by name, recursively aligning Struct/List fields
    /// and filling missing fields with null. Inspect all files before emitting rows;
    /// files are opened with bounded concurrency and their plans are retained
    /// for execution, including inference samples. Projection is applied during
    /// alignment; retained readers decode the full source schema.
    /// Zero-column inputs with no rows are skipped; an all-empty inferred set fails.
    /// The default remains exact projected-schema matching.
    pub fn with_union_by_name(mut self) -> Self {
        self.union_by_name = true;
        self
    }

    /// Flatten native file partitions into one ordered scan and executor budget.
    /// Streaming formats use one worker per file in multi-file scans, so framing
    /// and downloading can overlap. Single-file and independent native partitions
    /// retain chunk parallelism. Ordinary `open` remains sequential.
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

    fn capability(&self) -> RecordCapability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(self.inner.capability().ordering)
    }

    async fn metadata(&self, request: FileSetRequest) -> Result<RecordMetadata> {
        if self.union_by_name {
            self.union_metadata(request).await
        } else {
            self.strict_metadata(request).await
        }
    }

    async fn open(&self, request: FileSetRequest) -> Result<Box<dyn RecordReader>> {
        let plan = self.plan_files(request, None, "open_file_set").await?;
        Ok(Box::new(plan.into_reader_with_schema_error(
            super::error::schema_mismatch,
        )))
    }
}

#[async_trait]
impl<A> ScanPlanner for FileSetAccessor<A>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    async fn plan_scan(&self, request: FileSetRequest) -> Result<ScanPlan> {
        self.plan_files(request, self.file_planner, "plan_file_set")
            .await
    }
}

impl<A> FileSetAccessor<A>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    async fn strict_metadata(&self, request: FileSetRequest) -> Result<RecordMetadata> {
        // Strict mode retains empty schemas and validates every selected file.
        let mut inspected = stream::iter(request.into_requests())
            .map(|request| self.inner.metadata(request))
            .buffered(METADATA_CONCURRENCY);
        let mut schema = None;
        while let Some(file) = inspected.try_next().await? {
            let expected = schema.get_or_insert_with(|| file.schema.clone());
            super::validation::validate_schema(&file.schema, expected)?;
        }
        let schema = schema.ok_or_else(|| empty_selection("metadata_file_set"))?;
        Ok(RecordMetadata {
            schema,
            source: None,
        })
    }

    async fn union_metadata(&self, mut request: FileSetRequest) -> Result<RecordMetadata> {
        if request.objects.is_empty() {
            return Err(empty_selection("metadata_file_set"));
        }
        let options = request.options.clone();
        request.options.projection = None;
        let mut inspected = stream::iter(request.into_requests())
            .map(|request| self.inner.inspect_metadata(request))
            .buffered(METADATA_CONCURRENCY);
        let mut union = super::schema::SchemaUnion::default();
        let mut selected = false;
        while let Some(file) = inspected.next().await {
            let Some(file) = file? else {
                continue;
            };
            super::validation::validate_fields(file.schema.fields())?;
            union.merge(&file.schema)?;
            selected = true;
        }
        if !selected {
            return Err(super::error::no_inferred_schema("union_by_name"));
        }
        Ok(RecordMetadata {
            schema: options
                .resolve_projection(&Arc::new(union.finish()))?
                .schema,
            source: None,
        })
    }

    async fn plan_files(
        &self,
        request: FileSetRequest,
        file_planner: Option<FilePlanner<A>>,
        operation: &'static str,
    ) -> Result<ScanPlan> {
        if self.union_by_name {
            return super::union_by_name::plan(self.inner.clone(), file_planner, request).await;
        }
        let multiple_files = request.objects.len() > 1;
        let inner = self.inner.clone();
        let mut files = request
            .into_requests()
            .map(move |request| {
                try_maybe_done(open_plan(inner.clone(), file_planner, request).boxed())
            })
            .peekable();
        // Establish the schema from a cached plan without removing that file
        // from the common planning, validation and execution pipeline.
        let preview = files.peek_mut().ok_or_else(|| empty_selection(operation))?;
        (&mut *preview).await?;
        let preview = Pin::new(preview).output_mut().expect("completed file plan");
        let ordering = self.capability().ordering.combine(preview.ordering);
        let metadata = RecordMetadata {
            schema: preview.metadata.schema.clone(),
            source: None,
        };
        let file_streams = file_planner.is_none() || multiple_files && preview.streaming_discovery;
        let schema = metadata.schema.clone();
        let plans = files.map(move |mut file| {
            let schema = schema.clone();
            async move {
                (&mut file).await?;
                let plan = Pin::new(&mut file)
                    .take_output()
                    .expect("completed file plan");
                validate_file_ordering(ordering, plan.ordering)?;
                super::validation::validate_schema(&plan.metadata.schema, &schema)?;
                Ok(plan)
            }
        });
        if file_streams {
            let partitions = plans.map(|plan| {
                scan_partition(async move {
                    Ok(plan
                        .await?
                        .into_reader_with_schema_error(super::error::schema_mismatch))
                })
            });
            return Ok(ScanPlan::from_iter(metadata, partitions)
                .with_file_streams()
                .with_ordering(ordering));
        }
        let partitions = stream::iter(plans)
            .then(|plan| plan)
            .map_ok(move |plan| {
                if multiple_files && plan.streaming_discovery {
                    ScanPlan::whole_input(Box::new(
                        plan.into_reader_with_schema_error(super::error::schema_mismatch),
                    ))
                    .partitions
                } else {
                    plan.partitions
                }
            })
            .try_flatten();
        Ok(ScanPlan::from_stream(metadata, partitions).with_ordering(ordering))
    }
}

pub(super) async fn open_plan<A>(
    inner: Arc<A>,
    file_planner: Option<FilePlanner<A>>,
    request: ReadRequest,
) -> Result<ScanPlan>
where
    A: Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    let plan = match file_planner {
        Some(plan) => plan(inner, request).await?,
        None => ScanPlan::whole_input(inner.open(request).await?),
    };
    validate_file_policy(&plan)?;
    Ok(plan)
}

#[async_trait]
impl<A> StatisticsProvider for FileSetAccessor<A>
where
    A: StatisticsProvider<Request = ReadRequest> + 'static,
{
    /// Inspect file metadata with bounded concurrency; never poll record readers.
    /// Duplicate selections count separately. No file-membership snapshot is added.
    async fn statistics(&self, mut request: FileSetRequest) -> Result<ScanStatistics> {
        if request.objects.is_empty() {
            return Err(empty_selection("statistics_file_set"));
        }
        // These statistics are independent of column projection. In union mode,
        // a selected column need not exist in every child file.
        if self.union_by_name {
            request.options.projection = None;
        }
        let mut files = stream::iter(request.into_requests())
            .map(|request| self.inner.statistics(request))
            .buffered(METADATA_CONCURRENCY);
        let mut total = ScanStatistics {
            row_count: Statistic::Exact(0),
            source_bytes: Statistic::Exact(0),
        };
        while let Some(file) = files.try_next().await? {
            total.row_count = sum_statistic(total.row_count, file.row_count);
            total.source_bytes = sum_statistic(total.source_bytes, file.source_bytes);
        }
        Ok(total)
    }
}

fn sum_statistic(left: Statistic<u64>, right: Statistic<u64>) -> Statistic<u64> {
    use Statistic::{Estimated, Exact, Unknown};
    let (left, right, exact) = match (left, right) {
        (Exact(left), Exact(right)) => (left, right, true),
        (Exact(left) | Estimated(left), Exact(right) | Estimated(right)) => (left, right, false),
        _ => return Unknown,
    };
    match left.checked_add(right) {
        Some(value) if exact => Exact(value),
        Some(value) => Estimated(value),
        None => Unknown,
    }
}
