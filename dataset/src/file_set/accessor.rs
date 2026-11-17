use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, future::BoxFuture, stream};

use open_arrow_access_core::{
    Accessor, Capability, ReadRequest, RecordCapability, RecordMetadata, RecordReader, Result,
    ScanPartition, ScanPlan, ScanPlanner, async_trait,
};

use super::{
    FileSetRequest,
    error::empty_selection,
    validation::{validate_file_ordering, validate_file_policy},
};

pub(super) type FilePlanner<A> = fn(Arc<A>, ReadRequest) -> BoxFuture<'static, Result<ScanPlan>>;

/// Read files sequentially through one supplied format accessor.
///
/// The first file determines the fixed output schema. Later files must have
/// exactly the same projected schema, including metadata, unless union by name
/// is explicitly enabled. An empty selection is invalid; an empty file with a
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
    /// readers are then reopened lazily with only selected source columns.
    /// Their projected schema must match the corresponding inspected fields.
    /// Zero-column inputs with no rows are skipped; an all-empty inferred set fails.
    /// The default remains exact projected-schema matching.
    pub fn with_union_by_name(mut self) -> Self {
        self.union_by_name = true;
        self
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

    fn capability(&self) -> RecordCapability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
        .with_ordering(self.inner.capability().ordering)
    }

    async fn metadata(&self, mut request: FileSetRequest) -> Result<RecordMetadata> {
        let options = request.options.clone();
        if self.union_by_name {
            request.options.projection = None;
        }
        let mut inspected = stream::iter(request.into_requests())
            .map(|request| self.inner.inspect_metadata(request))
            .buffered(16);
        let first = inspected
            .next()
            .await
            .ok_or_else(|| empty_selection("metadata_file_set"))??;
        if !self.union_by_name {
            // As with ordinary open, the first file establishes the output schema.
            // Validate later files during inspection without opening record readers.
            let schema = first.metadata.schema;
            while let Some(file) = inspected.next().await {
                super::validation::validate_schema(&file?.metadata.schema, &schema)?;
            }
            return Ok(RecordMetadata {
                schema,
                source: None,
            });
        }
        let mut union = super::schema::SchemaUnion::default();
        let mut selected = false;
        let inspected = stream::once(async { Ok(first) }).chain(inspected);
        futures::pin_mut!(inspected);
        while let Some(file) = inspected.next().await {
            let file = file?;
            if file.metadata.schema.fields().is_empty() && file.is_empty == Some(true) {
                continue;
            }
            super::validation::validate_fields(file.metadata.schema.fields())?;
            union.merge(&file.metadata.schema)?;
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
    async fn plan_files(
        &self,
        request: FileSetRequest,
        file_planner: Option<FilePlanner<A>>,
        operation: &'static str,
    ) -> Result<ScanPlan> {
        if self.union_by_name {
            return super::union_by_name::plan(self.inner.clone(), file_planner, request).await;
        }
        let mut requests = request.into_requests();
        let first = requests.next().ok_or_else(|| empty_selection(operation))?;
        let first = match file_planner {
            Some(plan_file) => plan_file(self.inner.clone(), first).await?,
            None => ScanPlan::whole_input(self.inner.open(first).await?),
        };
        validate_file_policy(&first)?;
        let ordering = self.capability().ordering.combine(first.ordering);
        let metadata = RecordMetadata {
            schema: first.metadata.schema,
            source: None,
        };
        let inner = self.inner.clone();
        if let Some(plan_file) = file_planner {
            let schema = metadata.schema.clone();
            let later = stream::iter(requests).then(move |request| {
                let inner = inner.clone();
                let schema = schema.clone();
                async move {
                    let plan = plan_file(inner, request).await?;
                    validate_file_policy(&plan)?;
                    validate_file_ordering(ordering, plan.ordering)?;
                    super::validation::validate_schema(&plan.metadata.schema, &schema)?;
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
