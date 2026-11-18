use std::{num::NonZeroUsize, sync::Arc};

use futures::StreamExt;

use crate::{
    Accessor, BoundSource, Error, ErrorKind, ReadOptions, ReadOptionsProvider, RecordMetadata,
    RecordReader, Result, ScanPartition, ScanPlan, ScanPlanner, SchemaRef, StreamReader,
    async_trait, into_stream, schema_checked_reader, validate_schema,
};

/// A typed selection with a complete bound schema and execution-time read options.
///
/// Unlike [`crate::BoundSource`], each scan may choose its own projection and batch
/// size. Preparation opens an unprojected reader without polling rows; its schema
/// is the reference for all later scans. It does not cache format metadata or
/// provide a snapshot of the source contents. Accessor and request types are erased.
/// No runtime or producer tasks are created.
pub struct PreparedSource {
    selection: Box<dyn PreparedSelection>,
    bound: BoundSource,
    batch_size: NonZeroUsize,
}

/// Erased access to a saved selection and its optional partition planner.
#[async_trait]
trait PreparedSelection: Send + Sync {
    /// Open the saved selection with the supplied common read options.
    /// The caller validates the returned reader against the bound schema.
    async fn open(&self, options: ReadOptions) -> Result<Box<dyn RecordReader>>;

    /// Plan the saved selection with the supplied common read options.
    /// Sources may perform planning I/O; returned partition openers remain lazy.
    /// The default rejects planning when its interface was not retained.
    async fn plan_scan(&self, _options: ReadOptions) -> Result<ScanPlan> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "prepared_source_plan",
            "prepare_plannable is required to retain partition planning",
        ))
    }
}

struct Selection<A: Accessor> {
    accessor: A,
    request: A::Request,
}

#[async_trait]
impl<A> PreparedSelection for Selection<A>
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: Clone + Sync + ReadOptionsProvider,
{
    /// Clone the saved request and replace only its common read options.
    async fn open(&self, options: ReadOptions) -> Result<Box<dyn RecordReader>> {
        let mut request = self.request.clone();
        *request.read_options_mut() = options;
        self.accessor.open(request).await
    }
}

struct PlannableSelection<A: Accessor>(Selection<A>);

#[async_trait]
impl<A> PreparedSelection for PlannableSelection<A>
where
    A: ScanPlanner,
    A::Request: Clone + Sync + ReadOptionsProvider,
{
    /// Use the accessor's ordinary open path for a normal scan.
    async fn open(&self, options: ReadOptions) -> Result<Box<dyn RecordReader>> {
        self.0.open(options).await
    }

    /// Forward planning while retaining the accessor's ordering requirement.
    async fn plan_scan(&self, options: ReadOptions) -> Result<ScanPlan> {
        let mut request = self.0.request.clone();
        *request.read_options_mut() = options;
        Ok(self
            .0
            .accessor
            .plan_scan(request)
            .await?
            .require_ordering(self.0.accessor.capability().ordering))
    }
}

impl PreparedSource {
    /// Inspect the complete schema of a typed selection, retaining its first reader.
    ///
    /// The request's projection is cleared for preparation; pass the desired
    /// projection to [`Self::scan`]. Its batch size and all selection/provider/byte
    /// options are preserved. Only the common read options change between scans.
    /// Preparation fixes the schema, not the source version or file membership.
    ///
    /// ```no_run
    /// use open_arrow_access_core::{
    ///     Accessor, PreparedSource, ReadOptions, ReadRequest, RecordReader, Result,
    /// };
    ///
    /// async fn query(
    ///     accessor: impl Accessor<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
    /// ) -> Result<()> {
    ///     let prepared = PreparedSource::prepare(accessor, ReadRequest::new("data")).await?;
    ///     let mut options = ReadOptions::default();
    ///     options.projection = Some(vec![prepared.schema().field(0).name().clone()]);
    ///     let mut reader = prepared.scan(options).await?;
    ///     while let Some(batch) = reader.next().await? { /* consume batch */ }
    ///     Ok(())
    /// }
    /// ```
    pub async fn prepare<A>(accessor: A, request: A::Request) -> Result<Self>
    where
        A: Accessor<Reader = dyn RecordReader> + 'static,
        A::Request: Clone + Sync + ReadOptionsProvider + 'static,
    {
        Self::prepare_with(accessor, request, |accessor, request| {
            Box::new(Selection { accessor, request })
        })
        .await
    }

    /// Prepare a source while retaining its optional [`ScanPlanner`] interface.
    ///
    /// Like [`Self::prepare`], this retains the first unprojected reader through
    /// [`BoundSource`]. [`Self::scan`] still executes the accessor's `open` path;
    /// [`Self::plan_scan`] explicitly exposes its independent partitions.
    pub async fn prepare_plannable<A>(accessor: A, request: A::Request) -> Result<Self>
    where
        A: ScanPlanner + 'static,
        A::Request: Clone + Sync + ReadOptionsProvider + 'static,
    {
        Self::prepare_with(accessor, request, |accessor, request| {
            Box::new(PlannableSelection(Selection { accessor, request }))
        })
        .await
    }

    async fn prepare_with<A>(
        accessor: A,
        mut request: A::Request,
        selection: impl FnOnce(Arc<A>, A::Request) -> Box<dyn PreparedSelection>,
    ) -> Result<Self>
    where
        A: Accessor<Reader = dyn RecordReader> + 'static,
        A::Request: Clone + Sync + ReadOptionsProvider + 'static,
    {
        request.read_options_mut().projection = None;
        let batch_size = request.read_options().batch_size;
        let accessor = Arc::new(accessor);
        let bound = BoundSource::bind(accessor.clone(), request.clone()).await?;
        Ok(Self {
            selection: selection(accessor, request),
            bound,
            batch_size,
        })
    }

    /// Complete metadata established by preparation, without polling rows.
    pub fn metadata(&self) -> &RecordMetadata {
        self.bound.metadata()
    }

    /// Complete unprojected schema against which every scan is validated.
    pub fn schema(&self) -> SchemaRef {
        self.bound.schema()
    }

    /// Open an independent scan with execution-time batch size and projection.
    ///
    /// Unknown or duplicate fields fail before opening another reader. An empty
    /// projection requests zero columns with preserved row counts. The retained
    /// reader is consumed only by a scan with the preparation options; other scans
    /// reopen the same typed selection. Readers own their state and outlive this
    /// source. Both opened metadata and emitted batches must match the projected
    /// bound schema, including schema metadata; drift returns `Decode`.
    pub async fn scan(&self, options: ReadOptions) -> Result<Box<dyn RecordReader>> {
        let schema = options.resolve_projection(&self.schema())?.schema;
        let reader = if options.projection.is_none() && options.batch_size == self.batch_size {
            self.bound.reader().await?
        } else {
            self.selection.open(options).await?
        };
        schema_checked_reader(reader, schema, crate::error::prepared_schema_mismatch)
    }

    /// Plan independent partitions with execution-time projection and batch size.
    ///
    /// Returns `Unsupported` unless prepared with [`Self::prepare_plannable`].
    /// Plan, partition-reader, and batch schemas must match the projected bound
    /// schema. Discovery and opening stay lazy, and the initial reader is retained
    /// for a compatible [`Self::scan`]. Plans/readers outlive this source.
    ///
    /// Source ordering and the partition/prefetch counts and emission policy
    /// represented by [`ScanPlan`] are preserved. The consumer must honor those
    /// policies; [`ScanPlan::into_reader`] is only a sequential primitive. Use
    /// [`Self::scan`] to execute the composed accessor's policies. Accessors
    /// without [`ScanPlanner`] cannot use this planning path.
    pub async fn plan_scan(&self, options: ReadOptions) -> Result<ScanPlan> {
        let schema = options.resolve_projection(&self.schema())?.schema;
        let mut plan = self.selection.plan_scan(options).await?;
        validate_schema(
            &plan.metadata.schema,
            &schema,
            crate::error::prepared_schema_mismatch,
        )?;
        // Reuse terminal stream cleanup without polling discovery or openers.
        let partitions = into_stream(Box::new(StreamReader::new(plan.partitions, ())));
        plan.partitions = partitions
            .map(move |partition| {
                let schema = schema.clone();
                partition.map(|partition| {
                    Box::pin(async move {
                        schema_checked_reader(
                            partition.await?,
                            schema,
                            crate::error::prepared_schema_mismatch,
                        )
                    }) as ScanPartition
                })
            })
            .boxed();
        Ok(plan)
    }
}
