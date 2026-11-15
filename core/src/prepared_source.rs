use std::{num::NonZeroUsize, sync::Arc};

use crate::{
    Accessor, BoundSource, ReadOptions, ReadOptionsProvider, RecordMetadata, RecordReader, Result,
    SchemaRef, async_trait, map_record_reader, validation::validate_prepared_schema,
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

#[async_trait]
trait PreparedSelection: Send + Sync {
    async fn open(&self, options: ReadOptions) -> Result<Box<dyn RecordReader>>;
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
    async fn open(&self, options: ReadOptions) -> Result<Box<dyn RecordReader>> {
        let mut request = self.request.clone();
        *request.read_options_mut() = options;
        self.accessor.open(request).await
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
    pub async fn prepare<A>(accessor: A, mut request: A::Request) -> Result<Self>
    where
        A: Accessor<Reader = dyn RecordReader> + 'static,
        A::Request: Clone + Sync + ReadOptionsProvider + 'static,
    {
        request.read_options_mut().projection = None;
        let batch_size = request.read_options().batch_size;
        let accessor = Arc::new(accessor);
        let bound = BoundSource::bind(accessor.clone(), request.clone()).await?;
        Ok(Self {
            selection: Box::new(Selection { accessor, request }),
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
        let (schema, _) = options.resolve_projection(&self.schema())?;
        let reader = if options.projection.is_none() && options.batch_size == self.batch_size {
            self.bound.reader().await?
        } else {
            self.selection.open(options).await?
        };
        validate_prepared_schema(&reader.metadata().schema, &schema)?;
        let capability = reader.capability();
        Ok(Box::new(map_record_reader(
            reader,
            schema.clone(),
            capability,
            move |batch| {
                validate_prepared_schema(&batch.schema(), &schema)?;
                Ok(batch)
            },
        )))
    }
}
