use std::sync::Mutex;

use crate::{
    Accessor, Error, ErrorKind, ReadOptions, ReadOptionsProvider, RecordMetadata, RecordReader,
    Result, SchemaRef, async_trait, map_record_reader,
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
    metadata: RecordMetadata,
    initial: Mutex<Option<InitialReader>>,
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

struct InitialReader {
    options: ReadOptions,
    reader: Box<dyn RecordReader>,
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
    ///     Accessor, PreparedSource, ReadOptions, ReadOptionsProvider, RecordReader, Result,
    /// };
    ///
    /// async fn query<A>(accessor: A, request: A::Request) -> Result<()>
    /// where
    ///     A: Accessor<Reader = dyn RecordReader> + 'static,
    ///     A::Request: Clone + Sync + ReadOptionsProvider + 'static,
    /// {
    ///     let prepared = PreparedSource::prepare(accessor, request).await?;
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
        let options = request.read_options().clone();
        let reader = accessor.open(request.clone()).await?;
        let metadata = reader.metadata().clone();
        Ok(Self {
            selection: Box::new(Selection { accessor, request }),
            metadata,
            initial: Mutex::new(Some(InitialReader { options, reader })),
        })
    }

    /// Complete metadata established by preparation, without polling rows.
    pub fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    /// Complete unprojected schema against which every scan is validated.
    pub fn schema(&self) -> SchemaRef {
        self.metadata.schema.clone()
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
        let (schema, _) = options.resolve_projection(&self.metadata.schema)?;
        let initial = {
            let mut initial = self.initial.lock().map_err(|_| {
                Error::new(
                    ErrorKind::Io,
                    "prepared_source_scan",
                    "prepared source state is poisoned",
                )
            })?;
            if initial.as_ref().is_some_and(|initial| {
                initial.options.batch_size == options.batch_size
                    && initial.options.projection == options.projection
            }) {
                initial.take().map(|initial| initial.reader)
            } else {
                None
            }
        };
        let reader = match initial {
            Some(reader) => reader,
            None => self.selection.open(options).await?,
        };
        validate_schema(&reader.metadata().schema, &schema)?;
        let capability = reader.capability();
        Ok(Box::new(map_record_reader(
            reader,
            schema.clone(),
            capability,
            move |batch| {
                validate_schema(&batch.schema(), &schema)?;
                Ok(batch)
            },
        )))
    }
}

fn validate_schema(actual: &SchemaRef, expected: &SchemaRef) -> Result<()> {
    if actual != expected {
        return Err(Error::new(
            ErrorKind::Decode,
            "prepared_source_scan",
            "scan schema differs from the projected schema fixed at preparation",
        ));
    }
    Ok(())
}
