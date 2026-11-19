use std::sync::Arc;

use futures::StreamExt;
use open_arrow_access_core::{
    Accessor, ReadRequest, Reader, RecordCapability, RecordMetadata, RecordReader, Result,
    ScanPlan, ScanPlanner, StreamReader, async_trait, into_stream,
};

mod column;
pub(crate) use column::SourceColumn;
mod input;
pub use input::SourceColumnBatch;
mod output;
use output::SourceOutput;
#[cfg(feature = "source-positions")]
mod bytes;
#[cfg(feature = "source-positions")]
pub use bytes::{SourcePositionAccessor, SourcePositionLayer};

/// Append named source columns to decoded batches.
///
/// Object columns accept ordinary record readers. Offsets require a decoder
/// returning `PositionedBatch`: choose its positioned format before applying
/// this adapter. Coordinates are validated with each batch, without re-encoding
/// records, reopening readers, or buffering an entire source unit.
#[derive(Clone, Default)]
pub struct SourceColumns {
    columns: Vec<(SourceColumn, String)>,
}
impl SourceColumns {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_object(self, name: impl Into<String>) -> Self {
        self.with(SourceColumn::Object, name.into())
    }
    pub fn with_offset(self, name: impl Into<String>) -> Self {
        self.with(SourceColumn::Offset, name.into())
    }
    pub fn with_offset_in_frame(self, name: impl Into<String>) -> Self {
        self.with(SourceColumn::OffsetInFrame, name.into())
    }
    /// Adapt either ordinary or positioned batches into an ordinary Arrow reader.
    /// This changes the reader interface and therefore is an explicit adapter,
    /// rather than a type-preserving `Layer`.
    pub fn wrap<A>(&self, inner: A) -> SourceColumnsAccessor<A>
    where
        A: Accessor<Request = ReadRequest> + 'static,
        A::Reader: Reader<Metadata = RecordMetadata, Capability = RecordCapability>,
        <A::Reader as Reader>::Item: SourceColumnBatch,
    {
        SourceColumnsAccessor {
            inner: Arc::new(inner),
            config: self.clone(),
        }
    }

    fn with(mut self, column: SourceColumn, name: String) -> Self {
        if let Some((_, target)) = self.columns.iter_mut().find(|(c, _)| *c == column) {
            *target = name;
        } else {
            self.columns.push((column, name));
        }
        self
    }
}

pub struct SourceColumnsAccessor<A> {
    inner: Arc<A>,
    config: SourceColumns,
}
#[async_trait]
impl<A> Accessor for SourceColumnsAccessor<A>
where
    A: Accessor<Request = ReadRequest> + 'static,
    A::Reader: Reader<Metadata = RecordMetadata, Capability = RecordCapability>,
    <A::Reader as Reader>::Item: SourceColumnBatch,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        self.inner.capability()
    }
    async fn metadata(&self, mut request: ReadRequest) -> Result<RecordMetadata> {
        let object = request.object.clone();
        let context = |e: open_arrow_access_core::Error| e.with_object(Some(&object));
        crate::validation::validate_source_positions(
            &self.config.columns,
            <<A::Reader as Reader>::Item as SourceColumnBatch>::HAS_POSITIONS,
        )
        .map_err(context)?;
        let projection = request.options.projection.take();
        let mut metadata = self.inner.metadata(request).await.map_err(context)?;
        let output = SourceOutput::new(&metadata.schema, &self.config.columns, projection)
            .map_err(context)?;
        metadata.schema = output.schema();
        Ok(metadata)
    }

    async fn open(&self, mut request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        let object = request.object.clone();
        let context = |e: open_arrow_access_core::Error| e.with_object(Some(&object));
        crate::validation::validate_source_positions(
            &self.config.columns,
            <<A::Reader as Reader>::Item as SourceColumnBatch>::HAS_POSITIONS,
        )
        .map_err(context)?;
        let projection = request.options.projection.take();
        let reader = self.inner.open(request).await.map_err(context)?;
        let output = SourceOutput::new(&reader.metadata().schema, &self.config.columns, projection)
            .map_err(context)?;
        let mut metadata = reader.metadata().clone();
        metadata.schema = output.schema();
        let source = metadata.source.clone();
        let ordering = reader.capability().ordering;
        let batches = into_stream(reader).map(move |item| {
            let (batch, positions) = item?.into_source_parts();
            output.batch(batch, source.as_ref(), &positions)
        });
        Ok(Box::new(StreamReader::records(batches, metadata, ordering)))
    }
}
#[async_trait]
impl<A> ScanPlanner for SourceColumnsAccessor<A>
where
    A: Accessor<Request = ReadRequest> + 'static,
    A::Reader: Reader<Metadata = RecordMetadata, Capability = RecordCapability>,
    <A::Reader as Reader>::Item: SourceColumnBatch,
{
    async fn plan_scan(&self, request: ReadRequest) -> Result<ScanPlan> {
        let metadata = self.metadata(request.clone()).await?;
        let ordering = self.capability().ordering;
        let accessor = Self {
            inner: self.inner.clone(),
            config: self.config.clone(),
        };
        let partition = Box::pin(async move { accessor.open(request).await });
        Ok(ScanPlan::new(metadata, vec![partition]).with_ordering(ordering))
    }
}
