use std::sync::Arc;

use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, Bytes, FactoryAccessor, Layer, ReadRequest,
    ReaderFactory, ReaderFactoryExt, RecordCapability, RecordReader, Result, ScanPartition,
    ScanPlan, ScanPlanner, async_trait, bytes_reader,
};

mod column;
pub(crate) use column::SourceColumn;
mod framer;
mod output;
pub use framer::{SourceFramer, SourceUnit};
use output::SourceOutput;
#[cfg(feature = "source-positions")]
mod bytes;
#[cfg(feature = "source-positions")]
pub use bytes::{SourcePositionAccessor, SourcePositionLayer};

/// Opt-in source columns around an ordinary format factory.
///
/// Apply to a record-producing `FactoryAccessor`. Offsets require an explicit user-provided framer;
/// object-only columns use the factory's normal whole-input reader.
#[derive(Clone, Default)]
pub struct SourceColumnsLayer {
    framer: Option<Arc<dyn SourceFramer>>,
    columns: Vec<(SourceColumn, String)>,
}
impl SourceColumnsLayer {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_framer(mut self, framer: impl SourceFramer) -> Self {
        self.framer = Some(Arc::new(framer));
        self
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
    fn with(mut self, column: SourceColumn, name: String) -> Self {
        if let Some((_, target)) = self.columns.iter_mut().find(|(c, _)| *c == column) {
            *target = name;
        } else {
            self.columns.push((column, name));
        }
        self
    }
}

pub struct SourceColumnsAccessor<B, F> {
    inner: Arc<FactoryAccessor<B, F>>,
    layer: SourceColumnsLayer,
}
impl<B, F> Layer<FactoryAccessor<B, F>> for SourceColumnsLayer
where
    B: ByteAccessor + 'static,
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    type Layered = SourceColumnsAccessor<B, F>;
    fn layer(&self, inner: FactoryAccessor<B, F>) -> Self::Layered {
        SourceColumnsAccessor {
            inner: Arc::new(inner),
            layer: self.clone(),
        }
    }
}
#[async_trait]
impl<B, F> Accessor for SourceColumnsAccessor<B, F>
where
    B: ByteAccessor + 'static,
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    type Request = ReadRequest;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        self.inner.capability()
    }
    async fn open(&self, request: ReadRequest) -> Result<Box<dyn RecordReader>> {
        Ok(Box::new(self.plan_scan(request).await?.into_reader()))
    }
}
#[async_trait]
impl<B, F> ScanPlanner for SourceColumnsAccessor<B, F>
where
    B: ByteAccessor + 'static,
    F: ReaderFactory<Request = ReadRequest, Reader = dyn RecordReader> + 'static,
{
    async fn plan_scan(&self, mut request: ReadRequest) -> Result<ScanPlan> {
        let object = request.object.clone();
        let context = |e: open_arrow_access_core::Error| e.with_object(Some(&object));
        crate::validation::validate_source_framer(&self.layer.columns, self.layer.framer.is_some())
            .map_err(context)?;
        let projection = request.options.projection.take();
        let Some(framer) = &self.layer.framer else {
            let reader = self.inner.open(request).await?;
            let output =
                SourceOutput::new(&reader.metadata().schema, &self.layer.columns, projection)
                    .map_err(context)?;
            return Ok(ScanPlan::whole_input(output.object_reader(reader)));
        };
        // Validate format settings before opening storage or invoking user framing.
        self.inner.factory().prepare(&request).map_err(context)?;
        let input = self
            .inner
            .storage()
            .open(ByteReadRequest::from(&request))
            .await
            .map_err(context)?;
        let mut units = framer.frame(input).fuse();
        let first = units.try_next().await.map_err(context)?;
        let first = first.unwrap_or(SourceUnit {
            bytes: Bytes::new(),
            positions: Vec::new(),
        });
        let first_reader = self
            .inner
            .factory()
            .open_input(
                Box::new(bytes_reader(first.bytes, Some(object.clone()))),
                request.clone(),
            )
            .await?;
        let output = SourceOutput::new(
            &first_reader.metadata().schema,
            &self.layer.columns,
            projection,
        )
        .map_err(context)?;
        let mut metadata = first_reader.metadata().clone();
        metadata.schema = output.schema();
        let ordering = self
            .inner
            .capability()
            .ordering
            .combine(first_reader.capability().ordering);
        let first_output = output.clone();
        let first_object = object.clone();
        let first: ScanPartition = async move {
            first_output
                .unit_reader(first_reader, first.positions)
                .await
                .map_err(|e| e.with_object(Some(&first_object)))
        }
        .boxed();
        let inner = self.inner.clone();
        let rest = units.map(move |unit| {
            let unit = unit.map_err(|e| e.with_object(Some(&object)))?;
            let inner = inner.clone();
            let request = request.clone();
            let output = output.clone();
            let object = object.clone();
            Ok(async move {
                let reader = inner
                    .factory()
                    .open_input(
                        Box::new(bytes_reader(unit.bytes, Some(object.clone()))),
                        request,
                    )
                    .await?;
                output
                    .unit_reader(reader, unit.positions)
                    .await
                    .map_err(|e| e.with_object(Some(&object)))
            }
            .boxed())
        });
        Ok(
            ScanPlan::from_stream(metadata, stream::once(async { Ok(first) }).chain(rest))
                .with_ordering(ordering),
        )
    }
}
