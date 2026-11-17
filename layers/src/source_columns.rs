use arrow_array::RecordBatchOptions;
use arrow_schema::Schema;
use futures::{FutureExt, StreamExt, TryStreamExt};
use open_arrow_access_core::{
    Accessor, Layer, ReadOptions, ReadOptionsProvider, RecordBatch, RecordCapability,
    RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, SchemaRef, SourceColumn,
    StreamReader, async_trait, into_stream,
};
use std::sync::Arc;

/// Request provenance from the source and rename its reserved output columns.
/// Apply before generated-column evolution. Scan partitions retain their locators.
#[derive(Clone, Debug, Default)]
pub struct SourceColumnsLayer {
    columns: Vec<(SourceColumn, String)>,
}
#[cfg(feature = "source-positions")]
mod bytes;
#[cfg(feature = "source-positions")]
pub use bytes::{SourcePositionAccessor, SourcePositionLayer};

impl SourceColumnsLayer {
    /// Apply to raw byte storage before constructing the format accessor.
    /// This optional adapter preserves compressed frame boundaries without
    /// changing the ordinary decompression implementation.
    #[cfg(feature = "source-positions")]
    pub fn byte_positions(&self) -> SourcePositionLayer {
        SourcePositionLayer::default()
    }

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
    fn with(mut self, column: SourceColumn, name: String) -> Self {
        if let Some((_, target)) = self.columns.iter_mut().find(|(c, _)| *c == column) {
            *target = name;
        } else {
            self.columns.push((column, name));
        }
        self
    }
    fn request<R: ReadOptionsProvider>(&self, request: &mut R) -> Option<Vec<String>> {
        let options = request.read_options_mut();
        // Resolve projection after renaming, so callers may select the chosen names.
        options.projection.take()
    }
    fn transform(&self, input: &SchemaRef, projection: Option<Vec<String>>) -> Result<Transform> {
        crate::validation::validate_source_column_names(input, &self.columns)?;
        let fields = input
            .fields()
            .iter()
            .map(|field| {
                let target = self
                    .columns
                    .iter()
                    .find(|(c, _)| c.name() == field.name())
                    .map(|(_, n)| n.as_str())
                    .unwrap_or(field.name());
                field.as_ref().clone().with_name(target)
            })
            .collect::<Vec<_>>();
        let renamed = Arc::new(Schema::new_with_metadata(fields, input.metadata().clone()));
        let projection = ReadOptions {
            projection,
            ..Default::default()
        }
        .resolve_projection(&renamed)?;
        Ok(Transform {
            schema: projection.schema,
            indices: projection.indices,
        })
    }
}
#[derive(Clone)]
struct Transform {
    schema: SchemaRef,
    indices: Option<Vec<usize>>,
}
impl Transform {
    fn wrap(self, reader: Box<dyn RecordReader>) -> Box<dyn RecordReader> {
        let metadata = RecordMetadata {
            schema: self.schema.clone(),
            ..reader.metadata().clone()
        };
        let ordering = reader.capability().ordering;
        let batches = into_stream(reader)
            .map_ok(move |batch| {
                let columns = match &self.indices {
                    Some(indices) => indices.iter().map(|&i| batch.column(i).clone()).collect(),
                    None => batch.columns().to_vec(),
                };
                RecordBatch::try_new_with_options(
                    self.schema.clone(),
                    columns,
                    &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                )
                .map_err(crate::error::source_column_batch_error)
            })
            .and_then(futures::future::ready);
        Box::new(StreamReader::records(batches, metadata, ordering))
    }
}
pub struct SourceColumnsAccessor<A> {
    inner: A,
    layer: SourceColumnsLayer,
}
impl<A> SourceColumnsAccessor<A> {
    fn columns(&self) -> Vec<SourceColumn> {
        self.layer
            .columns
            .iter()
            .map(|(column, _)| *column)
            .collect()
    }
}
impl<A> Layer<A> for SourceColumnsLayer
where
    A: open_arrow_access_core::SourceColumnAccessor,
    A::Request: ReadOptionsProvider,
{
    type Layered = SourceColumnsAccessor<A>;
    fn layer(&self, inner: A) -> Self::Layered {
        SourceColumnsAccessor {
            inner,
            layer: self.clone(),
        }
    }
}
#[async_trait]
impl<A> Accessor for SourceColumnsAccessor<A>
where
    A: open_arrow_access_core::SourceColumnAccessor,
    A::Request: ReadOptionsProvider,
{
    type Request = A::Request;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        self.inner.capability()
    }
    async fn open(&self, mut request: Self::Request) -> Result<Box<dyn RecordReader>> {
        let projection = self.layer.request(&mut request);
        let columns = self.columns();
        let reader = self.inner.open_source_columns(request, &columns).await?;
        let transform = self
            .layer
            .transform(&reader.metadata().schema, projection)
            .map_err(|e| e.with_object(reader.metadata().source.as_ref()))?;
        Ok(transform.wrap(reader))
    }
}
#[async_trait]
impl<A> ScanPlanner for SourceColumnsAccessor<A>
where
    A: open_arrow_access_core::SourceColumnPlanner,
    A::Request: ReadOptionsProvider,
{
    async fn plan_scan(&self, mut request: Self::Request) -> Result<ScanPlan> {
        let projection = self.layer.request(&mut request);
        let columns = self.columns();
        let mut plan = self.inner.plan_source_columns(request, &columns).await?;
        let transform = self
            .layer
            .transform(&plan.metadata.schema, projection)
            .map_err(|e| e.with_object(plan.metadata.source.as_ref()))?;
        plan.metadata.schema = transform.schema.clone();
        plan.partitions = plan
            .partitions
            .map_ok(move |partition| {
                let transform = transform.clone();
                async move { Ok(transform.wrap(partition.await?)) }.boxed()
            })
            .boxed();
        Ok(plan)
    }
}
