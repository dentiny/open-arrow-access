use arrow_array::RecordBatchOptions;
use arrow_schema::Schema;
use futures::{FutureExt, StreamExt, TryStreamExt};
use open_arrow_access_core::{
    Accessor, Layer, ReadOptions, ReadOptionsProvider, RecordBatch, RecordCapability, RecordReader,
    ResolvedProjection, Result, ScanPlan, ScanPlanner, SchemaRef, SourceColumn,
    SourceColumnRequestAccessor, async_trait, map_record_reader,
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
    fn take_projection<R: ReadOptionsProvider>(&self, request: &mut R) -> Option<Vec<String>> {
        let options = request.read_options_mut();
        // Resolve projection after renaming, so callers may select the chosen names.
        options.projection.take()
    }
    fn projection(
        &self,
        input: &SchemaRef,
        projection: Option<Vec<String>>,
    ) -> Result<ResolvedProjection> {
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
        crate::validation::validate_source_column_names(input, &fields, &self.columns)?;
        let renamed = Arc::new(Schema::new_with_metadata(fields, input.metadata().clone()));
        ReadOptions {
            projection,
            ..Default::default()
        }
        .resolve_projection(&renamed)
    }
}

fn project_reader(
    reader: Box<dyn RecordReader>,
    projection: ResolvedProjection,
) -> Box<dyn RecordReader> {
    let capability = RecordCapability::sequential(reader.capability().ordering);
    let output = projection.schema.clone();
    Box::new(map_record_reader(
        reader,
        output,
        capability,
        move |batch| {
            let columns = match &projection.indices {
                Some(indices) => indices.iter().map(|&i| batch.column(i).clone()).collect(),
                None => batch.columns().to_vec(),
            };
            RecordBatch::try_new_with_options(
                projection.schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )
            .map_err(crate::error::source_column_batch_error)
        },
    ))
}
pub struct SourceColumnsAccessor<A> {
    inner: SourceColumnRequestAccessor<A>,
    layer: SourceColumnsLayer,
}
impl<A> Layer<A> for SourceColumnsLayer
where
    A: open_arrow_access_core::SourceColumnAccessor,
    A::Request: ReadOptionsProvider,
{
    type Layered = SourceColumnsAccessor<A>;
    fn layer(&self, inner: A) -> Self::Layered {
        SourceColumnsAccessor {
            inner: SourceColumnRequestAccessor::new(
                inner,
                self.columns.iter().map(|(column, _)| *column).collect(),
            ),
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
        let selection = self.layer.take_projection(&mut request);
        let reader = self.inner.open(request).await?;
        let projection = self
            .layer
            .projection(&reader.metadata().schema, selection)
            .map_err(|e| e.with_object(reader.metadata().source.as_ref()))?;
        Ok(project_reader(reader, projection))
    }
}
#[async_trait]
impl<A> ScanPlanner for SourceColumnsAccessor<A>
where
    A: open_arrow_access_core::SourceColumnPlanner,
    A::Request: ReadOptionsProvider,
{
    async fn plan_scan(&self, mut request: Self::Request) -> Result<ScanPlan> {
        let selection = self.layer.take_projection(&mut request);
        let mut plan = self.inner.plan_scan(request).await?;
        let projection = self
            .layer
            .projection(&plan.metadata.schema, selection)
            .map_err(|e| e.with_object(plan.metadata.source.as_ref()))?;
        plan.metadata.schema = projection.schema.clone();
        plan.partitions = plan
            .partitions
            .map_ok(move |partition| {
                let projection = projection.clone();
                async move { Ok(project_reader(partition.await?, projection)) }.boxed()
            })
            .boxed();
        Ok(plan)
    }
}
