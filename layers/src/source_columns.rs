use arrow_array::RecordBatchOptions;
use arrow_schema::Schema;
use futures::{FutureExt, StreamExt, TryStreamExt};
use open_arrow_access_core::{
    Accessor, Error, ErrorKind, Layer, ReadOptions, ReadOptionsProvider, RecordBatch,
    RecordCapability, RecordMetadata, RecordReader, Result, ScanPlan, ScanPlanner, SchemaRef,
    SourceColumn, StreamReader, async_trait, into_stream,
};
use std::{collections::HashSet, sync::Arc};

/// Request provenance from the source and rename its reserved output columns.
/// Apply before generated-column evolution. Scan partitions retain their locators.
#[derive(Clone, Debug, Default)]
pub struct SourceColumnsLayer {
    columns: Vec<(SourceColumn, String)>,
}
impl SourceColumnsLayer {
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
        for (column, _) in &self.columns {
            if !options.source_columns.contains(column) {
                options.source_columns.push(*column);
            }
        }
        // Resolve projection after renaming, so callers may select the chosen names.
        options.projection.take()
    }
    fn transform(&self, input: &SchemaRef, projection: Option<Vec<String>>) -> Result<Transform> {
        let mut names = HashSet::new();
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
                if target.is_empty() || !names.insert(target.to_owned()) {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "source_columns",
                        format!("conflicting output field {target:?}"),
                    ));
                }
                Ok(field.as_ref().clone().with_name(target))
            })
            .collect::<Result<Vec<_>>>()?;
        for (column, _) in &self.columns {
            if input.index_of(column.name()).is_err() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "source_columns",
                    format!("source did not provide {}", column.name()),
                ));
            }
        }
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
                .map_err(|error| {
                    Error::new(ErrorKind::Decode, "source_columns", error.to_string())
                        .with_source(error)
                })
            })
            .and_then(futures::future::ready);
        Box::new(StreamReader::records(batches, metadata, ordering))
    }
}
pub struct SourceColumnsAccessor<A> {
    inner: A,
    layer: SourceColumnsLayer,
}
impl<A> Layer<A> for SourceColumnsLayer
where
    A: Accessor<Reader = dyn RecordReader>,
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
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Request = A::Request;
    type Reader = dyn RecordReader;
    fn capability(&self) -> RecordCapability {
        self.inner.capability()
    }
    async fn open(&self, mut request: Self::Request) -> Result<Box<dyn RecordReader>> {
        let projection = self.layer.request(&mut request);
        let reader = self.inner.open(request).await?;
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
    A: ScanPlanner,
    A::Request: ReadOptionsProvider,
{
    async fn plan_scan(&self, mut request: Self::Request) -> Result<ScanPlan> {
        let projection = self.layer.request(&mut request);
        let mut plan = self.inner.plan_scan(request).await?;
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
