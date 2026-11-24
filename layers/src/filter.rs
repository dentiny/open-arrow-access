use std::{collections::HashSet, sync::Arc};

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use arrow_select::filter::filter_record_batch;
use datafusion::{common::DFSchema, logical_expr::Expr, physical_expr::PhysicalExpr};
use futures::{StreamExt, TryStreamExt, future};
use open_arrow_access_core::{
    Accessor, Layer, ReadOptionsProvider, RecordCapability, RecordMetadata, RecordReader,
    ResolvedProjection, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait, into_stream,
    project_batch, validate_schema,
};

/// Retain rows satisfying every predicate, preserving schema and relative order.
///
/// Predicates are row-level DataFusion expressions, including scalar UDFs. Only
/// `true` retains a row; `false` and SQL null discard it. All predicates evaluate
/// against the original batch in vector order, without short-circuit guarantees.
/// An empty vector passes the original reader through. Empty filtered batches
/// are skipped. Filtering happens after decoding, so it does not bound decoder
/// memory or avoid loading an oversized cell.
#[derive(Clone, Debug, Default)]
pub struct FilterLayer {
    predicates: Arc<[Expr]>,
}

impl FilterLayer {
    /// Create an AND filter. Names and Boolean result types are checked at open
    /// or metadata inspection, before polling source batches.
    pub fn new(predicates: Vec<Expr>) -> Self {
        Self {
            predicates: predicates.into(),
        }
    }
}

/// A record accessor decorated by [`FilterLayer`]. Predicate dependencies are
/// added to the inner projection and removed from the final output. Implements
/// `ScanPlanner` when the source does, filtering each lazy partition with one
/// shared compiled plan. Install outside `ParallelReadLayer` for serial filtering
/// or inside it to filter in partition workers.
#[derive(Clone, Debug)]
pub struct FilterAccessor<A> {
    inner: A,
    config: FilterLayer,
}

impl<A> Layer<A> for FilterLayer
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Layered = FilterAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        FilterAccessor {
            inner,
            config: self.clone(),
        }
    }
}

impl<A> FilterAccessor<A>
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    fn prepare_request(&self, request: &mut A::Request) -> Option<Vec<String>> {
        let output = request.read_options().projection.clone();
        if let Some(columns) = &mut request.read_options_mut().projection {
            let mut seen: HashSet<_> = columns.iter().cloned().collect();
            let mut dependencies: Vec<_> = self
                .config
                .predicates
                .iter()
                .flat_map(|expr| expr.column_refs().into_iter().map(|col| col.name.clone()))
                .collect();
            dependencies.sort_unstable();
            for name in dependencies {
                if seen.insert(name.clone()) {
                    columns.push(name);
                }
            }
        }
        output
    }

    fn plan(&self, schema: SchemaRef, projection: Option<&[String]>) -> Result<Arc<FilterPlan>> {
        FilterPlan::new(&self.config.predicates, schema, projection).map(Arc::new)
    }
}

#[async_trait]
impl<A> Accessor for FilterAccessor<A>
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.inner.capability()
    }

    async fn metadata(&self, mut request: A::Request) -> Result<RecordMetadata> {
        if self.config.predicates.is_empty() {
            return self.inner.metadata(request).await;
        }
        let projection = self.prepare_request(&mut request);
        let mut metadata = self.inner.metadata(request).await?;
        metadata.schema = self
            .plan(metadata.schema, projection.as_deref())?
            .output
            .schema
            .clone();
        Ok(metadata)
    }

    async fn inspect_metadata(&self, mut request: A::Request) -> Result<Option<RecordMetadata>> {
        if self.config.predicates.is_empty() {
            return self.inner.inspect_metadata(request).await;
        }
        let projection = self.prepare_request(&mut request);
        self.inner
            .inspect_metadata(request)
            .await?
            .map(|mut metadata| {
                metadata.schema = self
                    .plan(metadata.schema, projection.as_deref())?
                    .output
                    .schema
                    .clone();
                Ok(metadata)
            })
            .transpose()
    }

    async fn open(&self, mut request: A::Request) -> Result<Box<dyn RecordReader>> {
        if self.config.predicates.is_empty() {
            return self.inner.open(request).await;
        }
        let projection = self.prepare_request(&mut request);
        let reader = self.inner.open(request).await?;
        self.plan(reader.metadata().schema.clone(), projection.as_deref())?
            .wrap(reader)
    }
}

#[async_trait]
impl<A> ScanPlanner for FilterAccessor<A>
where
    A: ScanPlanner,
    A::Request: ReadOptionsProvider,
{
    async fn plan_scan(&self, mut request: A::Request) -> Result<ScanPlan> {
        if self.config.predicates.is_empty() {
            return self.inner.plan_scan(request).await;
        }
        let projection = self.prepare_request(&mut request);
        let mut scan = self.inner.plan_scan(request).await?;
        let plan = self.plan(scan.metadata.schema.clone(), projection.as_deref())?;
        scan.metadata.schema = plan.output.schema.clone();
        scan.partitions = scan
            .partitions
            .map(move |partition| {
                let plan = plan.clone();
                partition.map(|partition| {
                    Box::pin(async move { plan.wrap(partition.await?) }) as ScanPartition
                })
            })
            .boxed();
        Ok(scan)
    }
}

struct FilterPlan {
    schema: SchemaRef,
    output: ResolvedProjection,
    predicates: Vec<Arc<dyn PhysicalExpr>>,
}

impl FilterPlan {
    fn new(predicates: &[Expr], schema: SchemaRef, projection: Option<&[String]>) -> Result<Self> {
        let output = ResolvedProjection::resolve(&schema, projection)?;
        let input = DFSchema::try_from(schema.clone()).map_err(crate::error::filter_plan_error)?;
        let planner = crate::expression::ExpressionPlanner::new();
        let predicates = predicates
            .iter()
            .map(|expr| {
                let compiled = planner
                    .plan(expr.clone(), &input)
                    .map_err(crate::error::filter_plan_error)?;
                if compiled.output.data_type() != &DataType::Boolean {
                    return Err(crate::error::filter_predicate_type(
                        compiled.output.data_type(),
                    ));
                }
                Ok(compiled.expression)
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            schema,
            output,
            predicates,
        })
    }

    fn wrap(self: Arc<Self>, reader: Box<dyn RecordReader>) -> Result<Box<dyn RecordReader>> {
        validate_schema(
            &reader.metadata().schema,
            &self.schema,
            crate::error::filter_schema_mismatch,
        )?;
        let metadata = RecordMetadata {
            schema: self.output.schema.clone(),
            source: reader.metadata().source.clone(),
        };
        let capability = reader.capability();
        let stream =
            into_stream(reader).try_filter_map(move |batch| future::ready(self.apply(batch)));
        Ok(Box::new(
            open_arrow_access_core::StreamReader::records_with_capability(
                stream, metadata, capability,
            ),
        ))
    }

    fn apply(&self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        validate_schema(
            &batch.schema(),
            &self.schema,
            crate::error::filter_schema_mismatch,
        )?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let mut keep = vec![true; batch.num_rows()];
        for predicate in &self.predicates {
            let values = predicate
                .evaluate(&batch)
                .and_then(|value| value.into_array_of_size(batch.num_rows()))
                .map_err(crate::error::filter_error)?;
            let mask = values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| crate::error::filter_predicate_type(values.data_type()))?;
            for (keep, value) in keep.iter_mut().zip(mask.iter()) {
                *keep &= value == Some(true);
            }
        }
        if !keep.iter().any(|&value| value) {
            return Ok(None);
        }
        let batch = if keep.iter().all(|&value| value) {
            batch
        } else {
            filter_record_batch(&batch, &BooleanArray::from(keep))
                .map_err(crate::error::filter_error)?
        };
        project_batch(
            batch,
            self.output.indices.as_deref(),
            self.output.schema.clone(),
        )
        .map(Some)
    }
}
