use std::{collections::HashSet, sync::Arc};

use arrow_array::{BooleanArray, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Schema, SchemaRef};
use arrow_select::{filter::filter_record_batch, nullif::nullif};
use datafusion::{common::DFSchema, logical_expr::Expr, physical_expr::PhysicalExpr};
use futures::{StreamExt, TryStreamExt, future};
use open_arrow_access_core::{
    Accessor, Layer, ReadOptionsProvider, RecordCapability, RecordMetadata, RecordReader,
    ResolvedProjection, Result, ScanPartition, ScanPlan, ScanPlanner, async_trait, into_stream,
    project_batch, validate_schema,
};

/// A Boolean expression deciding whether to retain a row or a cell.
///
/// Only `true` retains the value; `false` and SQL null reject it. Every rule
/// reads the original batch, including columns masked by another cell rule.
#[derive(Clone, Debug)]
pub enum FilterRule {
    /// Reject the entire row when the predicate is not true.
    Row(Expr),
    /// Replace rejected cells in a top-level column with null, preserving rows.
    /// The output field becomes nullable. The predicate may reference any column.
    Cell { column: String, predicate: Expr },
}

impl FilterRule {
    fn predicate(&self) -> &Expr {
        match self {
            Self::Row(predicate) | Self::Cell { predicate, .. } => predicate,
        }
    }
}

/// Filter rows and mask cells using Boolean expressions, preserving relative order.
///
/// Use [`Self::new`] for row predicates or [`Self::with_rules`] to mix row and
/// cell rules. Multiple rules for the same row or cell are combined with AND.
/// Cell rules preserve row counts and make their target fields nullable.
///
/// Predicates are row-level DataFusion expressions, including scalar UDFs. Only
/// `true` retains a row or cell; `false` and SQL null reject it. All predicates evaluate
/// against the original batch in vector order, without short-circuit guarantees.
/// An empty vector passes the original reader through. Empty filtered batches
/// are skipped. Filtering happens after decoding, so it does not bound decoder
/// memory or avoid loading an oversized cell.
///
/// As with generated columns, callers supply scalar UDF expressions using
/// `udf.call(...)`; input columns are the UDF arguments. The UDF must return
/// Boolean (an array or a scalar expanded to the batch row count).
///
/// ```
/// use datafusion::{logical_expr::ScalarUDF, prelude::{col, lit}};
/// use open_arrow_access_layers::FilterLayer;
///
/// fn custom_filter(predicate: ScalarUDF) -> FilterLayer {
///     FilterLayer::new(vec![
///         predicate.call(vec![col("data"), col("content_type")]),
///         col("score").gt(lit(0_i64)),
///     ])
/// }
/// ```
#[derive(Clone, Debug, Default)]
pub struct FilterLayer {
    rules: Arc<[FilterRule]>,
}

impl FilterLayer {
    /// Create an AND filter. Names and Boolean result types are checked at open
    /// or metadata inspection, before polling source batches.
    pub fn new(predicates: Vec<Expr>) -> Self {
        Self::with_rules(predicates.into_iter().map(FilterRule::Row).collect())
    }

    /// Create a filter with explicit row and cell rules.
    ///
    /// Names, target columns and Boolean result types are checked before polling
    /// source batches. Cell targets must be top-level columns; Union and
    /// RunEndEncoded targets are unsupported because they lack a validity bitmap.
    ///
    /// ```
    /// use datafusion::prelude::{col, lit};
    /// use open_arrow_access_layers::{FilterLayer, FilterRule};
    ///
    /// let layer = FilterLayer::with_rules(vec![
    ///     FilterRule::Row(col("score").gt(lit(0_i64))),
    ///     FilterRule::Cell {
    ///         column: "data".into(),
    ///         predicate: col("size").lt_eq(lit(1024_i64)),
    ///     },
    /// ]);
    /// ```
    pub fn with_rules(rules: Vec<FilterRule>) -> Self {
        Self {
            rules: rules.into(),
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
                .rules
                .iter()
                .flat_map(|rule| {
                    let mut dependencies: Vec<_> = rule
                        .predicate()
                        .column_refs()
                        .into_iter()
                        .map(|col| col.name.clone())
                        .collect();
                    if let FilterRule::Cell { column, .. } = rule {
                        dependencies.push(column.clone());
                    }
                    dependencies
                })
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
        FilterPlan::new(&self.config.rules, schema, projection).map(Arc::new)
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
        if self.config.rules.is_empty() {
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
        if self.config.rules.is_empty() {
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
        if self.config.rules.is_empty() {
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
        if self.config.rules.is_empty() {
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

struct PlannedFilterRule {
    target: Option<usize>,
    predicate: Arc<dyn PhysicalExpr>,
}

struct FilterPlan {
    schema: SchemaRef,
    output: ResolvedProjection,
    rules: Vec<PlannedFilterRule>,
}

impl FilterPlan {
    fn new(rules: &[FilterRule], schema: SchemaRef, projection: Option<&[String]>) -> Result<Self> {
        let mut output = ResolvedProjection::resolve(&schema, projection)?;
        let input = DFSchema::try_from(schema.clone()).map_err(crate::error::filter_plan_error)?;
        let planner = crate::expression::ExpressionPlanner::new();
        let rules = rules
            .iter()
            .map(|rule| {
                let target = match rule {
                    FilterRule::Row(_) => None,
                    FilterRule::Cell { column, .. } => {
                        let index = schema
                            .index_of(column)
                            .map_err(|error| crate::error::filter_plan_error(error.into()))?;
                        let data_type = schema.field(index).data_type();
                        // These layouts encode nulls in children rather than a
                        // top-level validity bitmap, so nullif cannot mask them.
                        if matches!(data_type, DataType::Union(..) | DataType::RunEndEncoded(..)) {
                            return Err(crate::error::filter_plan_error(
                                datafusion::error::DataFusionError::Plan(format!(
                                    "cell filter target {column:?} has unsupported type {data_type}"
                                )),
                            ));
                        }
                        Some(index)
                    }
                };
                let compiled = planner
                    .plan(rule.predicate().clone(), &input)
                    .map_err(crate::error::filter_plan_error)?;
                if compiled.output.data_type() != &DataType::Boolean {
                    return Err(crate::error::filter_plan_error(
                        crate::error::filter_predicate_type(compiled.output.data_type()).into(),
                    ));
                }
                Ok(PlannedFilterRule {
                    target,
                    predicate: compiled.expression,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let fields: Vec<_> = output
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let source = output
                    .indices
                    .as_ref()
                    .map_or(index, |indices| indices[index]);
                if rules.iter().any(|rule| rule.target == Some(source)) {
                    Arc::new(field.as_ref().clone().with_nullable(true))
                } else {
                    field.clone()
                }
            })
            .collect();
        output.schema = Arc::new(Schema::new_with_metadata(
            fields,
            output.schema.metadata().clone(),
        ));
        Ok(Self {
            schema,
            output,
            rules,
        })
    }

    fn wrap(self: Arc<Self>, reader: Box<dyn RecordReader>) -> Result<Box<dyn RecordReader>> {
        validate_schema(
            &reader.metadata().schema,
            &self.schema,
            crate::error::filter_schema_mismatch,
        )?;
        let mut metadata = reader.metadata().clone();
        metadata.schema = self.output.schema.clone();
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
        let mut cells = Vec::new();
        for PlannedFilterRule { target, predicate } in &self.rules {
            let values = predicate
                .evaluate(&batch)
                .and_then(|value| value.into_array_of_size(batch.num_rows()))
                .map_err(crate::error::filter_error)?;
            let mask = values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| crate::error::filter_predicate_type(values.data_type()))
                .map_err(crate::error::filter_error)?;
            if let Some(target) = target {
                let reject = BooleanArray::from(
                    mask.iter()
                        .map(|value| value != Some(true))
                        .collect::<Vec<_>>(),
                );
                cells.push((*target, reject));
            } else {
                for (keep, value) in keep.iter_mut().zip(mask.iter()) {
                    *keep &= value == Some(true);
                }
            }
        }
        // Predicate-only columns are no longer needed; filter only the output.
        let batch = project_batch(
            batch,
            self.output.indices.as_deref(),
            self.output.schema.clone(),
        )?;
        let batch = if cells.is_empty() {
            batch
        } else {
            let mut columns = batch.columns().to_vec();
            for (target, reject) in cells {
                let output = match &self.output.indices {
                    Some(indices) => indices.iter().position(|&index| index == target),
                    None => Some(target),
                };
                if let Some(index) = output {
                    columns[index] = nullif(columns[index].as_ref(), &reject)
                        .map_err(crate::error::filter_error)?;
                }
            }
            RecordBatch::try_new_with_options(
                self.output.schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )
            .map_err(crate::error::filter_error)?
        };
        let batch = filter_record_batch(&batch, &BooleanArray::from(keep))
            .map_err(crate::error::filter_error)?;
        Ok((batch.num_rows() != 0).then_some(batch))
    }
}
