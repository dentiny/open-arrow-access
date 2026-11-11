use std::{collections::HashSet, sync::Arc};

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{FieldRef, Schema, SchemaRef};
use datafusion::{
    common::DFSchema,
    execution::{SessionState, context::SessionContext},
    optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext},
    physical_expr::PhysicalExpr,
};
use open_arrow_access_core::{
    Accessor, Capability, Layer, ReadOptionsProvider, Reader, RecordCapability, RecordMetadata,
    RecordReader, Result, async_trait,
};

use crate::{
    error::{
        generated_batch_error, generated_column_error, generated_plan_error,
        invalid_generated_column,
    },
    validation::{validate_expression, validate_generated_specs},
};

/// One row-level SQL expression. May reference source and earlier generated fields.
#[derive(Clone, Debug)]
pub struct GeneratedColumnSpec {
    pub column: String,
    pub expression: String,
}

/// Appends generated fields in declaration order, then applies the final projection.
///
/// Expressions are compiled once per open and evaluated batch by batch. This first
/// implementation reads all source columns to validate against the complete schema.
#[derive(Clone, Debug, Default)]
pub struct GeneratedColumnsLayer {
    specs: Arc<[GeneratedColumnSpec]>,
}

impl GeneratedColumnsLayer {
    /// Validate declarations without I/O. Expression planning happens at open.
    pub fn try_new(specs: Vec<GeneratedColumnSpec>) -> Result<Self> {
        validate_generated_specs(&specs)?;
        Ok(Self {
            specs: specs.into(),
        })
    }
}

/// A record accessor decorated by [`GeneratedColumnsLayer`].
#[derive(Clone, Debug)]
pub struct GeneratedColumnsAccessor<A> {
    inner: A,
    specs: Arc<[GeneratedColumnSpec]>,
}

impl<A> Layer<A> for GeneratedColumnsLayer
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Layered = GeneratedColumnsAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        GeneratedColumnsAccessor {
            inner,
            specs: Arc::clone(&self.specs),
        }
    }
}

#[async_trait]
impl<A> Accessor for GeneratedColumnsAccessor<A>
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        record_capability(self.inner.capability())
    }

    async fn open(&self, mut request: A::Request) -> Result<Box<dyn RecordReader>> {
        if self.specs.is_empty() {
            return self.inner.open(request).await;
        }
        let output_options = request.read_options().clone();
        // Generated names are not physical inputs. Inspect all fields, including
        // hidden ones, so projection cannot hide an invalid declaration/collision.
        request.read_options_mut().projection = None;
        let inner = self.inner.open(request).await?;
        let source = &inner.metadata().schema;
        let state = SessionContext::new().state();
        let mut schema = Arc::clone(source);
        let mut column_names: HashSet<&str> = source
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        let mut steps = Vec::with_capacity(self.specs.len());
        for spec in self.specs.iter() {
            if !column_names.insert(&spec.column) {
                return Err(invalid_generated_column(
                    &spec.column,
                    "column already exists",
                ));
            }
            let (expression, output) = plan_expression(&state, &spec.expression, &schema)
                .map_err(|error| generated_plan_error(&spec.column, error))?;
            let mut metadata = output.metadata().clone();
            metadata.insert(
                "open_arrow_access:generated_expression".into(),
                spec.expression.clone(),
            );
            let mut fields = schema.fields().to_vec();
            fields.push(Arc::new(
                output
                    .as_ref()
                    .clone()
                    .with_name(&spec.column)
                    .with_metadata(metadata),
            ));
            schema = Arc::new(Schema::new_with_metadata(fields, source.metadata().clone()));
            steps.push(GeneratedStep {
                output: Arc::clone(&schema),
                expression,
                column: spec.column.clone(),
            });
        }
        let (schema, projection) = output_options.resolve_projection(&schema)?;
        let metadata = RecordMetadata {
            schema,
            source: inner.metadata().source.clone(),
        };
        let capability = record_capability(inner.capability());
        Ok(Box::new(GeneratedColumnsReader {
            inner: Some(inner),
            metadata,
            steps,
            projection,
            capability,
        }))
    }
}

/// Plans one row-level SQL expression against the source and earlier generated fields.
///
/// Rejects non-row-level and volatile expressions before DataFusion coerces,
/// simplifies, and compiles the expression. The shared session state keeps stable
/// functions consistent across declarations. Returns the physical expression and
/// its inferred output field.
fn plan_expression(
    state: &SessionState,
    expression: &str,
    input: &SchemaRef,
) -> datafusion::error::Result<(Arc<dyn PhysicalExpr>, FieldRef)> {
    let df_schema = DFSchema::try_from(Arc::clone(input))?;
    let logical = state.create_logical_expr(expression, &df_schema)?;
    // Validate before simplification: a dead branch must not hide an
    // aggregate, subquery, or volatile function.
    validate_expression(&logical)?;
    let simplify_context = SimplifyContext::builder()
        .with_schema(Arc::new(df_schema.clone()))
        .with_config_options(Arc::clone(state.config_options()))
        .with_query_execution_start_time(state.execution_props().query_execution_start_time)
        .build();
    let simplifier = ExprSimplifier::new(simplify_context);
    let logical = simplifier.coerce(logical, &df_schema)?;
    let logical = simplifier.simplify(logical)?;
    let physical = state.create_physical_expr(logical, &df_schema)?;
    let output = physical.return_field(input)?;
    Ok((physical, output))
}

fn record_capability(inner: RecordCapability) -> RecordCapability {
    Capability {
        read: inner.read,
        read_parallel: inner.read_parallel,
        read_with_range: false,
        list: false,
    }
    .with_ordering(inner.ordering)
}

struct GeneratedStep {
    output: SchemaRef,
    expression: Arc<dyn PhysicalExpr>,
    column: String,
}

struct GeneratedColumnsReader {
    inner: Option<Box<dyn RecordReader>>,
    metadata: RecordMetadata,
    steps: Vec<GeneratedStep>,
    projection: Option<Vec<usize>>,
    capability: RecordCapability,
}

impl GeneratedColumnsReader {
    fn generate(&self, mut batch: RecordBatch) -> Result<RecordBatch> {
        let rows = batch.num_rows();
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        for step in &self.steps {
            let array = step
                .expression
                .evaluate(&batch)
                .and_then(|value| value.into_array(rows))
                .map_err(|error| generated_column_error(&step.column, error))?;
            let (_, mut columns, _) = batch.into_parts();
            columns.push(array);
            batch = RecordBatch::try_new_with_options(Arc::clone(&step.output), columns, &options)
                .map_err(generated_batch_error)?;
        }
        match &self.projection {
            Some(indices) => batch.project(indices).map_err(generated_batch_error),
            None => Ok(batch),
        }
    }
}

#[async_trait]
impl Reader for GeneratedColumnsReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    type Capability = RecordCapability;

    fn capability(&self) -> RecordCapability {
        self.capability
    }

    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        let Some(inner) = &mut self.inner else {
            return Ok(None);
        };
        let result = match inner.next().await {
            Ok(Some(batch)) => self.generate(batch).map(Some),
            other => other,
        };
        if !matches!(result, Ok(Some(_))) {
            self.inner = None;
        }
        result
    }
}
