use std::{collections::HashSet, sync::Arc};

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{FieldRef, Schema, SchemaRef};
use datafusion::{
    common::{
        DFSchema,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    error::DataFusionError,
    execution::{SessionState, context::SessionContext},
    logical_expr::{Expr, ExprSchemable},
    optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext},
    physical_expr::PhysicalExpr,
};
use open_arrow_access_core::{
    Accessor, Capability, Error, ErrorKind, Layer, ReadOptionsProvider, Reader, RecordMetadata,
    RecordReader, Result, async_trait,
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
        let mut names = HashSet::with_capacity(specs.len());
        for spec in &specs {
            if spec.column.trim().is_empty() || !names.insert(&spec.column) {
                return Err(invalid(
                    &spec.column,
                    "column names must be nonempty and distinct",
                ));
            }
            if spec.expression.trim().is_empty() {
                return Err(invalid(&spec.column, "expression must not be blank"));
            }
        }
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

    fn capability(&self) -> Capability {
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
        let context = SessionContext::new();
        let state = context.state();
        let mut fields = source.fields().to_vec();
        let mut steps = Vec::with_capacity(self.specs.len());
        for spec in self.specs.iter() {
            if fields.iter().any(|field| field.name() == &spec.column) {
                return Err(invalid(&spec.column, "column already exists"));
            }
            let input = Arc::new(Schema::new_with_metadata(
                fields.clone(),
                source.metadata().clone(),
            ));
            let (expression, output) = plan_expression(&context, &state, &spec.expression, &input)
                .map_err(|error| invalid(&spec.column, error.to_string()).with_source(error))?;
            let mut metadata = output.metadata().clone();
            metadata.insert(
                "open_arrow_access:generated_expression".into(),
                spec.expression.clone(),
            );
            fields.push(Arc::new(
                output
                    .as_ref()
                    .clone()
                    .with_name(&spec.column)
                    .with_metadata(metadata),
            ));
            steps.push(GeneratedStep {
                input,
                expression,
                column: spec.column.clone(),
            });
        }
        let full_schema = Arc::new(Schema::new_with_metadata(fields, source.metadata().clone()));
        let (schema, projection) = output_options.resolve_projection(&full_schema)?;
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

fn plan_expression(
    context: &SessionContext,
    state: &SessionState,
    expression: &str,
    input: &SchemaRef,
) -> datafusion::error::Result<(Arc<dyn PhysicalExpr>, FieldRef)> {
    let df_schema = DFSchema::try_from(Arc::clone(input))?;
    let logical = context.parse_sql_expr(expression, &df_schema)?;
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
    // Reuse the type-conversion layer's guard before constant
    // folding can execute an unsafe Arrow Decimal rescale.
    logical.apply(|expr| {
        let input = match expr {
            Expr::Cast(cast) => Some(cast.expr.as_ref()),
            Expr::TryCast(cast) => Some(cast.expr.as_ref()),
            // These built-ins become casts during simplification.
            Expr::ScalarFunction(function)
                if matches!(
                    function.func.name(),
                    "arrow_cast" | "arrow_try_cast" | "cast_to_type" | "try_cast_to_type"
                ) =>
            {
                function.args.first()
            }
            _ => None,
        };
        if let Some(input) = input {
            let from = input.get_type(&df_schema)?;
            let to = expr.get_type(&df_schema)?;
            crate::validation::validate_target(&to)?;
            // The recursive Decimal guard assumes an Arrow-supported
            // type pair, as it does for TypeConversionLayer.
            if !arrow_cast::can_cast_types(&from, &to) {
                return Err(DataFusionError::Plan(format!("cannot cast {from} to {to}")));
            }
            crate::validation::validate_decimal_cast(&from, &to)?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let logical = simplifier.simplify(logical)?;
    let physical = context.create_physical_expr(logical, &df_schema)?;
    let output = physical.return_field(input)?;
    Ok((physical, output))
}

fn invalid(column: &str, message: impl std::fmt::Display) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "plan_generated_column",
        format!("column {column:?}: {message}"),
    )
}

fn record_capability(inner: Capability) -> Capability {
    Capability {
        read: inner.read,
        read_parallel: inner.read_parallel,
        read_with_range: false,
        list: false,
    }
}

fn validate_expression(expression: &Expr) -> datafusion::error::Result<()> {
    if expression.is_volatile() {
        return Err(DataFusionError::Plan(
            "volatile functions are not supported".into(),
        ));
    }
    expression.apply(|expr| {
        if matches!(
            expr,
            Expr::AggregateFunction(_)
                | Expr::WindowFunction(_)
                | Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::SetComparison(_)
                | Expr::GroupingSet(_)
                | Expr::Unnest(_)
        ) {
            return Err(DataFusionError::Plan(
                "only row-level expressions are supported".into(),
            ));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

struct GeneratedStep {
    input: SchemaRef,
    expression: Arc<dyn PhysicalExpr>,
    column: String,
}

struct GeneratedColumnsReader {
    inner: Option<Box<dyn RecordReader>>,
    metadata: RecordMetadata,
    steps: Vec<GeneratedStep>,
    projection: Option<Vec<usize>>,
    capability: Capability,
}

impl GeneratedColumnsReader {
    fn generate(&self, mut batch: RecordBatch) -> Result<RecordBatch> {
        let rows = batch.num_rows();
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        let mut columns = batch.columns().to_vec();
        columns.reserve(self.steps.len());
        for (index, step) in self.steps.iter().enumerate() {
            let array = step
                .expression
                .evaluate(&batch)
                .and_then(|value| value.into_array(rows))
                .map_err(|error| {
                    Error::new(
                        ErrorKind::Decode,
                        "generate_column",
                        format!("column {:?}: {error}", step.column),
                    )
                    .with_source(error)
                })?;
            columns.push(array);
            // Only materialize an intermediate batch if a later expression needs
            // it. Cloning ArrayRefs preserves the original buffers.
            if let Some(next) = self.steps.get(index + 1) {
                batch = RecordBatch::try_new_with_options(
                    Arc::clone(&next.input),
                    columns.clone(),
                    &options,
                )
                .map_err(batch_error)?;
            }
        }
        if let Some(indices) = &self.projection {
            columns = indices
                .iter()
                .map(|&index| Arc::clone(&columns[index]))
                .collect();
        }
        RecordBatch::try_new_with_options(Arc::clone(&self.metadata.schema), columns, &options)
            .map_err(batch_error)
    }
}

fn batch_error(error: arrow_schema::ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "generate_column", error.to_string()).with_source(error)
}

#[async_trait]
impl Reader for GeneratedColumnsReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;

    fn capability(&self) -> Capability {
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
