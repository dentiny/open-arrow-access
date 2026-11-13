use std::{collections::HashSet, sync::Arc};

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{FieldRef, Schema, SchemaRef};
use datafusion::{
    common::DFSchema,
    execution::{SessionState, context::SessionContext},
    physical_expr::PhysicalExpr,
};
use open_arrow_access_core::{
    Accessor, Capability, Layer, ReadOptionsProvider, Reader, RecordCapability, RecordMetadata,
    RecordReader, Result, async_trait,
};

use crate::{
    error::{generated_batch_error, generated_column_error, generated_plan_error},
    validation::{validate_generated_names, validate_generated_schema, validate_generated_specs},
};

/// One row-level SQL expression. May reference source and earlier generated fields.
#[derive(Clone, Debug)]
pub struct GeneratedColumnSpec {
    pub column: String,
    pub expression: String,
}

/// Appends generated fields in declaration order, then applies the final projection.
///
/// Supply a complete source schema with `with_source_schema` to enable
/// dependency projection pushdown without opening an extra reader.
#[derive(Clone, Debug, Default)]
pub struct GeneratedColumnsLayer {
    specs: Arc<[GeneratedColumnSpec]>,
    source_schema: Option<SchemaRef>,
}

impl GeneratedColumnsLayer {
    /// Validate declarations without I/O. Expression planning happens at open.
    pub fn try_new(specs: Vec<GeneratedColumnSpec>) -> Result<Self> {
        validate_generated_specs(&specs)?;
        Ok(Self {
            specs: specs.into(),
            source_schema: None,
        })
    }

    pub fn with_source_schema(mut self, schema: SchemaRef) -> Self {
        self.source_schema = Some(schema);
        self
    }
}

/// A record accessor decorated by [`GeneratedColumnsLayer`].
#[derive(Clone, Debug)]
pub struct GeneratedColumnsAccessor<A> {
    inner: A,
    specs: Arc<[GeneratedColumnSpec]>,
    source_schema: Option<SchemaRef>,
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
            source_schema: self.source_schema.clone(),
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
        request.read_options_mut().projection = None;
        let state = SessionContext::new().state();
        let expected_source = if let (Some(source), Some(projection)) =
            (&self.source_schema, &output_options.projection)
        {
            let (_, _, mut required) = plan_steps(&state, &self.specs, source)?;
            required.extend(projection.iter().cloned());
            request.read_options_mut().projection = Some(
                source
                    .fields()
                    .iter()
                    .filter(|field| required.contains(field.name()))
                    .map(|field| field.name().clone())
                    .collect(),
            );
            Some(request.read_options().resolve_projection(source)?.0)
        } else {
            None
        };
        let inner = self.inner.open(request).await?;
        let source = &inner.metadata().schema;
        if let Some(expected) = expected_source {
            validate_generated_schema(source, &expected)?;
        }
        let (schema, steps, _) = plan_steps(&state, &self.specs, source)?;
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

fn plan_steps(
    state: &SessionState,
    specs: &[GeneratedColumnSpec],
    source: &SchemaRef,
) -> Result<(SchemaRef, Vec<GeneratedStep>, HashSet<String>)> {
    validate_generated_names(specs, source)?;
    let mut schema = Arc::clone(source);
    let source_names: HashSet<&str> = source.fields().iter().map(|f| f.name().as_str()).collect();
    let mut dependencies = HashSet::new();
    let mut steps = Vec::with_capacity(specs.len());
    for spec in specs {
        let (expression, output, columns) = plan_expression(state, &spec.expression, &schema)
            .map_err(|error| generated_plan_error(&spec.column, error))?;
        dependencies.extend(
            columns
                .into_iter()
                .filter(|name| source_names.contains(name.as_str())),
        );
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
    Ok((schema, steps, dependencies))
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
) -> datafusion::error::Result<(Arc<dyn PhysicalExpr>, FieldRef, HashSet<String>)> {
    let df_schema = DFSchema::try_from(Arc::clone(input))?;
    let logical = state.create_logical_expr(expression, &df_schema)?;
    let columns = logical
        .column_refs()
        .into_iter()
        .map(|column| column.name.clone())
        .collect();
    let (physical, output) = crate::expression::plan_expression(state, logical, &df_schema)?;
    Ok((physical, output, columns))
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
