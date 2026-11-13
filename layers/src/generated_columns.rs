use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema, SchemaRef};
use datafusion::{common::DFSchema, physical_expr::PhysicalExpr};
use futures::StreamExt;
use open_arrow_access_core::{
    Accessor, Capability, Layer, ReadOptionsProvider, RecordCapability, RecordMetadata,
    RecordReader, Result, StreamReader, async_trait, into_stream,
};

use crate::{
    batch::assemble_batch,
    error::{generated_batch_error, generated_column_error, generated_plan_error},
    expression::{ExpressionPlanner, PlannedExpression},
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
        let planner = ExpressionPlanner::new();
        let mut planned = None;
        let expected_source = if let (Some(source), Some(projection)) =
            (&self.source_schema, &output_options.projection)
        {
            let (schema, mut steps, mut required) = plan_steps(&planner, &self.specs, source)?;
            required.extend(projection.iter().cloned());
            let indices = source
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, field)| required.contains(field.name()))
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let positions = indices
                .iter()
                .enumerate()
                .map(|(projected, &original)| (original, projected))
                .collect::<HashMap<_, _>>();
            for step in &mut steps {
                remap_source_indices(&mut step.input.1, &positions);
            }
            request.read_options_mut().projection = Some(
                indices
                    .iter()
                    .map(|&index| source.field(index).name().clone())
                    .collect(),
            );
            planned = Some((schema, steps, positions));
            Some(request.read_options().resolve_projection(source)?.0)
        } else {
            None
        };
        let inner = self.inner.open(request).await?;
        let source = &inner.metadata().schema;
        if let Some(expected) = expected_source {
            validate_generated_schema(source, &expected)?;
        }
        let (schema, steps, positions) = match planned {
            Some(planned) => planned,
            None => {
                let (schema, steps, _) = plan_steps(&planner, &self.specs, source)?;
                (schema, steps, HashMap::new())
            }
        };
        let (schema, mut projection) = output_options.resolve_projection(&schema)?;
        if let Some(indices) = &mut projection {
            remap_source_indices(indices, &positions);
        }
        let metadata = RecordMetadata {
            schema,
            source: inner.metadata().source.clone(),
        };
        let capability = record_capability(inner.capability());
        let output_indices =
            projection.unwrap_or_else(|| (0..metadata.schema.fields().len()).collect());
        let plan = GeneratedColumnsPlan::new(steps, metadata.schema.clone(), output_indices);
        let batches = into_stream(inner).map(move |batch| plan.generate(batch?));
        Ok(Box::new(
            StreamReader::records(batches, metadata, capability.ordering)
                .with_capability(capability),
        ))
    }
}

fn remap_source_indices(indices: &mut [usize], positions: &HashMap<usize, usize>) {
    // Computed slots retain their full-schema indices; only source slots move.
    for index in indices {
        if let Some(&projected) = positions.get(index) {
            *index = projected;
        }
    }
}

fn plan_steps(
    planner: &ExpressionPlanner,
    specs: &[GeneratedColumnSpec],
    source: &SchemaRef,
) -> Result<(SchemaRef, Vec<GeneratedStep>, HashSet<String>)> {
    validate_generated_names(specs, source)?;
    DFSchema::try_from(source.clone())
        .map_err(|error| generated_plan_error(&specs[0].column, error))?;
    let mut fields = source.fields().to_vec();
    let mut names: HashMap<String, usize> = fields
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name().clone(), index))
        .collect();
    let mut dependencies = HashSet::new();
    let mut steps = Vec::with_capacity(specs.len());
    for spec in specs {
        let PlannedExpression {
            expression,
            output,
            input: (input_schema, indices),
        } = planner
            .plan_sql(&spec.expression, &fields, &names)
            .map_err(|error| generated_plan_error(&spec.column, error))?;
        dependencies.extend(
            indices
                .iter()
                .filter(|&&index| index < source.fields().len())
                .map(|&index| fields[index].name().clone()),
        );
        let mut metadata = output.metadata().clone();
        metadata.insert(
            "open_arrow_access:generated_expression".into(),
            spec.expression.clone(),
        );
        let field = Arc::new(
            output
                .as_ref()
                .clone()
                .with_name(&spec.column)
                .with_metadata(metadata),
        );
        let index = fields.len();
        names.insert(spec.column.clone(), index);
        fields.push(field.clone());
        steps.push(GeneratedStep {
            output: Arc::new(Schema::new(vec![field])),
            expression,
            input: (input_schema, indices),
            index,
            column: spec.column.clone(),
            release: Vec::new(),
        });
    }
    Ok((
        Arc::new(Schema::new_with_metadata(fields, source.metadata().clone())),
        steps,
        dependencies,
    ))
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
    input: (SchemaRef, Vec<usize>),
    index: usize,
    // Generated-value slot indices whose last use is this step. Removing them
    // afterward frees hidden intermediate arrays before final batch assembly.
    release: Vec<usize>,
}

struct GeneratedColumnsPlan {
    steps: Vec<GeneratedStep>,
    schema: SchemaRef,
    // Source-column or generated-value slot for each field in the output schema.
    output_indices: Vec<usize>,
}

impl GeneratedColumnsPlan {
    fn new(mut steps: Vec<GeneratedStep>, schema: SchemaRef, output_indices: Vec<usize>) -> Self {
        let source_len = steps[0].index;
        let mut last_uses = (0..steps.len()).collect::<Vec<_>>();
        for (position, step) in steps.iter().enumerate() {
            for &index in &step.input.1 {
                if index >= source_len {
                    last_uses[index - source_len] = position;
                }
            }
        }
        // Final outputs stay live until assembly; every other slot has one release.
        for &index in &output_indices {
            if index >= source_len {
                last_uses[index - source_len] = usize::MAX;
            }
        }
        for (index, position) in last_uses.into_iter().enumerate() {
            if position != usize::MAX {
                steps[position].release.push(source_len + index);
            }
        }
        Self {
            steps,
            schema,
            output_indices,
        }
    }

    fn generate(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let rows = batch.num_rows();
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        let mut values = HashMap::new();
        for step in &self.steps {
            let view = assemble_batch(&step.input.0, &step.input.1, &batch, &values)
                .map_err(generated_batch_error)?;
            let array = step
                .expression
                .evaluate(&view)
                .and_then(|value| value.into_array(rows))
                .map_err(|error| generated_column_error(&step.column, error))?;
            // Validate hidden results too, without revisiting unrelated columns.
            let result =
                RecordBatch::try_new_with_options(step.output.clone(), vec![array], &options)
                    .map_err(generated_batch_error)?;
            values.insert(step.index, result.column(0).clone());
            for index in &step.release {
                values.remove(index);
            }
        }
        assemble_batch(&self.schema, &self.output_indices, &batch, &values)
            .map_err(generated_batch_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, ArrayRef, Int64Array};
    use arrow_schema::{DataType, Field};

    #[test]
    fn branching_dependencies_keep_outputs_and_release_dead_values() {
        let batch =
            RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
                .unwrap();
        let specs = [
            ("a", "n + 1"),
            ("b", "a + 1"),
            ("dead", "100"),
            ("c", "a + b"),
            ("d", "c + 1"),
        ]
        .map(|(column, expression)| GeneratedColumnSpec {
            column: column.into(),
            expression: expression.into(),
        });
        let (schema, steps, _) =
            plan_steps(&ExpressionPlanner::new(), &specs, &batch.schema()).unwrap();
        let plan = GeneratedColumnsPlan::new(
            steps,
            Arc::new(schema.project(&[1, 5]).unwrap()),
            vec![1, 5],
        );
        assert!(plan.steps[0].release.is_empty());
        assert!(plan.steps[1].release.is_empty());
        assert_eq!(plan.steps[2].release, [3]);
        assert_eq!(plan.steps[3].release, [2]);
        assert_eq!(plan.steps[4].release, [4]);
        let output = plan.generate(batch).unwrap();
        assert_eq!(
            output.column(0).to_data(),
            Int64Array::from(vec![2]).to_data()
        );
        assert_eq!(
            output.column(1).to_data(),
            Int64Array::from(vec![6]).to_data()
        );
    }

    #[test]
    fn single_dependency_steps_keep_compact_schemas_and_slots() {
        const WIDTH: usize = 256;
        let source = Arc::new(Schema::new(
            (0..WIDTH)
                .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        let specs = (0..WIDTH)
            .map(|i| GeneratedColumnSpec {
                column: format!("g{i}"),
                expression: format!(
                    "{} + 1",
                    if i == 0 {
                        format!("c{}", WIDTH - 1)
                    } else {
                        format!("g{}", i - 1)
                    }
                ),
            })
            .collect::<Vec<_>>();
        let (schema, steps, dependencies) =
            plan_steps(&ExpressionPlanner::new(), &specs, &source).unwrap();
        assert_eq!(schema.fields().len(), WIDTH * 2);
        assert_eq!(dependencies, HashSet::from([format!("c{}", WIDTH - 1)]));
        assert_eq!(
            steps
                .iter()
                .map(|step| step.input.0.fields().len())
                .sum::<usize>(),
            WIDTH
        );
        assert_eq!(
            steps
                .iter()
                .map(|step| step.output.fields().len())
                .sum::<usize>(),
            WIDTH
        );
        for (i, step) in steps.iter().enumerate() {
            assert_eq!(step.input.1, vec![WIDTH - 1 + i]);
            assert_eq!(step.index, WIDTH + i);
        }
        let batch = RecordBatch::try_new(
            source,
            (0..WIDTH)
                .map(|i| Arc::new(Int64Array::from(vec![i as i64])) as ArrayRef)
                .collect(),
        )
        .unwrap();
        let plan = GeneratedColumnsPlan::new(
            steps,
            Arc::new(schema.project(&[WIDTH * 2 - 1]).unwrap()),
            vec![WIDTH * 2 - 1],
        );
        assert!(plan.steps[0].release.is_empty());
        for (i, step) in plan.steps.iter().enumerate().skip(1) {
            assert_eq!(step.release, [WIDTH + i - 1]);
        }
        let output = plan.generate(batch).unwrap();
        assert_eq!(
            output.column(0).to_data(),
            Int64Array::from(vec![(WIDTH * 2 - 1) as i64]).to_data()
        );
    }
}
