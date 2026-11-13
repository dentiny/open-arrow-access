use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, LargeStringArray, RecordBatch, RecordBatchOptions, cast::AsArray,
};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::{
    common::DFSchema, execution::context::SessionContext, logical_expr::Expr,
    physical_expr::PhysicalExpr,
};
use futures::stream;
use open_arrow_access_core::{
    Accessor, Layer, ReadOptionsProvider, RecordCapability, RecordMetadata, RecordReader, Result,
    StreamReader, async_trait,
};

use crate::validation;

/// One ordered change to the current top-level schema.
/// Names refer to the schema produced by preceding vector items.
#[derive(Clone, Debug)]
pub enum ColumnEvolution {
    AddColumn {
        name: String,
        data_type: DataType,
        expression: Expr,
    },
    RemoveColumn {
        name: String,
    },
    RenameColumn {
        source_name: String,
        target_name: String,
    },
    ChangeDataType {
        name: String,
        data_type: DataType,
    },
}

impl ColumnEvolution {
    fn name(&self) -> &str {
        match self {
            Self::AddColumn { name, .. }
            | Self::RemoveColumn { name }
            | Self::RenameColumn {
                source_name: name, ..
            }
            | Self::ChangeDataType { name, .. } => name,
        }
    }

    /// Append a nullable column computed from the current schema. The expression
    /// must produce the declared type; scalar results expand to the batch row count.
    pub fn add_column(name: impl Into<String>, data_type: DataType, expression: Expr) -> Self {
        Self::AddColumn {
            name: name.into(),
            data_type,
            expression,
        }
    }

    pub fn remove_column(name: impl Into<String>) -> Self {
        Self::RemoveColumn { name: name.into() }
    }

    pub fn rename_column(source_name: impl Into<String>, target_name: impl Into<String>) -> Self {
        Self::RenameColumn {
            source_name: source_name.into(),
            target_name: target_name.into(),
        }
    }

    pub fn change_data_type(name: impl Into<String>, data_type: DataType) -> Self {
        Self::ChangeDataType {
            name: name.into(),
            data_type,
        }
    }
}

/// Add, remove, rename, and cast top-level columns with a fixed output schema.
///
/// Changes run in vector order. Expressions see preceding additions, removals,
/// renames, and casts, even when omitted by the final projection.
/// Existing columns retain their order; additions append to the current schema.
#[derive(Clone, Debug, Default)]
pub struct SchemaEvolutionLayer {
    changes: Arc<[ColumnEvolution]>,
    source_schema: Option<SchemaRef>,
}

impl SchemaEvolutionLayer {
    /// Validate declarations without I/O. Source-dependent checks happen at open.
    pub fn try_new(changes: Vec<ColumnEvolution>) -> Result<Self> {
        validation::validate_evolution_specs(&changes)?;
        Ok(Self {
            changes: changes.into(),
            source_schema: None,
        })
    }

    /// Supply the complete source schema to push dependencies down without
    /// opening another reader for schema discovery.
    pub fn with_source_schema(mut self, schema: SchemaRef) -> Self {
        self.source_schema = Some(schema);
        self
    }
}

/// A record accessor decorated by [`SchemaEvolutionLayer`].
#[derive(Clone, Debug)]
pub struct SchemaEvolutionAccessor<A> {
    inner: A,
    changes: Arc<[ColumnEvolution]>,
    source_schema: Option<SchemaRef>,
}

impl<A> Layer<A> for SchemaEvolutionLayer
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Layered = SchemaEvolutionAccessor<A>;
    fn layer(&self, inner: A) -> Self::Layered {
        SchemaEvolutionAccessor {
            inner,
            changes: Arc::clone(&self.changes),
            source_schema: self.source_schema.clone(),
        }
    }
}

#[async_trait]
impl<A> Accessor for SchemaEvolutionAccessor<A>
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.inner.capability()
    }

    async fn open(&self, mut request: A::Request) -> Result<Box<dyn RecordReader>> {
        if self.changes.is_empty() {
            return self.inner.open(request).await;
        }
        let output_options = request.read_options().clone();
        let mut planned = self
            .source_schema
            .as_ref()
            .map(|source| plan_evolution(&self.changes, source, &output_options))
            .transpose()?;
        // A supplied schema separates namespace checks from source data reads.
        if let Some(plan) = &mut planned {
            request.read_options_mut().projection = Some(plan.project_source()?);
        } else if self.changes.iter().any(|change| match change {
            ColumnEvolution::ChangeDataType { .. } => false,
            ColumnEvolution::RenameColumn {
                source_name,
                target_name,
            } => source_name != target_name,
            _ => true,
        }) {
            request.read_options_mut().projection = None;
        } else if let Some(columns) = &mut request.read_options_mut().projection {
            let mut names: HashSet<_> = columns.iter().cloned().collect();
            for change in self.changes.iter() {
                let name = change.name();
                if names.insert(name.to_owned()) {
                    columns.push(name.to_owned());
                }
            }
        }
        let inner = self.inner.open(request).await?;
        let source = &inner.metadata().schema;
        let plan = if let Some(mut plan) = planned {
            validation::validate_evolution_input_schema(source, &plan.source)?;
            plan.source = source.clone();
            plan
        } else {
            plan_evolution(&self.changes, source, &output_options)?
        };
        if plan.steps.is_empty()
            && plan.schema == *source
            && plan
                .output_indices
                .iter()
                .copied()
                .eq(0..source.fields().len())
        {
            return Ok(inner);
        }
        let metadata = RecordMetadata {
            schema: Arc::clone(&plan.schema),
            source: inner.metadata().source.clone(),
        };
        let capability = inner.capability();
        let batches = stream::try_unfold((inner, plan), |(mut inner, plan)| async move {
            let Some(batch) = inner.next().await? else {
                return Ok(None);
            };
            Ok(Some((plan.convert(batch)?, (inner, plan))))
        });
        Ok(Box::new(
            StreamReader::records(batches, metadata, capability.ordering)
                .with_capability(capability),
        ))
    }
}

struct SchemaEvolutionPlan {
    source: SchemaRef,
    schema: SchemaRef,
    affected: Vec<usize>,
    steps: Vec<EvolutionStep>,
    output_indices: Vec<usize>,
}

enum EvolutionStep {
    Add {
        index: usize,
        field: FieldRef,
        expression: Arc<dyn PhysicalExpr>,
        input: (SchemaRef, Vec<usize>),
    },
    Cast {
        index: usize,
        field: FieldRef,
    },
    Remove(usize),
}

fn plan_evolution(
    changes: &[ColumnEvolution],
    source: &SchemaRef,
    options: &open_arrow_access_core::ReadOptions,
) -> Result<SchemaEvolutionPlan> {
    let mut names: HashMap<_, _> = source
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name().clone(), index))
        .collect();
    validation::validate_evolution_source(names.len(), source.fields().len())?;
    let mut fields: BTreeMap<_, _> = source.fields().iter().cloned().enumerate().collect();
    let mut next_index = source.fields().len();
    let mut affected = HashSet::new();
    let mut steps = Vec::new();
    let mut state = None;
    for change in changes {
        let index = names.get(change.name()).copied();
        if let Some(index) = index
            && index < source.fields().len()
        {
            affected.insert(index);
        }
        let resolve = || index.ok_or_else(|| crate::error::evolution_source(change.name()));
        match change {
            ColumnEvolution::AddColumn {
                name,
                data_type,
                expression,
            } => {
                validation::validate_evolution_target(None, index)?;
                let mut indices = expression
                    .column_refs()
                    .into_iter()
                    .map(|column| {
                        names
                            .get(&column.name)
                            .copied()
                            .ok_or_else(|| crate::error::evolution_source(&column.name))
                    })
                    .collect::<Result<Vec<_>>>()?;
                indices.sort_unstable();
                indices.dedup();
                affected.extend(
                    indices
                        .iter()
                        .copied()
                        .filter(|&index| index < source.fields().len()),
                );
                let input_schema = Arc::new(Schema::new_with_metadata(
                    indices
                        .iter()
                        .map(|index| fields[index].clone())
                        .collect::<Vec<_>>(),
                    source.metadata().clone(),
                ));
                let state = state.get_or_insert_with(|| SessionContext::new().state());
                let df_schema = DFSchema::try_from(input_schema.clone())
                    .map_err(|error| crate::error::evolution_plan_error(name, error))?;
                let (expression, output) =
                    crate::expression::plan_expression(state, expression.clone(), &df_schema)
                        .map_err(|error| crate::error::evolution_plan_error(name, error))?;
                validation::validate_evolution_expression_type(
                    name,
                    output.data_type(),
                    data_type,
                )?;
                let input = (input_schema, indices);
                let index = next_index;
                next_index += 1;
                let field = Arc::new(Field::new(name, data_type.clone(), true));
                steps.push(EvolutionStep::Add {
                    index,
                    field: field.clone(),
                    expression,
                    input,
                });
                names.insert(name.clone(), index);
                fields.insert(index, field);
            }
            ColumnEvolution::RemoveColumn { name } => {
                let index = resolve()?;
                names.remove(name);
                fields.remove(&index);
                steps.push(EvolutionStep::Remove(index));
            }
            ColumnEvolution::RenameColumn {
                source_name,
                target_name,
            } => {
                let index = resolve()?;
                validation::validate_evolution_target(
                    Some(index),
                    names.get(target_name).copied(),
                )?;
                if source_name != target_name {
                    names.remove(source_name);
                    names.insert(target_name.clone(), index);
                    fields.insert(
                        index,
                        Arc::new(fields[&index].as_ref().clone().with_name(target_name)),
                    );
                }
            }
            ColumnEvolution::ChangeDataType { data_type, .. } => {
                let index = resolve()?;
                let field = fields[&index].as_ref();
                if field.data_type() != data_type {
                    validation::validate_cast(field, data_type)?;
                    let field = Arc::new(field.clone().with_data_type(data_type.clone()));
                    fields.insert(index, field.clone());
                    steps.push(EvolutionStep::Cast { index, field });
                }
            }
        }
    }
    let mut output_indices: Vec<_> = fields.keys().copied().collect();
    let schema = Arc::new(Schema::new_with_metadata(
        fields.into_values().collect::<Vec<_>>(),
        source.metadata().clone(),
    ));
    let (schema, projection) = options.resolve_projection(&schema)?;
    if let Some(projection) = projection {
        output_indices = projection
            .into_iter()
            .map(|index| output_indices[index])
            .collect();
    }
    Ok(SchemaEvolutionPlan {
        source: source.clone(),
        schema,
        affected: affected.into_iter().collect(),
        steps,
        output_indices,
    })
}

fn assemble_batch(
    schema: &SchemaRef,
    indices: &[usize],
    source: &RecordBatch,
    values: &HashMap<usize, ArrayRef>,
) -> Result<RecordBatch> {
    let columns = indices
        .iter()
        .map(|&index| {
            values
                .get(&index)
                .unwrap_or_else(|| source.column(index))
                .clone()
        })
        .collect();
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(source.num_rows())),
    )
    .map_err(crate::error::evolution_batch)
}

impl SchemaEvolutionPlan {
    /// Bind stable source slots to the compact projection sent upstream.
    fn project_source(&mut self) -> Result<Vec<String>> {
        let mut required: HashSet<_> = self.output_indices.iter().copied().collect();
        for step in &self.steps {
            match step {
                EvolutionStep::Add { input, .. } => required.extend(input.1.iter().copied()),
                EvolutionStep::Cast { index, .. } => {
                    required.insert(*index);
                }
                EvolutionStep::Remove(_) => {}
            }
        }
        let mut indices: Vec<_> = required
            .into_iter()
            .filter(|&index| index < self.source.fields().len())
            .collect();
        indices.sort_unstable();
        let names = indices
            .iter()
            .map(|&index| self.source.field(index).name().clone())
            .collect();
        let input_indices: HashMap<_, _> = indices
            .iter()
            .enumerate()
            .map(|(input, &slot)| (slot, input))
            .collect();
        self.affected = self
            .affected
            .iter()
            .filter_map(|index| input_indices.get(index).copied())
            .collect();
        let rebind = |index: &mut usize| {
            if let Some(&input) = input_indices.get(index) {
                *index = input;
            }
        };
        // Removed source columns may not be read at all. Computed slots keep
        // their original indices, which cannot overlap compact input indices.
        self.steps.retain_mut(|step| match step {
            EvolutionStep::Remove(index)
                if *index < self.source.fields().len() && !input_indices.contains_key(index) =>
            {
                false
            }
            EvolutionStep::Add { input, .. } => {
                input.1.iter_mut().for_each(&rebind);
                true
            }
            EvolutionStep::Cast { index, .. } | EvolutionStep::Remove(index) => {
                rebind(index);
                true
            }
        });
        self.output_indices.iter_mut().for_each(rebind);
        self.source = Arc::new(
            self.source
                .project(&indices)
                .map_err(crate::error::evolution_batch)?,
        );
        Ok(names)
    }

    fn convert(&self, batch: RecordBatch) -> Result<RecordBatch> {
        validation::validate_evolution_batch_schema(
            batch.schema_ref(),
            &self.source,
            &self.affected,
        )?;
        let mut values = HashMap::new();
        let options = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        for step in &self.steps {
            match step {
                EvolutionStep::Add {
                    index,
                    field,
                    expression,
                    input,
                } => {
                    let view = assemble_batch(&input.0, &input.1, &batch, &values)?;
                    let array = expression
                        .evaluate(&view)
                        .and_then(|value| value.into_array(batch.num_rows()))
                        .map_err(|error| {
                            crate::error::evolution_expression_error(field.name(), error)
                        })?;
                    validation::validate_added_column(
                        field.name(),
                        array.as_ref(),
                        field.data_type(),
                        batch.num_rows(),
                    )?;
                    values.insert(*index, array);
                }
                EvolutionStep::Cast { index, field } => {
                    let column = values.get(index).unwrap_or_else(|| batch.column(*index));
                    let target = field.data_type();
                    let array = cast_with_options(column.as_ref(), target, &options)
                        .or_else(|error| match target {
                            DataType::Timestamp(_, _) if column.data_type().is_string() => {
                                cast_minute_timestamps(column.as_ref(), target, &options)
                            }
                            _ => Err(error),
                        })
                        .map_err(|error| {
                            crate::error::cast_error(
                                field.name(),
                                column.data_type(),
                                target,
                                error,
                            )
                        })?;
                    values.insert(*index, array);
                }
                EvolutionStep::Remove(index) => {
                    values.remove(index);
                }
            }
        }
        assemble_batch(&self.schema, &self.output_indices, &batch, &values)
    }
}

/// Supply omitted seconds, then let Arrow validate dates, offsets, timezones and range.
fn cast_minute_timestamps(
    input: &dyn Array,
    target: &DataType,
    options: &CastOptions<'_>,
) -> std::result::Result<ArrayRef, ArrowError> {
    let strings = cast_with_options(input, &DataType::LargeUtf8, options)?;
    let normalized = LargeStringArray::from_iter(strings.as_string::<i64>().iter().map(|value| {
        value.map(|value| {
            if value.as_bytes().get(13) == Some(&b':')
                && matches!(
                    value.as_bytes().get(16),
                    None | Some(b'Z' | b'z' | b'+' | b'-')
                )
                && value.get(..16).is_some()
            {
                format!("{}:00{}", &value[..16], &value[16..])
            } else {
                value.to_owned()
            }
        })
    }));
    cast_with_options(&normalized, target, options)
}
