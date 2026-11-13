use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, LargeStringArray, RecordBatch, RecordBatchOptions, cast::AsArray,
};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef};
use futures::stream;
use open_arrow_access_core::{
    Accessor, Layer, ReadOptionsProvider, RecordCapability, RecordMetadata, RecordReader, Result,
    StreamReader, async_trait,
};

use crate::validation;

/// Compute one added column from the batch after preceding changes.
pub type ColumnFunctor = dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync + 'static;

/// One ordered change to the current top-level schema.
/// Names refer to the schema produced by preceding vector items.
#[derive(Clone)]
pub enum ColumnEvolution {
    AddColumn {
        name: String,
        data_type: DataType,
        functor: Arc<ColumnFunctor>,
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

    /// Append a nullable column. The callback must return the declared type and
    /// exactly one value per input row, including for empty batches.
    pub fn add_column(
        name: impl Into<String>,
        data_type: DataType,
        functor: impl Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync + 'static,
    ) -> Self {
        Self::AddColumn {
            name: name.into(),
            data_type,
            functor: Arc::new(functor),
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

impl fmt::Debug for ColumnEvolution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (operation, field, name) = match self {
            Self::AddColumn { name, .. } => ("AddColumn", "name", name),
            Self::RemoveColumn { name } => ("RemoveColumn", "name", name),
            Self::RenameColumn {
                source_name: name, ..
            } => ("RenameColumn", "source_name", name),
            Self::ChangeDataType { name, .. } => ("ChangeDataType", "name", name),
        };
        f.debug_struct(operation)
            .field(field, name)
            .finish_non_exhaustive()
    }
}

/// Add, remove, rename, and cast top-level columns with a fixed output schema.
///
/// Changes run in vector order. Callbacks see preceding additions, removals,
/// renames, and casts, even when omitted by the final projection.
/// Existing columns retain their order; additions append to the current schema.
#[derive(Clone, Debug, Default)]
pub struct SchemaEvolutionLayer {
    changes: Arc<[ColumnEvolution]>,
}

impl SchemaEvolutionLayer {
    /// Validate declarations without I/O. Source-dependent checks happen at open.
    pub fn try_new(changes: Vec<ColumnEvolution>) -> Result<Self> {
        validation::validate_evolution_specs(&changes)?;
        Ok(Self {
            changes: changes.into(),
        })
    }
}

/// A record accessor decorated by [`SchemaEvolutionLayer`].
#[derive(Clone, Debug)]
pub struct SchemaEvolutionAccessor<A> {
    inner: A,
    changes: Arc<[ColumnEvolution]>,
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
        // Arbitrary functors need every source column. Schema changes also need
        // the complete namespace to detect collisions with unchanged fields.
        if self.changes.iter().any(|change| match change {
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
        let plan = plan_evolution(&self.changes, source, &output_options)?;
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
        functor: Arc<ColumnFunctor>,
        input: Option<(SchemaRef, Vec<usize>)>,
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
    let mut changed = false;
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
                functor,
            } => {
                validation::validate_evolution_target(None, index)?;
                let input = changed.then(|| resolve_schema(&fields, source));
                let index = next_index;
                next_index += 1;
                let field = Arc::new(Field::new(name, data_type.clone(), true));
                steps.push(EvolutionStep::Add {
                    index,
                    field: field.clone(),
                    functor: functor.clone(),
                    input,
                });
                names.insert(name.clone(), index);
                fields.insert(index, field);
                changed = true;
            }
            ColumnEvolution::RemoveColumn { name } => {
                let index = resolve()?;
                names.remove(name);
                fields.remove(&index);
                steps.push(EvolutionStep::Remove(index));
                changed = true;
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
                    changed = true;
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
                    changed = true;
                }
            }
        }
    }
    let (schema, mut output_indices) = resolve_schema(&fields, source);
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

// Slots are stable across removals and renames; only batch views need dense positions.
fn resolve_schema(
    fields: &BTreeMap<usize, FieldRef>,
    source: &SchemaRef,
) -> (SchemaRef, Vec<usize>) {
    let indices = fields.keys().copied().collect();
    let fields: Vec<_> = fields.values().cloned().collect();
    (
        Arc::new(Schema::new_with_metadata(fields, source.metadata().clone())),
        indices,
    )
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
                    functor,
                    input,
                } => {
                    let view = input
                        .as_ref()
                        .map(|(schema, indices)| assemble_batch(schema, indices, &batch, &values))
                        .transpose()?;
                    let array = functor(view.as_ref().unwrap_or(&batch))
                        .map_err(|error| crate::error::evolution_functor(field.name(), error))?;
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
