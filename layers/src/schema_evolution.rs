use std::{collections::HashSet, fmt, sync::Arc};

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

/// Compute one added column from the original source batch.
pub type ColumnFunctor = dyn Fn(&RecordBatch) -> Result<ArrayRef> + Send + Sync + 'static;

/// A declarative change to a top-level source column, or a new column to append.
/// Names in remove/rename/type changes refer to the original source schema.
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
/// Existing columns keep their order; added columns follow declaration order.
/// Renames are simultaneous, so swapping two names is supported. A source column
/// may be renamed and cast together, but cannot also be removed. Callbacks see
/// the original source batch and run even when omitted by the final projection.
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
        if self
            .changes
            .iter()
            .any(|change| !matches!(change, ColumnEvolution::ChangeDataType { .. }))
        {
            request.read_options_mut().projection = None;
        } else if let Some(columns) = &mut request.read_options_mut().projection {
            let mut names: HashSet<_> = columns.iter().cloned().collect();
            for change in self.changes.iter() {
                if let ColumnEvolution::ChangeDataType { name, .. } = change
                    && names.insert(name.clone())
                {
                    columns.push(name.clone());
                }
            }
        }
        let inner = self.inner.open(request).await?;
        let source = &inner.metadata().schema;
        let plan = plan_evolution(&self.changes, source, &output_options)?;
        if plan.casts.is_empty()
            && plan.additions.is_empty()
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
    casts: Vec<(usize, DataType)>,
    additions: Vec<(FieldRef, Arc<ColumnFunctor>)>,
    output_indices: Vec<usize>,
}

fn plan_evolution(
    changes: &[ColumnEvolution],
    source: &SchemaRef,
    options: &open_arrow_access_core::ReadOptions,
) -> Result<SchemaEvolutionPlan> {
    validation::validate_evolution_output(source.fields())?;
    let names = open_arrow_access_core::FieldNameIndex::new(source.fields());
    let mut fields = source.fields().to_vec();
    let mut removed = HashSet::new();
    let mut casts = Vec::new();
    let mut additions = Vec::new();
    for change in changes {
        let index = || {
            names
                .index_of(change.name())
                .map_err(crate::error::evolution_source)
        };
        match change {
            ColumnEvolution::AddColumn {
                name,
                data_type,
                functor,
            } => {
                let field = Arc::new(Field::new(name, data_type.clone(), true));
                fields.push(field.clone());
                additions.push((field, functor.clone()));
            }
            ColumnEvolution::RemoveColumn { .. } => {
                removed.insert(index()?);
            }
            ColumnEvolution::RenameColumn { target_name, .. } => {
                let index = index()?;
                fields[index] = Arc::new(fields[index].as_ref().clone().with_name(target_name));
            }
            ColumnEvolution::ChangeDataType { data_type, .. } => {
                let index = index()?;
                let field = fields[index].as_ref();
                if field.data_type() != data_type {
                    validation::validate_cast(source.field(index), data_type)?;
                    casts.push((index, data_type.clone()));
                }
                fields[index] = Arc::new(field.clone().with_data_type(data_type.clone()));
            }
        }
    }
    let mut output_indices: Vec<_> = (0..fields.len())
        .filter(|index| !removed.contains(index))
        .collect();
    let output_fields: Vec<_> = output_indices
        .iter()
        .map(|&index| fields[index].clone())
        .collect();
    validation::validate_evolution_output(&output_fields)?;
    let schema = Arc::new(Schema::new_with_metadata(
        output_fields,
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
        source: Arc::clone(source),
        schema,
        casts,
        additions,
        output_indices,
    })
}

impl SchemaEvolutionPlan {
    fn convert(&self, batch: RecordBatch) -> Result<RecordBatch> {
        validation::validate_evolution_batch_schema(batch.schema_ref(), &self.source)?;
        let mut columns = batch.columns().to_vec();
        let options = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        for &(index, ref target) in &self.casts {
            columns[index] = cast_with_options(columns[index].as_ref(), target, &options)
                .or_else(|error| match target {
                    // Keep Arrow's semantics, adding only the service's minute-precision fallback.
                    DataType::Timestamp(_, _) if columns[index].data_type().is_string() => {
                        cast_minute_timestamps(columns[index].as_ref(), target, &options)
                    }
                    _ => Err(error),
                })
                .map_err(|error| {
                    crate::error::cast_error(
                        batch.schema().field(index).name(),
                        columns[index].data_type(),
                        target,
                        error,
                    )
                })?;
        }

        for (field, functor) in &self.additions {
            let array = functor(&batch)
                .map_err(|error| crate::error::evolution_functor(field.name(), error))?;
            validation::validate_added_column(
                field.name(),
                array.as_ref(),
                field.data_type(),
                batch.num_rows(),
            )?;
            columns.push(array);
        }
        let columns = self
            .output_indices
            .iter()
            .map(|&index| Arc::clone(&columns[index]))
            .collect();
        RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(crate::error::evolution_batch)
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
