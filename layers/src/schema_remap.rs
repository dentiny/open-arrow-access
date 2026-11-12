use std::{collections::HashSet, sync::Arc};

use arrow_array::{
    Array, ArrayRef, LargeStringArray, RecordBatch, RecordBatchOptions, cast::AsArray,
};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{ArrowError, DataType, Schema};
use futures::stream;
use open_arrow_access_core::{
    Accessor, Error, ErrorKind, Layer, ReadOptionsProvider, RecordCapability, RecordMetadata,
    RecordReader, Result, StreamReader, async_trait,
};

use crate::validation;

/// Remap one top-level field; omitted fields remain unchanged.
#[derive(Clone, Debug)]
pub struct FieldRemap {
    pub source_name: String,
    /// None preserves the source name.
    pub target_name: Option<String>,
    /// None preserves the source type.
    pub target_data_type: Option<DataType>,
}

impl FieldRemap {
    pub(crate) fn output_name(&self) -> &str {
        self.target_name.as_deref().unwrap_or(&self.source_name)
    }
}

/// Rename top-level fields and optionally cast their types.
#[derive(Clone, Debug, Default)]
pub struct SchemaRemapLayer {
    specs: Arc<[FieldRemap]>,
}

impl SchemaRemapLayer {
    pub fn try_new(specs: Vec<FieldRemap>) -> Result<Self> {
        validation::validate_remap_specs(&specs)?;
        Ok(Self {
            specs: specs.into(),
        })
    }
}

/// A record accessor decorated by [`SchemaRemapLayer`].
#[derive(Clone, Debug)]
pub struct SchemaRemapAccessor<A> {
    inner: A,
    specs: Arc<[FieldRemap]>,
}

impl<A> Layer<A> for SchemaRemapLayer
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Layered = SchemaRemapAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        SchemaRemapAccessor {
            inner,
            specs: Arc::clone(&self.specs),
        }
    }
}

#[async_trait]
impl<A> Accessor for SchemaRemapAccessor<A>
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
        if self.specs.is_empty() {
            return self.inner.open(request).await;
        }
        let output_options = request.read_options().clone();
        // A rename needs the full schema to detect collisions with unchanged fields.
        let renames = self
            .specs
            .iter()
            .any(|spec| spec.source_name != spec.output_name());
        if renames {
            request.read_options_mut().projection = None;
        } else if let Some(columns) = &mut request.read_options_mut().projection {
            let mut names: HashSet<_> = columns.iter().cloned().collect();
            for spec in self.specs.iter() {
                if names.insert(spec.source_name.clone()) {
                    columns.push(spec.source_name.clone());
                }
            }
        }
        let inner = self.inner.open(request).await?;
        let source = &inner.metadata().schema;
        let indices = validation::validate_remap_sources(&self.specs, source)?;
        let mut fields = source.fields().to_vec();
        let mut casts = Vec::with_capacity(self.specs.len());
        for (spec, index) in self.specs.iter().zip(indices) {
            let field = &fields[index];
            let target = spec.target_data_type.as_ref().unwrap_or(field.data_type());
            if field.data_type() != target {
                casts.push((index, target.clone()));
            }
            fields[index] = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_name(spec.output_name())
                    .with_data_type(target.clone()),
            );
        }
        if renames {
            validation::validate_remap_output(&fields)?;
        }
        let schema = Arc::new(Schema::new_with_metadata(fields, source.metadata().clone()));
        let (schema, projection) = output_options.resolve_projection(&schema)?;
        let projection =
            projection.filter(|indices| indices.iter().copied().ne(0..source.fields().len()));
        if casts.is_empty() && projection.is_none() && schema == *source {
            return Ok(inner);
        }
        let metadata = RecordMetadata {
            schema,
            source: inner.metadata().source.clone(),
        };
        let capability = inner.capability();
        let plan = RemapPlan {
            schema: Arc::clone(&metadata.schema),
            casts,
            projection,
        };
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

struct RemapPlan {
    schema: Arc<Schema>,
    casts: Vec<(usize, DataType)>,
    projection: Option<Vec<usize>>,
}

impl RemapPlan {
    fn convert(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut columns = batch.columns().to_vec();
        let options = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        for (index, target) in &self.casts {
            let index = *index;
            columns[index] = cast_with_options(columns[index].as_ref(), target, &options)
                .or_else(|error| match target {
                    // Keep Arrow's semantics, adding only the service's minute-precision fallback.
                    DataType::Timestamp(_, _) if columns[index].data_type().is_string() => {
                        cast_minute_timestamps(columns[index].as_ref(), target, &options)
                    }
                    _ => Err(error),
                })
                .map_err(|error| {
                    Error::new(
                        ErrorKind::Decode,
                        "cast",
                        format!(
                            "column {:?}: {} to {target}: {error}",
                            batch.schema().field(index).name(),
                            columns[index].data_type(),
                        ),
                    )
                    .with_source(error)
                })?;
        }
        if let Some(indices) = &self.projection {
            columns = indices
                .iter()
                .map(|&index| Arc::clone(&columns[index]))
                .collect();
        }
        RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|error| {
            Error::new(ErrorKind::Decode, "cast", error.to_string()).with_source(error)
        })
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
