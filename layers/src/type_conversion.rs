use std::{collections::HashSet, sync::Arc};

use arrow_array::{
    Array, ArrayRef, LargeStringArray, RecordBatch, RecordBatchOptions, cast::AsArray,
};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{ArrowError, DataType, Schema};
use open_arrow_access_core::{
    Accessor, Capability, Error, ErrorKind, Layer, ReadOptionsProvider, Reader, RecordCapability,
    RecordMetadata, RecordReader, Result, async_trait,
};

use crate::{FieldRemap, validation};

/// A top-level column's output type. Other columns retain their input types.
#[derive(Clone, Debug)]
pub struct ColumnTypeSpec {
    pub column: String,
    pub data_type: DataType,
}

/// Casts Arrow columns with errors on invalid values, preserving row order and count.
///
/// Uses Arrow cast semantics: successful casts can still lose numeric or temporal
/// precision. All declared casts run before the final projection, even for hidden
/// columns. String-to-timestamp casts also accept ISO minute precision (for example,
/// `2000-05-31T04:00Z`). Naive timestamps use the target timezone, or UTC if absent;
/// invalid, ambiguous, or out-of-range values fail. Storage and decoding schemas
/// are unchanged.
#[derive(Clone, Debug, Default)]
pub struct TypeConversionLayer {
    specs: Arc<[FieldRemap]>,
}

impl TypeConversionLayer {
    /// Validate column declarations without opening a source.
    pub fn try_new(specs: Vec<ColumnTypeSpec>) -> Result<Self> {
        validation::validate_specs(&specs)?;
        Ok(Self {
            specs: specs
                .into_iter()
                .map(|spec| FieldRemap {
                    source: spec.column.clone(),
                    target: spec.column,
                    data_type: Some(spec.data_type),
                })
                .collect(),
        })
    }
}

/// Shared record accessor for type conversion and schema remapping.
#[derive(Clone, Debug)]
pub struct TypeConversionAccessor<A> {
    inner: A,
    specs: Arc<[FieldRemap]>,
}

impl<A> TypeConversionAccessor<A> {
    pub(crate) fn with_remaps(inner: A, specs: Arc<[FieldRemap]>) -> Self {
        Self { inner, specs }
    }
}

impl<A> Layer<A> for TypeConversionLayer
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Layered = TypeConversionAccessor<A>;

    fn layer(&self, inner: A) -> Self::Layered {
        TypeConversionAccessor {
            inner,
            specs: Arc::clone(&self.specs),
        }
    }
}

#[async_trait]
impl<A> Accessor for TypeConversionAccessor<A>
where
    A: Accessor<Reader = dyn RecordReader>,
    A::Request: ReadOptionsProvider,
{
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        let inner = self.inner.capability();
        Capability {
            read: inner.read,
            read_with_range: false,
            read_parallel: inner.read_parallel,
            list: false,
        }
        .with_ordering(self.inner.capability().ordering)
    }

    async fn open(&self, mut request: A::Request) -> Result<Box<dyn RecordReader>> {
        if self.specs.is_empty() {
            return self.inner.open(request).await;
        }
        let output_options = request.read_options().clone();
        // A rename needs the full schema to detect collisions with unchanged fields.
        let renames = self.specs.iter().any(|spec| spec.source != spec.target);
        if renames {
            request.read_options_mut().projection = None;
        } else if let Some(columns) = &mut request.read_options_mut().projection {
            let mut names: HashSet<_> = columns.iter().cloned().collect();
            for spec in self.specs.iter() {
                if names.insert(spec.source.clone()) {
                    columns.push(spec.source.clone());
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
            let target = spec.data_type.as_ref().unwrap_or(field.data_type());
            if field.data_type() != target {
                casts.push(ColumnCast {
                    source_index: index,
                    target_type: target.clone(),
                });
            }
            fields[index] = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_name(&spec.target)
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
        Ok(Box::new(TypeConversionReader {
            inner,
            metadata,
            casts,
            projection,
            done: false,
        }))
    }
}

struct ColumnCast {
    /// Position in the inner reader's batch, before the final projection.
    source_index: usize,
    target_type: DataType,
}

struct TypeConversionReader {
    inner: Box<dyn RecordReader>,
    metadata: RecordMetadata,
    /// Casts planned at open, including configured columns omitted from the output.
    casts: Vec<ColumnCast>,
    /// Inner-batch column indices in output order. None keeps all columns;
    /// Some([]) preserves row counts with zero columns.
    projection: Option<Vec<usize>>,
    done: bool,
}

impl TypeConversionReader {
    fn convert(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut columns = batch.columns().to_vec();
        let options = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        for cast in &self.casts {
            let index = cast.source_index;
            let target = &cast.target_type;
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
            Arc::clone(&self.metadata.schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|error| {
            Error::new(ErrorKind::Decode, "cast", error.to_string()).with_source(error)
        })
    }
}

#[async_trait]
impl Reader for TypeConversionReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    type Capability = RecordCapability;

    fn capability(&self) -> RecordCapability {
        let inner = self.inner.capability();
        Capability {
            read: inner.read,
            read_with_range: false,
            read_parallel: inner.read_parallel,
            list: false,
        }
        .with_ordering(self.inner.capability().ordering)
    }

    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        let result = match self.inner.next().await {
            Ok(Some(batch)) => self.convert(batch).map(Some),
            other => other,
        };
        if !matches!(result, Ok(Some(_))) {
            self.done = true;
        }
        result
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
