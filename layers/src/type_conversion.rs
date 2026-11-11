use std::{collections::HashSet, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, cast::AsArray, timezone::Tz,
};
use arrow_cast::{CastOptions, cast_with_options, parse::string_to_datetime};
use arrow_schema::{ArrowError, DataType, Schema, TimeUnit};
use chrono::format::{Parsed, StrftimeItems, parse};
use open_arrow_access_core::{
    Accessor, Capability, Error, ErrorKind, FieldNameIndex, Layer, ReadOptionsProvider, Reader,
    RecordCapability, RecordMetadata, RecordReader, Result, async_trait,
};

use crate::validation;

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
    specs: Arc<[ColumnTypeSpec]>,
}

impl TypeConversionLayer {
    /// Validate column declarations without opening a source.
    pub fn try_new(specs: Vec<ColumnTypeSpec>) -> Result<Self> {
        validation::validate_specs(&specs)?;
        Ok(Self {
            specs: specs.into(),
        })
    }
}

/// A record accessor decorated by [`TypeConversionLayer`].
#[derive(Clone, Debug)]
pub struct TypeConversionAccessor<A> {
    inner: A,
    specs: Arc<[ColumnTypeSpec]>,
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
        if let Some(columns) = &mut request.read_options_mut().projection {
            let mut names: HashSet<_> = columns.iter().cloned().collect();
            for spec in self.specs.iter() {
                if names.insert(spec.column.clone()) {
                    columns.push(spec.column.clone());
                }
            }
        }
        let inner = self.inner.open(request).await?;
        let source = &inner.metadata().schema;
        let field_index = FieldNameIndex::new(source.fields());
        let mut fields = source.fields().to_vec();
        let mut casts = Vec::with_capacity(self.specs.len());
        for spec in self.specs.iter() {
            let index = field_index.index_of(&spec.column).map_err(|error| {
                Error::new(ErrorKind::InvalidInput, "plan_cast", error.to_string())
                    .with_source(error)
            })?;
            let field = &fields[index];
            if field.data_type() == &spec.data_type {
                continue;
            }
            validation::validate_cast(field, &spec.data_type)?;
            fields[index] = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(spec.data_type.clone()),
            );
            casts.push(ColumnCast {
                source_index: index,
                target_type: spec.data_type.clone(),
            });
        }
        let schema = Arc::new(Schema::new_with_metadata(fields, source.metadata().clone()));
        let (schema, projection) = output_options.resolve_projection(&schema)?;
        let projection =
            projection.filter(|indices| indices.iter().copied().ne(0..source.fields().len()));
        if casts.is_empty() && projection.is_none() {
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
                    DataType::Timestamp(unit, timezone)
                        if columns[index].data_type().is_string() =>
                    {
                        parse_timestamp(columns[index].as_ref(), *unit, timezone.clone())
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

fn parse_timestamp(
    input: &dyn Array,
    unit: TimeUnit,
    timezone: Option<Arc<str>>,
) -> std::result::Result<ArrayRef, ArrowError> {
    let parsed_timezone: Tz = timezone.as_deref().unwrap_or("UTC").parse()?;
    let target = DataType::Timestamp(unit, timezone);
    let strings = cast_with_options(
        input,
        &DataType::LargeUtf8,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?;
    let parse_error = |error: chrono::ParseError| ArrowError::ParseError(error.to_string());
    let values = strings
        .as_string::<i64>()
        .iter()
        .map(|value| {
            value
                .map(|value| {
                    let datetime =
                        string_to_datetime(&parsed_timezone, value).or_else(|error| {
                            // Arrow handles second precision; Chrono handles ISO minute precision.
                            for format in [
                                "%FT%R%#z", "%F %R%#z", "%Ft%R%#z", "%FT%R", "%F %R", "%Ft%R",
                            ] {
                                let mut parsed = Parsed::new();
                                if parse(&mut parsed, value, StrftimeItems::new(format)).is_err() {
                                    continue;
                                }
                                return if parsed.offset.is_some() {
                                    parsed
                                        .to_datetime()
                                        .map(|value| value.with_timezone(&parsed_timezone))
                                        .map_err(parse_error)
                                } else {
                                    parsed
                                        .to_datetime_with_timezone(&parsed_timezone)
                                        .map_err(parse_error)
                                };
                            }
                            Err(error)
                        })?;
                    Ok(match unit {
                        TimeUnit::Second => datetime.timestamp(),
                        TimeUnit::Millisecond => datetime.timestamp_millis(),
                        TimeUnit::Microsecond => datetime.timestamp_micros(),
                        TimeUnit::Nanosecond => {
                            datetime.timestamp_nanos_opt().ok_or_else(|| {
                                ArrowError::CastError(format!(
                                    "timestamp out of nanosecond range: {value}"
                                ))
                            })?
                        }
                    })
                })
                .transpose()
        })
        .collect::<std::result::Result<Vec<Option<i64>>, ArrowError>>()?;
    let data = Int64Array::from(values)
        .into_data()
        .into_builder()
        .data_type(target)
        .build()?;
    Ok(arrow_array::make_array(data))
}
