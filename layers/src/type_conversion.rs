use std::{collections::HashSet, sync::Arc};

use arrow_array::{
    RecordBatch, RecordBatchOptions,
    types::{
        Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
        validate_decimal_precision_and_scale,
    },
};
use arrow_cast::{CastOptions, can_cast_types, cast_with_options};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use open_arrow_access_core::{
    Accessor, Capability, Error, ErrorKind, FieldNameIndex, Layer, ReadOptionsProvider, Reader,
    RecordMetadata, RecordReader, Result, async_trait,
};

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
/// columns. Storage and decoding schemas are unchanged.
#[derive(Clone, Debug, Default)]
pub struct TypeConversionLayer {
    specs: Arc<[ColumnTypeSpec]>,
}

impl TypeConversionLayer {
    /// Validate column declarations without opening a source.
    pub fn try_new(specs: Vec<ColumnTypeSpec>) -> Result<Self> {
        let mut names = HashSet::with_capacity(specs.len());
        for spec in &specs {
            if spec.column.is_empty() || !names.insert(&spec.column) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "configure_cast",
                    "cast columns must have nonempty, distinct names",
                ));
            }
            validate_target(&spec.data_type).map_err(|error| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "configure_cast",
                    format!("column {:?}: {error}", spec.column),
                )
                .with_source(error)
            })?;
        }
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

    fn capability(&self) -> Capability {
        let inner = self.inner.capability();
        Capability {
            read: inner.read,
            read_with_range: false,
            read_parallel: inner.read_parallel,
            list: false,
        }
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
            if has_extension(field) || has_extension_type(&spec.data_type) {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "plan_cast",
                    format!(
                        "column {:?}: casts involving extension fields are unsupported",
                        spec.column
                    ),
                ));
            }
            if !can_cast_types(field.data_type(), &spec.data_type) {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "plan_cast",
                    format!(
                        "column {:?}: cannot cast {} to {}",
                        spec.column,
                        field.data_type(),
                        spec.data_type
                    ),
                ));
            }
            fields[index] = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(spec.data_type.clone()),
            );
            casts.push((index, spec.data_type.clone()));
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

fn validate_target(data_type: &DataType) -> std::result::Result<(), ArrowError> {
    let invalid = || {
        ArrowError::InvalidArgumentError(format!("invalid or unsupported cast target {data_type}"))
    };
    match data_type {
        DataType::Time32(unit) if !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) => {
            return Err(invalid());
        }
        DataType::Time64(unit) if !matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond) => {
            return Err(invalid());
        }
        DataType::FixedSizeBinary(size) | DataType::FixedSizeList(_, size) if *size < 0 => {
            return Err(invalid());
        }
        DataType::Dictionary(key, _) if !key.is_dictionary_key_type() => return Err(invalid()),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields)
                if !entries.is_nullable() && fields.len() == 2 && !fields[0].is_nullable() => {}
            _ => return Err(invalid()),
        },
        DataType::Union(fields, _) => {
            let mut ids = HashSet::with_capacity(fields.len());
            if fields.is_empty() || fields.iter().any(|(id, _)| id < 0 || !ids.insert(id)) {
                return Err(invalid());
            }
        }
        // Arrow's null-to-run-end cast can panic when the batch exceeds the key range.
        DataType::RunEndEncoded(_, _) => return Err(invalid()),
        DataType::Decimal32(precision, scale) => {
            return validate_decimal_precision_and_scale::<Decimal32Type>(*precision, *scale);
        }
        DataType::Decimal64(precision, scale) => {
            return validate_decimal_precision_and_scale::<Decimal64Type>(*precision, *scale);
        }
        DataType::Decimal128(precision, scale) => {
            return validate_decimal_precision_and_scale::<Decimal128Type>(*precision, *scale);
        }
        DataType::Decimal256(precision, scale) => {
            return validate_decimal_precision_and_scale::<Decimal256Type>(*precision, *scale);
        }
        _ => {}
    }
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => validate_target(field.data_type())?,
        DataType::Struct(fields) => {
            for field in fields {
                validate_target(field.data_type())?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                validate_target(field.data_type())?;
            }
        }
        DataType::Dictionary(_, value) => validate_target(value)?,
        _ => {}
    }
    Ok(())
}

fn has_extension(field: &Field) -> bool {
    field.metadata().contains_key("ARROW:extension:name") || has_extension_type(field.data_type())
}

fn has_extension_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => has_extension(field),
        DataType::Struct(fields) => fields.iter().any(|field| has_extension(field)),
        DataType::Union(fields, _) => fields.iter().any(|(_, field)| has_extension(field)),
        DataType::Dictionary(_, value) => has_extension_type(value),
        DataType::RunEndEncoded(ends, values) => has_extension(ends) || has_extension(values),
        _ => false,
    }
}

struct TypeConversionReader {
    inner: Box<dyn RecordReader>,
    metadata: RecordMetadata,
    casts: Vec<(usize, DataType)>,
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
        for (index, target) in &self.casts {
            columns[*index] = cast_with_options(columns[*index].as_ref(), target, &options)
                .map_err(|error| {
                    Error::new(
                        ErrorKind::Decode,
                        "cast",
                        format!(
                            "column {:?}: {} to {target}: {error}",
                            batch.schema().field(*index).name(),
                            columns[*index].data_type(),
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

    fn capability(&self) -> Capability {
        let inner = self.inner.capability();
        Capability {
            read: inner.read,
            read_with_range: false,
            read_parallel: inner.read_parallel,
            list: false,
        }
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
