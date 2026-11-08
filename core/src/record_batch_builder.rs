use std::collections::HashSet;

use arrow_array::{RecordBatchOptions, builder::*};
use arrow_schema::{DataType, Field, Fields};

use crate::{Error, ErrorKind, ReadOptions, RecordBatch, Result, SchemaRef};

/// Build metadata batches using Arrow's native builders and named fields.
///
/// Supports the PMC/Gutenberg types: bool, i32/Date32, u32, u64, Utf8/LargeUtf8,
/// Binary/LargeBinary, List/LargeList and Struct. Projection skips unselected
/// fields and list callbacks. Source parsing and validation remain with callers.
/// The full schema must use these types, including fields omitted by projection.
/// An append error invalidates the builder. `finish` preserves schema metadata
/// and zero-column row counts, and consumes the builder.
///
/// ```
/// use std::sync::Arc;
/// use arrow_schema::{DataType, Field, Schema};
/// use open_arrow_access_core::RecordBatchBuilder;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::UInt64, false),
///     Field::new("data", DataType::LargeBinary, false),
/// ]));
/// let mut builder = RecordBatchBuilder::new(schema, None)?;
/// builder.push(|row| {
///     row.append("id", 1u64)?;
///     row.append("data", [0, 255].as_slice())
/// })?;
/// assert_eq!(builder.finish()?.num_rows(), 1);
/// # Ok::<(), open_arrow_access_core::Error>(())
/// ```
pub struct RecordBatchBuilder {
    schema: SchemaRef,
    fields: Fields,
    builder: Option<StructBuilder>,
    rows: usize,
}

impl RecordBatchBuilder {
    /// Select top-level fields in caller order; reject unknown or duplicate names.
    pub fn new(schema: SchemaRef, projection: Option<Vec<String>>) -> Result<Self> {
        let fields = schema.fields().clone();
        let (schema, _) = ReadOptions {
            projection,
            ..Default::default()
        }
        .resolve_projection(&schema)?;
        validate_fields(&fields)?;
        let builder = StructBuilder::from_fields(schema.fields().clone(), 0);
        Ok(Self {
            schema,
            fields,
            builder: Some(builder),
            rows: 0,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    pub fn len(&self) -> usize {
        self.rows
    }
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Write one row. Missing nullable fields become null; required fields must be set.
    pub fn push(
        &mut self,
        write: impl FnOnce(&mut RecordBatchRow<'_>) -> Result<()>,
    ) -> Result<()> {
        let mut builder = self.builder.take().ok_or_else(failed_builder)?;
        let mut row = RecordBatchRow::new(&mut builder, self.fields.clone());
        write(&mut row)?;
        row.finish()?;
        self.builder = Some(builder);
        self.rows += 1;
        Ok(())
    }

    pub fn finish(self) -> Result<RecordBatch> {
        let columns = self
            .builder
            .ok_or_else(failed_builder)?
            .finish()
            .columns()
            .to_vec();
        RecordBatch::try_new_with_options(
            self.schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(self.rows)),
        )
        .map_err(|error| decode(error.to_string()).with_source(error))
    }
}

/// Values used by metadata readers. `Option<T>` writes null for `None`.
pub enum FieldValue<'a> {
    Null,
    Bool(bool),
    Int32(i32),
    UInt32(u32),
    UInt64(u64),
    Text(&'a str),
    Binary(&'a [u8]),
}

macro_rules! field_values {
    ($($ty:ty => $variant:ident),* $(,)?) => {
        $(impl<'a> From<$ty> for FieldValue<'a> {
            fn from(value: $ty) -> Self { Self::$variant(value) }
        })*
    };
}
field_values!(bool => Bool, i32 => Int32, u32 => UInt32, u64 => UInt64, &'a str => Text, &'a [u8] => Binary);
impl<'a, T: Into<FieldValue<'a>>> From<Option<T>> for FieldValue<'a> {
    fn from(value: Option<T>) -> Self {
        value.map(Into::into).unwrap_or(Self::Null)
    }
}

/// A row callback writes named fields, including lists and nested metadata rows.
pub struct RecordBatchRow<'a> {
    builder: &'a mut StructBuilder,
    fields: Fields,
    seen: Vec<bool>,
    failed: bool,
}

impl<'a> RecordBatchRow<'a> {
    fn new(builder: &'a mut StructBuilder, fields: Fields) -> Self {
        let seen = vec![false; builder.num_fields()];
        Self {
            builder,
            fields,
            seen,
            failed: false,
        }
    }

    pub fn append<'v>(&mut self, name: &str, value: impl Into<FieldValue<'v>>) -> Result<()> {
        self.write(name, |builder, field| {
            append_value(builder, field, value.into())
        })
    }

    /// Write a primitive list. The iterator is not consumed if the field is omitted.
    pub fn append_list<'v, I>(&mut self, name: &str, values: I) -> Result<()>
    where
        I: IntoIterator,
        I::Item: Into<FieldValue<'v>>,
    {
        self.write(name, |builder, field| {
            append_list(builder, field, values, |builder, field, value| {
                append_value(builder, field, value.into())
            })
        })
    }

    /// Write a list of structs using one callback per selected element.
    pub fn append_structs<I: IntoIterator>(
        &mut self,
        name: &str,
        values: I,
        mut write: impl FnMut(&mut RecordBatchRow<'_>, I::Item) -> Result<()>,
    ) -> Result<()> {
        self.write(name, |builder, field| {
            append_list(builder, field, values, |builder, field, value| {
                append_struct(builder, field, |row| write(row, value))
            })
        })
    }

    pub fn append_struct(
        &mut self,
        name: &str,
        write: impl FnOnce(&mut RecordBatchRow<'_>) -> Result<()>,
    ) -> Result<()> {
        self.write(name, |builder, field| append_struct(builder, field, write))
    }

    fn write(
        &mut self,
        name: &str,
        write: impl FnOnce(&mut dyn ArrayBuilder, &Field) -> Result<()>,
    ) -> Result<()> {
        let result = (|| {
            // TODO: Precompute field-name indexes for O(1) lookups in both schemas.
            if !self.fields.iter().any(|field| field.name() == name) {
                return Err(decode(format!("unknown field {name:?}")));
            }
            let Some(index) = self
                .builder
                .fields()
                .iter()
                .position(|field| field.name() == name)
            else {
                return Ok(());
            };
            if self.seen[index] {
                return Err(decode(format!("field {name:?} was already written")));
            }
            let field = self.builder.fields()[index].clone();
            write(self.builder.field_builders_mut()[index].as_mut(), &field)?;
            self.seen[index] = true;
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }

    fn finish(self) -> Result<()> {
        if self.failed {
            return Err(failed_builder());
        }
        let fields = self.builder.fields().clone();
        for (index, field) in fields.iter().enumerate() {
            if !self.seen[index] {
                append_value(
                    self.builder.field_builders_mut()[index].as_mut(),
                    field,
                    FieldValue::Null,
                )?;
            }
        }
        self.builder.append(true);
        Ok(())
    }
}

fn downcast<T: ArrayBuilder>(builder: &mut dyn ArrayBuilder) -> &mut T {
    builder
        .as_any_mut()
        .downcast_mut()
        .expect("builder matches its validated field type")
}

fn append_value(
    builder: &mut dyn ArrayBuilder,
    field: &Field,
    value: FieldValue<'_>,
) -> Result<()> {
    if matches!(value, FieldValue::Null) {
        if !field.is_nullable() {
            return Err(decode(format!("field {:?} is not nullable", field.name())));
        }
        append_null(builder, field.data_type());
        return Ok(());
    }
    match (value, field.data_type()) {
        (FieldValue::Bool(value), DataType::Boolean) => {
            downcast::<BooleanBuilder>(builder).append_value(value)
        }
        (FieldValue::Int32(value), DataType::Int32) => {
            downcast::<Int32Builder>(builder).append_value(value)
        }
        (FieldValue::Int32(value), DataType::Date32) => {
            downcast::<Date32Builder>(builder).append_value(value)
        }
        (FieldValue::UInt32(value), DataType::UInt32) => {
            downcast::<UInt32Builder>(builder).append_value(value)
        }
        (FieldValue::UInt64(value), DataType::UInt64) => {
            downcast::<UInt64Builder>(builder).append_value(value)
        }
        (FieldValue::Text(value), DataType::Utf8) => {
            downcast::<StringBuilder>(builder).append_value(value)
        }
        (FieldValue::Text(value), DataType::LargeUtf8) => {
            downcast::<LargeStringBuilder>(builder).append_value(value)
        }
        (FieldValue::Binary(value), DataType::Binary) => {
            downcast::<BinaryBuilder>(builder).append_value(value)
        }
        (FieldValue::Binary(value), DataType::LargeBinary) => {
            downcast::<LargeBinaryBuilder>(builder).append_value(value)
        }
        _ => {
            return Err(decode(format!(
                "value does not match field {:?} ({})",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}

fn append_struct(
    builder: &mut dyn ArrayBuilder,
    field: &Field,
    write: impl FnOnce(&mut RecordBatchRow<'_>) -> Result<()>,
) -> Result<()> {
    let DataType::Struct(fields) = field.data_type() else {
        return Err(decode("expected a struct field"));
    };
    let mut row = RecordBatchRow::new(downcast(builder), fields.clone());
    write(&mut row)?;
    row.finish()
}

fn append_list<I: IntoIterator>(
    builder: &mut dyn ArrayBuilder,
    field: &Field,
    values: I,
    mut append: impl FnMut(&mut dyn ArrayBuilder, &Field, I::Item) -> Result<()>,
) -> Result<()> {
    let (item, child): (_, &mut dyn ArrayBuilder) = match field.data_type() {
        DataType::List(item) => (
            item,
            downcast::<ListBuilder<Box<dyn ArrayBuilder>>>(builder)
                .values()
                .as_mut(),
        ),
        DataType::LargeList(item) => (
            item,
            downcast::<LargeListBuilder<Box<dyn ArrayBuilder>>>(builder)
                .values()
                .as_mut(),
        ),
        _ => return Err(decode("expected a list field")),
    };
    for value in values {
        append(child, item, value)?;
    }
    match field.data_type() {
        DataType::List(_) => downcast::<ListBuilder<Box<dyn ArrayBuilder>>>(builder).append(true),
        DataType::LargeList(_) => {
            downcast::<LargeListBuilder<Box<dyn ArrayBuilder>>>(builder).append(true)
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn append_null(builder: &mut dyn ArrayBuilder, data_type: &DataType) {
    match data_type {
        DataType::Boolean => downcast::<BooleanBuilder>(builder).append_null(),
        DataType::Int32 => downcast::<Int32Builder>(builder).append_null(),
        DataType::Date32 => downcast::<Date32Builder>(builder).append_null(),
        DataType::UInt32 => downcast::<UInt32Builder>(builder).append_null(),
        DataType::UInt64 => downcast::<UInt64Builder>(builder).append_null(),
        DataType::Utf8 => downcast::<StringBuilder>(builder).append_null(),
        DataType::LargeUtf8 => downcast::<LargeStringBuilder>(builder).append_null(),
        DataType::Binary => downcast::<BinaryBuilder>(builder).append_null(),
        DataType::LargeBinary => downcast::<LargeBinaryBuilder>(builder).append_null(),
        DataType::List(_) => downcast::<ListBuilder<Box<dyn ArrayBuilder>>>(builder).append(false),
        DataType::LargeList(_) => {
            downcast::<LargeListBuilder<Box<dyn ArrayBuilder>>>(builder).append(false)
        }
        DataType::Struct(fields) => {
            let builder = downcast::<StructBuilder>(builder);
            for (field, child) in fields.iter().zip(builder.field_builders_mut()) {
                append_null(child.as_mut(), field.data_type());
            }
            builder.append(false);
        }
        _ => unreachable!("unsupported types fail at construction"),
    }
}

fn validate_fields(fields: &Fields) -> Result<()> {
    let mut seen = HashSet::new();
    for field in fields {
        if !seen.insert(field.name()) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "build_record_batch",
                "duplicate schema field name",
            ));
        }
        validate_type(field.data_type())?;
    }
    Ok(())
}

fn validate_type(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Boolean
        | DataType::Int32
        | DataType::Date32
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary => Ok(()),
        DataType::Struct(fields) => validate_fields(fields),
        DataType::List(field) | DataType::LargeList(field) => validate_type(field.data_type()),
        _ => Err(Error::new(
            ErrorKind::Unsupported,
            "build_record_batch",
            format!("unsupported metadata type {data_type}"),
        )),
    }
}

fn decode(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "build_record_batch", message)
}
fn failed_builder() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "build_record_batch",
        "builder is invalid after an append error",
    )
}
