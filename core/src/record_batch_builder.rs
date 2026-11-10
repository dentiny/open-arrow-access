use arrow_array::{RecordBatchOptions, builder::*};
use arrow_schema::{DataType, Field, Fields};

use crate::{Error, ErrorKind, FieldNameIndex, ReadOptions, RecordBatch, Result, SchemaRef};
use crate::{error::invalid_output_schema, validation::validate_record_batch_fields};

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
    layout: StructLayout,
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
        validate_record_batch_fields(&fields)?;
        Self::from_fields(fields, schema)
    }

    /// Build one row with a resolved output schema, retaining its Arc and metadata.
    /// Output fields must be unique, unchanged fields from the full schema.
    pub fn build_row(
        schema: SchemaRef,
        output_schema: SchemaRef,
        write: impl FnOnce(&mut RecordBatchRow<'_>) -> Result<()>,
    ) -> Result<RecordBatch> {
        validate_record_batch_fields(schema.fields())?;
        let mut builder = Self::from_fields(schema.fields().clone(), output_schema)?;
        builder.push(write)?;
        builder.finish()
    }

    fn from_fields(fields: Fields, schema: SchemaRef) -> Result<Self> {
        let layout = StructLayout::new(&fields, schema.fields())?;
        let builder = StructBuilder::from_fields(schema.fields().clone(), 0);
        Ok(Self {
            schema,
            layout,
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
        let mut row = RecordBatchRow::new(&mut builder, &self.layout);
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

// Cached once per builder, including nested structs inside lists. Row callbacks
// borrow these layouts instead of rebuilding indexes for every row/list element.
struct StructLayout {
    names: FieldNameIndex,
    output_indices: Vec<Option<usize>>,
    values: Vec<ValueLayout>,
}

impl StructLayout {
    fn new(full: &Fields, output: &Fields) -> Result<Self> {
        let names = FieldNameIndex::new(full);
        let mut output_indices = vec![None; full.len()];
        for (index, field) in output.iter().enumerate() {
            let source = names
                .get(field.name())
                .filter(|&source| &full[source] == field)
                .ok_or_else(invalid_output_schema)?;
            if output_indices[source].replace(index).is_some() {
                return Err(invalid_output_schema());
            }
        }
        let values = output
            .iter()
            .map(|field| ValueLayout::new(field.data_type()))
            .collect::<Result<_>>()?;
        Ok(Self {
            names,
            output_indices,
            values,
        })
    }
}

enum ValueLayout {
    Scalar,
    Struct(Box<StructLayout>),
    List(Box<ValueLayout>),
}

impl ValueLayout {
    fn new(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Struct(fields) => Self::Struct(Box::new(StructLayout::new(fields, fields)?)),
            DataType::List(field) | DataType::LargeList(field) => {
                Self::List(Box::new(Self::new(field.data_type())?))
            }
            _ => Self::Scalar,
        })
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
    layout: &'a StructLayout,
    seen: Vec<bool>,
    failed: bool,
}

impl<'a> RecordBatchRow<'a> {
    fn new(builder: &'a mut StructBuilder, layout: &'a StructLayout) -> Self {
        let seen = vec![false; builder.num_fields()];
        Self {
            builder,
            layout,
            seen,
            failed: false,
        }
    }

    pub fn append<'v>(&mut self, name: &str, value: impl Into<FieldValue<'v>>) -> Result<()> {
        self.write(name, |field| field.append_value(value.into()))
    }

    /// Write a primitive list. The iterator is not consumed if the field is omitted.
    pub fn append_list<'v, I>(&mut self, name: &str, values: I) -> Result<()>
    where
        I: IntoIterator,
        I::Item: Into<FieldValue<'v>>,
    {
        self.write(name, |field| {
            field.append_list(values, |field, value| field.append_value(value.into()))
        })
    }

    /// Write a list of structs using one callback per selected element.
    pub fn append_structs<I: IntoIterator>(
        &mut self,
        name: &str,
        values: I,
        mut write: impl FnMut(&mut RecordBatchRow<'_>, I::Item) -> Result<()>,
    ) -> Result<()> {
        self.write(name, |field| {
            field.append_list(values, |field, value| {
                field.append_struct(|row| write(row, value))
            })
        })
    }

    pub fn append_struct(
        &mut self,
        name: &str,
        write: impl FnOnce(&mut RecordBatchRow<'_>) -> Result<()>,
    ) -> Result<()> {
        self.write(name, |field| field.append_struct(write))
    }

    fn write(
        &mut self,
        name: &str,
        write: impl FnOnce(FieldWriter<'_>) -> Result<()>,
    ) -> Result<()> {
        let result = (|| {
            let source_index = self
                .layout
                .names
                .get(name)
                .ok_or_else(|| decode(format!("unknown field {name:?}")))?;
            let Some(index) = self.layout.output_indices[source_index] else {
                return Ok(());
            };
            if self.seen[index] {
                return Err(decode(format!("field {name:?} was already written")));
            }
            let field = self.builder.fields()[index].clone();
            write(FieldWriter {
                builder: self.builder.field_builders_mut()[index].as_mut(),
                field: &field,
                layout: &self.layout.values[index],
            })?;
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
                FieldWriter {
                    builder: self.builder.field_builders_mut()[index].as_mut(),
                    field,
                    layout: &self.layout.values[index],
                }
                .append_value(FieldValue::Null)?;
            }
        }
        self.builder.append(true);
        Ok(())
    }
}

struct FieldWriter<'a> {
    builder: &'a mut dyn ArrayBuilder,
    field: &'a Field,
    layout: &'a ValueLayout,
}

impl FieldWriter<'_> {
    fn downcast<T: ArrayBuilder>(builder: &mut dyn ArrayBuilder) -> &mut T {
        builder
            .as_any_mut()
            .downcast_mut()
            .expect("builder matches its validated field type")
    }

    fn append_value(self, value: FieldValue<'_>) -> Result<()> {
        let Self {
            builder,
            field,
            layout,
        } = self;
        if matches!(value, FieldValue::Null) {
            if !field.is_nullable() {
                return Err(decode(format!("field {:?} is not nullable", field.name())));
            }
            Self {
                builder,
                field,
                layout,
            }
            .append_null();
            return Ok(());
        }
        match (value, field.data_type()) {
            (FieldValue::Bool(value), DataType::Boolean) => {
                Self::downcast::<BooleanBuilder>(builder).append_value(value)
            }
            (FieldValue::Int32(value), DataType::Int32) => {
                Self::downcast::<Int32Builder>(builder).append_value(value)
            }
            (FieldValue::Int32(value), DataType::Date32) => {
                Self::downcast::<Date32Builder>(builder).append_value(value)
            }
            (FieldValue::UInt32(value), DataType::UInt32) => {
                Self::downcast::<UInt32Builder>(builder).append_value(value)
            }
            (FieldValue::UInt64(value), DataType::UInt64) => {
                Self::downcast::<UInt64Builder>(builder).append_value(value)
            }
            (FieldValue::Text(value), DataType::Utf8) => {
                Self::downcast::<StringBuilder>(builder).append_value(value)
            }
            (FieldValue::Text(value), DataType::LargeUtf8) => {
                Self::downcast::<LargeStringBuilder>(builder).append_value(value)
            }
            (FieldValue::Binary(value), DataType::Binary) => {
                Self::downcast::<BinaryBuilder>(builder).append_value(value)
            }
            (FieldValue::Binary(value), DataType::LargeBinary) => {
                Self::downcast::<LargeBinaryBuilder>(builder).append_value(value)
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
        self,
        write: impl FnOnce(&mut RecordBatchRow<'_>) -> Result<()>,
    ) -> Result<()> {
        let Self {
            builder,
            field,
            layout,
        } = self;
        if !matches!(field.data_type(), DataType::Struct(_)) {
            return Err(decode("expected a struct field"));
        }
        let ValueLayout::Struct(layout) = layout else {
            unreachable!("layout matches its validated field type");
        };
        let mut row = RecordBatchRow::new(Self::downcast(builder), layout);
        write(&mut row)?;
        row.finish()
    }

    fn append_list<I: IntoIterator>(
        self,
        values: I,
        mut append: impl FnMut(FieldWriter<'_>, I::Item) -> Result<()>,
    ) -> Result<()> {
        let Self {
            builder,
            field,
            layout,
        } = self;
        let (item, child): (_, &mut dyn ArrayBuilder) = match field.data_type() {
            DataType::List(item) => (
                item,
                Self::downcast::<ListBuilder<Box<dyn ArrayBuilder>>>(builder)
                    .values()
                    .as_mut(),
            ),
            DataType::LargeList(item) => (
                item,
                Self::downcast::<LargeListBuilder<Box<dyn ArrayBuilder>>>(builder)
                    .values()
                    .as_mut(),
            ),
            _ => return Err(decode("expected a list field")),
        };
        let ValueLayout::List(item_layout) = layout else {
            unreachable!("layout matches its validated field type");
        };
        for value in values {
            append(
                FieldWriter {
                    builder: &mut *child,
                    field: item,
                    layout: item_layout,
                },
                value,
            )?;
        }
        match field.data_type() {
            DataType::List(_) => {
                Self::downcast::<ListBuilder<Box<dyn ArrayBuilder>>>(builder).append(true)
            }
            DataType::LargeList(_) => {
                Self::downcast::<LargeListBuilder<Box<dyn ArrayBuilder>>>(builder).append(true)
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn append_null(self) {
        let Self {
            builder,
            field,
            layout,
        } = self;
        match field.data_type() {
            DataType::Boolean => Self::downcast::<BooleanBuilder>(builder).append_null(),
            DataType::Int32 => Self::downcast::<Int32Builder>(builder).append_null(),
            DataType::Date32 => Self::downcast::<Date32Builder>(builder).append_null(),
            DataType::UInt32 => Self::downcast::<UInt32Builder>(builder).append_null(),
            DataType::UInt64 => Self::downcast::<UInt64Builder>(builder).append_null(),
            DataType::Utf8 => Self::downcast::<StringBuilder>(builder).append_null(),
            DataType::LargeUtf8 => Self::downcast::<LargeStringBuilder>(builder).append_null(),
            DataType::Binary => Self::downcast::<BinaryBuilder>(builder).append_null(),
            DataType::LargeBinary => Self::downcast::<LargeBinaryBuilder>(builder).append_null(),
            DataType::List(_) => {
                Self::downcast::<ListBuilder<Box<dyn ArrayBuilder>>>(builder).append(false)
            }
            DataType::LargeList(_) => {
                Self::downcast::<LargeListBuilder<Box<dyn ArrayBuilder>>>(builder).append(false)
            }
            DataType::Struct(fields) => {
                let builder = Self::downcast::<StructBuilder>(builder);
                let ValueLayout::Struct(layout) = layout else {
                    unreachable!("layout matches its validated field type");
                };
                for ((field, child), value_layout) in fields
                    .iter()
                    .zip(builder.field_builders_mut())
                    .zip(&layout.values)
                {
                    FieldWriter {
                        builder: child.as_mut(),
                        field,
                        layout: value_layout,
                    }
                    .append_null();
                }
                builder.append(false);
            }
            _ => unreachable!("unsupported types fail at construction"),
        }
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
