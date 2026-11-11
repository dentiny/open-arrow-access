use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, RecordBatch, RecordBatchOptions, StructArray, make_array, new_null_array,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use open_arrow_access_core::{Error, ErrorKind, Result, SchemaRef};

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, "merge_file_set", message)
}

fn arrow_error(source: arrow_schema::ArrowError) -> Error {
    invalid(source.to_string()).with_source(source)
}

pub(super) fn validate(fields: &Fields) -> Result<()> {
    let mut names = HashSet::new();
    for field in fields {
        if !names.insert(field.name()) {
            return Err(invalid(format!("duplicate field '{}'", field.name())));
        }
        match field.data_type() {
            DataType::Struct(fields) => validate(fields)?,
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::FixedSizeList(item, _)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::Map(item, _) => {
                validate(&vec![item.clone()].into())?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub(super) fn merge(left: &Schema, right: &Schema) -> Result<Schema> {
    let mut metadata = left.metadata().clone();
    for (key, value) in right.metadata() {
        if metadata.get(key).is_some_and(|existing| existing != value) {
            return Err(invalid(format!(
                "conflicting schema metadata for key '{key}'"
            )));
        }
        metadata.insert(key.clone(), value.clone());
    }
    Ok(Schema::new_with_metadata(
        merge_fields(left.fields(), right.fields())?,
        metadata,
    ))
}

fn merge_fields(left: &Fields, right: &Fields) -> Result<Fields> {
    let right_by_name: HashMap<_, _> = right.iter().map(|field| (field.name(), field)).collect();
    let left_names: HashSet<_> = left.iter().map(|field| field.name()).collect();
    let mut fields = Vec::with_capacity(left.len() + right.len());
    for field in left {
        fields.push(match right_by_name.get(field.name()) {
            Some(other) => Arc::new(merge_field(field, other)?),
            None => Arc::new(field.as_ref().clone().with_nullable(true)),
        });
    }
    for field in right {
        if !left_names.contains(field.name()) {
            fields.push(Arc::new(field.as_ref().clone().with_nullable(true)));
        }
    }
    Ok(fields.into())
}

fn merge_field(left: &Field, right: &Field) -> Result<Field> {
    if left.name() != right.name() {
        return Err(invalid(format!(
            "list element names differ: '{}' and '{}'",
            left.name(),
            right.name()
        )));
    }
    let data_type = merge_type(left.data_type(), right.data_type()).map_err(|source| {
        invalid(format!("field '{}': {source}", left.name())).with_source(source)
    })?;
    let nullable = left.is_nullable()
        || right.is_nullable()
        || left.data_type() == &DataType::Null
        || right.data_type() == &DataType::Null;
    let mut field = left
        .clone()
        .with_data_type(data_type.clone())
        .with_nullable(nullable);
    // Arrow enforces field metadata and dictionary-order compatibility.
    field
        .try_merge(&right.clone().with_data_type(data_type))
        .map_err(arrow_error)?;
    Ok(field)
}

fn merge_type(left: &DataType, right: &DataType) -> Result<DataType> {
    if left == right {
        return Ok(left.clone());
    }
    Ok(match (left, right) {
        (DataType::Null, other) | (other, DataType::Null) => other.clone(),
        (DataType::Struct(left), DataType::Struct(right)) => {
            DataType::Struct(merge_fields(left, right)?)
        }
        (DataType::List(left), DataType::List(right)) => {
            DataType::List(Arc::new(merge_field(left, right)?))
        }
        (DataType::LargeList(left), DataType::LargeList(right)) => {
            DataType::LargeList(Arc::new(merge_field(left, right)?))
        }
        (DataType::ListView(left), DataType::ListView(right)) => {
            DataType::ListView(Arc::new(merge_field(left, right)?))
        }
        (DataType::LargeListView(left), DataType::LargeListView(right)) => {
            DataType::LargeListView(Arc::new(merge_field(left, right)?))
        }
        (DataType::FixedSizeList(left, n), DataType::FixedSizeList(right, m)) if n == m => {
            DataType::FixedSizeList(Arc::new(merge_field(left, right)?), *n)
        }
        _ => {
            return Err(invalid(format!(
                "incompatible field types: {left} and {right}"
            )));
        }
    })
}

pub(super) fn align(batch: RecordBatch, target: SchemaRef) -> Result<RecordBatch> {
    if batch.schema() == target {
        return Ok(batch);
    }
    let columns = align_fields(
        batch.schema().fields(),
        batch.columns(),
        target.fields(),
        batch.num_rows(),
    )?;
    RecordBatch::try_new_with_options(
        target,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(arrow_error)
}

fn align_fields(
    source: &Fields,
    columns: &[ArrayRef],
    target: &Fields,
    len: usize,
) -> Result<Vec<ArrayRef>> {
    let positions: HashMap<_, _> = source
        .iter()
        .enumerate()
        .map(|(i, field)| (field.name(), i))
        .collect();
    target
        .iter()
        .map(|field| match positions.get(field.name()) {
            Some(&index) => align_array(&columns[index], field.data_type()),
            None => Ok(new_null_array(field.data_type(), len)),
        })
        .collect()
}

fn align_array(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(array.clone());
    }
    if array.data_type() == &DataType::Null {
        return Ok(new_null_array(target, array.len()));
    }
    if let (DataType::Struct(source), DataType::Struct(target)) = (array.data_type(), target) {
        let values = array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");
        let columns = align_fields(source, values.columns(), target, array.len())?;
        return StructArray::try_new_with_length(
            target.clone(),
            columns,
            array.nulls().cloned(),
            array.len(),
        )
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(arrow_error);
    }
    match target {
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::FixedSizeList(item, _)
        | DataType::ListView(item)
        | DataType::LargeListView(item) => {
            let data = array.to_data();
            let child = make_array(data.child_data()[0].clone());
            let child = align_array(&child, item.data_type())?;
            data.into_builder()
                .data_type(target.clone())
                .child_data(vec![child.to_data()])
                .build()
                .map(make_array)
                .map_err(arrow_error)
        }
        _ => Err(invalid("array type differs from its inspected schema")),
    }
}
