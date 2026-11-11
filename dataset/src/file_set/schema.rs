use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, RecordBatch, RecordBatchOptions, StructArray, make_array, new_null_array,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use open_arrow_access_core::{Result, SchemaRef};

use super::{error, validation};

pub(super) fn union_by_name(left: &Schema, right: &Schema) -> Result<Schema> {
    let mut metadata = left.metadata().clone();
    validation::validate_schema_metadata(left.metadata(), right.metadata())?;
    metadata.extend(right.metadata().clone());
    Ok(Schema::new_with_metadata(
        union_fields(left.fields(), right.fields())?,
        metadata,
    ))
}

fn union_fields(left: &Fields, right: &Fields) -> Result<Fields> {
    let right_by_name: HashMap<_, _> = right.iter().map(|field| (field.name(), field)).collect();
    let left_names: HashSet<_> = left.iter().map(|field| field.name()).collect();
    let mut fields = Vec::with_capacity(left.len() + right.len());
    for field in left {
        fields.push(match right_by_name.get(field.name()) {
            Some(other) => Arc::new(union_field(field, other)?),
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

fn union_field(left: &Field, right: &Field) -> Result<Field> {
    validation::validate_field_names(left, right)?;
    let data_type = union_type(left.data_type(), right.data_type())
        .map_err(|source| error::field_union_error(left.name(), source))?;
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
        .map_err(error::arrow_error)?;
    Ok(field)
}

fn union_type(left: &DataType, right: &DataType) -> Result<DataType> {
    if left == right {
        return Ok(left.clone());
    }
    Ok(match (left, right) {
        (DataType::Null, other) | (other, DataType::Null) => other.clone(),
        (DataType::Struct(left), DataType::Struct(right)) => {
            DataType::Struct(union_fields(left, right)?)
        }
        (DataType::List(left), DataType::List(right)) => {
            DataType::List(Arc::new(union_field(left, right)?))
        }
        (DataType::LargeList(left), DataType::LargeList(right)) => {
            DataType::LargeList(Arc::new(union_field(left, right)?))
        }
        (DataType::ListView(left), DataType::ListView(right)) => {
            DataType::ListView(Arc::new(union_field(left, right)?))
        }
        (DataType::LargeListView(left), DataType::LargeListView(right)) => {
            DataType::LargeListView(Arc::new(union_field(left, right)?))
        }
        (DataType::FixedSizeList(left, n), DataType::FixedSizeList(right, m)) if n == m => {
            DataType::FixedSizeList(Arc::new(union_field(left, right)?), *n)
        }
        _ => return Err(error::incompatible_types(left, right)),
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
    .map_err(error::arrow_error)
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
        .map_err(error::arrow_error);
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
                .map_err(error::arrow_error)
        }
        _ => Err(error::array_schema_mismatch()),
    }
}
