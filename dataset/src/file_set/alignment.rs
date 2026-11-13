use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, RecordBatch, RecordBatchOptions, StructArray, make_array, new_null_array,
};
use arrow_schema::{DataType, Fields};
use open_arrow_access_core::{Result, SchemaRef};

use super::error;

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
