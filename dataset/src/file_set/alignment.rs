use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, RecordBatch, RecordBatchOptions, StructArray, make_array, new_null_array,
};
use arrow_schema::{DataType, Fields};
use open_arrow_access_core::{Result, SchemaRef};

use super::{
    error,
    validation::{validate_null_array, validate_schema},
};

/// Fixed field mappings shared by all batches and partitions of one inspected file.
pub(super) struct AlignmentPlan {
    pub(super) source: SchemaRef,
    pub(super) target: SchemaRef,
    fields: Option<Vec<FieldAlignment>>,
}

impl AlignmentPlan {
    pub(super) fn new(source: SchemaRef, target: SchemaRef) -> Result<Self> {
        let fields = if source == target {
            None
        } else {
            Some(plan_fields(source.fields(), target.fields())?)
        };
        Ok(Self {
            source,
            target,
            fields,
        })
    }

    pub(super) fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        validate_schema(&batch.schema(), &self.source)?;
        let Some(fields) = &self.fields else {
            return Ok(batch);
        };
        let columns = align_fields(fields, batch.columns(), batch.num_rows())?;
        RecordBatch::try_new_with_options(
            self.target.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(error::arrow_error)
    }
}

enum FieldAlignment {
    Source { index: usize, array: ArrayAlignment },
    Missing(DataType),
}

enum ArrayAlignment {
    Identity,
    Null(DataType),
    Struct {
        target: Fields,
        fields: Vec<FieldAlignment>,
    },
    List {
        target: DataType,
        item: Box<ArrayAlignment>,
    },
}

fn plan_fields(source: &Fields, target: &Fields) -> Result<Vec<FieldAlignment>> {
    let positions: HashMap<_, _> = source
        .iter()
        .enumerate()
        .map(|(i, field)| (field.name(), i))
        .collect();
    target
        .iter()
        .map(|field| {
            Ok(match positions.get(field.name()).copied() {
                Some(index) => FieldAlignment::Source {
                    index,
                    array: ArrayAlignment::new(source[index].data_type(), field.data_type())?,
                },
                None => FieldAlignment::Missing(field.data_type().clone()),
            })
        })
        .collect()
}

fn align_fields(
    plan: &[FieldAlignment],
    columns: &[ArrayRef],
    len: usize,
) -> Result<Vec<ArrayRef>> {
    plan.iter()
        .map(|field| match field {
            FieldAlignment::Source { index, array } => array.apply(&columns[*index]),
            FieldAlignment::Missing(target) => null_array(target, len),
        })
        .collect()
}

impl ArrayAlignment {
    fn new(source: &DataType, target: &DataType) -> Result<Self> {
        if source == target {
            return Ok(Self::Identity);
        }
        if source == &DataType::Null {
            return Ok(Self::Null(target.clone()));
        }
        match (source, target) {
            (DataType::Struct(source), DataType::Struct(target)) => Ok(Self::Struct {
                target: target.clone(),
                fields: plan_fields(source, target)?,
            }),
            (
                DataType::List(source)
                | DataType::LargeList(source)
                | DataType::FixedSizeList(source, _)
                | DataType::ListView(source)
                | DataType::LargeListView(source),
                DataType::List(target_item)
                | DataType::LargeList(target_item)
                | DataType::FixedSizeList(target_item, _)
                | DataType::ListView(target_item)
                | DataType::LargeListView(target_item),
            ) => Ok(Self::List {
                target: target.clone(),
                item: Box::new(Self::new(source.data_type(), target_item.data_type())?),
            }),
            _ => Err(error::array_schema_mismatch()),
        }
    }

    fn apply(&self, array: &ArrayRef) -> Result<ArrayRef> {
        match self {
            Self::Identity => Ok(array.clone()),
            Self::Null(target) => null_array(target, array.len()),
            Self::Struct { target, fields } => {
                let values = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("struct array");
                let columns = align_fields(fields, values.columns(), array.len())?;
                StructArray::try_new_with_length(
                    target.clone(),
                    columns,
                    array.nulls().cloned(),
                    array.len(),
                )
                .map(|array| Arc::new(array) as ArrayRef)
                .map_err(error::arrow_error)
            }
            Self::List { target, item } => {
                let data = array.to_data();
                let child = make_array(data.child_data()[0].clone());
                let child = item.apply(&child)?;
                data.into_builder()
                    .data_type(target.clone())
                    .child_data(vec![child.to_data()])
                    .build()
                    .map(make_array)
                    .map_err(error::arrow_error)
            }
        }
    }
}

fn null_array(data_type: &DataType, len: usize) -> Result<ArrayRef> {
    validate_null_array(data_type, len)?;
    Ok(new_null_array(data_type, len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, UnionFields, UnionMode};

    #[test]
    fn null_fills_keep_supported_run_end_lengths_and_empty_children() {
        let encoded = |key| {
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", key, false)),
                Arc::new(Field::new("values", DataType::Int64, true)),
            )
        };
        let item = Arc::new(Field::new("item", encoded(DataType::Int16), true));
        for (data_type, len) in [
            (encoded(DataType::Int16), 0),
            (encoded(DataType::Int16), 32767),
            (encoded(DataType::Int32), 32768),
            (DataType::FixedSizeList(item.clone(), 2), 16383),
            (DataType::List(item.clone()), 32768),
            (
                DataType::Union(
                    UnionFields::try_new(
                        [0, 1],
                        [Arc::new(Field::new("plain", DataType::Int64, true)), item],
                    )
                    .unwrap(),
                    UnionMode::Dense,
                ),
                32768,
            ),
        ] {
            let array = null_array(&data_type, len).unwrap();
            assert_eq!(array.len(), len);
            assert_eq!(array.logical_null_count(), len);
        }
    }
}
