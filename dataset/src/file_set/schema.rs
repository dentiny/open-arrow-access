use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, RecordBatch, RecordBatchOptions, StructArray, make_array, new_null_array,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use open_arrow_access_core::{Result, SchemaRef};

use super::{error, validation};

#[derive(Default)]
pub(super) struct SchemaUnion {
    fields: FieldUnion,
    metadata: HashMap<String, String>,
}

impl SchemaUnion {
    pub(super) fn merge(&mut self, schema: &Schema) -> Result<()> {
        validation::validate_schema_metadata(&self.metadata, schema.metadata())?;
        self.metadata.extend(schema.metadata().clone());
        self.fields.merge(schema.fields())
    }

    pub(super) fn finish(self) -> Schema {
        Schema::new_with_metadata(self.fields.finish(), self.metadata)
    }
}

#[derive(Default)]
struct FieldUnion {
    fields: Vec<UnionField>,
    positions: HashMap<String, usize>,
    inputs: usize,
}

impl FieldUnion {
    fn merge(&mut self, fields: &Fields) -> Result<()> {
        self.inputs += 1;
        for field in fields {
            match self.positions.get(field.name()) {
                Some(&index) => self.fields[index].merge(field)?,
                None => {
                    self.positions
                        .insert(field.name().clone(), self.fields.len());
                    self.fields.push(UnionField::new(field));
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> Fields {
        self.fields
            .into_iter()
            .map(|field| Arc::new(field.finish(Some(self.inputs))))
            .collect()
    }
}

struct UnionField {
    field: Field,
    metadata: HashMap<String, String>,
    data_type: UnionType,
    occurrences: usize,
}

impl UnionField {
    fn new(field: &Field) -> Self {
        Self {
            // Keep Arrow's field flags, but merge types and metadata incrementally.
            field: field
                .clone()
                .with_data_type(DataType::Boolean)
                .with_metadata(HashMap::new()),
            metadata: field.metadata().clone(),
            data_type: UnionType::new(field.data_type()),
            occurrences: 1,
        }
    }

    fn merge(&mut self, field: &Field) -> Result<()> {
        validation::validate_field_names(&self.field, field)?;
        let nullable = self.field.is_nullable()
            || field.is_nullable()
            || matches!(self.data_type, UnionType::Leaf(DataType::Null))
            || field.data_type() == &DataType::Null;
        self.data_type
            .merge(field.data_type())
            .map_err(|source| error::field_union_error(field.name(), source))?;
        // A primitive placeholder avoids rebuilding accumulated nested fields in try_merge.
        self.field
            .try_merge(
                &field
                    .clone()
                    .with_data_type(DataType::Boolean)
                    .with_metadata(HashMap::new()),
            )
            .map_err(error::arrow_error)?;
        for (key, value) in field.metadata() {
            if self
                .metadata
                .get(key)
                .is_some_and(|existing| existing != value)
            {
                return Err(error::field_metadata_conflict(field.name(), key));
            }
            self.metadata.insert(key.clone(), value.clone());
        }
        self.field.set_nullable(nullable);
        self.occurrences += 1;
        Ok(())
    }

    fn finish(self, inputs: Option<usize>) -> Field {
        let nullable =
            self.field.is_nullable() || inputs.is_some_and(|inputs| self.occurrences < inputs);
        self.field
            .with_nullable(nullable)
            .with_data_type(self.data_type.finish())
            .with_metadata(self.metadata)
    }
}

enum UnionType {
    Leaf(DataType),
    Struct(FieldUnion),
    List {
        kind: ListKind,
        item: Box<UnionField>,
    },
}

#[derive(Clone, Copy, PartialEq)]
enum ListKind {
    List,
    LargeList,
    ListView,
    LargeListView,
    FixedSizeList(i32),
}

impl ListKind {
    fn from_type(data_type: &DataType) -> Option<(Self, &Field)> {
        Some(match data_type {
            DataType::List(item) => (Self::List, item),
            DataType::LargeList(item) => (Self::LargeList, item),
            DataType::ListView(item) => (Self::ListView, item),
            DataType::LargeListView(item) => (Self::LargeListView, item),
            DataType::FixedSizeList(item, size) => (Self::FixedSizeList(*size), item),
            _ => return None,
        })
    }

    fn finish(self, item: Field) -> DataType {
        let item = Arc::new(item);
        match self {
            Self::List => DataType::List(item),
            Self::LargeList => DataType::LargeList(item),
            Self::ListView => DataType::ListView(item),
            Self::LargeListView => DataType::LargeListView(item),
            Self::FixedSizeList(size) => DataType::FixedSizeList(item, size),
        }
    }
}

impl UnionType {
    fn new(data_type: &DataType) -> Self {
        if let DataType::Struct(fields) = data_type {
            let mut union = FieldUnion::default();
            union.merge(fields).expect("a new field has no conflicts");
            Self::Struct(union)
        } else if let Some((kind, item)) = ListKind::from_type(data_type) {
            Self::List {
                kind,
                item: Box::new(UnionField::new(item)),
            }
        } else {
            Self::Leaf(data_type.clone())
        }
    }

    fn merge(&mut self, data_type: &DataType) -> Result<()> {
        if data_type == &DataType::Null {
            return Ok(());
        }
        if matches!(self, Self::Leaf(DataType::Null)) {
            *self = Self::new(data_type);
            return Ok(());
        }
        match (self, data_type) {
            (Self::Struct(union), DataType::Struct(fields)) => union.merge(fields),
            (Self::List { kind, item }, other)
                if ListKind::from_type(other).is_some_and(|(other, _)| *kind == other) =>
            {
                item.merge(ListKind::from_type(other).unwrap().1)
            }
            (Self::Leaf(left), right) if left == right => Ok(()),
            (left, right) => {
                // Materialize the accumulated type only to explain an incompatible input.
                Err(error::incompatible_types(&left.as_data_type(), right))
            }
        }
    }

    fn as_data_type(&self) -> DataType {
        // Only used on an error path; finishing successful unions consumes their state.
        match self {
            Self::Leaf(data_type) => data_type.clone(),
            Self::Struct(union) => DataType::Struct(
                union
                    .fields
                    .iter()
                    .map(|field| {
                        Arc::new(
                            field
                                .field
                                .clone()
                                .with_data_type(field.data_type.as_data_type()),
                        )
                    })
                    .collect(),
            ),
            Self::List { kind, item } => kind.finish(
                item.field
                    .clone()
                    .with_data_type(item.data_type.as_data_type()),
            ),
        }
    }

    fn finish(self) -> DataType {
        match self {
            Self::Leaf(data_type) => data_type,
            Self::Struct(union) => DataType::Struct(union.finish()),
            Self::List { kind, item } => kind.finish(item.finish(None)),
        }
    }
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

#[cfg(test)]
#[path = "../../tests/unit/schema_union.rs"]
mod tests;
