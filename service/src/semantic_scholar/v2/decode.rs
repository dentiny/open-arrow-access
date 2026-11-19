use super::{
    error::{annotation_error, arrow_error},
    schema, validation,
};
use arrow_array::builder::{ListBuilder, StringBuilder, StructBuilder, UInt64Builder};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field};
use open_arrow_access_core::Result;
use std::sync::Arc;

fn spans(input: &StringArray) -> Result<ArrayRef> {
    let mut list =
        ListBuilder::new(StructBuilder::from_fields(schema::span(), input.len())).with_field(
            Arc::new(Field::new("item", DataType::Struct(schema::span()), false)),
        );
    for value in input.iter() {
        let Some(value) = value else {
            list.append(false);
            continue;
        };
        for span in validation::parse_spans(value)? {
            let builder = list.values();
            builder
                .field_builder::<UInt64Builder>(0)
                .unwrap()
                .append_value(span.start);
            builder
                .field_builder::<UInt64Builder>(1)
                .unwrap()
                .append_value(span.end);
            let attrs = builder.field_builder::<StructBuilder>(2).unwrap();
            let values = span.attributes;
            attrs
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_option(values.as_ref().and_then(|a| a.n.as_deref()));
            attrs
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_option(values.as_ref().and_then(|a| a.id.as_deref()));
            attrs
                .field_builder::<StringBuilder>(2)
                .unwrap()
                .append_option(values.as_ref().and_then(|a| a.ref_id.as_deref()));
            attrs
                .field_builder::<UInt64Builder>(3)
                .unwrap()
                .append_option(values.as_ref().and_then(|a| a.matched_paper_id));
            attrs.append(values.is_some());
            builder.append(true);
        }
        list.append(true);
    }
    Ok(Arc::new(list.finish()))
}

fn section(input: &StructArray, output: &DataType) -> Result<ArrayRef> {
    let DataType::Struct(fields) = output else {
        unreachable!()
    };
    let annotations = input
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let DataType::Struct(annotation_fields) = fields[1].data_type() else {
        unreachable!()
    };
    let arrays = annotations
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            spans(column.as_any().downcast_ref::<StringArray>().unwrap())
                .map_err(|e| annotation_error(annotation_fields[index].name(), e))
        })
        .collect::<Result<Vec<_>>>()?;
    let annotations: ArrayRef = Arc::new(
        StructArray::try_new(
            annotation_fields.clone(),
            arrays,
            annotations.nulls().cloned(),
        )
        .map_err(arrow_error)?,
    );
    Ok(Arc::new(
        StructArray::try_new(
            fields.clone(),
            vec![input.column(0).clone(), annotations],
            input.nulls().cloned(),
        )
        .map_err(arrow_error)?,
    ))
}

pub(super) fn batch(input: RecordBatch) -> Result<RecordBatch> {
    let schema = schema::s2orc_v2_schema();
    let mut columns = input.columns().to_vec();
    for index in [4, 5] {
        columns[index] = section(
            input
                .column(index)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap(),
            schema.field(index).data_type(),
        )?;
    }
    RecordBatch::try_new(schema, columns).map_err(arrow_error)
}
