use super::schema;
use arrow_array::builder::{ListBuilder, StringBuilder, StructBuilder, UInt64Builder};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, StructArray, UInt64Array};
use arrow_schema::{ArrowError, DataType, Field};
use open_arrow_access_core::{Error, ErrorKind, Result};
use serde::Deserialize;
use std::sync::Arc;

pub(super) fn arrow_error(error: ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "decode_s2orc_v2", error.to_string()).with_source(error)
}
fn error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Decode, "decode_s2orc_v2", message)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Attributes {
    n: Option<String>,
    id: Option<String>,
    ref_id: Option<String>,
    matched_paper_id: Option<u64>,
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Offset {
    Number(u64),
    Decimal(String),
}
impl Offset {
    fn value(self) -> Result<u64> {
        match self {
            Self::Number(n) => Ok(n),
            Self::Decimal(s) if !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()) => {
                s.parse().map_err(|_| error("span offset overflows UInt64"))
            }
            _ => Err(error(
                "span offset must be an unsigned integer or decimal string",
            )),
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Span {
    start: Offset,
    end: Offset,
    attributes: Option<Attributes>,
}

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
        let spans: Vec<Span> = serde_json::from_str(value)
            .map_err(|_| error("invalid annotation payload or unknown span/attribute field"))?;
        for span in spans {
            let start = span.start.value()?;
            let end = span.end.value()?;
            if start > end {
                return Err(error("annotation start exceeds end"));
            }
            let builder = list.values();
            builder
                .field_builder::<UInt64Builder>(0)
                .unwrap()
                .append_value(start);
            builder
                .field_builder::<UInt64Builder>(1)
                .unwrap()
                .append_value(end);
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
                .map_err(|e| error(format!("{}: {e}", annotation_fields[index].name())))
        })
        .collect::<Result<Vec<_>>>()?;
    for row in 0..input.len() {
        if input.is_valid(row) && input.column(0).is_null(row) && annotations.is_valid(row) {
            for array in &arrays {
                let list = array
                    .as_any()
                    .downcast_ref::<arrow_array::ListArray>()
                    .unwrap();
                if list.is_valid(row) && list.value_length(row) > 0 {
                    return Err(error("populated annotation spans require section text"));
                }
            }
        }
    }
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
    let ids = input
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    if ids.iter().any(|id| id.is_none_or(|id| id == 0)) {
        return Err(error("corpusid must be a positive UInt64"));
    }
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
