use super::{decode, schema, validation};
use arrow_array::{
    ArrayRef, BooleanArray, RecordBatchOptions, UInt32Array,
    builder::{LargeStringBuilder, ListBuilder, StringBuilder},
};
use arrow_schema::{DataType, Field};
use open_arrow_access_core::{RecordBatch, Result, SchemaRef};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::{collections::HashMap, sync::Arc};

pub(super) fn decode_batch(
    raw: &[u8],
    path: &str,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    let raw = std::str::from_utf8(raw)
        .map_err(|error| decode(format!("{path}: {error}")).with_source(error))?;
    // Preserve duplicate keys and unknown values verbatim; mapped keys use the last value.
    let fields: HashMap<String, &RawValue> = parse(raw, path)?;
    let mut columns = vec![None; output_schema.fields().len()];
    for field in schema().fields() {
        let index = output_schema.index_of(field.name()).ok();
        let column = if field.name() == "raw_metadata_json" {
            index.map(|_| large_string(Some(raw)))
        } else {
            column(
                field,
                fields.get(field.name()).map_or("null", |value| value.get()),
                index.is_some(),
                path,
            )?
        };
        if let Some(index) = index {
            columns[index] = column;
        }
    }
    RecordBatch::try_new_with_options(
        output_schema,
        columns
            .into_iter()
            .map(|column| column.expect("selected fields always build a column"))
            .collect(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(|error| decode(error.to_string()).with_source(error))
}

// Validate every known field, but allocate Arrow buffers only for selected columns.
fn column(field: &Field, raw: &str, selected: bool, path: &str) -> Result<Option<ArrayRef>> {
    Ok(match field.data_type() {
        DataType::Utf8 => {
            let value: Option<String> = if field.name() == "pmid" {
                parse::<Option<Pmid>>(raw, path)?.map(|value| match value {
                    Pmid::Text(text) => text,
                    Pmid::Number(number) => number.to_string(),
                })
            } else {
                parse(raw, path)?
            };
            if field.name() == "pmcid" {
                validation::validate_pmcid(value.as_deref().unwrap_or_default(), path)?;
            }
            selected.then(|| string(value.as_deref()))
        }
        DataType::LargeUtf8 => {
            let value: Option<String> = parse(raw, path)?;
            selected.then(|| large_string(value.as_deref()))
        }
        DataType::UInt32 => {
            let value: Option<u32> = parse(raw, path)?;
            if field.name() == "version" {
                validation::validate_article_version(value, path)?;
            }
            selected.then(|| Arc::new(UInt32Array::from(vec![value])) as ArrayRef)
        }
        DataType::Boolean => {
            let value: Option<bool> = parse(raw, path)?;
            selected.then(|| Arc::new(BooleanArray::from(vec![value])) as ArrayRef)
        }
        DataType::List(_) => {
            let value: Option<Vec<String>> = parse(raw, path)?;
            selected.then(|| {
                let urls = value.as_deref().unwrap_or_default();
                let mut builder = ListBuilder::with_capacity(
                    StringBuilder::with_capacity(urls.len(), urls.iter().map(String::len).sum()),
                    1,
                );
                for url in urls {
                    builder.values().append_value(url);
                }
                builder.append(value.is_some());
                Arc::new(builder.finish()) as ArrayRef
            })
        }
        _ => unreachable!("PMC schema contains only supported metadata types"),
    })
}

// Size single-row string buffers from their values, including zero bytes for nulls.
fn string(value: Option<&str>) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(1, value.map_or(0, str::len));
    builder.append_option(value);
    Arc::new(builder.finish())
}

fn large_string(value: Option<&str>) -> ArrayRef {
    let mut builder = LargeStringBuilder::with_capacity(1, value.map_or(0, str::len));
    builder.append_option(value);
    Arc::new(builder.finish())
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Pmid {
    Text(String),
    Number(u64),
}

fn parse<'a, T: Deserialize<'a>>(raw: &'a str, path: &str) -> Result<T> {
    serde_json::from_str(raw).map_err(|error| decode(format!("{path}: {error}")).with_source(error))
}
