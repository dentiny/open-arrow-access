use std::collections::HashMap;

use arrow_schema::{DataType, Field};
use open_arrow_access_core::{
    FieldValue, RecordBatch, RecordBatchBuilder, RecordBatchRow, Result, SchemaRef,
};
use serde::Deserialize;
use serde_json::value::RawValue;

use super::{error::decode, schema, validation};

pub(super) fn decode_batch(
    raw: &[u8],
    path: &str,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    let raw = std::str::from_utf8(raw)
        .map_err(|error| decode(format!("{path}: {error}")).with_source(error))?;
    // Preserve raw JSON; duplicate fields use the last value.
    let fields: HashMap<String, &RawValue> = parse(raw, path)?;
    RecordBatchBuilder::build_row(schema(), output_schema, |row| {
        for field in schema().fields() {
            if field.name() == "raw_metadata_json" {
                row.append(field.name(), raw)?;
            } else {
                append_field(
                    row,
                    field,
                    fields.get(field.name()).map_or("null", |value| value.get()),
                    path,
                )?;
            }
        }
        Ok(())
    })
}

fn append_field(row: &mut RecordBatchRow<'_>, field: &Field, raw: &str, path: &str) -> Result<()> {
    match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let value: Option<String> = parse(raw, path)?;
            if field.name() == "pmcid" {
                validation::validate_pmcid(value.as_deref().unwrap_or_default(), path)?;
            }
            row.append(field.name(), value.as_deref())
        }
        DataType::UInt32 => {
            let value: Option<u32> = parse(raw, path)?;
            if field.name() == "version" {
                validation::validate_article_version(value, path)?;
            }
            row.append(field.name(), value)
        }
        DataType::Boolean => row.append(field.name(), parse::<Option<bool>>(raw, path)?),
        DataType::List(_) => {
            let value: Option<Vec<String>> = parse(raw, path)?;
            match value {
                Some(urls) => row.append_list(field.name(), urls.iter().map(String::as_str)),
                None => row.append(field.name(), FieldValue::Null),
            }
        }
        _ => unreachable!("PMC schema only contains metadata fields"),
    }
}

fn parse<'a, T: Deserialize<'a>>(raw: &'a str, path: &str) -> Result<T> {
    serde_json::from_str(raw).map_err(|error| decode(format!("{path}: {error}")).with_source(error))
}
