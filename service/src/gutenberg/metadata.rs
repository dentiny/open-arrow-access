use arrow_array::{Array, StructArray, cast::AsArray};
use arrow_schema::{DataType, Field, Fields};
use chrono::NaiveDate;
use futures::io::Cursor;
use open_arrow_access_core::{
    FieldValue, RecordBatch, RecordBatchBuilder, RecordBatchRow, Result, SchemaRef,
};
use xml_arrow::{ReaderOptions, XmlReader};

use super::{decode, decode_source};

/// Validated XML extraction, retained without a second metadata batch.
pub(super) struct BookMetadata {
    source: StructArray,
}

impl BookMetadata {
    /// Validate every mapped field, independently of the output projection.
    pub(super) async fn parse(raw: &[u8]) -> Result<Self> {
        let mut reader = XmlReader::new(
            Cursor::new(raw),
            super::mapping(),
            /*record_path=*/ "/r:RDF",
            ReaderOptions {
                // DocumentAccessor already bounds the input. Accept larger caller-
                // configured documents without imposing XML's default batch limit.
                max_record_bytes: usize::MAX - 1,
                max_batch_bytes: usize::MAX,
                max_depth: 65534,
                ..Default::default()
            },
        )
        .map_err(decode_source)?;
        let batch = reader
            .next_batch()
            .await
            .map_err(decode_source)?
            .ok_or_else(|| decode("expected an RDF document containing one ebook"))?;
        if batch.num_rows() != 1 || reader.next_batch().await.map_err(decode_source)?.is_some() {
            return Err(decode("expected one RDF document"));
        }
        let book = Self {
            source: batch.column(0).as_struct().clone(),
        };
        convert_fields(super::schema().fields(), &book.source, 0, None)?;
        Ok(book)
    }

    pub(super) fn into_batch(self, schema: SchemaRef) -> Result<RecordBatch> {
        RecordBatchBuilder::build_row(super::schema(), schema.clone(), |row| {
            convert_fields(schema.fields(), &self.source, 0, Some(row))
        })
    }
}

// Validate every source field with no writer, or write the requested fields.
fn convert_fields(
    fields: &Fields,
    source: &StructArray,
    index: usize,
    mut row: Option<&mut RecordBatchRow<'_>>,
) -> Result<()> {
    for field in fields {
        let name = field.name().as_str();
        let column = source.column_by_name(name).unwrap();
        match field.data_type() {
            DataType::List(item) | DataType::LargeList(item) => {
                let values = column.as_list::<i32>().value(index);
                if let DataType::Struct(fields) = item.data_type() {
                    let entries = values.as_struct().column(0).as_struct();
                    if let Some(row) = row.as_deref_mut() {
                        row.append_structs(name, 0..entries.len(), |row, index| {
                            convert_fields(fields, entries, index, Some(row))
                        })?;
                    } else {
                        for index in 0..entries.len() {
                            convert_fields(fields, entries, index, None)?;
                        }
                    }
                } else if let Some(row) = row.as_deref_mut() {
                    row.append_list(
                        name,
                        values
                            .as_string::<i32>()
                            .iter()
                            .flatten()
                            .filter(|s| !s.is_empty()),
                    )?;
                }
            }
            _ => {
                let value = scalar_value(field, column.as_ref(), index)?;
                if let Some(row) = row.as_deref_mut() {
                    row.append(name, value)?;
                }
            }
        }
    }
    Ok(())
}

fn scalar_value<'a>(field: &Field, column: &'a dyn Array, index: usize) -> Result<FieldValue<'a>> {
    let name = field.name().as_str();
    let text = (!column.is_null(index))
        .then(|| column.as_string::<i32>().value(index))
        .filter(|s| !s.is_empty() || matches!(name, "uri" | "scheme"));
    if !field.is_nullable() && text.is_none_or(str::is_empty) {
        return Err(decode(format!("missing {name}")));
    }
    Ok(match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => text.into(),
        DataType::UInt64 if name == "ebook_id" => ebook_id(text.unwrap_or_default())?.into(),
        DataType::UInt64 => number::<u64>(text, name)?.into(),
        DataType::Int32 => number::<i32>(text, name)?.into(),
        DataType::Date32 => text.map(issued).transpose()?.into(),
        _ => unreachable!("unsupported Gutenberg metadata type"),
    })
}

fn number<T: std::str::FromStr>(value: Option<&str>, field: &str) -> Result<Option<T>> {
    value
        .map(|value| {
            value
                .parse()
                .map_err(|_| decode(format!("invalid {field}: {value:?}")))
        })
        .transpose()
}

fn ebook_id(about: &str) -> Result<u64> {
    let id = about
        .strip_prefix("ebooks/")
        .or_else(|| about.strip_prefix("http://www.gutenberg.org/ebooks/"))
        .or_else(|| about.strip_prefix("https://www.gutenberg.org/ebooks/"))
        .ok_or_else(|| decode("invalid ebook URI"))?;
    id.parse::<u64>()
        .ok()
        .filter(|id| *id != 0)
        .filter(|_| id.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| decode("invalid ebook ID"))
}

fn issued(value: &str) -> Result<i32> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(decode_source)?;
    Ok(date
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32)
}
