use crate::arrow_utils::string_value;
use arrow_array::{Array, StructArray, cast::AsArray};
use arrow_schema::DataType;
use chrono::NaiveDate;
use open_arrow_access_core::{
    Bytes, FieldValue, ReadRequest, RecordBatch, RecordBatchBuilder, RecordBatchRow, Result,
    SchemaRef,
};
use open_arrow_access_file_format::{
    read_document,
    xml::{XmlFormat, XmlLimits},
};

use super::{Mapping, error, validation};

/// Validate all mapped fields and build one projected row.
pub(super) async fn read_metadata(
    raw: &[u8],
    request: ReadRequest,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let format = XmlFormat::new("/r:RDF")
        .with_schema(super::mapping())
        .with_limits(XmlLimits {
            max_record_bytes: usize::MAX - 1,
            max_batch_bytes: usize::MAX,
            max_depth: 65534,
        });
    let batch = read_document(
        Bytes::copy_from_slice(raw),
        format,
        ReadRequest::new(request.object),
    )
    .await?;
    let source = batch.column(0).as_struct();
    let definitions = super::definitions();
    convert_fields(definitions.fields.iter().enumerate(), source, 0, None)?;
    let fields = schema.fields().iter().map(|field| {
        let index = definitions
            .field_index
            .index_of(field.name())
            .expect("validated Gutenberg projection");
        (index, &definitions.fields[index])
    });
    RecordBatchBuilder::build_row(definitions.schema.clone(), schema.clone(), |row| {
        convert_fields(fields, source, 0, Some(row))
    })
}

// Validate every source field with no writer, or write the requested fields.
fn convert_fields<'m>(
    fields: impl IntoIterator<Item = (usize, &'m Mapping)>,
    source: &StructArray,
    index: usize,
    mut row: Option<&mut RecordBatchRow<'_>>,
) -> Result<()> {
    for (column_index, mapping) in fields {
        let field = &mapping.field;
        let name = field.name().as_str();
        // The XML mapping fixes the source column order before reading.
        let column = source.column(column_index);
        match field.data_type() {
            DataType::List(item) => {
                let values = column.as_list::<i32>().value(index);
                if matches!(item.data_type(), DataType::Struct(_)) {
                    let entries = values.as_struct().column(0).as_struct();
                    if let Some(row) = row.as_deref_mut() {
                        row.append_structs(name, 0..entries.len(), |row, index| {
                            convert_fields(
                                mapping.children.iter().enumerate(),
                                entries,
                                index,
                                Some(row),
                            )
                        })?;
                    } else {
                        for index in 0..entries.len() {
                            convert_fields(
                                mapping.children.iter().enumerate(),
                                entries,
                                index,
                                None,
                            )?;
                        }
                    }
                } else {
                    let texts = values
                        .as_string::<i32>()
                        .iter()
                        .flatten()
                        .filter(|s| !s.is_empty());
                    if matches!(item.data_type(), DataType::UInt64) {
                        let parsed = texts
                            .map(|text| number::<u64>(text, name))
                            .collect::<Result<Vec<_>>>()?;
                        if let Some(row) = row.as_deref_mut() {
                            row.append_list(name, parsed)?;
                        }
                    } else if let Some(row) = row.as_deref_mut() {
                        row.append_list(name, texts)?;
                    }
                }
            }
            _ => {
                let value = scalar_value(mapping, column.as_ref(), index)?;
                if let Some(row) = row.as_deref_mut() {
                    row.append(name, value)?;
                }
            }
        }
    }
    Ok(())
}

fn scalar_value<'a>(
    mapping: &Mapping,
    column: &'a dyn Array,
    index: usize,
) -> Result<FieldValue<'a>> {
    let field = &mapping.field;
    let name = field.name().as_str();
    // Empty attributes are values; empty optional element text becomes null.
    let keep_empty = matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8)
        && mapping.xml.path().contains('@');
    let text =
        string_value(column.as_string::<i32>(), index).filter(|s| !s.is_empty() || keep_empty);
    validation::required_field(name, field.is_nullable(), text)?;
    if let Some(parse) = mapping.parser {
        return Ok(text.map(parse).transpose()?.unwrap_or(FieldValue::Null));
    }
    Ok(match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => text.into(),
        DataType::UInt64 => text
            .map(|text| number::<u64>(text, name))
            .transpose()?
            .into(),
        DataType::Int32 => text
            .map(|text| number::<i32>(text, name))
            .transpose()?
            .into(),
        DataType::Date32 => text.map(parse_issued_date).transpose()?.into(),
        _ => unreachable!("unsupported Gutenberg metadata type"),
    })
}

fn number<T: std::str::FromStr>(value: &str, field: &str) -> Result<T> {
    value
        .parse()
        .map_err(|_| error::invalid_number(field, value))
}

pub(super) fn parse_ebook_id(about: &str) -> Result<FieldValue<'_>> {
    let id = about
        .strip_prefix("ebooks/")
        .or_else(|| about.strip_prefix("http://www.gutenberg.org/ebooks/"))
        .or_else(|| about.strip_prefix("https://www.gutenberg.org/ebooks/"))
        .ok_or_else(error::invalid_ebook_uri)?;
    let value = id.parse::<u64>().map_err(|_| error::invalid_ebook_id())?;
    validation::ebook_id(id, value)?;
    Ok(FieldValue::UInt64(value))
}

fn parse_issued_date(value: &str) -> Result<i32> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(error::decode_source)?;
    Ok(date
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32)
}
