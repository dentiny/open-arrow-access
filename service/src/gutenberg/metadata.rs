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

use super::{Mapping, decode, decode_source};

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
    convert_fields(&definitions.fields, source, 0, None)?;
    let fields = schema.fields().iter().map(|field| {
        definitions
            .fields
            .iter()
            .find(|mapping| mapping.field.name() == field.name())
            .unwrap()
    });
    RecordBatchBuilder::build_row(definitions.schema.clone(), schema.clone(), |row| {
        convert_fields(fields, source, 0, Some(row))
    })
}

// Validate every source field with no writer, or write the requested fields.
fn convert_fields<'m>(
    fields: impl IntoIterator<Item = &'m Mapping>,
    source: &StructArray,
    index: usize,
    mut row: Option<&mut RecordBatchRow<'_>>,
) -> Result<()> {
    for mapping in fields {
        let field = &mapping.field;
        let name = field.name().as_str();
        let column = source.column_by_name(name).unwrap();
        match field.data_type() {
            DataType::List(item) => {
                let values = column.as_list::<i32>().value(index);
                if matches!(item.data_type(), DataType::Struct(_)) {
                    let entries = values.as_struct().column(0).as_struct();
                    if let Some(row) = row.as_deref_mut() {
                        row.append_structs(name, 0..entries.len(), |row, index| {
                            convert_fields(&mapping.children, entries, index, Some(row))
                        })?;
                    } else {
                        for index in 0..entries.len() {
                            convert_fields(&mapping.children, entries, index, None)?;
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
    let text = (!column.is_null(index))
        .then(|| column.as_string::<i32>().value(index))
        .filter(|s| !s.is_empty() || keep_empty);
    if !field.is_nullable() && text.is_none_or(str::is_empty) {
        return Err(decode(format!("missing {name}")));
    }
    if let Some(parse) = mapping.parser {
        return Ok(text.map(parse).transpose()?.unwrap_or(FieldValue::Null));
    }
    Ok(match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => text.into(),
        DataType::UInt64 => number::<u64>(text, name)?.into(),
        DataType::Int32 => number::<i32>(text, name)?.into(),
        DataType::Date32 => text.map(parse_issued_date).transpose()?.into(),
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

pub(super) fn parse_ebook_id(about: &str) -> Result<FieldValue<'_>> {
    let id = about
        .strip_prefix("ebooks/")
        .or_else(|| about.strip_prefix("http://www.gutenberg.org/ebooks/"))
        .or_else(|| about.strip_prefix("https://www.gutenberg.org/ebooks/"))
        .ok_or_else(|| decode("invalid ebook URI"))?;
    id.parse::<u64>()
        .ok()
        .filter(|id| *id != 0)
        .filter(|_| id.bytes().all(|byte| byte.is_ascii_digit()))
        .map(FieldValue::UInt64)
        .ok_or_else(|| decode("invalid ebook ID"))
}

fn parse_issued_date(value: &str) -> Result<i32> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(decode_source)?;
    Ok(date
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32)
}
