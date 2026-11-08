use std::collections::HashSet;

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
pub(super) struct BookMetadata<'a> {
    raw: &'a [u8],
    source: StructArray,
}

impl<'a> BookMetadata<'a> {
    /// Validate every mapped field, independently of the output projection.
    pub(super) async fn parse(raw: &'a [u8]) -> Result<Self> {
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
            raw,
            source: batch.column(0).as_struct().clone(),
        };
        validate_fields(super::metadata_schema().fields(), &book.source, 0)?;
        let mut seen = HashSet::new();
        for uri in book
            .files()
            .column_by_name("uri")
            .unwrap()
            .as_string::<i32>()
            .iter()
            .flatten()
        {
            if uri.is_empty() || !seen.insert(uri) {
                return Err(decode("missing or duplicate file URI in RDF"));
            }
        }
        Ok(book)
    }

    fn files(&self) -> &StructArray {
        self.source
            .column_by_name("files")
            .unwrap()
            .as_list::<i32>()
            .values()
            .as_struct()
            .column(0)
            .as_struct()
    }

    pub(super) fn file_index(&self, uri: &str) -> Option<usize> {
        self.files()
            .column_by_name("uri")
            .unwrap()
            .as_string::<i32>()
            .iter()
            .position(|candidate| candidate == Some(uri))
    }

    pub(super) fn into_batch(
        self,
        selected: &[(usize, Vec<u8>)],
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let metadata = super::metadata_schema();
        let DataType::LargeList(item) = metadata.field_with_name("files").unwrap().data_type()
        else {
            unreachable!()
        };
        let DataType::Struct(file_fields) = item.data_type() else {
            unreachable!()
        };
        RecordBatchBuilder::build_row(super::schema(), schema, |row| {
            row.append("metadata_rdf", self.raw)?;
            append_fields(row, metadata.fields(), &self.source, 0)?;
            row.append_structs("files", selected, |row, (index, data)| {
                append_fields(row, file_fields, self.files(), *index)?;
                row.append("data", data.as_slice())
            })
        })
    }
}

fn validate_fields(fields: &Fields, source: &StructArray, index: usize) -> Result<()> {
    for field in fields {
        if field.name() == "metadata_rdf" {
            continue;
        }
        let column = source.column_by_name(field.name()).unwrap();
        match field.data_type() {
            DataType::List(item) | DataType::LargeList(item) => {
                if let DataType::Struct(fields) = item.data_type() {
                    let values = column.as_list::<i32>().value(index);
                    let entries = values.as_struct().column(0).as_struct();
                    for index in 0..entries.len() {
                        validate_fields(fields, entries, index)?;
                    }
                }
            }
            _ => {
                scalar_value(field, column.as_ref(), index)?;
            }
        }
    }
    Ok(())
}

// Conversion only writes projected columns; validation has already checked all
// mapped fields, including nested values that projected list callbacks skip.
fn append_fields(
    row: &mut RecordBatchRow<'_>,
    fields: &Fields,
    source: &StructArray,
    index: usize,
) -> Result<()> {
    for field in fields {
        let name = field.name().as_str();
        if matches!(name, "metadata_rdf" | "files") {
            continue;
        }
        let column = source.column_by_name(name).unwrap();
        match field.data_type() {
            DataType::List(item) | DataType::LargeList(item) => {
                let values = column.as_list::<i32>().value(index);
                if let DataType::Struct(fields) = item.data_type() {
                    let entries = values.as_struct().column(0).as_struct();
                    row.append_structs(name, 0..entries.len(), |row, index| {
                        append_fields(row, fields, entries, index)
                    })?;
                } else {
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
            _ => row.append(name, scalar_value(field, column.as_ref(), index)?)?,
        }
    }
    Ok(())
}

fn scalar_value<'a>(field: &Field, column: &'a dyn Array, index: usize) -> Result<FieldValue<'a>> {
    let name = field.name().as_str();
    let text = (!column.is_null(index))
        .then(|| column.as_string::<i32>().value(index))
        .filter(|s| !s.is_empty() || matches!(name, "uri" | "scheme"));
    if text.is_none() && !field.is_nullable() {
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
