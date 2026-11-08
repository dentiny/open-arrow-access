use std::{collections::HashSet, sync::Arc};

use arrow_array::{
    Array, ArrayRef, LargeBinaryArray, LargeListArray, RecordBatchOptions, StructArray,
    UInt64Array, cast::AsArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Fields};
use arrow_select::take::take;
use chrono::NaiveDate;
use futures::io::Cursor;
use open_arrow_access_core::{RecordBatch, RecordBatchBuilder, RecordBatchRow, Result, SchemaRef};
use xml_arrow::{ReaderOptions, XmlReader};

use super::{decode, decode_source};

/// Validated RDF metadata and file declarations, without file content.
pub(super) struct BookMetadata {
    batch: RecordBatch,
}

impl BookMetadata {
    /// Use the XML engine behind XmlAccessor; validate all metadata before projection.
    pub(super) async fn parse(raw: &[u8]) -> Result<Self> {
        let mut reader = XmlReader::new(
            Cursor::new(raw),
            super::mapping(),
            /* record_path = */ "/r:RDF",
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
        let schema = super::metadata_schema();
        let book = Self {
            batch: RecordBatchBuilder::build_row(schema.clone(), schema.clone(), |row| {
                row.append("metadata_rdf", raw)?;
                append_fields(row, schema.fields(), batch.column(0).as_struct(), 0)
            })?,
        };
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
        self.batch
            .column_by_name("files")
            .unwrap()
            .as_list::<i64>()
            .values()
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
        let files = schema
            .field_with_name("files")
            .ok()
            .map(|field| self.build_files_column(selected, field))
            .transpose()?;
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                if field.name() == "files" {
                    files.as_ref().unwrap().clone()
                } else {
                    self.batch.column_by_name(field.name()).unwrap().clone()
                }
            })
            .collect();
        RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .map_err(decode_source)
    }

    // Combine selected RDF declarations with bytes read from storage once, by name.
    fn build_files_column(&self, selected: &[(usize, Vec<u8>)], field: &Field) -> Result<ArrayRef> {
        let DataType::LargeList(item) = field.data_type() else {
            unreachable!()
        };
        let DataType::Struct(fields) = item.data_type() else {
            unreachable!()
        };
        let indices =
            UInt64Array::from_iter_values(selected.iter().map(|(index, _)| *index as u64));
        let metadata = take(self.files(), &indices, None).map_err(decode_source)?;
        let data: ArrayRef = Arc::new(
            selected
                .iter()
                .map(|(_, data)| Some(data.as_slice()))
                .collect::<LargeBinaryArray>(),
        );
        let columns = fields
            .iter()
            .map(|field| {
                if field.name() == "data" {
                    data.clone()
                } else {
                    metadata
                        .as_struct()
                        .column_by_name(field.name())
                        .unwrap()
                        .clone()
                }
            })
            .collect();
        let files = StructArray::new(fields.clone(), columns, None);
        Ok(Arc::new(LargeListArray::new(
            item.clone(),
            OffsetBuffer::from_lengths([selected.len()]),
            Arc::new(files),
            None,
        )))
    }
}

// The shared XML mapping handles namespaces and containers. Keep Gutenberg's
// empty-text and strict numeric/date semantics when writing its typed Arrow row.
fn append_fields(
    row: &mut RecordBatchRow<'_>,
    fields: &Fields,
    source: &StructArray,
    index: usize,
) -> Result<()> {
    for field in fields {
        let name = field.name().as_str();
        if name == "metadata_rdf" {
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
            ty => {
                let text = (!column.is_null(index))
                    .then(|| column.as_string::<i32>().value(index))
                    .filter(|s| !s.is_empty() || matches!(name, "uri" | "scheme"));
                match ty {
                    DataType::Utf8 | DataType::LargeUtf8 => row.append(name, text)?,
                    DataType::UInt64 if name == "ebook_id" => {
                        row.append(name, ebook_id(text.unwrap_or_default())?)?
                    }
                    DataType::UInt64 => row.append(name, number::<u64>(text, name)?)?,
                    DataType::Int32 => row.append(name, number::<i32>(text, name)?)?,
                    DataType::Date32 => row.append(name, text.map(issued).transpose()?)?,
                    _ => unreachable!("unsupported Gutenberg metadata type"),
                }
            }
        }
    }
    Ok(())
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
