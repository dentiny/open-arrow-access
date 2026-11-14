use std::{io::Cursor, sync::Arc};

use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, stream};
use open_arrow_access_core::{ByteReader, Result, SchemaRef, StreamReader};

use super::{CsvDialect, CsvFormat, error::inference_error, planning::CsvChunks};

/// Per-input schema sampling and header policy. Byte targets measure canonical
/// CSV bytes and are checked between records, allowing one oversized record.
#[derive(Clone, Copy, Debug)]
pub struct CsvInferenceOptions {
    pub max_records: usize,
    pub max_bytes: usize,
    /// None detects headers from the sample; ambiguous string inputs use a header.
    pub has_header: Option<bool>,
}

impl Default for CsvInferenceOptions {
    fn default() -> Self {
        Self {
            max_records: 1000,
            max_bytes: 64 * 1024 * 1024,
            has_header: None,
        }
    }
}

fn infer_sample(sample: &[u8], has_header: bool, allow_truncated_rows: bool) -> Result<Schema> {
    CsvFormat::default()
        .with_header(has_header)
        .with_truncated_rows(allow_truncated_rows)
        .infer_schema(Cursor::new(sample), None)
        .map(|(schema, _)| schema)
        .map_err(inference_error)
}

fn detect_header(schema: &Schema, first: &[u8], allow_truncated_rows: bool) -> Result<bool> {
    let first = infer_sample(first, false, allow_truncated_rows)?;
    // Only string labels provide evidence for a header. Decoding the first row
    // against later rows' types would mistake type widening (e.g. float then int)
    // or a column with no later values for a header and silently discard data.
    let ambiguous = first
        .fields()
        .iter()
        .chain(schema.fields())
        .all(|field| field.data_type().is_string() || field.data_type() == &DataType::Null);
    Ok(ambiguous
        || first
            .fields()
            .iter()
            .zip(schema.fields())
            .any(|(first, later)| {
                first.data_type().is_string()
                    && !later.data_type().is_string()
                    && later.data_type() != &DataType::Null
            }))
}

pub(super) async fn infer(
    input: Box<dyn ByteReader>,
    dialect: CsvDialect,
    options: CsvInferenceOptions,
    all_varchar: bool,
) -> Result<(SchemaRef, bool, Box<dyn ByteReader>)> {
    let mut metadata = input.metadata().clone();
    // Frame every row, including the potential header, with the source dialect.
    let mut records = CsvChunks::new(
        input,
        CsvDialect {
            has_header: false,
            ..dialect
        },
    )
    .await?;
    let mut sample = Vec::new();
    let mut first_end = 0;
    // With a known header policy, string columns only need names/width from the
    // first record. Keep subsequent records in the same streaming framer.
    let max_records = if all_varchar && options.has_header.is_some() {
        options.max_records.min(1)
    } else {
        options.max_records
    };
    for index in 0..max_records {
        let Some(record) = records.next_chunk(1, options.max_bytes).await? else {
            break;
        };
        sample.extend_from_slice(&record);
        if index == 0 {
            first_end = sample.len();
        }
        if sample.len() >= options.max_bytes {
            break;
        }
    }
    super::validation::validate_sample(&sample)?;
    let has_header = match options.has_header {
        Some(value) => value,
        None => detect_header(
            &infer_sample(&sample, true, dialect.allow_truncated_rows)?,
            &sample[..first_end],
            dialect.allow_truncated_rows,
        )?,
    };
    let schema = if all_varchar {
        infer_strings(&sample, has_header)?
    } else {
        infer_sample(&sample, has_header, dialect.allow_truncated_rows)?
    };
    super::validation::validate_inferred_schema(&schema)?;
    // Canonicalization preserves field contents, but changes the byte representation.
    metadata.content_length = None;
    let remaining = stream::try_unfold(records, |mut records| async move {
        Ok(records
            .next_chunk(1024, 4 * 1024 * 1024)
            .await?
            .map(|bytes| (bytes, records)))
    });
    let replay = stream::once(async move { Ok(sample.into()) }).chain(remaining);
    Ok((
        Arc::new(schema),
        has_header,
        Box::new(StreamReader::new(replay, metadata)),
    ))
}

/// Preserve the column namespace while replacing data types and nullability.
pub(super) fn varchar_schema(schema: &Schema) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Utf8)
                    .with_nullable(true)
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

fn infer_strings(sample: &[u8], has_header: bool) -> Result<Schema> {
    // CsvChunks already normalized the source dialect. Read only the first
    // canonical record to obtain its names or width, without inferring values.
    let mut records = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(sample);
    let record = records
        .records()
        .next()
        .transpose()
        .map_err(inference_error)?
        .ok_or_else(super::error::empty_inference_sample)?;
    Ok(Schema::new(
        record
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let name = if has_header {
                    value.to_owned()
                } else {
                    format!("column_{}", index + 1)
                };
                Field::new(name, DataType::Utf8, true)
            })
            .collect::<Vec<_>>(),
    ))
}
