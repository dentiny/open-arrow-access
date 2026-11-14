use std::{io::Cursor, sync::Arc};

use arrow_schema::{DataType, Schema};
use futures::{StreamExt, stream};
use open_arrow_access_core::{ByteReader, Result, SchemaRef, StreamReader};

use super::{CsvDialect, CsvFormat, CsvHeader, error::inference_error, planning::CsvChunks};

/// Per-input schema sampling targets. Byte targets measure canonical
/// CSV bytes and are checked between records, allowing one oversized record.
#[derive(Clone, Copy, Debug)]
pub struct CsvInferenceOptions {
    pub max_records: usize,
    pub max_bytes: usize,
}

impl Default for CsvInferenceOptions {
    fn default() -> Self {
        Self {
            max_records: 1000,
            max_bytes: 64 * 1024 * 1024,
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

fn detect_header(schema: &Schema, first_bytes: &[u8], allow_truncated_rows: bool) -> Result<bool> {
    let first = infer_sample(first_bytes, false, allow_truncated_rows)?;
    // Only string labels provide evidence for a header. Decoding the first row
    // against later rows' types would mistake type widening (e.g. float then int)
    // or a column with no later values for a header and silently discard data.
    let values = first_record(first_bytes)?;
    Ok(first
        .fields()
        .iter()
        .zip(schema.fields())
        .zip(values.iter())
        .any(|((first, later), value)| {
            first.data_type().is_string()
                && !later.data_type().is_string()
                && later.data_type() != &DataType::Null
                // Arrow may infer numeric-looking text (e.g. +1 or an integer
                // beyond i64) as Utf8. It must not become header evidence.
                && std::str::from_utf8(value)
                    .ok()
                    .and_then(|value| value.parse::<f64>().ok())
                    .is_none()
        }))
}

fn first_record(sample: &[u8]) -> Result<csv::ByteRecord> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(sample);
    let mut first = csv::ByteRecord::new();
    reader
        .read_byte_record(&mut first)
        .map_err(super::error::write_error)?;
    Ok(first)
}

pub(super) async fn resolve(
    input: Box<dyn ByteReader>,
    dialect: CsvDialect,
    options: CsvInferenceOptions,
    explicit_schema: Option<SchemaRef>,
) -> Result<(SchemaRef, bool, Box<dyn ByteReader>)> {
    let mut metadata = input.metadata().clone();
    // Frame every row, including the potential header, with the source dialect.
    let mut records = CsvChunks::new(
        input,
        CsvDialect {
            header: CsvHeader::Absent,
            ..dialect
        },
    )
    .await?;
    let mut sample = Vec::new();
    let mut first_end = 0;
    let max_records = if explicit_schema.is_some() {
        1
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
    let (schema, has_header) = if let Some(schema) = explicit_schema {
        // Start with DuckDB's full supplied-column-name match.
        // Deliberately keep mismatches as data: a conversion failure is not proof
        // of a header, and all-string data must not lose its first row.
        let first = first_record(&sample)?;
        let has_header = first
            .iter()
            .eq(schema.fields().iter().map(|field| field.name().as_bytes()));
        (schema, has_header)
    } else {
        super::validation::validate_sample(&sample)?;
        let has_header = match dialect.header {
            CsvHeader::Present => true,
            CsvHeader::Absent => false,
            CsvHeader::Auto => detect_header(
                &infer_sample(&sample, true, dialect.allow_truncated_rows)?,
                &sample[..first_end],
                dialect.allow_truncated_rows,
            )?,
        };
        let schema = infer_sample(&sample, has_header, dialect.allow_truncated_rows)?;
        super::validation::validate_inferred_schema(&schema)?;
        (Arc::new(schema), has_header)
    };
    // Canonicalization preserves field contents, but changes the byte representation.
    metadata.content_length = None;
    let remaining = stream::try_unfold(records, |mut records| async move {
        Ok(records
            .next_chunk(1024, 4 * 1024 * 1024)
            .await?
            .map(|bytes| (bytes, records)))
    });
    let replay = stream::iter((!sample.is_empty()).then(|| Ok(sample.into()))).chain(remaining);
    Ok((
        schema,
        has_header,
        Box::new(StreamReader::new(replay, metadata)),
    ))
}
