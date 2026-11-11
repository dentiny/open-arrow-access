use std::{collections::HashSet, io::Cursor, sync::Arc};

use arrow_csv::ReaderBuilder;
use arrow_schema::{DataType, Schema};
use futures::{StreamExt, stream};
use open_arrow_access_core::{ByteReader, Error, ErrorKind, Result, SchemaRef, StreamReader};

use super::{CsvDialect, CsvFormat, planning::CsvChunks};

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

impl CsvInferenceOptions {
    pub(super) fn validate(self) -> Result<()> {
        if self.max_records == 0 || self.max_bytes == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "infer_csv",
                "inference record and byte limits must be positive",
            ));
        }
        Ok(())
    }
}

fn inference_error(source: arrow_schema::ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "infer_csv", source.to_string()).with_source(source)
}

fn infer_sample(sample: &[u8], has_header: bool, allow_truncated_rows: bool) -> Result<Schema> {
    CsvFormat::default()
        .with_header(has_header)
        .with_truncated_rows(allow_truncated_rows)
        .infer_schema(Cursor::new(sample), None)
        .map(|(schema, _)| schema)
        .map_err(inference_error)
}

fn detect_header(schema: &Schema, first: &[u8]) -> bool {
    // The candidate schema was inferred from records after the first. All-string
    // samples are ambiguous and follow the common headered-CSV convention.
    if schema
        .fields()
        .iter()
        .all(|field| field.data_type().is_string())
        || schema
            .fields()
            .iter()
            .any(|field| field.data_type() == &DataType::Null)
    {
        return true;
    }
    !ReaderBuilder::new(Arc::new(schema.clone()))
        .with_batch_size(1)
        .build(Cursor::new(first))
        .is_ok_and(|mut reader| matches!(reader.next(), Some(Ok(_))))
}

pub(super) async fn infer(
    input: Box<dyn ByteReader>,
    dialect: CsvDialect,
    options: CsvInferenceOptions,
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
    for index in 0..options.max_records {
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
    if sample.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "infer_csv",
            "cannot infer a schema from empty CSV input; supply an explicit schema",
        ));
    }
    let has_header = match options.has_header {
        Some(value) => value,
        None => detect_header(
            &infer_sample(&sample, true, dialect.allow_truncated_rows)?,
            &sample[..first_end],
        ),
    };
    let schema = infer_sample(&sample, has_header, dialect.allow_truncated_rows)?;
    let mut names = HashSet::new();
    for field in schema.fields() {
        if field.name().is_empty() || !names.insert(field.name()) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "infer_csv",
                "CSV headers must be nonempty and distinct",
            ));
        }
        if field.data_type() == &DataType::Null {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "infer_csv",
                "cannot infer types for header-only or all-null CSV columns; supply an explicit schema",
            ));
        }
    }
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
