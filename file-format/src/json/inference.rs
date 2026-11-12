use std::sync::Arc;

use arrow_json::reader::infer_json_schema_from_iterator;
use futures::{StreamExt, stream};
use open_arrow_access_core::{ByteReader, Result, SchemaRef, StreamReader, into_stream};
use serde_json::{Deserializer, Value};

const DEFAULT_MAX_RECORDS: usize = 1000;
const DEFAULT_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Sampling limits for JSON schema inference. Limits are checked between complete
/// objects; one object and an upstream byte chunk can exceed the byte target.
#[derive(Clone, Copy, Debug)]
pub struct JsonInferenceOptions {
    pub max_records: usize,
    pub max_bytes: usize,
}

impl Default for JsonInferenceOptions {
    fn default() -> Self {
        Self {
            max_records: DEFAULT_MAX_RECORDS,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

pub(super) async fn infer(
    mut input: Box<dyn ByteReader>,
    options: JsonInferenceOptions,
) -> Result<(SchemaRef, Box<dyn ByteReader>)> {
    let metadata = input.metadata().clone();
    let mut buffered = Vec::new();
    let mut offset = 0;
    let mut records = Vec::new();
    let mut eof = false;
    'sampling: loop {
        // Normalize each fetched tail once, then consume all complete objects.
        // Rebuilding the normalized tail per object repeatedly scans lookahead.
        let sample = super::nonfinite::inference_sample(&buffered[offset..], eof);
        let start = offset;
        let mut values = Deserializer::from_slice(&sample).into_iter::<Value>();
        while let Some(value) = values.next() {
            match value {
                Ok(value) => {
                    super::validation::validate_record(&value)?;
                    offset = start + values.byte_offset();
                    records.push(value);
                    if records.len() >= options.max_records || offset >= options.max_bytes {
                        break 'sampling;
                    }
                }
                Err(source) if !source.is_eof() || eof => {
                    return Err(super::error::inference_error(source));
                }
                Err(_) => break,
            }
        }
        if eof {
            break;
        }
        match input.next().await? {
            Some(bytes) if bytes.is_empty() => {
                return Err(super::error::empty_inference_chunk());
            }
            Some(bytes) => buffered.extend_from_slice(&bytes),
            None => eof = true,
        }
    }
    super::validation::validate_sample(&records)?;
    let schema = infer_json_schema_from_iterator(records.iter().map(Ok))
        .map_err(super::error::inference_error)?;
    // Replay every fetched byte, including any lookahead, without reopening storage.
    let replay = stream::iter((!buffered.is_empty()).then_some(Ok(buffered.into())))
        .chain(into_stream(input));
    Ok((
        Arc::new(schema),
        Box::new(StreamReader::new(replay, metadata)),
    ))
}
