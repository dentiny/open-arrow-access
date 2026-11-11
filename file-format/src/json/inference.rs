use std::sync::Arc;

use arrow_json::reader::infer_json_schema_from_iterator;
use futures::{StreamExt, stream};
use open_arrow_access_core::{
    ByteReader, Error, ErrorKind, Result, SchemaRef, StreamReader, into_stream,
};
use serde_json::{Deserializer, Value};

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
            max_records: 1000,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}

impl JsonInferenceOptions {
    pub(super) fn validate(self) -> Result<()> {
        if self.max_records == 0 || self.max_bytes == 0 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "infer_json",
                "inference record and byte limits must be positive",
            ));
        }
        Ok(())
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
    loop {
        let mut values = Deserializer::from_slice(&buffered[offset..]).into_iter::<Value>();
        match values.next() {
            Some(Ok(value)) => {
                if !value.is_object() {
                    return Err(Error::new(
                        ErrorKind::Decode,
                        "infer_json",
                        "expected a JSON object",
                    ));
                }
                offset += values.byte_offset();
                records.push(value);
                if records.len() >= options.max_records || offset >= options.max_bytes {
                    break;
                }
                continue;
            }
            Some(Err(source)) if !source.is_eof() || eof => {
                return Err(
                    Error::new(ErrorKind::Decode, "infer_json", source.to_string())
                        .with_source(source),
                );
            }
            None if eof => break,
            _ => {}
        }
        match input.next().await? {
            Some(bytes) if bytes.is_empty() => {
                return Err(Error::new(
                    ErrorKind::Decode,
                    "infer_json",
                    "byte reader emitted an empty chunk",
                ));
            }
            Some(bytes) => buffered.extend_from_slice(&bytes),
            None => eof = true,
        }
    }
    if records.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "infer_json",
            "cannot infer a schema from empty JSON input; supply an explicit schema",
        ));
    }
    let schema = infer_json_schema_from_iterator(records.iter().map(Ok)).map_err(|source| {
        Error::new(ErrorKind::Decode, "infer_json", source.to_string()).with_source(source)
    })?;
    // Replay every fetched byte, including any lookahead, without reopening storage.
    let replay = stream::iter((!buffered.is_empty()).then_some(Ok(buffered.into())))
        .chain(into_stream(input));
    Ok((
        Arc::new(schema),
        Box::new(StreamReader::new(replay, metadata)),
    ))
}
