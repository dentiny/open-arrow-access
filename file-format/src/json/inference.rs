use std::sync::Arc;

use arrow_json::reader::infer_json_schema_from_iterator;
use futures::{StreamExt, stream};
use open_arrow_access_core::{
    ByteReader, Result, SchemaRef, StreamReader, empty_chunk, into_stream, next_nonempty,
};
use serde_json::Value;

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

pub(super) struct InferredJson {
    pub schema: SchemaRef,
    pub input: Box<dyn ByteReader>,
}

pub(super) async fn infer(
    mut input: Box<dyn ByteReader>,
    options: JsonInferenceOptions,
) -> Result<Option<InferredJson>> {
    let metadata = input.metadata().clone();
    let mut buffered = Vec::new();
    let mut offset = 0;
    let mut records = Vec::new();
    let mut boundary = RecordBoundary::default();
    let mut eof = false;
    loop {
        let tail = &buffered[offset..];
        let end = boundary.scan(tail).or_else(|| eof.then_some(tail.len()));
        if let Some(end) = end {
            let sample = super::nonfinite::inference_sample(&tail[..end], true);
            if !sample
                .iter()
                .all(|&byte| matches!(byte, b' ' | b'\r' | b'\n' | b'\t'))
            {
                let value: Value =
                    serde_json::from_slice(&sample).map_err(super::error::inference_error)?;
                super::validation::validate_record(&value)?;
                records.push(value);
            }
            offset += end;
            boundary = RecordBoundary::default();
            if records.len() >= options.max_records || offset >= options.max_bytes || eof {
                break;
            }
            continue;
        }
        match next_nonempty(input.as_mut(), || empty_chunk("infer_json")).await? {
            Some(bytes) => buffered.extend_from_slice(&bytes),
            None => eof = true,
        }
    }
    if records.is_empty() {
        return Ok(None);
    }
    let schema = infer_json_schema_from_iterator(records.iter().map(Ok))
        .map_err(super::error::inference_error)?;
    // Replay every fetched byte, including any lookahead, without reopening storage.
    let replay = stream::iter((!buffered.is_empty()).then_some(Ok(buffered.into())))
        .chain(into_stream(input));
    Ok(Some(InferredJson {
        schema: Arc::new(schema),
        input: Box::new(StreamReader::new(replay, metadata)),
    }))
}

/// Locate object boundaries without reparsing an incomplete record. Each byte is
/// visited once, including when strings and escape sequences span input chunks.
/// This only frames objects; serde_json remains responsible for JSON validation.
#[derive(Default)]
struct RecordBoundary {
    scanned: usize,
    depth: usize,
    in_string: bool,
    escaped: bool,
}

impl RecordBoundary {
    fn scan(&mut self, bytes: &[u8]) -> Option<usize> {
        while self.scanned < bytes.len() {
            let byte = bytes[self.scanned];
            self.scanned += 1;
            if self.in_string {
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.in_string = false;
                }
                continue;
            }
            if self.depth == 0 && !matches!(byte, b' ' | b'\r' | b'\n' | b'\t') && byte != b'{' {
                // Only objects are supported. Let the existing parser/validator
                // report non-object values or invalid JSON without reading ahead.
                return Some(self.scanned);
            }
            match byte {
                b'"' => self.in_string = true,
                b'{' | b'[' => self.depth += 1,
                b'}' | b']' => {
                    self.depth -= 1;
                    if self.depth == 0 {
                        return Some(self.scanned);
                    }
                }
                _ => {}
            }
        }
        None
    }
}
