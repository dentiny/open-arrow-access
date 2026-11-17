use super::{JsonKind, error::resolve as error};
use open_arrow_access_core::{
    ByteAccessor, ByteReadOptions, ByteReadRequest, ErrorKind, ObjectRef, Result,
};

use std::num::NonZeroUsize;

/// How many selected files to inspect during content sniffing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SniffPolicy {
    /// Inspect every file; reject different content kinds.
    #[default]
    EveryFile,
    /// Trust the first nonempty file. Later files are not checked for mixed kinds.
    FirstFile,
}

/// Content sniffing policy and preflight budgets, independent of record execution.
/// Currently used to distinguish ordinary JSON and ATIF inputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SniffConfig {
    pub policy: SniffPolicy,
    /// Maximum bytes consumed per file while identifying its content kind.
    pub max_bytes: NonZeroUsize,
    /// Maximum concurrent file probes. `FirstFile` always probes sequentially.
    pub concurrency: NonZeroUsize,
}

impl Default for SniffConfig {
    fn default() -> Self {
        Self {
            policy: SniffPolicy::EveryFile,
            max_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            concurrency: NonZeroUsize::new(8).unwrap(),
        }
    }
}

fn classify(value: &serde_json::Value, multiline: bool) -> JsonKind {
    use open_arrow_access_file_format::AtifFormat;
    if value
        .get("schema_version")
        .and_then(|value| value.as_str())
        .is_some_and(|value| value.starts_with("ATIF-"))
    {
        JsonKind::Atif(if multiline {
            AtifFormat::Json
        } else {
            AtifFormat::JsonLines
        })
    } else {
        JsonKind::Records
    }
}

fn inspect(bytes: &[u8], object: &ObjectRef) -> Result<Option<JsonKind>> {
    match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(value) => Ok(Some(classify(&value, bytes.contains(&b'\n')))),
        Err(source) if source.is_eof() => Ok(None),
        Err(source) => Err(super::error::invalid_json(source, object)),
    }
}

pub(super) async fn probe(
    storage: &dyn ByteAccessor,
    object: &ObjectRef,
    options: ByteReadOptions,
    limit: usize,
) -> Result<Option<JsonKind>> {
    let mut input = storage
        .open(ByteReadRequest::new(object.clone()).with_options(options))
        .await
        .map_err(|error| error.with_object(Some(object)))?;
    let mut line = Vec::new();
    let mut consumed = 0;
    while let Some(chunk) = input
        .next()
        .await
        .map_err(|error| error.with_object(Some(object)))?
    {
        if chunk.is_empty() {
            return Err(error(
                ErrorKind::Io,
                "byte reader returned an empty chunk",
                Some(object),
            ));
        }
        for &byte in chunk.iter() {
            if consumed == limit {
                return Err(error(
                    ErrorKind::InvalidInput,
                    format!("JSON detection exceeds {limit}-byte limit"),
                    Some(object),
                ));
            }
            consumed += 1;
            if byte == b'\n' {
                if line.iter().any(|b: &u8| !b.is_ascii_whitespace()) {
                    if let Some(kind) = inspect(&line, object)? {
                        return Ok(Some(kind));
                    }
                    line.push(byte);
                    continue;
                }
                line.clear();
            } else {
                line.push(byte);
            }
        }
    }
    if line.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(None);
    }
    inspect(&line, object)?
        .map(Some)
        .ok_or_else(|| super::error::incomplete_json(object))
}
